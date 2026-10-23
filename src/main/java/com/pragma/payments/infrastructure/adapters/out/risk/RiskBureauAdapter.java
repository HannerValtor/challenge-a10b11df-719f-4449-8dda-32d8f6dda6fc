package com.pragma.payments.infrastructure.adapters.out.risk;

import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.config.ExternalTimeoutProperties;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * Adaptador de salida hacia el buró de riesgo (Fase 3, H2/H9, D2/D6).
 *
 * <p>Las políticas de resiliencia (Circuit Breaker y Retry de Resilience4j) se declaran sobre los
 * métodos <b>públicos</b> del bean ({@link #getRiskScore(Transaction)} y
 * {@link #isAccountBlacklisted(String)}), que son invocados a través del límite del bean. De este
 * modo el proxy AOP de Spring/Resilience4j <b>sí intercepta</b> las anotaciones (corrige H2: las
 * anotaciones ya no viven sobre métodos privados ni auto-invocados).</p>
 *
 * <p>El timeout del buró se aplica desde {@link ExternalTimeoutProperties#getRisk()} (único lugar
 * de verdad, D2), que el reto exige estrictamente mayor que 2 segundos (por defecto 2500ms).</p>
 *
 * <p><b>Distinción fallback vs. fallo transitorio (Requirements 2.5, 2.6):</b> el método de
 * fallback solo produce el resultado degradado (score 50) cuando el Circuit Breaker está
 * <b>abierto</b> (señal {@link CallNotPermittedException}). Ante cualquier fallo transitorio con el
 * Circuit Breaker <b>cerrado</b> (incluido un timeout superior a 2500ms, un error 5xx o un bulkhead
 * lleno), el fallback <b>re-propaga</b> el error sin devolver el resultado degradado, de modo que
 * la clave de idempotencia quede elegible para un reintento posterior.</p>
 */
@Component
public class RiskBureauAdapter implements RiskBureauPort {

    private static final Logger log = LoggerFactory.getLogger(RiskBureauAdapter.class);

    /** Score de riesgo por defecto devuelto en modo degradado cuando el CB está abierto. */
    private static final int FALLBACK_RISK_SCORE = 50;

    private final WebClient webClient;
    private final String baseUrl;
    private final Duration riskTimeout;
    private final Bulkhead bulkhead;

    public RiskBureauAdapter(
            WebClient webClient,
            BulkheadRegistry bulkheadRegistry,
            ExternalTimeoutProperties timeoutProperties,
            @Value("${external.services.risk-bureau.url:http://localhost:8083}") String baseUrl) {
        this.webClient = webClient;
        this.baseUrl = baseUrl;
        // Único lugar de verdad del timeout de riesgo (D2): externalizado y > 2s por invariante.
        this.riskTimeout = timeoutProperties.getRisk();
        this.bulkhead = bulkheadRegistry.bulkhead("riskBureauService");
    }

    @Override
    @CircuitBreaker(name = "riskBureauCircuitBreaker", fallbackMethod = "getRiskScoreFallback")
    @Retry(name = "riskBureauRetry")
    public Mono<Integer> getRiskScore(Transaction transaction) {
        log.info("Consultando risk score para cuenta: {}, monto: {}",
                transaction.getAccountId(), transaction.getAmount());

        return webClient
                .post()
                .uri(baseUrl + "/api/v1/risk/score")
                .bodyValue(buildRiskRequest(transaction))
                .retrieve()
                .bodyToMono(RiskScoreResponse.class)
                .timeout(riskTimeout)
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .map(RiskScoreResponse::getScore)
                .doOnSuccess(score -> log.info("Risk score obtenido: {} para cuenta: {}",
                        score, transaction.getAccountId()))
                .doOnError(WebClientResponseException.class, e ->
                        log.error("Error HTTP {} al consultar risk bureau: {}",
                                e.getStatusCode().value(), e.getMessage()))
                .doOnError(TimeoutException.class, e ->
                        log.error("Timeout (> {}ms) al consultar risk bureau para cuenta: {}",
                                riskTimeout.toMillis(), transaction.getAccountId()));
    }

    @Override
    @CircuitBreaker(name = "riskBureauCircuitBreaker", fallbackMethod = "isAccountBlacklistedFallback")
    @Retry(name = "riskBureauRetry")
    public Mono<Boolean> isAccountBlacklisted(String accountId) {
        log.info("Verificando si cuenta {} está en lista negra", accountId);

        return webClient
                .get()
                .uri(baseUrl + "/api/v1/risk/blacklist/{accountId}", accountId)
                .retrieve()
                .bodyToMono(BlacklistResponse.class)
                .timeout(riskTimeout)
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .map(BlacklistResponse::isBlacklisted)
                .doOnSuccess(result -> log.info("Cuenta {} en blacklist: {}", accountId, result))
                .doOnError(WebClientResponseException.class, e ->
                        log.error("Error HTTP {} al verificar blacklist: {}",
                                e.getStatusCode().value(), e.getMessage()));
    }

    private RiskRequest buildRiskRequest(Transaction transaction) {
        return new RiskRequest(
                transaction.getAccountId(),
                transaction.getAmount(),
                transaction.getCurrency(),
                transaction.getChannel() != null ? transaction.getChannel() : "API"
        );
    }

    /**
     * Fallback de {@link #getRiskScore(Transaction)}.
     *
     * <p>Solo degrada a {@link #FALLBACK_RISK_SCORE} cuando el Circuit Breaker está abierto
     * ({@link CallNotPermittedException}); ante cualquier otro fallo (transitorio, CB cerrado)
     * re-propaga el error para que la clave quede elegible a reintento (Requirements 2.5, 2.6).</p>
     */
    Mono<Integer> getRiskScoreFallback(Transaction transaction, Throwable t) {
        if (t instanceof CallNotPermittedException) {
            log.warn("Circuit breaker ABIERTO para getRiskScore. Cuenta: {}, aplicando score por defecto {}",
                    transaction.getAccountId(), FALLBACK_RISK_SCORE);
            return Mono.just(FALLBACK_RISK_SCORE);
        }
        log.error("Fallo transitorio en getRiskScore con circuit breaker cerrado. Cuenta: {}, Error: {}. "
                        + "Se propaga sin fallback (clave elegible a reintento).",
                transaction.getAccountId(), t.getMessage());
        return Mono.error(t);
    }

    /**
     * Fallback de {@link #isAccountBlacklisted(String)}.
     *
     * <p>Solo degrada a {@code false} cuando el Circuit Breaker está abierto; ante un fallo
     * transitorio con el Circuit Breaker cerrado re-propaga el error (Requirements 2.5, 2.6).</p>
     */
    Mono<Boolean> isAccountBlacklistedFallback(String accountId, Throwable t) {
        if (t instanceof CallNotPermittedException) {
            log.warn("Circuit breaker ABIERTO para isAccountBlacklisted. Cuenta: {}, degradando a false",
                    accountId);
            return Mono.just(false);
        }
        log.error("Fallo transitorio en isAccountBlacklisted con circuit breaker cerrado. Cuenta: {}, "
                        + "Error: {}. Se propaga sin fallback.", accountId, t.getMessage());
        return Mono.error(t);
    }

    private static class RiskRequest {
        private final String accountId;
        private final java.math.BigDecimal amount;
        private final String currency;
        private final String channel;

        public RiskRequest(String accountId, java.math.BigDecimal amount,
                          String currency, String channel) {
            this.accountId = accountId;
            this.amount = amount;
            this.currency = currency;
            this.channel = channel;
        }

        public String getAccountId() { return accountId; }
        public java.math.BigDecimal getAmount() { return amount; }
        public String getCurrency() { return currency; }
        public String getChannel() { return channel; }
    }

    private static class RiskScoreResponse {
        private int score;

        public int getScore() { return score; }
        public void setScore(int score) { this.score = score; }
    }

    private static class BlacklistResponse {
        private boolean blacklisted;

        public boolean isBlacklisted() { return blacklisted; }
        public void setBlacklisted(boolean blacklisted) { this.blacklisted = blacklisted; }
    }
}
