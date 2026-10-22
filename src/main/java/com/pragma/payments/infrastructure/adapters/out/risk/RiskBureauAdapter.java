package com.pragma.payments.infrastructure.adapters.out.risk;

import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.model.Transaction;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
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

@Component
public class RiskBureauAdapter implements RiskBureauPort {

    private static final Logger log = LoggerFactory.getLogger(RiskBureauAdapter.class);
    // Timeout alineado a 2000 ms: el buró de riesgos no debe retener la llamada más de 2s
    // (Requirement 5.1). Supera ese umbral ⇒ timeout + liberación del permiso del bulkhead.
    private static final int DEFAULT_TIMEOUT_SECONDS = 2;

    private final WebClient webClient;
    private final String baseUrl;
    private final int timeoutSeconds;
    private final Bulkhead bulkhead;

    public RiskBureauAdapter(
            WebClient webClient,
            BulkheadRegistry bulkheadRegistry,
            @Value("${external.services.risk-bureau.url:http://localhost:8083}") String baseUrl,
            @Value("${external.services.risk-bureau.timeout-seconds:2}") int timeoutSeconds) {
        this.webClient = webClient;
        this.baseUrl = baseUrl;
        this.timeoutSeconds = timeoutSeconds > 0 ? timeoutSeconds : DEFAULT_TIMEOUT_SECONDS;
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
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .map(RiskScoreResponse::getScore)
                .doOnSuccess(score -> log.info("Risk score obtenido: {} para cuenta: {}", 
                        score, transaction.getAccountId()))
                .doOnError(WebClientResponseException.class, e -> 
                        log.error("Error HTTP {} al consultar risk bureau: {}", 
                                e.getStatusCode().value(), e.getMessage()))
                .doOnError(TimeoutException.class, e -> 
                        log.error("Timeout al consultar risk bureau para cuenta: {}", 
                                transaction.getAccountId()));
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
                .timeout(Duration.ofSeconds(timeoutSeconds))
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

    private Mono<Integer> getRiskScoreFallback(Transaction transaction, Throwable t) {
        log.warn("Fallback ejecutado para getRiskScore. Cuenta: {}, Error: {}", 
                transaction.getAccountId(), t.getMessage());
        return Mono.just(50);
    }

    private Mono<Boolean> isAccountBlacklistedFallback(String accountId, Throwable t) {
        log.warn("Fallback ejecutado para isAccountBlacklisted. Cuenta: {}, Error: {}", 
                accountId, t.getMessage());
        return Mono.just(false);
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