package com.pragma.payments.infrastructure.adapters.out.core;

import com.pragma.payments.application.ports.out.CoreBankingPort;
import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.config.ExternalTimeoutProperties;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

/**
 * Adaptador de salida hacia el core bancario.
 *
 * <p>La resiliencia (Circuit Breaker + Retry) se declara sobre {@link #processTransaction(Transaction)},
 * un método público del bean invocado a través del límite del bean por los colaboradores de la
 * cadena externa. Al vivir sobre un método público interceptable (no privado ni auto-invocado), el
 * proxy AOP de Spring/Resilience4j la intercepta efectivamente (corrige H2, Requirement 2.3, D6).</p>
 *
 * <p>El timeout del core se aplica desde {@link ExternalTimeoutProperties#getCore()} como único
 * lugar de verdad (D2, Requirement 1.7). Al expirar, la invocación se aborta y libera el permiso del
 * bulkhead sin esperar la respuesta pendiente (Requirements 1.8, 1.9).</p>
 *
 * <p>Cuando el circuit breaker está abierto (o se agotan los reintentos), el fallback devuelve la
 * referencia {@code "DECLINED"} para que el sistema siga respondiendo en modo degradado sin invocar
 * el servicio externo (Requirement 2.3).</p>
 */
@Component
public class CoreBankingAdapter implements CoreBankingPort {

    private static final Logger log = LoggerFactory.getLogger(CoreBankingAdapter.class);
    private static final String CIRCUIT_BREAKER_NAME = "coreBankingCircuitBreaker";
    private static final String RETRY_NAME = "coreBankingRetry";
    /** Timeout por defecto usado solo por el constructor de conveniencia para pruebas unitarias. */
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(1);
    /** Referencia de rechazo retornada por el fallback cuando el circuit breaker está abierto. */
    private static final String DECLINED_REFERENCE = "DECLINED";

    private final WebClient webClient;
    private final String baseUrl;
    private final Bulkhead bulkhead;
    private final Duration coreTimeout;

    public CoreBankingAdapter(
            WebClient webClient,
            BulkheadRegistry bulkheadRegistry,
            ExternalTimeoutProperties timeoutProperties,
            @Value("${app.services.core-banking.base-url:http://localhost:8081}") String baseUrl) {
        this.webClient = webClient;
        this.baseUrl = baseUrl;
        this.bulkhead = bulkheadRegistry.bulkhead("coreBankingService");
        this.coreTimeout = timeoutProperties.getCore();
        log.info("CoreBankingAdapter initialized with baseUrl: {}, coreTimeout: {}ms",
                baseUrl, coreTimeout.toMillis());
    }

    /** Constructor de conveniencia para pruebas unitarias (sin contexto Spring). */
    public CoreBankingAdapter(String baseUrl) {
        this.webClient = WebClient.builder().build();
        this.baseUrl = baseUrl;
        this.bulkhead = BulkheadRegistry.ofDefaults().bulkhead("coreBankingService");
        this.coreTimeout = DEFAULT_TIMEOUT;
    }

    /**
     * Procesa la transacción contra el core bancario. Método público interceptable por AOP donde
     * residen las políticas de Circuit Breaker y Retry (Requirement 2.3). Aplica el timeout de core
     * desde {@link ExternalTimeoutProperties} y, ante circuit breaker abierto, delega en el fallback
     * que devuelve {@code "DECLINED"}.
     *
     * @param transaction transacción a procesar
     * @return referencia del core en caso de éxito, o {@code "DECLINED"} vía fallback
     */
    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "processTransactionFallback")
    @Retry(name = RETRY_NAME)
    public Mono<String> processTransaction(Transaction transaction) {
        log.info("Procesando transacción {} en el core bancario", transaction.getId());

        return webClient.post()
                .uri(baseUrl + "/api/v1/transactions/process")
                .bodyValue(buildProcessRequest(transaction))
                .retrieve()
                .onStatus(HttpStatusCode::is5xxServerError,
                        response -> Mono.error(new RuntimeException(
                                "Core Banking no disponible: " + response.statusCode())))
                .bodyToMono(String.class)
                .timeout(coreTimeout, Mono.error(new ExternalServiceTimeoutException(
                        "CORE_BANKING", "/process", coreTimeout, coreTimeout)))
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .doOnSuccess(response -> log.info("Transacción {} procesada por el core: {}",
                        transaction.getId(), response))
                .doOnError(error -> log.error("Error procesando transacción {} en el core: {}",
                        transaction.getId(), error.getMessage()));
    }

    @Override
    public Mono<Transaction> sendTransaction(Transaction transaction) {
        return processTransaction(transaction)
                .map(reference -> {
                    transaction.markAsCompleted(reference);
                    return transaction;
                });
    }

    @Override
    public Mono<Transaction> getTransactionStatus(String transactionId) {
        log.info("Consultando estado de la transacción {} en el core bancario", transactionId);

        return webClient.get()
                .uri(baseUrl + "/api/v1/transactions/{id}", transactionId)
                .retrieve()
                .bodyToMono(Transaction.class)
                .timeout(coreTimeout, Mono.error(new ExternalServiceTimeoutException(
                        "CORE_BANKING", "/transactions/{id}", coreTimeout, coreTimeout)));
    }

    @Override
    public Mono<Boolean> isAvailable() {
        return webClient.get()
                .uri(baseUrl + "/actuator/health")
                .retrieve()
                .bodyToMono(String.class)
                .map(body -> true)
                .timeout(coreTimeout)
                .onErrorReturn(false);
    }

    private Map<String, Object> buildProcessRequest(Transaction transaction) {
        return Map.of(
                "transactionId", transaction.getId(),
                "accountId", transaction.getAccountId() != null ? transaction.getAccountId() : "",
                "amount", transaction.getAmount() != null ? transaction.getAmount().toString() : "0",
                "currency", transaction.getCurrency() != null ? transaction.getCurrency() : "",
                "channel", transaction.getChannel() != null ? transaction.getChannel() : "");
    }

    /**
     * Fallback invocado cuando el circuit breaker del core está abierto (o se agotan los reintentos
     * ante un fallo). Devuelve la referencia {@code "DECLINED"} sin invocar el servicio externo, de
     * modo que el sistema siga operando en modo degradado (Requirement 2.3).
     *
     * @param transaction transacción en curso
     * @param t causa que activó el fallback
     * @return un {@link Mono} que emite {@code "DECLINED"}
     */
    private Mono<String> processTransactionFallback(Transaction transaction, Throwable t) {
        log.error("Fallback de core banking para transacción {}. Error: {}",
                transaction.getId(), t.getMessage());
        return Mono.just(DECLINED_REFERENCE);
    }
}
