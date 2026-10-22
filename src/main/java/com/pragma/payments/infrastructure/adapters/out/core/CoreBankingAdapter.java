package com.pragma.payments.infrastructure.adapters.out.core;

import com.pragma.payments.application.ports.out.CoreBankingPort;
import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
import com.pragma.payments.domain.model.Transaction;
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

@Component
public class CoreBankingAdapter implements CoreBankingPort {

    private static final Logger log = LoggerFactory.getLogger(CoreBankingAdapter.class);
    private static final String CIRCUIT_BREAKER_NAME = "coreBankingCircuitBreaker";
    private static final String RETRY_NAME = "coreBankingRetry";
    // Timeout alineado a 2000 ms (Requirement 5.1).
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);

    private final WebClient webClient;
    private final String baseUrl;
    private final Bulkhead bulkhead;

    public CoreBankingAdapter(
            WebClient webClient,
            BulkheadRegistry bulkheadRegistry,
            @Value("${app.services.core-banking.base-url:http://localhost:8081}") String baseUrl) {
        this.webClient = webClient;
        this.baseUrl = baseUrl;
        this.bulkhead = bulkheadRegistry.bulkhead("coreBankingService");
        log.info("CoreBankingAdapter initialized with baseUrl: {}", baseUrl);
    }

    /** Constructor de conveniencia para pruebas unitarias (sin contexto Spring). */
    public CoreBankingAdapter(String baseUrl) {
        this.webClient = WebClient.builder().build();
        this.baseUrl = baseUrl;
        this.bulkhead = BulkheadRegistry.ofDefaults().bulkhead("coreBankingService");
    }

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
                .timeout(DEFAULT_TIMEOUT, Mono.error(new ExternalServiceTimeoutException(
                        "CORE_BANKING", "/process", DEFAULT_TIMEOUT, DEFAULT_TIMEOUT)))
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
                .timeout(DEFAULT_TIMEOUT, Mono.error(new ExternalServiceTimeoutException(
                        "CORE_BANKING", "/transactions/{id}", DEFAULT_TIMEOUT, DEFAULT_TIMEOUT)));
    }

    @Override
    public Mono<Boolean> isAvailable() {
        return webClient.get()
                .uri(baseUrl + "/actuator/health")
                .retrieve()
                .bodyToMono(String.class)
                .map(body -> true)
                .timeout(DEFAULT_TIMEOUT)
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

    private Mono<String> processTransactionFallback(Transaction transaction, Throwable t) {
        log.error("Fallback de core banking para transacción {}. Error: {}",
                transaction.getId(), t.getMessage());
        return Mono.just("DECLINED");
    }
}
