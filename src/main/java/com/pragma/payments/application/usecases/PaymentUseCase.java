package com.pragma.payments.application.usecases;

import com.pragma.payments.application.ports.in.PaymentServicePort;
import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.core.CoreBankingAdapter;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class PaymentUseCase implements PaymentServicePort {

    private static final Logger log = LoggerFactory.getLogger(PaymentUseCase.class);
    private static final Duration EXTERNAL_SERVICE_TIMEOUT = Duration.ofSeconds(3);
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final int HIGH_RISK_THRESHOLD = 70;

    private final FraudDetectionPort fraudDetectionPort;
    private final RiskBureauPort riskBureauPort;
    private final CoreBankingAdapter coreBankingAdapter;
    private final Map<String, Transaction> transactionCache;

    public PaymentUseCase(
            FraudDetectionPort fraudDetectionPort,
            RiskBureauPort riskBureauPort,
            CoreBankingAdapter coreBankingAdapter) {
        this.fraudDetectionPort = fraudDetectionPort;
        this.riskBureauPort = riskBureauPort;
        this.coreBankingAdapter = coreBankingAdapter;
        this.transactionCache = new ConcurrentHashMap<>();
    }

    @Override
    @CircuitBreaker(name = "paymentCircuitBreaker", fallbackMethod = "processPaymentFallback")
    @Retry(name = "paymentRetry")
    public Mono<Transaction> processPayment(Mono<Transaction> transactionMono) {
        return transactionMono
                .flatMap(this::validateIdempotency)
                .flatMap(this::executeFraudDetection)
                .flatMap(this::executeRiskEvaluation)
                .flatMap(this::finalizeTransaction)
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Boolean> isTransactionProcessed(IdempotencyKey idempotencyKey) {
        String cacheKey = buildCacheKey(idempotencyKey);
        return Mono.fromCallable(() -> transactionCache.containsKey(cacheKey))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Transaction> getProcessedTransaction(IdempotencyKey idempotencyKey) {
        String cacheKey = buildCacheKey(idempotencyKey);
        return Mono.fromCallable(() -> transactionCache.get(cacheKey))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(transaction -> transaction != null
                        ? Mono.just(transaction)
                        : Mono.empty());
    }

    private Mono<Transaction> validateIdempotency(Transaction transaction) {
        IdempotencyKey key = transaction.getIdempotencyKey();
        String cacheKey = buildCacheKey(key);

        return Mono.fromCallable(() -> transactionCache.get(cacheKey))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(existing -> {
                    if (existing != null) {
                        log.warn("Transacción duplicada detectada para clave: {}", cacheKey);
                        return Mono.<Transaction>error(new TransactionAlreadyProcessedException(
                                cacheKey,
                                key != null ? key.getChannel() : "unknown",
                                transaction.getId(),
                                Instant.now(),
                                key != null ? key.getOperationNumber() : "unknown"));
                    }
                    return Mono.just(transaction);
                })
                .switchIfEmpty(Mono.just(transaction));
    }

    @CircuitBreaker(name = "fraudDetectionCircuitBreaker", fallbackMethod = "fraudDetectionFallback")
    private Mono<Transaction> executeFraudDetection(Transaction transaction) {
        return fraudDetectionPort.evaluateTransaction(transaction)
                .timeout(EXTERNAL_SERVICE_TIMEOUT)
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(isFraudulent -> {
                    if (Boolean.TRUE.equals(isFraudulent)) {
                        log.warn("Transacción {} rechazada por motor antifraude", transaction.getId());
                        transaction.setStatus(Transaction.TransactionStatus.REJECTED);
                        transaction.setFailureReason("Rechazada por motor antifraude");
                    } else {
                        log.info("Transacción {} aprobada por motor antifraude", transaction.getId());
                    }
                    return Mono.just(transaction);
                })
                .onErrorResume(Exception.class, e -> {
                    log.error("Error en detección de fraude para transacción {}: {}",
                            transaction.getId(), e.getMessage());
                    return Mono.error(new ExternalServiceTimeoutException(
                            "FRAUD_DETECTION", "/evaluate",
                            EXTERNAL_SERVICE_TIMEOUT, EXTERNAL_SERVICE_TIMEOUT));
                });
    }

    @CircuitBreaker(name = "riskBureauCircuitBreaker", fallbackMethod = "riskEvaluationFallback")
    private Mono<Transaction> executeRiskEvaluation(Transaction transaction) {
        if (Transaction.TransactionStatus.REJECTED.name().equals(transaction.getStatus())) {
            return Mono.just(transaction);
        }

        return riskBureauPort.getRiskScore(transaction)
                .timeout(EXTERNAL_SERVICE_TIMEOUT)
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(score -> {
                    transaction.setRiskScore(score);
                    log.info("Puntuación de riesgo para transacción {}: {}", transaction.getId(), score);

                    if (score >= HIGH_RISK_THRESHOLD) {
                        transaction.setStatus(Transaction.TransactionStatus.REJECTED);
                        transaction.setFailureReason("Riesgo alto detectado: puntuación " + score);
                        log.warn("Transacción {} rechazada por riesgo alto: {}", transaction.getId(), score);
                    }
                    return Mono.just(transaction);
                })
                .onErrorResume(Exception.class, e -> {
                    log.error("Error en evaluación de riesgo para transacción {}: {}",
                            transaction.getId(), e.getMessage());
                    return Mono.error(new ExternalServiceTimeoutException(
                            "RISK_BUREAU", "/score",
                            EXTERNAL_SERVICE_TIMEOUT, EXTERNAL_SERVICE_TIMEOUT));
                });
    }

    private Mono<Transaction> finalizeTransaction(Transaction transaction) {
        return Mono.fromCallable(() -> {
            if (!Transaction.TransactionStatus.REJECTED.name().equals(transaction.getStatus())) {
                transaction.setStatus(Transaction.TransactionStatus.COMPLETED);
            }
            transaction.setProcessedAt(Instant.now());

            String cacheKey = buildCacheKey(transaction.getIdempotencyKey());
            transactionCache.put(cacheKey, transaction);

            log.info("Transacción {} finalizada con estado: {}",
                    transaction.getId(), transaction.getStatus());
            return transaction;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private String buildCacheKey(IdempotencyKey key) {
        if (key == null) {
            return "unknown";
        }
        return key.getOperationNumber() + "_" + key.getChannel();
    }

    private Mono<Transaction> processPaymentFallback(Mono<Transaction> transactionMono, Throwable t) {
        log.error("Circuit breaker activado para processPayment. Fallback ejecutado. Error: {}",
                t.getMessage());
        return transactionMono.map(transaction -> {
            transaction.setStatus(Transaction.TransactionStatus.PENDING);
            transaction.setFailureReason("Servicio temporalmente no disponible. Por favor intente más tarde.");
            return transaction;
        });
    }

    private Mono<Transaction> fraudDetectionFallback(Transaction transaction, Throwable t) {
        log.warn("Fallback de detección de fraude para transacción {}. Error: {}",
                transaction.getId(), t.getMessage());
        return Mono.just(transaction);
    }

    private Mono<Transaction> riskEvaluationFallback(Transaction transaction, Throwable t) {
        log.warn("Fallback de evaluación de riesgo para transacción {}. Error: {}",
                transaction.getId(), t.getMessage());
        return Mono.just(transaction);
    }
}
