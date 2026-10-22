package com.pragma.payments.application.usecases;

import com.pragma.payments.application.ports.in.PaymentServicePort;
import com.pragma.payments.application.ports.out.DuplicateReservationException;
import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.core.CoreBankingAdapter;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;

@Service
public class PaymentUseCase implements PaymentServicePort {

    private static final Logger log = LoggerFactory.getLogger(PaymentUseCase.class);
    private static final Duration EXTERNAL_SERVICE_TIMEOUT = Duration.ofSeconds(3);
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final int HIGH_RISK_THRESHOLD = 70;

    private final FraudDetectionPort fraudDetectionPort;
    private final RiskBureauPort riskBureauPort;
    private final CoreBankingAdapter coreBankingAdapter;
    private final IdempotencyStorePort idempotencyStore;
    private final IdempotencyProperties idempotencyProperties;

    public PaymentUseCase(
            FraudDetectionPort fraudDetectionPort,
            RiskBureauPort riskBureauPort,
            CoreBankingAdapter coreBankingAdapter,
            IdempotencyStorePort idempotencyStore,
            IdempotencyProperties idempotencyProperties) {
        this.fraudDetectionPort = fraudDetectionPort;
        this.riskBureauPort = riskBureauPort;
        this.coreBankingAdapter = coreBankingAdapter;
        this.idempotencyStore = idempotencyStore;
        this.idempotencyProperties = idempotencyProperties;
    }

    @Override
    @CircuitBreaker(name = "paymentCircuitBreaker", fallbackMethod = "processPaymentFallback")
    @Retry(name = "paymentRetry")
    public Mono<Transaction> processPayment(Mono<Transaction> transactionMono) {
        return transactionMono.flatMap(transaction -> {
            IdempotencyKey key = transaction.getIdempotencyKey();
            // Req 2.1: se consulta el store ANTES de ejecutar la cadena de servicios externos.
            // Req 2.2: si find falla técnicamente, el error se propaga y nunca se ejecuta la cadena.
            return idempotencyStore.find(key)
                    .flatMap(existing -> decideByExistingRecord(existing, transaction))
                    // Record ausente/expirado: continuar a la reserva (Req 6.5, 7.4).
                    .switchIfEmpty(Mono.defer(() -> reserveAndProcess(key, transaction)));
        });
    }

    /**
     * Aplica la decisión de idempotencia a partir de un registro ya existente (Req 3.1, 7.1, 7.4):
     * <ul>
     *     <li>{@code COMPLETED} ⇒ retorna el Processing_Result cacheado sin ejecutar la cadena externa.</li>
     *     <li>{@code IN_PROGRESS} ⇒ lanza {@link TransactionAlreadyProcessedException} (→ HTTP 409).</li>
     *     <li>{@code FAILED} ⇒ continúa a una nueva reserva para permitir el reintento.</li>
     * </ul>
     */
    private Mono<Transaction> decideByExistingRecord(IdempotencyRecord existing, Transaction transaction) {
        RecordStatus status = existing.getStatus();
        if (status == RecordStatus.COMPLETED) {
            log.info("Solicitud duplicada COMPLETED para clave {}: se retorna el resultado cacheado",
                    canonicalKey(existing.getKey()));
            return Mono.just(existing.getResult());
        }
        if (status == RecordStatus.IN_PROGRESS) {
            log.warn("Solicitud duplicada IN_PROGRESS para clave {}: se rechaza (409)",
                    canonicalKey(existing.getKey()));
            return Mono.error(toAlreadyProcessed(existing));
        }
        // FAILED: elegible para reintento (Req 7.4).
        log.info("Registro FAILED para clave {}: se reintenta reservando de nuevo",
                canonicalKey(existing.getKey()));
        return reserveAndProcess(transaction.getIdempotencyKey(), transaction);
    }

    /**
     * Reserva atómicamente la clave en estado {@code IN_PROGRESS} (Req 4.1) y, si gana la carrera,
     * ejecuta la cadena de servicios externos intacta (fraude → riesgo → core) y luego completa el
     * registro (Req 4.2). Si pierde la carrera por un duplicado (violación de unicidad traducida a
     * {@link DuplicateReservationException}), relee el registro y aplica la misma decisión (Req 5.2).
     * Ante un fallo transitorio de la cadena externa, marca el registro como {@code FAILED} o libera
     * la reserva para habilitar un reintento posterior (Req 7.3).
     */
    private Mono<Transaction> reserveAndProcess(IdempotencyKey key, Transaction transaction) {
        IdempotencyRecord reservation =
                IdempotencyRecord.reserve(key, Instant.now(), idempotencyProperties.getTtl());

        return idempotencyStore.reserve(reservation)
                .flatMap(reserved -> executeChainAndComplete(key, transaction))
                // Pérdida de la carrera: relee y aplica la decisión de Duplicate_Request (Req 5.2).
                .onErrorResume(DuplicateReservationException.class, dup -> {
                    log.warn("Reserva perdida por carrera para clave {}: se relee el registro",
                            canonicalKey(key));
                    return idempotencyStore.find(key)
                            .flatMap(existing -> decideByExistingRecord(existing, transaction))
                            .switchIfEmpty(Mono.error(dup));
                });
    }

    /**
     * Ejecuta la cadena de servicios externos existente (sin cambios) y, al terminar con éxito,
     * persiste el resultado completando el registro de idempotencia. Ante un fallo transitorio
     * (cualquier error que no sea duplicado), marca el registro como {@code FAILED}/lo libera y
     * propaga el error.
     */
    private Mono<Transaction> executeChainAndComplete(IdempotencyKey key, Transaction transaction) {
        return executeFraudDetection(transaction)
                .flatMap(this::executeRiskEvaluation)
                .flatMap(this::finalizeTransaction)
                .flatMap(result -> idempotencyStore.complete(key, result).thenReturn(result))
                .onErrorResume(error -> {
                    if (error instanceof TransactionAlreadyProcessedException
                            || error instanceof DuplicateReservationException) {
                        return Mono.error(error);
                    }
                    // Fallo transitorio de la cadena externa: dejar la clave elegible a reintento (Req 7.3).
                    return idempotencyStore.markFailedOrRelease(key)
                            .then(Mono.error(error));
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Boolean> isTransactionProcessed(IdempotencyKey idempotencyKey) {
        return idempotencyStore.find(idempotencyKey)
                .map(record -> record.getStatus() == RecordStatus.COMPLETED)
                .defaultIfEmpty(false);
    }

    @Override
    public Mono<Transaction> getProcessedTransaction(IdempotencyKey idempotencyKey) {
        return idempotencyStore.find(idempotencyKey)
                .filter(record -> record.getStatus() == RecordStatus.COMPLETED)
                .map(IdempotencyRecord::getResult);
    }

    private TransactionAlreadyProcessedException toAlreadyProcessed(IdempotencyRecord record) {
        IdempotencyKey key = record.getKey();
        Transaction result = record.getResult();
        return new TransactionAlreadyProcessedException(
                canonicalKey(key),
                key != null ? key.getChannel() : "unknown",
                result != null ? result.getId() : null,
                result != null ? result.getProcessedAt() : null,
                key != null ? key.getOperationNumber() : "unknown");
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
        // Si ya fue rechazada (fraude o riesgo alto), no se envía al core bancario.
        if (Transaction.TransactionStatus.REJECTED.name().equals(transaction.getStatus())) {
            return persistFinalState(transaction);
        }

        // Envío al core bancario como paso final de la cadena de servicios externos.
        // El timeout de 2000 ms y el bulkhead viven en el propio CoreBankingAdapter (Req 5.1, 5.3).
        return coreBankingAdapter.processTransaction(transaction)
                .timeout(EXTERNAL_SERVICE_TIMEOUT)
                .flatMap(coreResult -> {
                    transaction.setExternalReference(coreResult);
                    transaction.setStatus(Transaction.TransactionStatus.COMPLETED);
                    return persistFinalState(transaction);
                })
                .onErrorResume(Exception.class, e -> {
                    log.error("Error del core bancario para transacción {}: {}",
                            transaction.getId(), e.getMessage());
                    return Mono.error(new ExternalServiceTimeoutException(
                            "CORE_BANKING", "/process",
                            EXTERNAL_SERVICE_TIMEOUT, EXTERNAL_SERVICE_TIMEOUT));
                });
    }

    private Mono<Transaction> persistFinalState(Transaction transaction) {
        return Mono.fromCallable(() -> {
            transaction.setProcessedAt(Instant.now());
            log.info("Transacción {} finalizada con estado: {}",
                    transaction.getId(), transaction.getStatus());
            return transaction;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    private String canonicalKey(IdempotencyKey key) {
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
