package com.pragma.payments.application.chain;

import com.pragma.payments.application.ports.out.CoreBankingPort;
import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Implementación de {@link PaymentProcessingChain} que ejecuta la cadena de servicios externos
 * en el orden fraude &rarr; riesgo &rarr; core (Requirement 3.5).
 *
 * <p>Esta clase es un colaborador de aplicación puro, aislado de la idempotencia y de la
 * persistencia: no invoca operaciones de {@code IdempotencyStorePort} ni depende de los tipos
 * {@code IdempotencyRecord} o {@code RecordStatus} (D1, Requirement 3.5). La orquestación de
 * idempotencia es responsabilidad exclusiva del orquestador.</p>
 *
 * <p>El estado de la transacción se compara <strong>únicamente</strong> mediante el enum
 * {@link TransactionStatus}, nunca contra literales de tipo {@code String} (corrige H3,
 * Requirement 3.6). Cuando la transacción alcanza {@link TransactionStatus#REJECTED} (por fraude o
 * por riesgo alto), los pasos externos posteriores se omiten mediante short-circuit, sin invocar
 * ningún servicio externo adicional (Requirement 3.7).</p>
 *
 * <p>No se aplica {@code subscribeOn(boundedElastic)} aquí: la cadena solo invoca puertos externos
 * (I/O de red no bloqueante), y el único salto de scheduler para la persistencia JPA bloqueante es
 * propiedad exclusiva del {@code IdempotencyStoreAdapter}.</p>
 */
@Component
public class PaymentProcessingChainImpl implements PaymentProcessingChain {

    private static final Logger log = LoggerFactory.getLogger(PaymentProcessingChainImpl.class);

    /** Umbral de riesgo alto preservado del flujo original: score &gt;= 70 rechaza la transacción. */
    private static final int HIGH_RISK_THRESHOLD = 70;

    private final FraudDetectionPort fraudDetectionPort;
    private final RiskBureauPort riskBureauPort;
    private final CoreBankingPort coreBankingPort;

    public PaymentProcessingChainImpl(
            FraudDetectionPort fraudDetectionPort,
            RiskBureauPort riskBureauPort,
            CoreBankingPort coreBankingPort) {
        this.fraudDetectionPort = fraudDetectionPort;
        this.riskBureauPort = riskBureauPort;
        this.coreBankingPort = coreBankingPort;
    }

    @Override
    public Mono<Transaction> execute(Transaction transaction) {
        return fraud(transaction)
                .flatMap(this::risk)
                .flatMap(this::core);
    }

    /**
     * Paso de fraude. Si el motor antifraude marca la transacción como sospechosa, la transacción
     * pasa a {@link TransactionStatus#REJECTED} y los pasos posteriores se omiten.
     */
    private Mono<Transaction> fraud(Transaction transaction) {
        return fraudDetectionPort.evaluateTransaction(transaction)
                .map(isFraudulent -> {
                    if (Boolean.TRUE.equals(isFraudulent)) {
                        log.warn("Transacción {} rechazada por motor antifraude", transaction.getId());
                        transaction.setFraudDetected(true);
                        transaction.setStatus(TransactionStatus.REJECTED);
                        transaction.setFailureReason("Rechazada por motor antifraude");
                    } else {
                        transaction.setFraudDetected(false);
                        log.info("Transacción {} aprobada por motor antifraude", transaction.getId());
                    }
                    return transaction;
                });
    }

    /**
     * Paso de riesgo. Short-circuit (comparación por enum, Requirement 3.6/3.7) si la transacción
     * ya fue rechazada en fraude: no se invoca al buró de riesgo. En caso contrario, un score
     * &gt;= {@value #HIGH_RISK_THRESHOLD} rechaza la transacción.
     */
    private Mono<Transaction> risk(Transaction transaction) {
        if (isRejected(transaction)) {
            return Mono.just(transaction);
        }
        return riskBureauPort.getRiskScore(transaction)
                .map(score -> {
                    transaction.setRiskScore(score);
                    log.info("Puntuación de riesgo para transacción {}: {}", transaction.getId(), score);
                    if (score >= HIGH_RISK_THRESHOLD) {
                        transaction.setStatus(TransactionStatus.REJECTED);
                        transaction.setFailureReason("Riesgo alto detectado: puntuación " + score);
                        log.warn("Transacción {} rechazada por riesgo alto: {}",
                                transaction.getId(), score);
                    }
                    return transaction;
                });
    }

    /**
     * Paso de core. Short-circuit (comparación por enum) si la transacción ya fue rechazada por
     * fraude o riesgo: no se envía al core bancario y se devuelve en su estado terminal
     * {@link TransactionStatus#REJECTED} (Requirement 3.7). En caso contrario, se delega en el
     * core bancario, que finaliza la transacción en estado {@link TransactionStatus#COMPLETED}.
     */
    private Mono<Transaction> core(Transaction transaction) {
        if (isRejected(transaction)) {
            transaction.setProcessedAt(Instant.now());
            log.info("Transacción {} finalizada con estado: {}",
                    transaction.getId(), transaction.getStatus());
            return Mono.just(transaction);
        }
        return coreBankingPort.sendTransaction(transaction)
                .doOnNext(result -> log.info("Transacción {} finalizada con estado: {}",
                        result.getId(), result.getStatus()));
    }

    /**
     * Compara el estado de la transacción únicamente mediante el enum {@link TransactionStatus}
     * (identidad de enum), sin derivar ni comparar literales de tipo {@code String} (corrige H3 de
     * raíz, Requirement 3.6). Posible ahora que {@code Transaction.getStatus()} devuelve el enum.
     */
    private boolean isRejected(Transaction transaction) {
        return transaction.getStatus() == TransactionStatus.REJECTED;
    }
}
