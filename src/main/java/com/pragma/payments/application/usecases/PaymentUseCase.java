package com.pragma.payments.application.usecases;

import com.pragma.payments.application.orchestrator.IdempotencyOrchestrator;
import com.pragma.payments.application.ports.in.PaymentServicePort;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Coordinador delgado del procesamiento de pagos (Req 3.1).
 *
 * <p>Delega por completo la orquestación de idempotencia y la ejecución de la
 * cadena de servicios externos en {@link IdempotencyOrchestrator}. No posee
 * lógica de cadena externa ni acceso directo al almacén para el flujo principal.</p>
 *
 * <p>La resiliencia efectiva (Circuit Breaker/Retry) vive ahora en los adapters
 * de salida interceptables; se eliminaron las anotaciones inertes sobre métodos
 * privados/auto-invocados (H2, Req 1.10, 2.4).</p>
 */
@Service
public class PaymentUseCase implements PaymentServicePort {

    private static final Logger log = LoggerFactory.getLogger(PaymentUseCase.class);

    private final IdempotencyOrchestrator orchestrator;
    private final IdempotencyStorePort idempotencyStore;

    public PaymentUseCase(IdempotencyOrchestrator orchestrator, IdempotencyStorePort idempotencyStore) {
        this.orchestrator = orchestrator;
        this.idempotencyStore = idempotencyStore;
    }

    @Override
    public Mono<Transaction> processPayment(Mono<Transaction> transactionMono) {
        return transactionMono.flatMap(transaction -> {
            IdempotencyKey key = transaction.getIdempotencyKey();
            log.debug("Delegando resolución de idempotencia al orquestador para la clave {}", canonicalKey(key));
            return orchestrator.resolve(key, transaction);
        });
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

    private String canonicalKey(IdempotencyKey key) {
        if (key == null) {
            return "unknown";
        }
        return key.getOperationNumber() + "_" + key.getChannel();
    }
}
