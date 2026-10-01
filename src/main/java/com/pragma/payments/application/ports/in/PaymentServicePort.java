package com.pragma.payments.application.ports.in;

import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import reactor.core.publisher.Mono;

/**
 * Puerto de entrada para el procesamiento de pagos.
 * Define las operaciones que el dominio expone para procesar transacciones
 * con manejo de concurrencia e idempotencia.
 */
public interface PaymentServicePort {
    /**
     * Procesa una solicitud de pago con idempotencia garantizada.
     *
     * @param transactionMono Mono con la transacción a procesar.
     * @return Mono<Transaction> que emite la transacción procesada o error en caso de fallo.
     */
    Mono<Transaction> processPayment(Mono<Transaction> transactionMono);

    /**
     * Verifica si una transacción ya fue procesada con la misma clave de idempotencia.
     * 
     * @param idempotencyKey Clave de idempotencia compuesta por operationNumber + channel.
     * @return Mono<Boolean> que emite true si la transacción ya existe, false en caso contrario.
     */
    Mono<Boolean> isTransactionProcessed(IdempotencyKey idempotencyKey);

    /**
     * Recupera una transacción procesada por su clave de idempotencia.
     * 
     * @param idempotencyKey Clave de idempotencia compuesta por operationNumber + channel.
     * @return Mono<Transaction> que emite la transacción si existe, o Mono.empty() si no.
     */
    Mono<Transaction> getProcessedTransaction(IdempotencyKey idempotencyKey);
}