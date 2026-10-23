package com.pragma.payments.application.chain;

import com.pragma.payments.domain.model.Transaction;
import reactor.core.publisher.Mono;

/**
 * Colaborador de aplicación que ejecuta la cadena de servicios externos
 * (fraude &rarr; riesgo &rarr; core) de forma aislada.
 *
 * <p>Este contrato es intencionalmente independiente de la idempotencia y de la
 * persistencia: la implementación no consulta ni modifica el almacén de
 * idempotencia ({@code IdempotencyStorePort}) y no depende de los tipos
 * {@code IdempotencyRecord} o {@code RecordStatus}. Su única responsabilidad es
 * orquestar los pasos externos y devolver la transacción en su estado final,
 * dejando la orquestación de idempotencia a un colaborador separado.</p>
 */
public interface PaymentProcessingChain {

    /**
     * Ejecuta la cadena de servicios externos (fraude &rarr; riesgo &rarr; core)
     * y devuelve la transacción en estado final.
     *
     * @param transaction transacción a procesar a través de la cadena externa.
     * @return {@code Mono<Transaction>} que emite la transacción en su estado
     *         terminal, o un error si algún paso externo falla.
     */
    Mono<Transaction> execute(Transaction transaction);
}
