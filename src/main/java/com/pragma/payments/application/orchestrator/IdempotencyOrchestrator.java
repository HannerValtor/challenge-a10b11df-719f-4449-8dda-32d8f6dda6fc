package com.pragma.payments.application.orchestrator;

import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import reactor.core.publisher.Mono;

/**
 * Colaborador de aplicación que posee la orquestación de idempotencia del
 * procesamiento de pagos (ciclo de vida: find &rarr; decidir &rarr; reserve
 * &rarr; ejecutar cadena &rarr; complete/fail).
 *
 * <p>Su responsabilidad es garantizar la semántica de <em>exactamente-una-ejecución</em>:
 * consulta el almacén de idempotencia por la {@link IdempotencyKey}, decide el
 * flujo según el estado del registro existente ({@code COMPLETED} &rarr; resultado
 * almacenado, {@code IN_PROGRESS} &rarr; conflicto, {@code FAILED}/ausente &rarr;
 * reservar y procesar), reserva atómicamente mediante la restricción de unicidad
 * del almacén y, solo si gana la reserva, ejecuta la cadena de servicios externos
 * exactamente una vez, finalizando el registro como {@code COMPLETED} en éxito o
 * marcándolo {@code FAILED}/liberando la reserva ante un fallo transitorio.</p>
 *
 * <p>Este contrato separa la orquestación de idempotencia de la ejecución de la
 * cadena externa, que queda delegada en un colaborador independiente.</p>
 */
public interface IdempotencyOrchestrator {

    /**
     * Resuelve la transacción respetando la idempotencia exactamente-una-ejecución.
     *
     * <p>Ejecuta la cadena de servicios externos únicamente cuando la solicitud
     * gana la reserva atómica sobre la {@link IdempotencyKey}; en caso contrario
     * decide a partir del registro de idempotencia existente sin volver a ejecutar
     * la cadena.</p>
     *
     * @param key         clave de idempotencia determinista de la transacción.
     * @param transaction transacción a resolver.
     * @return {@code Mono<Transaction>} que emite la transacción en su estado
     *         final (nueva ejecución o resultado previamente almacenado), o un
     *         error cuando corresponda según el estado del registro o un fallo
     *         transitorio de la cadena.
     */
    Mono<Transaction> resolve(IdempotencyKey key, Transaction transaction);
}
