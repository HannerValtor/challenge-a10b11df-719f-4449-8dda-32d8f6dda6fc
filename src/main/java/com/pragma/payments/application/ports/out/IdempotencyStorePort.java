package com.pragma.payments.application.ports.out;

import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.Transaction;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Puerto de salida reactivo para el almacenamiento persistente de idempotencia.
 *
 * <p>Define el contrato que la infraestructura debe implementar para reservar,
 * consultar, actualizar y eliminar registros de idempotencia (Idempotency_Record).
 * El almacenamiento sobrevive a reinicios del servicio y apoya la garantía de
 * "exactamente-una-ejecución" bajo carrera en una restricción de unicidad del
 * motor de persistencia.</p>
 *
 * <p>Todas las operaciones devuelven tipos reactivos ({@link Mono}) para no
 * bloquear el event-loop reactivo de WebFlux. Cuando el adaptador concreto usa
 * un controlador de acceso bloqueante, debe aislar dicho acceso en un planificador
 * acotado ({@code Schedulers.boundedElastic()}).</p>
 */
public interface IdempotencyStorePort {

    /**
     * Reserva un registro de idempotencia mediante una inserción condicional atómica.
     *
     * <p>Si la clave estaba libre, emite el registro reservado en estado
     * {@code IN_PROGRESS}. Si la clave ya existe, señala un error de duplicado que el
     * llamador trata como solicitud duplicada (Duplicate_Request); dicho error no se
     * propaga al cliente.</p>
     *
     * @param record el registro a reservar, con su clave y estado {@code IN_PROGRESS}
     * @return Mono que emite el registro reservado, o un error de duplicado si la clave ya existe
     */
    Mono<IdempotencyRecord> reserve(IdempotencyRecord record);

    /**
     * Consulta un registro de idempotencia por su clave.
     *
     * @param key la clave de idempotencia a consultar
     * @return Mono que emite el registro si existe, o {@link Mono#empty()} si no existe o fue expirado
     */
    Mono<IdempotencyRecord> find(IdempotencyKey key);

    /**
     * Completa un registro reservado, transicionándolo de {@code IN_PROGRESS} a
     * {@code COMPLETED} y asociándole el resultado de procesamiento (Processing_Result)
     * en una única operación de actualización.
     *
     * @param key    la clave del registro a completar
     * @param result la transacción resultante que se persiste como resultado asociado
     * @return Mono que emite el registro actualizado a {@code COMPLETED}
     */
    Mono<IdempotencyRecord> complete(IdempotencyKey key, Transaction result);

    /**
     * Marca un registro reservado como {@code FAILED} o libera la reserva, de modo que
     * la clave quede elegible para un reintento posterior tras un fallo transitorio.
     *
     * @param key la clave del registro a marcar como fallido o liberar
     * @return Mono que completa cuando la transición o liberación ha terminado
     */
    Mono<Void> markFailedOrRelease(IdempotencyKey key);

    /**
     * Elimina del almacenamiento únicamente los registros cuya marca de expiración es
     * anterior al instante indicado, conservando los no expirados.
     *
     * @param now el instante de referencia; se eliminan los registros con {@code expiresAt < now}
     * @return Mono que emite el número de registros eliminados
     */
    Mono<Long> deleteExpired(Instant now);
}
