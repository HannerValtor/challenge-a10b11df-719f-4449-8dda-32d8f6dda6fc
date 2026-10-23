package com.pragma.payments.infrastructure.adapters.out.persistence;

import com.pragma.payments.application.ports.out.DuplicateReservationException;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;

/**
 * Adaptador de salida que implementa {@link IdempotencyStorePort} sobre el
 * repositorio JPA (bloqueante) {@link IdempotencyRecordRepository}.
 *
 * <p>Es la única pieza que conoce el motor de persistencia concreto, cumpliendo el
 * principio de puertos y adaptadores: el dominio y la capa de aplicación solo
 * dependen del contrato reactivo del puerto. Conforme a la decisión D1 del diseño,
 * dado que Spring Data JPA es bloqueante, <strong>todo</strong> acceso al repositorio
 * se aísla en {@code Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())},
 * preservando el contrato reactivo sin bloquear el Event Loop (Req 8.1, 8.2).</p>
 *
 * <p><strong>Único salto de scheduler por operación (D5 / H7, Req 6.4, 7.10).</strong>
 * Cada operación de persistencia ({@link #reserve}, {@link #find}, {@link #complete},
 * {@link #markFailedOrRelease}, {@link #deleteExpired}) aplica <strong>exactamente un</strong>
 * {@code subscribeOn(Schedulers.boundedElastic())} y ese salto es el único autorizado para
 * el acceso bloqueante al almacén de idempotencia. Este adaptador es el <strong>propietario
 * exclusivo</strong> del cambio de scheduler bloqueante: los colaboradores de la capa de
 * aplicación (orquestador, cadena, use case) <strong>no deben</strong> envolver estas
 * llamadas en un segundo {@code subscribeOn}/{@code publishOn} hacia {@code boundedElastic},
 * evitando así los saltos de scheduler redundantes identificados en H7 sin renunciar al
 * único salto necesario que mantiene la JPA bloqueante fuera del event-loop de Netty.</p>
 *
 * <p>La atomicidad de la reserva bajo condiciones de carrera se delega a la restricción
 * de unicidad sobre {@code idempotency_key}: {@link #reserve(IdempotencyRecord)} ejecuta
 * una inserción condicional ({@code INSERT} puro) y traduce la violación de unicidad
 * ({@link DataIntegrityViolationException} / {@link DuplicateKeyException}) en una señal
 * de duplicado ({@link DuplicateReservationException}) que <strong>no se propaga al
 * cliente</strong>, sino que el {@code PaymentUseCase} usa para releer el registro y
 * decidir la respuesta de solicitud duplicada (Req 4.1, 4.3, 5.1, 5.2, 5.3).</p>
 */
@Repository
public class IdempotencyStoreAdapter implements IdempotencyStorePort {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyStoreAdapter.class);

    private final IdempotencyRecordRepository repository;
    private final IdempotencyRecordMapper mapper;

    public IdempotencyStoreAdapter(IdempotencyRecordRepository repository,
                                   IdempotencyRecordMapper mapper) {
        this.repository = repository;
        this.mapper = mapper;
    }

    /**
     * Reserva atómica mediante inserción condicional. Si la clave está libre, inserta un
     * registro {@code IN_PROGRESS} y lo emite; si ya existe, el motor rechaza por violación
     * de unicidad y se emite una {@link DuplicateReservationException} (Req 4.1, 4.3, 5.3).
     */
    @Override
    public Mono<IdempotencyRecord> reserve(IdempotencyRecord record) {
        return Mono.fromCallable(() -> {
                    IdempotencyRecordEntity entity = mapper.toEntity(record);
                    repository.insertNew(
                            entity.getIdempotencyKey(),
                            entity.getStatus(),
                            entity.getResultPayload(),
                            entity.getCreatedAt(),
                            entity.getExpiresAt());
                    log.debug("Reserva creada para la clave de idempotencia '{}' en estado {}",
                            entity.getIdempotencyKey(), entity.getStatus());
                    return record;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(this::isUniqueViolation, ex -> {
                    String key = record.getKey().toUniqueString();
                    log.debug("Reserva perdida por carrera para la clave '{}': clave ya existente", key);
                    return Mono.error(new DuplicateReservationException(key, ex));
                });
    }

    /**
     * Consulta por clave. Emite el registro si existe o {@link Mono#empty()} si no (Req 2.1).
     */
    @Override
    public Mono<IdempotencyRecord> find(IdempotencyKey key) {
        return Mono.fromCallable(() ->
                        repository.findById(key.toUniqueString())
                                .map(mapper::toDomain)
                                .orElse(null))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Transición {@code IN_PROGRESS -> COMPLETED} asociando el Processing_Result en una
     * única operación de actualización (Req 4.2). Carga el registro, aplica
     * {@link IdempotencyRecord#withResult(Transaction)} y persiste.
     */
    @Override
    public Mono<IdempotencyRecord> complete(IdempotencyKey key, Transaction result) {
        return Mono.fromCallable(() -> {
                    String canonical = key.toUniqueString();
                    IdempotencyRecordEntity existing = repository.findById(canonical)
                            .orElseThrow(() -> new IllegalStateException(
                                    "No existe un registro reservado para completar con la clave '" + canonical + "'"));
                    IdempotencyRecord completed = mapper.toDomain(existing).withResult(result);
                    IdempotencyRecordEntity saved = repository.save(mapper.toEntity(completed));
                    log.debug("Registro '{}' completado y actualizado a {}", canonical, saved.getStatus());
                    return mapper.toDomain(saved);
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Marca el registro reservado como {@code FAILED} para dejar la clave elegible a
     * reintento tras un fallo transitorio (Req 7.3). Si el registro no existe, completa sin
     * error (la reserva ya fue liberada).
     */
    @Override
    public Mono<Void> markFailedOrRelease(IdempotencyKey key) {
        return Mono.<Void>fromRunnable(() -> {
                    String canonical = key.toUniqueString();
                    repository.findById(canonical).ifPresentOrElse(
                            existing -> {
                                IdempotencyRecord failed = mapper.toDomain(existing).asFailed();
                                repository.save(mapper.toEntity(failed));
                                log.debug("Registro '{}' marcado como FAILED; clave elegible a reintento", canonical);
                            },
                            () -> log.debug("No hay registro que marcar como FAILED para la clave '{}'", canonical));
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Elimina únicamente los registros con {@code expiresAt < now}, conservando los no
     * expirados, y emite el número de registros eliminados (Req 6.3, 6.4).
     */
    @Override
    public Mono<Long> deleteExpired(Instant now) {
        return Mono.fromCallable(() -> {
                    long deleted = repository.deleteByExpiresAtBefore(now);
                    log.debug("Limpieza por expiración: {} registro(s) eliminado(s) con expiresAt < {}", deleted, now);
                    return deleted;
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * Determina si el error corresponde a una violación de la restricción de unicidad
     * (clave de idempotencia ya existente), traducible a señal de duplicado.
     */
    private boolean isUniqueViolation(Throwable throwable) {
        return throwable instanceof DuplicateKeyException
                || throwable instanceof DataIntegrityViolationException;
    }
}
