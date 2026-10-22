package com.pragma.payments.infrastructure.adapters.out.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Repositorio Spring Data (JPA, bloqueante) para la entidad
 * {@link IdempotencyRecordEntity}.
 *
 * <p>El {@code pom.xml} del servicio incluye {@code spring-boot-starter-data-jpa}
 * (acceso bloqueante) y el driver JDBC de PostgreSQL; no hay starter R2DBC. Por
 * tanto, conforme a la decisión D1 del diseño, este repositorio es la opción de
 * respaldo basada en JPA: expone operaciones bloqueantes que el adaptador de
 * salida {@code IdempotencyStoreAdapter} (tarea 4.1) envolverá en
 * {@code Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())} para
 * preservar el contrato reactivo del puerto sin bloquear el Event Loop
 * (Req 8.1, 8.2).</p>
 *
 * <p>La clave primaria es la forma canónica de la clave de idempotencia
 * ({@code operationNumber + "_" + channel}), de tipo {@link String}, cuya
 * restricción de unicidad fundamenta la reserva atómica bajo condiciones de
 * carrera (Req 1.1, 1.3, 5.3). Las operaciones CRUD heredadas de
 * {@link JpaRepository} cubren consulta, inserción condicional y actualización.</p>
 */
@Repository
public interface IdempotencyRecordRepository extends JpaRepository<IdempotencyRecordEntity, String> {

    /**
     * Elimina del almacenamiento únicamente los registros cuya marca de tiempo de
     * expiración es anterior al instante indicado, conservando los no expirados
     * (Req 6.3, 6.4). Da soporte a la operación {@code deleteExpired} del puerto.
     *
     * @param now instante de referencia; se eliminan los registros con
     *            {@code expiresAt < now}
     * @return número de registros eliminados
     */
    @Transactional
    @Modifying
    @Query("DELETE FROM IdempotencyRecordEntity r WHERE r.expiresAt < :now")
    long deleteByExpiresAtBefore(@Param("now") Instant now);

    /**
     * Inserción condicional atómica del registro de idempotencia mediante una
     * sentencia {@code INSERT} nativa.
     *
     * <p>A diferencia de {@link JpaRepository#save(Object)} —que, al tener la clave
     * primaria asignada manualmente, aplica semántica de {@code merge} (SELECT +
     * UPDATE/INSERT) y no fallaría ante un duplicado— esta sentencia es un
     * {@code INSERT} puro. Si la clave ya existe, el motor rechaza la operación por
     * violación de la restricción de unicidad sobre {@code idempotency_key}, lo que
     * el adaptador traduce a una señal de duplicado. Esto fundamenta la reserva
     * atómica sin ventana TOCTOU bajo condiciones de carrera (Req 4.1, 4.3, 5.1, 5.3).</p>
     *
     * @return número de filas insertadas (siempre 1 cuando la clave estaba libre)
     */
    @Transactional
    @Modifying
    @Query(value = "INSERT INTO idempotency_record " +
            "(idempotency_key, status, result_payload, created_at, expires_at) " +
            "VALUES (:idempotencyKey, :status, :resultPayload, :createdAt, :expiresAt)",
            nativeQuery = true)
    int insertNew(@Param("idempotencyKey") String idempotencyKey,
                  @Param("status") String status,
                  @Param("resultPayload") String resultPayload,
                  @Param("createdAt") Instant createdAt,
                  @Param("expiresAt") Instant expiresAt);
}
