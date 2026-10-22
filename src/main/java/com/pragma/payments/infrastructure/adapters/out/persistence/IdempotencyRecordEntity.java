package com.pragma.payments.infrastructure.adapters.out.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.Objects;

/**
 * Entidad de persistencia (JPA) que representa una fila de la tabla
 * {@code idempotency_record} en el almacenamiento relacional.
 *
 * <p>Es la representación de infraestructura del modelo de dominio
 * {@code com.pragma.payments.domain.model.IdempotencyRecord}. El mapeo entre
 * ambas representaciones se realiza en {@code IdempotencyRecordMapper} (tarea 3.2),
 * de modo que el dominio permanece libre de dependencias de framework.</p>
 *
 * <p>La columna {@code idempotency_key} es la clave primaria y, por tanto,
 * aporta la restricción de unicidad que fundamenta la reserva atómica bajo
 * condiciones de carrera (Req 4.1, 4.3, 5.3). Sobre {@code expires_at} se define
 * un índice para que el barrido de limpieza por TTL sea eficiente (Req 6.3).</p>
 *
 * <p>Nota sobre la estrategia reactiva: el {@code pom.xml} del servicio utiliza
 * Spring Data JPA (bloqueante). Conforme a la decisión D1 del diseño, todo acceso
 * a este repositorio se aislará en {@code Schedulers.boundedElastic()} dentro del
 * adaptador de salida, preservando el contrato reactivo del puerto sin bloquear el
 * Event Loop (Req 8.1, 8.2).</p>
 */
@Entity
@Table(
        name = "idempotency_record",
        indexes = {
                @Index(name = "idx_idempotency_record_expires_at", columnList = "expires_at")
        }
)
public class IdempotencyRecordEntity {

    /**
     * Forma canónica de la clave de idempotencia ({@code operationNumber + "_" + channel}).
     * Clave primaria: garantiza la unicidad a nivel de almacenamiento persistente (Req 5.3).
     */
    @Id
    @Column(name = "idempotency_key", nullable = false, unique = true, length = 80)
    private String idempotencyKey;

    /**
     * Estado del registro ({@code IN_PROGRESS}, {@code COMPLETED}, {@code FAILED}).
     * Se persiste como texto para desacoplar el esquema del orden del enum de dominio.
     */
    @Column(name = "status", nullable = false, length = 20)
    private String status;

    /**
     * Processing_Result serializado (la {@code Transaction} resultante). Nullable:
     * solo está presente cuando el registro alcanza el estado {@code COMPLETED}.
     */
    @Column(name = "result_payload", columnDefinition = "TEXT")
    private String resultPayload;

    /** Marca de tiempo de creación del registro (Req 6.1). */
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    /** Marca de tiempo de expiración ({@code createdAt + TTL}); indexada (Req 6.1, 6.3). */
    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    /** Constructor sin argumentos requerido por JPA. */
    public IdempotencyRecordEntity() {
    }

    public IdempotencyRecordEntity(String idempotencyKey,
                                   String status,
                                   String resultPayload,
                                   Instant createdAt,
                                   Instant expiresAt) {
        this.idempotencyKey = idempotencyKey;
        this.status = status;
        this.resultPayload = resultPayload;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getResultPayload() {
        return resultPayload;
    }

    public void setResultPayload(String resultPayload) {
        this.resultPayload = resultPayload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdempotencyRecordEntity that = (IdempotencyRecordEntity) o;
        return Objects.equals(idempotencyKey, that.idempotencyKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idempotencyKey);
    }

    @Override
    public String toString() {
        return "IdempotencyRecordEntity{" +
                "idempotencyKey='" + idempotencyKey + '\'' +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                ", expiresAt=" + expiresAt +
                '}';
    }
}
