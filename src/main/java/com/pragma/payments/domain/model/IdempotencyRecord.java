package com.pragma.payments.domain.model;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Entidad de dominio que representa un registro de idempotencia persistido.
 *
 * Encapsula la {@link IdempotencyKey} de una solicitud, su estado de procesamiento
 * ({@link RecordStatus}), el resultado asociado ({@link Transaction}, disponible solo
 * cuando el estado es {@link RecordStatus#COMPLETED}) y las marcas de tiempo de
 * creación y expiración usadas por el proceso de limpieza por TTL.
 *
 * Es un POJO de dominio puro, sin dependencias de framework. Mantiene el invariante
 * {@code expiresAt = createdAt + TTL} y, para un TTL positivo, {@code expiresAt > createdAt}.
 */
public class IdempotencyRecord {

    private final IdempotencyKey key;
    private final RecordStatus status;
    private final Transaction result;
    private final Instant createdAt;
    private final Instant expiresAt;

    public IdempotencyRecord(IdempotencyKey key,
                             RecordStatus status,
                             Transaction result,
                             Instant createdAt,
                             Instant expiresAt) {
        if (key == null) {
            throw new IllegalArgumentException("La clave de idempotencia no puede ser nula");
        }
        if (status == null) {
            throw new IllegalArgumentException("El estado del registro no puede ser nulo");
        }
        if (createdAt == null) {
            throw new IllegalArgumentException("La marca de creación no puede ser nula");
        }
        if (expiresAt == null) {
            throw new IllegalArgumentException("La marca de expiración no puede ser nula");
        }
        this.key = key;
        this.status = status;
        this.result = result;
        this.createdAt = createdAt;
        this.expiresAt = expiresAt;
    }

    /**
     * Crea un nuevo registro reservado en estado {@link RecordStatus#IN_PROGRESS},
     * calculando la marca de expiración como {@code createdAt + ttl}.
     *
     * Garantiza el invariante {@code expiresAt > createdAt} exigiendo un TTL positivo.
     *
     * @param key       clave de idempotencia; obligatoria
     * @param createdAt instante de creación; obligatorio
     * @param ttl       periodo de vida; debe ser positivo (mayor que cero)
     * @return un registro en estado {@code IN_PROGRESS} sin resultado asociado
     */
    public static IdempotencyRecord reserve(IdempotencyKey key, Instant createdAt, Duration ttl) {
        if (createdAt == null) {
            throw new IllegalArgumentException("La marca de creación no puede ser nula");
        }
        if (ttl == null) {
            throw new IllegalArgumentException("El TTL no puede ser nulo");
        }
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("El TTL debe ser positivo para garantizar expiresAt > createdAt");
        }
        Instant expiresAt = createdAt.plus(ttl);
        return new IdempotencyRecord(key, RecordStatus.IN_PROGRESS, null, createdAt, expiresAt);
    }

    /**
     * Devuelve una copia de este registro en estado {@link RecordStatus#COMPLETED}
     * asociándole el resultado de procesamiento, conservando las marcas de tiempo.
     *
     * @param result resultado del procesamiento a cachear; obligatorio
     * @return un nuevo registro {@code COMPLETED} con el resultado asociado
     */
    public IdempotencyRecord withResult(Transaction result) {
        if (result == null) {
            throw new IllegalArgumentException("El resultado no puede ser nulo para un registro COMPLETED");
        }
        return new IdempotencyRecord(this.key, RecordStatus.COMPLETED, result, this.createdAt, this.expiresAt);
    }

    /**
     * Devuelve una copia de este registro en estado {@link RecordStatus#FAILED},
     * conservando el resto de atributos.
     *
     * @return un nuevo registro {@code FAILED}
     */
    public IdempotencyRecord asFailed() {
        return new IdempotencyRecord(this.key, RecordStatus.FAILED, this.result, this.createdAt, this.expiresAt);
    }

    public IdempotencyKey getKey() {
        return key;
    }

    public RecordStatus getStatus() {
        return status;
    }

    public Transaction getResult() {
        return result;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    /**
     * Indica si el registro está expirado respecto al instante proporcionado.
     *
     * @param now instante de referencia
     * @return {@code true} si {@code expiresAt < now}
     */
    public boolean isExpired(Instant now) {
        return now != null && expiresAt.isBefore(now);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdempotencyRecord that = (IdempotencyRecord) o;
        return Objects.equals(key, that.key) &&
               status == that.status &&
               Objects.equals(result, that.result) &&
               Objects.equals(createdAt, that.createdAt) &&
               Objects.equals(expiresAt, that.expiresAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, status, result, createdAt, expiresAt);
    }

    @Override
    public String toString() {
        return "IdempotencyRecord{" +
                "key=" + key +
                ", status=" + status +
                ", result=" + result +
                ", createdAt=" + createdAt +
                ", expiresAt=" + expiresAt +
                '}';
    }
}
