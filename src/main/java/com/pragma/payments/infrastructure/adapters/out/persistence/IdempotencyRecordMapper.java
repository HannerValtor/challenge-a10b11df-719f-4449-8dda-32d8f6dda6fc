package com.pragma.payments.infrastructure.adapters.out.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import org.springframework.stereotype.Component;

/**
 * Mapper entre el modelo de dominio {@link IdempotencyRecord} y su representación de
 * persistencia {@link IdempotencyRecordEntity}.
 *
 * <p>Esta es la única pieza que conoce ambas representaciones, de modo que el dominio
 * permanece libre de dependencias de framework (Req 1.2). El mapeo traduce:</p>
 * <ul>
 *     <li>La {@link IdempotencyKey} a/desde su forma canónica
 *         ({@code operationNumber + "_" + channel}) usada como clave primaria.</li>
 *     <li>El {@link RecordStatus} a/desde su nombre de enum (texto).</li>
 *     <li>El Processing_Result ({@link Transaction}) a/desde JSON mediante
 *         {@link ObjectMapper}. Se maneja {@code null} cuando el estado no es
 *         {@link RecordStatus#COMPLETED} y, por tanto, no hay resultado asociado.</li>
 * </ul>
 *
 * <p>El {@link ObjectMapper} se inyecta por constructor (convención del proyecto: sin
 * {@code @Autowired} de campo). Se reutiliza el bean autoconfigurado por Spring Boot,
 * que ya incluye el módulo {@code jackson-datatype-jsr310} necesario para serializar
 * los {@link java.time.Instant}/{@link java.time.LocalDateTime} de la transacción.</p>
 */
@Component
public class IdempotencyRecordMapper {

    private final ObjectMapper objectMapper;

    public IdempotencyRecordMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Convierte un registro de dominio en su entidad de persistencia.
     *
     * @param record registro de dominio; obligatorio
     * @return la entidad lista para persistir
     * @throws IllegalArgumentException si {@code record} es {@code null}
     * @throws IllegalStateException    si el Processing_Result no puede serializarse
     */
    public IdempotencyRecordEntity toEntity(IdempotencyRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("El registro de dominio no puede ser nulo");
        }
        return new IdempotencyRecordEntity(
                record.getKey().toUniqueString(),
                record.getStatus().name(),
                serializeResult(record.getResult()),
                record.getCreatedAt(),
                record.getExpiresAt()
        );
    }

    /**
     * Reconstruye el registro de dominio a partir de su entidad de persistencia.
     *
     * @param entity entidad persistida; obligatoria
     * @return el registro de dominio equivalente
     * @throws IllegalArgumentException si {@code entity} es {@code null}
     * @throws IllegalStateException    si el Processing_Result no puede deserializarse
     */
    public IdempotencyRecord toDomain(IdempotencyRecordEntity entity) {
        if (entity == null) {
            throw new IllegalArgumentException("La entidad de persistencia no puede ser nula");
        }
        return new IdempotencyRecord(
                toKey(entity.getIdempotencyKey()),
                RecordStatus.valueOf(entity.getStatus()),
                deserializeResult(entity.getResultPayload()),
                entity.getCreatedAt(),
                entity.getExpiresAt()
        );
    }

    /**
     * Reconstruye una {@link IdempotencyKey} a partir de su forma canónica
     * {@code operationNumber + "_" + channel}.
     *
     * <p>El canal no contiene guiones bajos adicionales (patrón {@code ^[A-Za-z0-9_]+$}
     * en su validación), pero el número de operación tampoco los admite
     * ({@code ^[A-Za-z0-9\-]+$}); por tanto el primer {@code '_'} separa ambos campos de
     * forma inequívoca.</p>
     *
     * @param canonical forma canónica de la clave; obligatoria y con el separador {@code '_'}
     * @return la clave de idempotencia reconstruida
     * @throws IllegalArgumentException si la forma canónica es nula o no contiene el separador
     */
    private IdempotencyKey toKey(String canonical) {
        if (canonical == null) {
            throw new IllegalArgumentException("La forma canónica de la clave no puede ser nula");
        }
        int separatorIndex = canonical.indexOf('_');
        if (separatorIndex < 0) {
            throw new IllegalArgumentException(
                    "La forma canónica de la clave no contiene el separador esperado '_': " + canonical);
        }
        String operationNumber = canonical.substring(0, separatorIndex);
        String channel = canonical.substring(separatorIndex + 1);
        return new IdempotencyKey(operationNumber, channel);
    }

    private String serializeResult(Transaction result) {
        if (result == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "No se pudo serializar el Processing_Result de la transacción", e);
        }
    }

    private Transaction deserializeResult(String payload) {
        if (payload == null || payload.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readValue(payload, Transaction.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "No se pudo deserializar el Processing_Result almacenado", e);
        }
    }
}
