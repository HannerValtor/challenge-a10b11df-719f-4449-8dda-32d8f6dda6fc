package com.pragma.payments.application.ports.out;

/**
 * Señal de "clave ya existente" emitida por {@link IdempotencyStorePort#reserve}
 * cuando una inserción condicional pierde la carrera frente a la restricción de
 * unicidad del almacenamiento persistente.
 *
 * <p>Conforme a la estrategia de manejo de errores del diseño, esta señal
 * <strong>no se propaga al cliente</strong>: traduce internamente la violación de
 * unicidad ({@code DataIntegrityViolationException} / {@code DuplicateKeyException})
 * para que el llamador ({@code PaymentUseCase}) relea el registro existente y aplique
 * la decisión de solicitud duplicada (Duplicate_Request): un registro
 * {@code COMPLETED} retorna el resultado cacheado (HTTP 200) y uno {@code IN_PROGRESS}
 * se rechaza (HTTP 409). Evita así falsos 500 bajo condiciones de carrera
 * (Req 4.3, 5.2).</p>
 *
 * <p>Vive en la capa de aplicación (junto al puerto) porque es parte del contrato
 * semántico de {@code reserve}: cualquier adaptador que implemente el puerto debe
 * señalar los duplicados con esta excepción, con independencia del motor concreto.</p>
 */
public class DuplicateReservationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String idempotencyKey;

    public DuplicateReservationException(String idempotencyKey, Throwable cause) {
        super("La clave de idempotencia '" + idempotencyKey + "' ya existe (reserva perdida por carrera)", cause);
        this.idempotencyKey = idempotencyKey;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }
}
