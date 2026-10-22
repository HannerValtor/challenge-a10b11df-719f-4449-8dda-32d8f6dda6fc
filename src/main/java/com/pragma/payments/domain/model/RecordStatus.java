package com.pragma.payments.domain.model;

/**
 * Estado del ciclo de vida de un {@link IdempotencyRecord}.
 *
 * <ul>
 *     <li>{@link #IN_PROGRESS}: la clave fue reservada y el procesamiento está en curso.
 *         Una solicitud duplicada en este estado se rechaza (HTTP 409).</li>
 *     <li>{@link #COMPLETED}: el procesamiento terminó con un resultado disponible
 *         (incluye transacciones aprobadas y rechazadas por negocio). Una solicitud
 *         duplicada en este estado retorna el resultado cacheado (HTTP 200).</li>
 *     <li>{@link #FAILED}: el procesamiento terminó por un fallo transitorio; la clave
 *         queda elegible para un reintento posterior.</li>
 * </ul>
 */
public enum RecordStatus {
    IN_PROGRESS,
    COMPLETED,
    FAILED
}
