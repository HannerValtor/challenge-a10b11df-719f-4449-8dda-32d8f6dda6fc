package com.pragma.payments.infrastructure.adapters.in.web;

import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
import com.pragma.payments.domain.exception.InvalidChannelException;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;
import java.util.Map;

/**
 * Manejo centralizado de errores → HTTP para la capa web reactiva (Requirements 2.3, 4.4, 5.1, 5.2, 5.5).
 *
 * <p>Todos los errores de dominio y de resiliencia se traducen aquí a un código HTTP semántico,
 * sin bloquear el event-loop: los handlers devuelven {@link ResponseEntity} de forma no bloqueante
 * (Spring WebFlux adapta el valor de retorno a un {@code Mono}).</p>
 *
 * <ul>
 *   <li>{@link InvalidChannelException} → 400 Bad Request</li>
 *   <li>{@link TransactionAlreadyProcessedException} → 409 Conflict</li>
 *   <li>{@link ExternalServiceTimeoutException} → 503 Service Unavailable</li>
 *   <li>{@link BulkheadFullException} (saturación de concurrencia) → 503 Service Unavailable</li>
 *   <li>cualquier otra excepción → 500 Internal Server Error</li>
 * </ul>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(InvalidChannelException.class)
    public ResponseEntity<Map<String, Object>> handleInvalidChannel(InvalidChannelException ex) {
        log.warn("Canal inválido: {}", ex.getChannel());
        return build(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(TransactionAlreadyProcessedException.class)
    public ResponseEntity<Map<String, Object>> handleDuplicate(TransactionAlreadyProcessedException ex) {
        log.warn("Transacción duplicada: {}", ex.getMessage());
        return build(HttpStatus.CONFLICT, "La transacción ya fue procesada");
    }

    @ExceptionHandler(ExternalServiceTimeoutException.class)
    public ResponseEntity<Map<String, Object>> handleTimeout(ExternalServiceTimeoutException ex) {
        log.error("Timeout de servicio externo [{}]: {}", ex.getServiceName(), ex.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "Servicio temporalmente no disponible");
    }

    @ExceptionHandler(BulkheadFullException.class)
    public ResponseEntity<Map<String, Object>> handleBulkheadFull(BulkheadFullException ex) {
        log.warn("Saturación de concurrencia (bulkhead lleno): {}", ex.getMessage());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "Servicio saturado, intente más tarde");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneric(Exception ex) {
        log.error("Error inesperado procesando la solicitud: {}", ex.getMessage(), ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "Error interno del servidor");
    }

    private ResponseEntity<Map<String, Object>> build(HttpStatus status, String message) {
        Map<String, Object> body = Map.of(
                "status", status.value(),
                "error", status.getReasonPhrase(),
                "message", message,
                "timestamp", Instant.now().toString());
        return ResponseEntity.status(status).body(body);
    }
}
