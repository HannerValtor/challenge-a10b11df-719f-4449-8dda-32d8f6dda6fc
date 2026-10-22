package com.pragma.payments.infrastructure.adapters.in.web;

import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
import com.pragma.payments.domain.exception.InvalidChannelException;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests del mapeo centralizado error → HTTP (Tarea 8).
 * Valida Requirements 4.4 (canal inválido → 400), 3.2 (duplicado → 409),
 * 5.1/5.5 (timeout y saturación → 503).
 */
@DisplayName("GlobalExceptionHandler - Mapeo de errores a HTTP")
class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    @DisplayName("Canal inválido se mapea a 400 Bad Request")
    void invalidChannelMapsTo400() {
        ResponseEntity<Map<String, Object>> response =
                handler.handleInvalidChannel(new InvalidChannelException("telepatia"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).containsEntry("status", 400);
    }

    @Test
    @DisplayName("Transacción duplicada se mapea a 409 Conflict")
    void duplicateMapsTo409() {
        var ex = new TransactionAlreadyProcessedException(
                "OP-1_web", "web", "tx-1", Instant.now(), "OP-1");

        ResponseEntity<Map<String, Object>> response = handler.handleDuplicate(ex);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    @DisplayName("Timeout de servicio externo se mapea a 503 Service Unavailable")
    void timeoutMapsTo503() {
        var ex = new ExternalServiceTimeoutException(
                "RISK_BUREAU", "/score", Duration.ofSeconds(2), Duration.ofSeconds(2));

        ResponseEntity<Map<String, Object>> response = handler.handleTimeout(ex);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    @DisplayName("Bulkhead lleno (saturación) se mapea a 503 Service Unavailable")
    void bulkheadFullMapsTo503() {
        Bulkhead bulkhead = Bulkhead.of("test",
                BulkheadConfig.custom().maxConcurrentCalls(1).maxWaitDuration(Duration.ZERO).build());

        ResponseEntity<Map<String, Object>> response =
                handler.handleBulkheadFull(BulkheadFullException.createBulkheadFullException(bulkhead));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    @DisplayName("Error genérico se mapea a 500 Internal Server Error")
    void genericMapsTo500() {
        ResponseEntity<Map<String, Object>> response =
                handler.handleGeneric(new RuntimeException("boom"));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
