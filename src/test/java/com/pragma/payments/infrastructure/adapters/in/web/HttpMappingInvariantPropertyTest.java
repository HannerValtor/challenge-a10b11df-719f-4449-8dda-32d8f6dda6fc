package com.pragma.payments.infrastructure.adapters.in.web;

import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
import com.pragma.payments.domain.exception.InvalidChannelException;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 4: Mapeo HTTP invariante.
 *
 * <p>Validates: Requirements 7.5, 7.6, 7.7, 7.8</p>
 *
 * <p>Para toda instancia (con datos aleatorios) de cada familia de error, el
 * {@link GlobalExceptionHandler} produce SIEMPRE el mismo código HTTP, sin cambios tras el refactor
 * de Fase 3:</p>
 * <ul>
 *   <li>canal inválido ⇒ 400 (Requirement 7.5);</li>
 *   <li>duplicado {@code IN_PROGRESS} ⇒ 409 (Requirement 7.6);</li>
 *   <li>timeout de servicio externo o bulkhead lleno ⇒ 503 (Requirement 7.7);</li>
 *   <li>cualquier otro error técnico no clasificado ⇒ 500 (Requirement 7.8).</li>
 * </ul>
 *
 * <p>Se ejercita el handler directamente (sin servidor) para aislar el invariante de mapeo de la
 * capa de transporte; los tests de slice web ({@code PaymentControllerWebMappingTest}) cubren la
 * integración HTTP de extremo a extremo para los casos 200/409.</p>
 */
class HttpMappingInvariantPropertyTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Property(tries = 100)
    void invalidChannelAlwaysMapsTo400(@ForAll("channels") String channel) {
        ResponseEntity<Map<String, Object>> response =
                handler.handleInvalidChannel(new InvalidChannelException(channel));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).containsEntry("status", 400);
    }

    @Property(tries = 100)
    void inProgressDuplicateAlwaysMapsTo409(
            @ForAll("identifiers") String operationNumber,
            @ForAll("channels") String channel) {
        var ex = new TransactionAlreadyProcessedException(
                operationNumber + "_" + channel, channel, "tx-" + operationNumber, Instant.now(), operationNumber);
        ResponseEntity<Map<String, Object>> response = handler.handleDuplicate(ex);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Property(tries = 100)
    void externalTimeoutAlwaysMapsTo503(
            @ForAll("services") String service,
            @ForAll @IntRange(min = 1, max = 10000) int configuredMs,
            @ForAll @IntRange(min = 0, max = 60000) int actualMs) {
        var ex = new ExternalServiceTimeoutException(
                service, "/endpoint", Duration.ofMillis(configuredMs), Duration.ofMillis(actualMs));
        ResponseEntity<Map<String, Object>> response = handler.handleTimeout(ex);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Property(tries = 100)
    void bulkheadFullAlwaysMapsTo503(@ForAll @IntRange(min = 1, max = 100) int maxConcurrent) {
        Bulkhead bulkhead = Bulkhead.of("bh-" + maxConcurrent,
                BulkheadConfig.custom().maxConcurrentCalls(maxConcurrent).maxWaitDuration(Duration.ZERO).build());
        ResponseEntity<Map<String, Object>> response =
                handler.handleBulkheadFull(BulkheadFullException.createBulkheadFullException(bulkhead));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Property(tries = 100)
    void unclassifiedErrorAlwaysMapsTo500(@ForAll("messages") String rawInternalMessage) {
        ResponseEntity<Map<String, Object>> response =
                handler.handleGeneric(new RuntimeException(rawInternalMessage));
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        // No se exponen detalles internos del error (Requirement 7.8): el cuerpo devuelve un
        // mensaje genérico fijo, nunca el mensaje crudo de la excepción interna.
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().get("message")).isEqualTo("Error interno del servidor");
        assertThat(response.getBody()).containsEntry("status", 500);
    }

    @Provide
    Arbitrary<String> channels() {
        // Canales arbitrarios (incluye no soportados): el mapeo a 400 es invariante en todos.
        return Arbitraries.strings().withCharRange('a', 'z').ofMinLength(1).ofMaxLength(15);
    }

    @Provide
    Arbitrary<String> identifiers() {
        return Arbitraries.strings()
                .withCharRange('A', 'Z').withCharRange('a', 'z').withCharRange('0', '9')
                .ofMinLength(1).ofMaxLength(20);
    }

    @Provide
    Arbitrary<String> services() {
        return Arbitraries.of("FRAUD", "RISK_BUREAU", "CORE_BANKING");
    }

    @Provide
    Arbitrary<String> messages() {
        return Arbitraries.strings().ofMinLength(1).ofMaxLength(40);
    }
}
