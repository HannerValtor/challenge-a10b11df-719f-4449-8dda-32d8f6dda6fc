package com.pragma.payments.infrastructure.adapters.in.web;

import com.pragma.payments.application.ports.in.PaymentServicePort;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;

/**
 * Tests de ejemplo de los mapeos web puntuales (Tarea 10.2) usando WebFlux {@link WebTestClient}
 * contra {@link PaymentController} + {@link GlobalExceptionHandler}.
 *
 * <p>Verifica los dos mapeos HTTP exigidos por el diseño:</p>
 * <ul>
 *   <li>Duplicado {@code COMPLETED} ⇒ el use case retorna el Processing_Result cacheado y el
 *       controller responde <b>HTTP 200</b> (Req 3.3).</li>
 *   <li>Duplicado {@code IN_PROGRESS} ⇒ el use case señala
 *       {@link TransactionAlreadyProcessedException}, que el {@link GlobalExceptionHandler}
 *       mapea a <b>HTTP 409</b> (Req 7.2).</li>
 * </ul>
 *
 * <p>Se usa el slice {@code @WebFluxTest(PaymentController.class)} con el {@link PaymentServicePort}
 * mockeado. Se importa explícitamente el {@link GlobalExceptionHandler} para garantizar que el
 * mapeo de errores a HTTP está presente en el slice. Se excluyen las autoconfiguraciones de
 * JPA/DataSource (el starter JPA está en el classpath y {@code application.yml} define un
 * datasource PostgreSQL real) para que el slice web no intente abrir una conexión a BD.</p>
 *
 * <p>Validates: Requirements 3.3, 7.2</p>
 */
@WebFluxTest(
        controllers = PaymentController.class,
        excludeAutoConfiguration = {
                DataSourceAutoConfiguration.class,
                DataSourceTransactionManagerAutoConfiguration.class,
                HibernateJpaAutoConfiguration.class
        })
@Import({GlobalExceptionHandler.class, PaymentControllerWebMappingTest.NoOpInitConfig.class})
@TestPropertySource(properties = {
        // Evita que el slice intente ejecutar schema.sql / inicializar el datasource.
        "spring.sql.init.mode=never"
})
@DisplayName("PaymentController - Mapeos web de duplicados (WebTestClient)")
class PaymentControllerWebMappingTest {

    @Autowired
    private WebTestClient webTestClient;

    @MockBean
    private PaymentServicePort paymentServicePort;

    /**
     * Configuración vacía auxiliar: existe solo para dar un punto de anclaje si fuera
     * necesario registrar beans adicionales en el slice sin arrastrar el contexto completo.
     */
    @TestConfiguration
    static class NoOpInitConfig {
    }

    private PaymentController.PaymentRequest validRequest() {
        return new PaymentController.PaymentRequest(
                "OP-1001",
                "web",
                "ACC-123",
                new BigDecimal("150.00"),
                "USD",
                "Pago de prueba");
    }

    @Test
    @DisplayName("Duplicado COMPLETED retorna el resultado cacheado con HTTP 200 (Req 3.3)")
    void duplicateCompletedReturns200() {
        // Validates: Requirements 3.3
        IdempotencyKey key = new IdempotencyKey("OP-1001", "web");
        Transaction completed = Transaction.builder()
                .id("tx-original-1")
                .idempotencyKey(key)
                .accountId("ACC-123")
                .amount(new BigDecimal("150.00"))
                .currency("USD")
                .channel("web")
                .description("Pago de prueba")
                .status(TransactionStatus.COMPLETED)
                .createdAt(Instant.now())
                .build();
        completed.setProcessedAt(Instant.now());
        completed.setExternalReference("EXT-REF-1");

        // El use case resuelve el duplicado COMPLETED retornando el Processing_Result cacheado.
        Mockito.when(paymentServicePort.processPayment(any()))
                .thenReturn(Mono.just(completed));

        webTestClient.post()
                .uri("/api/v1/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(validRequest())
                .exchange()
                .expectStatus().isEqualTo(200)
                .expectBody()
                .jsonPath("$.transactionId").isEqualTo("tx-original-1")
                .jsonPath("$.status").isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("Duplicado IN_PROGRESS se rechaza con HTTP 409 (Req 7.2)")
    void duplicateInProgressReturns409() {
        // Validates: Requirements 7.2
        // Un duplicado con record IN_PROGRESS se señala con TransactionAlreadyProcessedException,
        // que el GlobalExceptionHandler mapea a 409 Conflict.
        TransactionAlreadyProcessedException inProgressDuplicate =
                new TransactionAlreadyProcessedException(
                        "OP-1001_web", "web", "tx-original-1", Instant.now(), "OP-1001");

        Mockito.when(paymentServicePort.processPayment(any()))
                .thenReturn(Mono.error(inProgressDuplicate));

        webTestClient.post()
                .uri("/api/v1/payments")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(validRequest())
                .exchange()
                .expectStatus().isEqualTo(409)
                .expectBody()
                .jsonPath("$.status").isEqualTo(409);
    }
}
