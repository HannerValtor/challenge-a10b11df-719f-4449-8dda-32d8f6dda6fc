package com.pragma.payments.integration;

import com.pragma.payments.application.orchestrator.IdempotencyOrchestrator;
import com.pragma.payments.application.ports.out.CoreBankingPort;
import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Test de RESILIENCIA ante fallos de servicios externos con el almacén de idempotencia real sobre
 * PostgreSQL (Testcontainers) (Fase 3, Tarea 10.5, Requirement 8.5).
 *
 * <p>Ejercita el {@link IdempotencyOrchestrator} real con la cadena de procesamiento real y el
 * {@link IdempotencyStorePort} real (JPA/Hikari sobre PostgreSQL), sustituyendo únicamente los
 * puertos externos por {@code @MockitoBean}. Simula un timeout del buró de riesgo superior a 2s
 * ({@link ExternalServiceTimeoutException}) y verifica que:</p>
 * <ul>
 *   <li>el fallo transitorio se propaga (la capa web lo mapearía a HTTP 503);</li>
 *   <li>el registro de idempotencia queda en estado {@code FAILED} (persistido en PostgreSQL),
 *       dejando la clave elegible a reintento; y</li>
 *   <li>un reintento posterior, con el servicio externo ya recuperado, tiene éxito y completa el
 *       registro en estado {@code COMPLETED} (Requirement 8.5).</li>
 * </ul>
 *
 * <p><strong>Requiere Docker.</strong> Se SALTA (SKIPPED, no FAILED) cuando Docker no está
 * disponible, mediante {@link EnabledIf} sobre {@link #dockerAvailable()}.</p>
 */
@Testcontainers
@SpringBootTest
@EnabledIf("com.pragma.payments.integration.ExternalFailureResilienceTest#dockerAvailable")
class ExternalFailureResilienceTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("payments_db")
            .withUsername("payments_user")
            .withPassword("payments_pass");

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", POSTGRES::getDriverClassName);
    }

    static boolean dockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Autowired
    private IdempotencyOrchestrator orchestrator;

    @Autowired
    private IdempotencyStorePort store;

    @MockitoBean
    private FraudDetectionPort fraudDetectionPort;

    @MockitoBean
    private RiskBureauPort riskBureauPort;

    @MockitoBean
    private CoreBankingPort coreBankingPort;

    private Transaction newTransaction(IdempotencyKey key) {
        return Transaction.builder()
                .idempotencyKey(key)
                .accountId("ACC-RES-1")
                .amount(new BigDecimal("200.00"))
                .currency("USD")
                .channel("web")
                .description("pago resiliencia")
                .status(TransactionStatus.PENDING)
                .build();
    }

    @Test
    void riskBureauTimeoutLeavesKeyFailedAndRetrySucceeds() {
        IdempotencyKey key = new IdempotencyKey("OP-RES-1", "web");

        // Fraude OK en ambos intentos (no fraudulento).
        when(fraudDetectionPort.evaluateTransaction(any())).thenReturn(Mono.just(false));

        // 1er intento: el buró de riesgo excede el timeout (> 2s) -> fallo transitorio.
        when(riskBureauPort.getRiskScore(any()))
                .thenReturn(Mono.error(new ExternalServiceTimeoutException(
                        "RISK_BUREAU", "/score", Duration.ofMillis(2500), Duration.ofMillis(3000))));

        StepVerifier.create(orchestrator.resolve(key, newTransaction(key)))
                .expectError(ExternalServiceTimeoutException.class)
                .verify();

        // El registro quedó FAILED en PostgreSQL, elegible a reintento (Requirement 8.5).
        StepVerifier.create(store.find(key))
                .assertNext(record -> assertThat(record.getStatus()).isEqualTo(RecordStatus.FAILED))
                .verifyComplete();

        // 2do intento: el buró de riesgo ya responde; el core completa la transacción.
        when(riskBureauPort.getRiskScore(any())).thenReturn(Mono.just(10));
        when(coreBankingPort.sendTransaction(any())).thenAnswer(inv -> {
            Transaction tx = inv.getArgument(0);
            tx.markAsCompleted("CORE-REF-RETRY");
            return Mono.just(tx);
        });

        StepVerifier.create(orchestrator.resolve(key, newTransaction(key)))
                .assertNext(result -> assertThat(result.getStatus()).isEqualTo(TransactionStatus.COMPLETED))
                .verifyComplete();

        // El registro final quedó COMPLETED tras el reintento exitoso.
        StepVerifier.create(store.find(key))
                .assertNext(record -> {
                    assertThat(record.getStatus()).isEqualTo(RecordStatus.COMPLETED);
                    assertThat(record.getResult()).isNotNull();
                })
                .verifyComplete();
    }
}
