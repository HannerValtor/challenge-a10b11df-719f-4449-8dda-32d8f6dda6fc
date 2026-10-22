package com.pragma.payments.integration;

import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.persistence.IdempotencyRecordRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test de integración de DURABILIDAD del almacenamiento de idempotencia contra un
 * PostgreSQL real levantado con Testcontainers (Req 1.1, 1.5).
 *
 * <p>Ejercita el adaptador de salida real {@code IdempotencyStoreAdapter} (sobre el
 * repositorio JPA {@code IdempotencyRecordRepository} envuelto en
 * {@code Schedulers.boundedElastic()}), el {@code IdempotencyRecordMapper} y el esquema
 * {@code schema.sql}. Persiste un {@code IdempotencyRecord} (reserve + complete) y verifica
 * que SOBREVIVE tras limpiar el contexto de persistencia (detach + clear del
 * {@link EntityManager}), forzando una relectura real desde la base de datos en lugar del
 * caché de primer nivel de Hibernate. Esto demuestra durabilidad efectiva de la fila
 * (Req 1.1) y su conservación frente a un "reinicio" del contexto de persistencia (Req 1.5).</p>
 *
 * <p><strong>Requiere Docker.</strong> Mediante {@link EnabledIf} sobre
 * {@link #dockerAvailable()} ({@code DockerClientFactory.instance().isDockerAvailable()}),
 * el test se SALTA automáticamente (estado SKIPPED, no FAILED) cuando Docker no está
 * presente. Así el build local pasa (test SKIPPED) y en CI con Docker el test se ejecuta.
 * Se nombra con sufijo {@code Test} (no {@code IT}) para que Surefire lo ejecute en
 * {@code mvn test} y reporte el estado SKIPPED cuando no hay Docker.</p>
 *
 * <p>Se ubica en {@code com.pragma.payments.integration} (no bajo {@code adapters/out}) porque
 * el {@code maven-compiler-plugin} excluye de la compilación de test los ficheros
 * {@code **}/adapters/out/**}/*Test.java}; este paquete no está excluido.</p>
 */
@Testcontainers
@SpringBootTest
@EnabledIf("com.pragma.payments.integration.IdempotencyDurabilityTest#dockerAvailable")
class IdempotencyDurabilityTest {

    /** Postgres real; la restricción de unicidad y la durabilidad son del motor, no simuladas. */
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("payments_db")
            .withUsername("payments_user")
            .withPassword("payments_pass");

    /** Sobrescribe el datasource JDBC de la app con el del contenedor efímero. */
    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", POSTGRES::getDriverClassName);
    }

    /** Condición de habilitación: solo se ejecuta si hay un daemon Docker accesible. */
    static boolean dockerAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Autowired
    private IdempotencyStorePort store;

    @Autowired
    private IdempotencyRecordRepository repository;

    @PersistenceContext
    private EntityManager entityManager;

    @Test
    @Transactional
    void recordSurvivesPersistenceContextReset() {
        IdempotencyKey key = new IdempotencyKey("OP-DUR-1", "web");
        Instant createdAt = Instant.now();
        Transaction result = Transaction.builder()
                .idempotencyKey(key)
                .accountId("ACC-1")
                .amount(new BigDecimal("150.00"))
                .currency("USD")
                .channel("web")
                .description("pago durabilidad")
                .status(Transaction.TransactionStatus.COMPLETED)
                .createdAt(createdAt)
                .build();
        result.setExternalReference("EXT-REF-DUR-1");

        // 1) Reservar (IN_PROGRESS) y completar (COMPLETED + resultado) vía el adaptador real.
        StepVerifier.create(store.reserve(IdempotencyRecord.reserve(key, createdAt, Duration.ofHours(24))))
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(store.complete(key, result))
                .assertNext(record -> {
                    assertThat(record.getStatus()).isEqualTo(RecordStatus.COMPLETED);
                    assertThat(record.getResult()).isNotNull();
                })
                .verifyComplete();

        // 2) "Reinicio" del contexto de persistencia: se descartan todas las entidades
        //    gestionadas para forzar una relectura real desde PostgreSQL (no desde el caché
        //    de primer nivel de Hibernate).
        entityManager.flush();
        entityManager.clear();

        // 3) La fila persiste en el motor y se relee con su estado y resultado intactos.
        assertThat(repository.findById(key.toUniqueString())).isPresent();

        StepVerifier.create(store.find(key))
                .assertNext(reread -> {
                    assertThat(reread.getStatus())
                            .as("el registro sobrevive al reinicio del contexto de persistencia")
                            .isEqualTo(RecordStatus.COMPLETED);
                    assertThat(reread.getResult()).isNotNull();
                    assertThat(reread.getResult().getExternalReference()).isEqualTo("EXT-REF-DUR-1");
                    assertThat(reread.getKey().toUniqueString()).isEqualTo(key.toUniqueString());
                })
                .verifyComplete();
    }
}
