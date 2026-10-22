package com.pragma.payments.integration;

import com.pragma.payments.application.ports.out.DuplicateReservationException;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.infrastructure.adapters.out.persistence.IdempotencyRecordRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Smoke de esquema: verifica que la tabla {@code idempotency_record} creada por
 * {@code schema.sql} aplica realmente la restricción {@code UNIQUE}/{@code PRIMARY KEY}
 * sobre {@code idempotency_key} en un PostgreSQL real (Req 5.3).
 *
 * <p>Dos inserciones de la MISMA clave de idempotencia deben violar la restricción de
 * unicidad: la primera {@code reserve} tiene éxito y la segunda es rechazada por el motor.
 * El adaptador real {@code IdempotencyStoreAdapter} traduce esa violación
 * ({@code DataIntegrityViolationException}/{@code DuplicateKeyException}) en una
 * {@link DuplicateReservationException}; a nivel de repositorio (INSERT nativo directo)
 * la violación se manifiesta como {@link DataIntegrityViolationException}. Ambas rutas se
 * comprueban aquí, confirmando que la unicidad es efectiva a nivel de almacenamiento
 * persistente y no solo a nivel de aplicación.</p>
 *
 * <p><strong>Requiere Docker.</strong> Vía {@link EnabledIf} sobre {@link #dockerAvailable()}
 * se SALTA automáticamente (SKIPPED) cuando Docker no está disponible, de modo que el build
 * local pasa y en CI con Docker se ejecuta. Se nombra con sufijo {@code Test} (no {@code IT})
 * para que Surefire lo ejecute en {@code mvn test} y reporte el estado SKIPPED sin Docker.</p>
 */
@Testcontainers
@SpringBootTest
@EnabledIf("com.pragma.payments.integration.IdempotencySchemaConstraintTest#dockerAvailable")
class IdempotencySchemaConstraintTest {

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
    private IdempotencyStorePort store;

    @Autowired
    private IdempotencyRecordRepository repository;

    @Test
    void secondReserveWithSameKeyViolatesUniqueConstraint() {
        IdempotencyKey key = new IdempotencyKey("OP-UNIQUE-1", "web");
        Instant createdAt = Instant.now();
        Duration ttl = Duration.ofHours(24);

        // Primera reserva: la clave estaba libre, la fila se inserta.
        StepVerifier.create(store.reserve(IdempotencyRecord.reserve(key, createdAt, ttl)))
                .expectNextCount(1)
                .verifyComplete();

        // Segunda reserva de la MISMA clave: el motor rechaza por violación de PK/UNIQUE,
        // que el adaptador traduce a señal de duplicado (no se propaga como error crudo).
        StepVerifier.create(store.reserve(IdempotencyRecord.reserve(key, createdAt, ttl)))
                .expectError(DuplicateReservationException.class)
                .verify();

        // Confirmación a nivel de esquema: un INSERT nativo directo de la misma clave
        // también es rechazado por la restricción de unicidad de la tabla.
        assertThatThrownBy(() -> repository.insertNew(
                key.toUniqueString(), "IN_PROGRESS", null, createdAt, createdAt.plus(ttl)))
                .isInstanceOf(DataIntegrityViolationException.class);

        // Solo existe una fila para la clave: la unicidad se mantuvo.
        assertThat(repository.findById(key.toUniqueString())).isPresent();
    }
}
