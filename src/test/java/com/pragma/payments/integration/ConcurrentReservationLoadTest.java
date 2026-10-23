package com.pragma.payments.integration;

import com.pragma.payments.application.ports.out.DuplicateReservationException;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.infrastructure.adapters.out.persistence.IdempotencyRecordRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test de CARGA/CONCURRENCIA del almacenamiento de idempotencia contra un PostgreSQL real
 * levantado con Testcontainers (Fase 3, Tarea 10.4, Requirements 8.4, 7.1).
 *
 * <p>Lanza al menos 16 reservas concurrentes sobre la MISMA {@link IdempotencyKey} a través del
 * {@link IdempotencyStorePort} real (adaptador JPA/Hikari sobre {@code boundedElastic}) y verifica
 * que:</p>
 * <ul>
 *   <li>exactamente UNA reserva gana (la restricción UNIQUE real del motor garantiza la
 *       atomicidad), y las N-1 restantes se traducen a {@link DuplicateReservationException}
 *       (sin escrituras duplicadas, Requirement 7.1);</li>
 *   <li>ninguna solicitud queda bloqueada de forma indefinida: todas terminan dentro de un tiempo
 *       acotado (ausencia de interbloqueos y de agotamiento del pool de conexiones,
 *       Requirement 8.4); y</li>
 *   <li>tras la carrera existe exactamente una fila para la clave, en estado {@code IN_PROGRESS}.</li>
 * </ul>
 *
 * <p><strong>Requiere Docker.</strong> Mediante {@link EnabledIf} sobre {@link #dockerAvailable()}
 * el test se SALTA (estado SKIPPED, no FAILED) cuando Docker no está disponible, de modo que el
 * build local pasa y en CI con Docker se ejecuta. Se nombra con sufijo {@code Test} para que
 * Surefire lo ejecute en {@code mvn test} y ubicado en {@code integration} (no excluido por el
 * compilador de test).</p>
 */
@Testcontainers
@SpringBootTest
@EnabledIf("com.pragma.payments.integration.ConcurrentReservationLoadTest#dockerAvailable")
class ConcurrentReservationLoadTest {

    /** Número de solicitudes concurrentes; el requisito exige al menos 16. */
    private static final int CONCURRENCY = 24;

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
    void concurrentReservationsOnSameKeyProduceExactlyOneWinnerWithoutPoolExhaustion() throws InterruptedException {
        IdempotencyKey key = new IdempotencyKey("OP-LOAD-1", "web");
        Instant now = Instant.now();
        Duration ttl = Duration.ofHours(24);

        AtomicInteger winners = new AtomicInteger(0);
        AtomicInteger duplicates = new AtomicInteger(0);
        ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();

        ExecutorService pool = Executors.newFixedThreadPool(CONCURRENCY);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(CONCURRENCY);

        try {
            for (int i = 0; i < CONCURRENCY; i++) {
                pool.submit(() -> {
                    try {
                        startGate.await();
                        IdempotencyRecord reserved =
                                store.reserve(IdempotencyRecord.reserve(key, now, ttl)).block();
                        if (reserved != null && reserved.getStatus() == RecordStatus.IN_PROGRESS) {
                            winners.incrementAndGet();
                        } else {
                            unexpected.add(new AssertionError("reserva sin IN_PROGRESS: " + reserved));
                        }
                    } catch (DuplicateReservationException dup) {
                        duplicates.incrementAndGet();
                    } catch (Throwable t) {
                        unexpected.add(t);
                    } finally {
                        done.countDown();
                    }
                });
            }

            startGate.countDown();
            // Ausencia de bloqueo indefinido / agotamiento del pool: todas terminan a tiempo.
            boolean finished = done.await(60, TimeUnit.SECONDS);
            assertThat(finished)
                    .as("todas las reservas concurrentes terminan a tiempo (sin interbloqueo ni agotamiento de pool)")
                    .isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(unexpected).as("sin errores inesperados en la carrera").isEmpty();
        assertThat(winners.get()).as("exactamente una reserva gana la carrera").isEqualTo(1);
        assertThat(duplicates.get())
                .as("las N-1 restantes se tratan como duplicados, sin escrituras duplicadas")
                .isEqualTo(CONCURRENCY - 1);

        // Exactamente una fila en el motor para la clave (unicidad real de PostgreSQL).
        assertThat(repository.findById(key.toUniqueString())).isPresent();
        IdempotencyRecord stored = store.find(key).block();
        assertThat(stored).isNotNull();
        assertThat(stored.getStatus()).isEqualTo(RecordStatus.IN_PROGRESS);
    }
}
