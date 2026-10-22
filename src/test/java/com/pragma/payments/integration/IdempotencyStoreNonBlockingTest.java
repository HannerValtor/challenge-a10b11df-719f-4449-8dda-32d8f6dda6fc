package com.pragma.payments.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.persistence.IdempotencyRecordEntity;
import com.pragma.payments.infrastructure.adapters.out.persistence.IdempotencyRecordMapper;
import com.pragma.payments.infrastructure.adapters.out.persistence.IdempotencyRecordRepository;
import com.pragma.payments.infrastructure.adapters.out.persistence.IdempotencyStoreAdapter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

/**
 * Test de NO BLOQUEO del adaptador de idempotencia con BlockHound (Req 2.3, 4.4, 8.1, 8.2).
 *
 * <p>BlockHound instrumenta la JVM para detectar llamadas potencialmente bloqueantes
 * ejecutadas sobre hilos marcados como no bloqueantes (p. ej. los {@code parallel} de
 * Reactor, equivalentes al Event Loop de WebFlux). Como el adaptador real
 * {@code IdempotencyStoreAdapter} envuelve TODO acceso al repositorio JPA (bloqueante) en
 * {@code Mono.fromCallable(...).subscribeOn(Schedulers.boundedElastic())}, el bloqueo ocurre
 * exclusivamente dentro del {@code boundedElastic} (permitido) y NUNCA sobre el hilo no
 * bloqueante. Este test verifica exactamente eso para las cinco operaciones del puerto:
 * {@code find}, {@code reserve}, {@code complete}, {@code markFailedOrRelease} y
 * {@code deleteExpired}.</p>
 *
 * <p>Se ejecuta SIN Docker: el {@link IdempotencyRecordRepository} se sustituye por un doble
 * (Mockito) cuyas respuestas incluyen un {@link Thread#sleep(long)} deliberado. Ese sleep es
 * una llamada bloqueante real; si el adaptador no la aislara en {@code boundedElastic},
 * BlockHound la detectaría al suscribir sobre un hilo no bloqueante y el pipeline fallaría con
 * {@link BlockingOperationError}. Al completar sin ese error, se demuestra el aislamiento
 * correcto del bloqueo (Req 8.1, 8.2) y, por extensión, el acceso no bloqueante al store
 * durante consulta, reserva, actualización y eliminación (Req 2.3, 4.4).</p>
 *
 * <p>Para forzar explícitamente la ejecución sobre un hilo no bloqueante, cada pipeline se
 * suscribe con {@code subscribeOn(Schedulers.parallel())} antes de entrar al adaptador; el
 * {@code subscribeOn(boundedElastic())} interno del adaptador reubica solo el acceso bloqueante.</p>
 */
class IdempotencyStoreNonBlockingTest {

    @BeforeAll
    static void installBlockHound() {
        // Idempotente: múltiples install() son seguros. Detecta bloqueos en hilos no bloqueantes.
        BlockHound.install();
    }

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final IdempotencyRecordMapper mapper = new IdempotencyRecordMapper(objectMapper);

    private final IdempotencyKey key = new IdempotencyKey("OP-NB-1", "web");
    private final Instant createdAt = Instant.now();
    private final Duration ttl = Duration.ofHours(24);

    /** Entidad IN_PROGRESS consistente con la clave, para las respuestas del repositorio mock. */
    private IdempotencyRecordEntity inProgressEntity() {
        return mapper.toEntity(IdempotencyRecord.reserve(key, createdAt, ttl));
    }

    private Transaction completedResult() {
        Transaction tx = Transaction.builder()
                .idempotencyKey(key)
                .accountId("ACC-NB")
                .amount(new BigDecimal("10.00"))
                .currency("USD")
                .channel("web")
                .status(Transaction.TransactionStatus.COMPLETED)
                .createdAt(createdAt)
                .build();
        tx.setExternalReference("EXT-NB");
        return tx;
    }

    /** Simula una E/S bloqueante (como la que haría JDBC) para dar sentido a la verificación. */
    private static void simulateBlockingIo() {
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Suscribe el pipeline del adaptador sobre un hilo NO bloqueante ({@code parallel}) y
     * verifica que completa sin que BlockHound dispare {@link BlockingOperationError}. Captura
     * cualquier error del pipeline para distinguir una violación de bloqueo de un fallo normal.
     */
    private void assertNonBlockingOnParallel(Mono<?> adapterCall) {
        AtomicReference<Throwable> error = new AtomicReference<>();
        adapterCall
                .subscribeOn(Schedulers.parallel())
                .doOnError(error::set)
                .onErrorResume(t -> Mono.empty())
                .blockOptional(Duration.ofSeconds(10));

        Throwable captured = error.get();
        boolean blockingDetected = captured instanceof BlockingOperationError
                || (captured != null && captured.getCause() instanceof BlockingOperationError);
        assertThat(blockingDetected)
                .as("el acceso bloqueante al store debe aislarse en boundedElastic, "
                        + "nunca ejecutarse sobre el hilo no bloqueante: %s", captured)
                .isFalse();
    }

    @Test
    void findDoesNotBlockTheNonBlockingThread() {
        IdempotencyRecordRepository repository = mock(IdempotencyRecordRepository.class);
        when(repository.findById(anyString())).thenAnswer(inv -> {
            simulateBlockingIo();
            return Optional.of(inProgressEntity());
        });
        IdempotencyStoreAdapter adapter = new IdempotencyStoreAdapter(repository, mapper);

        assertNonBlockingOnParallel(adapter.find(key));
    }

    @Test
    void reserveDoesNotBlockTheNonBlockingThread() {
        IdempotencyRecordRepository repository = mock(IdempotencyRecordRepository.class);
        when(repository.insertNew(anyString(), anyString(), any(), any(), any()))
                .thenAnswer(inv -> {
                    simulateBlockingIo();
                    return 1;
                });
        IdempotencyStoreAdapter adapter = new IdempotencyStoreAdapter(repository, mapper);

        assertNonBlockingOnParallel(adapter.reserve(IdempotencyRecord.reserve(key, createdAt, ttl)));
    }

    @Test
    void completeDoesNotBlockTheNonBlockingThread() {
        IdempotencyRecordRepository repository = mock(IdempotencyRecordRepository.class);
        when(repository.findById(anyString())).thenAnswer(inv -> {
            simulateBlockingIo();
            return Optional.of(inProgressEntity());
        });
        when(repository.save(any(IdempotencyRecordEntity.class))).thenAnswer(inv -> {
            simulateBlockingIo();
            return inv.getArgument(0);
        });
        IdempotencyStoreAdapter adapter = new IdempotencyStoreAdapter(repository, mapper);

        assertNonBlockingOnParallel(adapter.complete(key, completedResult()));
    }

    @Test
    void markFailedOrReleaseDoesNotBlockTheNonBlockingThread() {
        IdempotencyRecordRepository repository = mock(IdempotencyRecordRepository.class);
        when(repository.findById(anyString())).thenAnswer(inv -> {
            simulateBlockingIo();
            return Optional.of(inProgressEntity());
        });
        when(repository.save(any(IdempotencyRecordEntity.class))).thenAnswer(inv -> {
            simulateBlockingIo();
            return inv.getArgument(0);
        });
        IdempotencyStoreAdapter adapter = new IdempotencyStoreAdapter(repository, mapper);

        assertNonBlockingOnParallel(adapter.markFailedOrRelease(key).then(Mono.just("done")));
    }

    @Test
    void deleteExpiredDoesNotBlockTheNonBlockingThread() {
        IdempotencyRecordRepository repository = mock(IdempotencyRecordRepository.class);
        when(repository.deleteByExpiresAtBefore(any())).thenAnswer(inv -> {
            simulateBlockingIo();
            return 3L;
        });
        IdempotencyStoreAdapter adapter = new IdempotencyStoreAdapter(repository, mapper);

        assertNonBlockingOnParallel(adapter.deleteExpired(Instant.now()));
    }
}
