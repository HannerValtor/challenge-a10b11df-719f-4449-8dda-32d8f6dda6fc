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
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Property 6: No bloqueo del event-loop.
 *
 * <p><strong>Validates: Requirements 6.4, 7.10</strong></p>
 *
 * <p>∀ ejecución, ningún acceso JPA/bloqueante del {@code IdempotencyStoreAdapter} corre sobre
 * un hilo de event-loop de Netty; el bloqueo permanece aislado en {@code boundedElastic}
 * (Req 7.10) gracias al único {@code subscribeOn(Schedulers.boundedElastic())} por operación
 * (Req 6.4). Esta propiedad se verifica con BlockHound, que instrumenta la JVM para detectar
 * llamadas bloqueantes ejecutadas sobre hilos marcados como <em>no bloqueantes</em>
 * (los {@code parallel} de Reactor, equivalentes al Event Loop de WebFlux).</p>
 *
 * <p>A diferencia del test de ejemplo {@link IdempotencyStoreNonBlockingTest} —que fija una
 * sola clave y cubre cada operación una vez— este test de propiedad genera aleatoriamente la
 * clave de idempotencia (número de operación y canal) y la operación de persistencia a
 * ejercer, de modo que la invariante de no bloqueo se comprueba sobre el espacio de entradas
 * y a través de las cinco operaciones del puerto ({@code find}, {@code reserve},
 * {@code complete}, {@code markFailedOrRelease}, {@code deleteExpired}).</p>
 *
 * <p>El repositorio JPA se sustituye por un doble (Mockito) cuyas respuestas incluyen un
 * {@link Thread#sleep(long)} deliberado: una llamada bloqueante real. El pipeline del
 * adaptador se suscribe explícitamente sobre un hilo {@code parallel} (no bloqueante); el
 * {@code subscribeOn(boundedElastic())} interno del adaptador debe reubicar el acceso
 * bloqueante. Se afirma además que el {@code sleep} ocurrió efectivamente sobre un hilo
 * {@code boundedElastic} y nunca sobre el hilo no bloqueante, cerrando la propiedad en ambos
 * sentidos: BlockHound no dispara {@link BlockingOperationError} y el bloqueo vive donde debe.</p>
 *
 * <p><em>Nota sobre la mecánica de espera y el scheduler no bloqueante:</em> cada iteración
 * fuerza la suscripción del pipeline sobre un hilo <strong>no bloqueante</strong> para
 * preservar la semántica de detección de BlockHound (el acceso bloqueante debe reubicarse
 * fuera de ese hilo). Sin embargo, se usa un scheduler no bloqueante <strong>dedicado</strong>
 * a este contenedor ({@code Schedulers.newParallel}) en lugar del global
 * {@code Schedulers.parallel()}: BlockHound marca igualmente sus hilos como no bloqueantes
 * (el nombre contiene {@code parallel}), de modo que la detección es idéntica, pero al no
 * compartirse con el resto de la suite se elimina la contención del reducido pool global que,
 * bajo la suite completa y con ≥100 iteraciones, agotaba el timeout de espera. La espera del
 * resultado se gobierna además con {@link StepVerifier}, que gestiona suscripción y
 * <em>await</em> de forma determinista con un timeout acotado. La propiedad verificada no
 * cambia.</p>
 *
 * <p><em>Nota sobre iteraciones:</em> se usan 200 iteraciones ({@code tries}), muy por encima
 * del mínimo de 100, ya que la instrumentación de BlockHound se instala una sola vez por
 * contenedor y cada iteración es un pipeline reactivo corto; el coste agregado es moderado y
 * no hace impracticable un conteo alto.</p>
 */
class IdempotencyStoreNonBlockingPropertyTest {

    private static final Duration TTL = Duration.ofHours(24);

    /**
     * Scheduler NO bloqueante dedicado a este contenedor. BlockHound marca sus hilos como no
     * bloqueantes (su nombre contiene {@code parallel}), igual que {@code Schedulers.parallel()},
     * por lo que la semántica de detección es idéntica; pero al estar aislado del pool global
     * compartido por toda la suite de surefire, no sufre la contención que agotaba el timeout
     * de espera bajo la suite completa.
     */
    private static Scheduler nonBlockingScheduler;

    @BeforeContainer
    static void installBlockHound() {
        // Idempotente: instalar BlockHound varias veces es seguro. Detecta bloqueos en hilos no bloqueantes.
        BlockHound.install();
        // Pool pequeño pero suficiente; los hilos "parallel" son no bloqueantes para BlockHound.
        nonBlockingScheduler = Schedulers.newParallel("nb-prop", 4);
    }

    @AfterContainer
    static void disposeScheduler() {
        if (nonBlockingScheduler != null) {
            nonBlockingScheduler.dispose();
        }
    }

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private final IdempotencyRecordMapper mapper = new IdempotencyRecordMapper(objectMapper);

    /**
     * Operaciones de persistencia del puerto que se ejercen aleatoriamente.
     */
    private enum StoreOperation {
        FIND, RESERVE, COMPLETE, MARK_FAILED_OR_RELEASE, DELETE_EXPIRED
    }

    @Property(tries = 200)
    void blockingJpaAccessStaysOnBoundedElasticAndNeverOnTheEventLoop(
            @ForAll("keys") IdempotencyKey key,
            @ForAll StoreOperation operation) {

        Instant createdAt = Instant.now();
        AtomicReference<String> blockingThreadName = new AtomicReference<>();

        IdempotencyRecordRepository repository = mock(IdempotencyRecordRepository.class);
        stubRepository(repository, key, createdAt, blockingThreadName);

        IdempotencyStoreAdapter adapter = new IdempotencyStoreAdapter(repository, mapper);
        Mono<?> adapterCall = invoke(adapter, operation, key, createdAt);

        AtomicReference<Throwable> error = new AtomicReference<>();
        // Suscribir sobre un hilo NO bloqueante dedicado (≈ Event Loop de Netty para BlockHound):
        // si el adaptador no aislara el bloqueo en boundedElastic, BlockHound lo detectaría aquí.
        // La espera se gobierna con StepVerifier (determinista, timeout acotado) en lugar de
        // un block() que compita por el reducido pool parallel compartido por toda la suite.
        // Si BlockHound disparara, onErrorResume lo reduce a 0 elementos; de lo contrario,
        // cada operación emite exactamente 1. Se toleran ambos para que la violación la
        // reporten las aserciones explícitas de abajo con un mensaje claro, no el verifier.
        StepVerifier.create(
                        adapterCall
                                .subscribeOn(nonBlockingScheduler)
                                .doOnError(error::set)
                                .onErrorResume(t -> Mono.empty()))
                .expectSubscription()
                .thenConsumeWhile(v -> true)
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        Throwable captured = error.get();
        boolean blockingDetected = captured instanceof BlockingOperationError
                || (captured != null && captured.getCause() instanceof BlockingOperationError);
        assertThat(blockingDetected)
                .as("[%s %s] el acceso bloqueante al store debe aislarse en boundedElastic, "
                        + "nunca ejecutarse sobre el hilo no bloqueante: %s", operation, key.toUniqueString(), captured)
                .isFalse();

        // Sentido inverso de la propiedad: el bloqueo (sleep) ocurrió realmente y lo hizo
        // sobre un hilo boundedElastic, no sobre el hilo no bloqueante (parallel).
        String ranOn = blockingThreadName.get();
        assertThat(ranOn)
                .as("[%s %s] el acceso bloqueante debió ejecutarse sobre un hilo boundedElastic", operation, key.toUniqueString())
                .isNotNull()
                .contains("boundedElastic");
        assertThat(ranOn)
                .as("[%s %s] el acceso bloqueante NUNCA debe correr sobre un hilo no bloqueante (parallel)", operation, key.toUniqueString())
                .doesNotContain("parallel");
    }

    /**
     * Configura el repositorio mock para la operación solicitada, registrando el nombre del
     * hilo sobre el que se ejecuta la E/S bloqueante simulada.
     */
    private void stubRepository(IdempotencyRecordRepository repository,
                                IdempotencyKey key,
                                Instant createdAt,
                                AtomicReference<String> blockingThreadName) {
        when(repository.findById(anyString())).thenAnswer(inv -> {
            simulateBlockingIo(blockingThreadName);
            return Optional.of(inProgressEntity(key, createdAt));
        });
        when(repository.insertNew(anyString(), anyString(), any(), any(), any())).thenAnswer(inv -> {
            simulateBlockingIo(blockingThreadName);
            return 1;
        });
        when(repository.save(any(IdempotencyRecordEntity.class))).thenAnswer(inv -> {
            simulateBlockingIo(blockingThreadName);
            return inv.getArgument(0);
        });
        when(repository.deleteByExpiresAtBefore(any())).thenAnswer(inv -> {
            simulateBlockingIo(blockingThreadName);
            return 2L;
        });
    }

    private Mono<?> invoke(IdempotencyStoreAdapter adapter,
                           StoreOperation operation,
                           IdempotencyKey key,
                           Instant createdAt) {
        return switch (operation) {
            case FIND -> adapter.find(key);
            case RESERVE -> adapter.reserve(IdempotencyRecord.reserve(key, createdAt, TTL));
            case COMPLETE -> adapter.complete(key, completedResult(key, createdAt));
            case MARK_FAILED_OR_RELEASE -> adapter.markFailedOrRelease(key).then(Mono.just("done"));
            case DELETE_EXPIRED -> adapter.deleteExpired(createdAt);
        };
    }

    private IdempotencyRecordEntity inProgressEntity(IdempotencyKey key, Instant createdAt) {
        return mapper.toEntity(IdempotencyRecord.reserve(key, createdAt, TTL));
    }

    private Transaction completedResult(IdempotencyKey key, Instant createdAt) {
        Transaction tx = Transaction.builder()
                .idempotencyKey(key)
                .accountId("ACC-NB")
                .amount(new BigDecimal("10.00"))
                .currency("USD")
                .channel(key.getChannel())
                .status(Transaction.TransactionStatus.COMPLETED)
                .createdAt(createdAt)
                .build();
        tx.setExternalReference("EXT-NB");
        return tx;
    }

    /** Simula una E/S bloqueante (como la que haría JDBC) y registra el hilo donde corre. */
    private static void simulateBlockingIo(AtomicReference<String> blockingThreadName) {
        blockingThreadName.set(Thread.currentThread().getName());
        try {
            Thread.sleep(2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Genera claves de idempotencia válidas: número de operación con el patrón permitido
     * ({@code [A-Za-z0-9\-]+}) y canal con el patrón permitido ({@code [A-Za-z0-9_]+}),
     * respetando los límites de longitud del dominio.
     */
    @Provide
    Arbitrary<IdempotencyKey> keys() {
        Arbitrary<String> operationNumbers = Arbitraries.strings()
                .withCharRange('a', 'z')
                .withCharRange('A', 'Z')
                .withCharRange('0', '9')
                .ofMinLength(1)
                .ofMaxLength(20);
        Arbitrary<String> channels = Arbitraries.of("web", "mobile", "api", "ivr", "batch_1");
        return operationNumbers.flatMap(op -> channels.map(ch -> new IdempotencyKey(op, ch)));
    }
}
