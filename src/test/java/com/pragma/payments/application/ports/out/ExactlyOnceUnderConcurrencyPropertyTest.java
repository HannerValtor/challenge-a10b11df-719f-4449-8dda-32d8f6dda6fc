package com.pragma.payments.application.ports.out;

import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test de propiedad para la exactamente-una-ejecución de la reserva bajo concurrencia real.
 *
 * <p>Ejercita el contrato de {@link IdempotencyStorePort#reserve} sobre un doble de test en
 * memoria que respeta la restricción UNIQUE de forma <em>atómica</em> vía
 * {@link ConcurrentHashMap#putIfAbsent} (equivalente a la restricción UNIQUE del almacenamiento
 * persistente). Lanza K suscripciones concurrentes a {@code reserve} sobre la MISMA clave y
 * verifica que exactamente una gana la reserva (emite {@code IN_PROGRESS}) y las K-1 restantes
 * se tratan como Duplicate_Request ({@link DuplicateReservationException}), sin otros errores.</p>
 *
 * <p>La concurrencia real se dispara con un {@link ExecutorService} y un {@link CountDownLatch}
 * de arranque ({@code startGate}) que libera todos los hilos a la vez, igual que el test
 * {@code exactlyOnceUnderConcurrency} de {@code PaymentIdempotencyTest}. El doble en memoria se
 * mantiene muy liviano para que 100 iteraciones x hasta 32 hilos ejecuten con rapidez.</p>
 *
 * <p>Se ubica en {@code application.ports.out} (no en {@code adapters/out}) porque el
 * {@code maven-compiler-plugin} excluye de la compilación de test los ficheros bajo
 * {@code **}/adapters/out/**}/*Test.java}; situarlo aquí garantiza que la propiedad se compile
 * y ejecute, siguiendo el precedente de {@code AtomicReservationPropertyTest}.</p>
 */
class ExactlyOnceUnderConcurrencyPropertyTest {

    // Feature: implementacion-idempotencia, Property 7: Exactamente una ejecución bajo concurrencia
    // Validates: Requirements 5.1, 5.2, 5.3
    //
    // Para cualquier clave y para cualquier número K >= 2 de solicitudes idénticas lanzadas
    // concurrentemente sin record previo, exactamente una gana la reserva (y ejecutaría la cadena
    // de servicios externos una sola vez) y las K-1 restantes se tratan como Duplicate_Request.
    @Property(tries = 100)
    void exactlyOneReservationWinsUnderConcurrentReserveOnSameKey(
            @ForAll("idempotencyKeys") IdempotencyKey key,
            @ForAll @IntRange(min = 2, max = 32) int concurrency) throws InterruptedException {

        final int k = concurrency;
        final IdempotencyStorePort store = new UniqueConstrainedInMemoryStore();
        final Instant now = Instant.now();
        final Duration ttl = Duration.ofHours(24);

        // Contadores de resultados de la carrera.
        final AtomicInteger winners = new AtomicInteger(0);     // reservas exitosas -> IN_PROGRESS
        final AtomicInteger duplicates = new AtomicInteger(0);  // perdedoras -> DuplicateReservationException
        final ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();

        final ExecutorService pool = Executors.newFixedThreadPool(k);
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(k);

        try {
            for (int i = 0; i < k; i++) {
                pool.submit(() -> {
                    try {
                        startGate.await();
                        // Todos los hilos reservan la MISMA clave (mismo record lógico).
                        IdempotencyRecord candidate = IdempotencyRecord.reserve(key, now, ttl);
                        IdempotencyRecord reserved = store.reserve(candidate).block();
                        if (reserved != null && reserved.getStatus() == RecordStatus.IN_PROGRESS) {
                            winners.incrementAndGet();
                        } else {
                            // Una reserva sin señal de duplicado pero sin IN_PROGRESS es inesperada.
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

            startGate.countDown();                  // libera los K hilos a la vez
            boolean finished = done.await(30, TimeUnit.SECONDS);
            assertThat(finished).as("todas las K reservas concurrentes terminan a tiempo").isTrue();
        } finally {
            pool.shutdownNow();
        }

        // Sin errores inesperados: las únicas salidas válidas son ganar o recibir duplicado.
        assertThat(unexpected).as("sin errores inesperados en la carrera").isEmpty();

        // Exactamente una ejecución bajo concurrencia: 1 ganador, K-1 duplicados (Req 5.1, 5.2).
        assertThat(winners.get()).as("exactamente una reserva gana la carrera").isEqualTo(1);
        assertThat(duplicates.get()).as("las K-1 restantes se tratan como Duplicate_Request").isEqualTo(k - 1);

        // El store contiene exactamente un record IN_PROGRESS para la clave (unicidad, Req 5.3).
        IdempotencyRecord stored = store.find(key).block();
        assertThat(stored).as("existe exactamente un registro tras la carrera").isNotNull();
        assertThat(stored.getStatus()).isEqualTo(RecordStatus.IN_PROGRESS);
        assertThat(stored.getKey()).isEqualTo(key);
    }

    @Provide
    Arbitrary<IdempotencyKey> idempotencyKeys() {
        // operationNumber conforme a ^[A-Za-z0-9\-]+$ (1..50) y channel conforme a ^[A-Za-z0-9_]+$ (1..20).
        Arbitrary<String> operationNumbers = Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1).ofMaxLength(50);
        Arbitrary<String> channels = Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")
                .ofMinLength(1).ofMaxLength(20);
        return Combinators.combine(operationNumbers, channels).as(IdempotencyKey::new);
    }

    /**
     * Doble de test del {@link IdempotencyStorePort} en memoria que respeta la restricción UNIQUE
     * de forma atómica mediante {@link ConcurrentHashMap#putIfAbsent}, igual que el
     * {@code InMemoryIdempotencyStore} de {@code PaymentIdempotencyTest}. Solo se implementan las
     * operaciones relevantes para Property 7 ({@code reserve} y {@code find}).
     */
    private static final class UniqueConstrainedInMemoryStore implements IdempotencyStorePort {

        private final Map<String, IdempotencyRecord> store = new ConcurrentHashMap<>();

        @Override
        public Mono<IdempotencyRecord> reserve(IdempotencyRecord record) {
            return Mono.defer(() -> {
                String canonical = canonical(record.getKey());
                IdempotencyRecord previous = store.putIfAbsent(canonical, record);
                if (previous != null) {
                    return Mono.error(new DuplicateReservationException(canonical, null));
                }
                return Mono.just(record);
            });
        }

        @Override
        public Mono<IdempotencyRecord> find(IdempotencyKey key) {
            return Mono.defer(() -> Mono.justOrEmpty(store.get(canonical(key))));
        }

        @Override
        public Mono<IdempotencyRecord> complete(IdempotencyKey key, Transaction result) {
            return Mono.error(new UnsupportedOperationException("no usado en Property 7"));
        }

        @Override
        public Mono<Void> markFailedOrRelease(IdempotencyKey key) {
            return Mono.error(new UnsupportedOperationException("no usado en Property 7"));
        }

        @Override
        public Mono<Long> deleteExpired(Instant now) {
            return Mono.error(new UnsupportedOperationException("no usado en Property 7"));
        }

        private String canonical(IdempotencyKey key) {
            return key.getOperationNumber() + "_" + key.getChannel();
        }
    }
}
