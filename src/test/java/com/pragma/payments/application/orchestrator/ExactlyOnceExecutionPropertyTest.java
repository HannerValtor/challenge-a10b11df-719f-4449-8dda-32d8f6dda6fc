package com.pragma.payments.application.orchestrator;

import com.pragma.payments.application.chain.PaymentProcessingChain;
import com.pragma.payments.application.ports.out.DuplicateReservationException;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 1: Exactamente-una-ejecución (idempotencia).
 *
 * <p><b>Validates: Requirements 7.1, 7.2, 7.3</b></p>
 *
 * <p>Para un número {@code K} aleatorio de solicitudes concurrentes en el rango [2, 32] con la
 * <em>misma</em> {@link IdempotencyKey} y sin registro previo, se verifica que
 * {@link IdempotencyOrchestratorImpl#resolve(IdempotencyKey, Transaction)}:</p>
 * <ul>
 *   <li>ejecuta la cadena externa ({@link PaymentProcessingChain#execute}) <b>exactamente una vez</b>
 *       a través de las K solicitudes (Req 7.1);</li>
 *   <li>otorga la reserva atómica a exactamente una solicitud; las K-1 restantes pierden la carrera y
 *       se tratan como duplicadas sin ejecutar la cadena (Req 7.2);</li>
 *   <li>toda solicitud que retorna un <b>resultado</b> retorna el <b>mismo</b> (idéntico) resultado de
 *       procesamiento determinista (Req 7.3).</li>
 * </ul>
 *
 * <p>Para que la propiedad sea significativa a nivel unitario sin una BD real, se usa un
 * {@code InMemoryIdempotencyStore} que emula la restricción de unicidad del motor de persistencia:
 * el primer {@code reserve(key)} gana (almacena IN_PROGRESS); los siguientes {@code reserve(key)}
 * mientras el registro existe emiten {@link DuplicateReservationException}. {@code complete(key, result)}
 * almacena COMPLETED + resultado, y {@code find(key)} devuelve el registro actual o vacío. La estructura
 * es thread-safe ({@link ConcurrentHashMap} con {@code putIfAbsent}/{@code compute}) para que la
 * concurrencia se ejercite de verdad.</p>
 *
 * <p>Las K llamadas se disparan concurrentemente con {@code Flux.flatMap} + {@code subscribeOn} sobre el
 * scheduler {@code parallel}. Nota sobre la carrera IN_PROGRESS: una solicitud duplicada que llega
 * <em>antes</em> de que la ganadora complete puede aflorar como {@link TransactionAlreadyProcessedException}
 * (equivalente a un 409). La propiedad tolera ese resultado de carrera: los invariantes CLAVE son que
 * {@code execute()} corre exactamente una vez y que toda solicitud que entrega un RESULTADO entrega el
 * resultado idéntico.</p>
 */
class ExactlyOnceExecutionPropertyTest {

    @Property(tries = 100)
    void exactlyOneExecutionAcrossKConcurrentRequests(
            @ForAll @IntRange(min = 2, max = 32) int k,
            @ForAll("operationNumbers") String operationNumber,
            @ForAll("channels") String channel) {

        // Contador de ejecuciones reales de la cadena externa (debe quedar en 1).
        AtomicInteger executeCount = new AtomicInteger(0);

        // Cadena externa estable y determinista: marca la transacción como COMPLETED con una
        // referencia externa constante por clave, de modo que el resultado cacheado sea idéntico.
        PaymentProcessingChain chain = tx -> Mono.fromCallable(() -> {
            executeCount.incrementAndGet();
            tx.markAsCompleted("EXT-REF-" + tx.getIdempotencyKey().toUniqueString());
            return tx;
        });

        InMemoryIdempotencyStore store = new InMemoryIdempotencyStore();
        IdempotencyProperties properties = new IdempotencyProperties();

        IdempotencyOrchestratorImpl orchestrator =
                new IdempotencyOrchestratorImpl(store, chain, properties);

        IdempotencyKey key = new IdempotencyKey(operationNumber, channel);

        // Disparo concurrente de K solicitudes para la MISMA clave.
        List<SettledOutcome> outcomes = Flux.range(0, k)
                .flatMap(i -> {
                    Transaction tx = Transaction.builder()
                            .idempotencyKey(new IdempotencyKey(operationNumber, channel))
                            .accountId("acc-" + i)
                            .channel(channel)
                            .build();
                    return orchestrator.resolve(key, tx)
                            .subscribeOn(Schedulers.parallel())
                            .map(SettledOutcome::success)
                            // 409 (IN_PROGRESS) es un resultado de carrera tolerado: no entrega resultado.
                            .onErrorResume(TransactionAlreadyProcessedException.class,
                                    e -> Mono.just(SettledOutcome.conflict()))
                            .onErrorResume(DuplicateReservationException.class,
                                    e -> Mono.just(SettledOutcome.conflict()));
                }, k)
                .collectList()
                .block();

        assertThat(outcomes).isNotNull();
        assertThat(outcomes).hasSize(k);

        // Invariante 1 (Req 7.1): la cadena externa se ejecutó EXACTAMENTE UNA VEZ.
        assertThat(executeCount.get())
                .as("La cadena externa debe ejecutarse exactamente una vez para K=%d solicitudes concurrentes", k)
                .isEqualTo(1);

        // Invariante 2 (Req 7.2): la reserva atómica se otorgó exactamente una vez; las K-1 restantes
        // perdieron la carrera (se reflejan como duplicadas: resultado cacheado o conflicto 409).
        assertThat(store.reserveSuccessCount())
                .as("Exactamente una solicitud debe ganar la reserva atómica")
                .isEqualTo(1);

        // Al menos una solicitud debe entregar un resultado (la ganadora, y las duplicadas COMPLETED).
        List<Transaction> results = outcomes.stream()
                .filter(SettledOutcome::isSuccess)
                .map(SettledOutcome::result)
                .toList();
        assertThat(results)
                .as("Al menos la solicitud ganadora debe entregar un resultado de procesamiento")
                .isNotEmpty();

        // Invariante 3 (Req 7.3): toda solicitud que entrega un resultado entrega el MISMO resultado
        // determinista (misma identidad de transacción y misma referencia externa).
        Transaction first = results.get(0);
        assertThat(results).allSatisfy(r -> {
            assertThat(r.getId())
                    .as("Todas las solicitudes que retornan resultado deben retornar la misma transacción")
                    .isEqualTo(first.getId());
            assertThat(r.getExternalReference())
                    .as("El resultado duplicado debe ser idéntico (misma referencia externa)")
                    .isEqualTo(first.getExternalReference());
            assertThat(r.getStatus())
                    .as("El resultado determinista debe estar COMPLETED")
                    .isEqualTo(Transaction.TransactionStatus.COMPLETED);
        });
    }

    // ---- Resultado de una solicitud resuelta -------------------------------

    private record SettledOutcome(boolean isSuccess, Transaction result) {
        static SettledOutcome success(Transaction tx) {
            return new SettledOutcome(true, tx);
        }

        static SettledOutcome conflict() {
            return new SettledOutcome(false, null);
        }
    }

    // ---- Fake in-memory store que emula la restricción de unicidad de la BD -

    /**
     * Implementación en memoria y thread-safe de {@link IdempotencyStorePort} que emula la restricción
     * de unicidad del almacenamiento persistente: {@code reserve} usa {@code putIfAbsent} de un
     * {@link ConcurrentHashMap} para garantizar que solo la primera inserción gane; cualquier reserva
     * sobre una clave ya presente emite {@link DuplicateReservationException}.
     */
    private static final class InMemoryIdempotencyStore implements IdempotencyStorePort {

        private final ConcurrentHashMap<String, IdempotencyRecord> records = new ConcurrentHashMap<>();
        private final AtomicInteger reserveSuccesses = new AtomicInteger(0);

        int reserveSuccessCount() {
            return reserveSuccesses.get();
        }

        @Override
        public Mono<IdempotencyRecord> reserve(IdempotencyRecord record) {
            return Mono.defer(() -> {
                String k = record.getKey().toUniqueString();
                IdempotencyRecord previous = records.putIfAbsent(k, record);
                if (previous != null) {
                    // La clave ya existe: pierde la carrera (restricción de unicidad).
                    return Mono.error(new DuplicateReservationException(k, null));
                }
                reserveSuccesses.incrementAndGet();
                return Mono.just(record);
            });
        }

        @Override
        public Mono<IdempotencyRecord> find(IdempotencyKey key) {
            return Mono.defer(() -> {
                IdempotencyRecord current = records.get(key.toUniqueString());
                return current == null ? Mono.empty() : Mono.just(current);
            });
        }

        @Override
        public Mono<IdempotencyRecord> complete(IdempotencyKey key, Transaction result) {
            return Mono.defer(() -> {
                IdempotencyRecord completed = records.compute(key.toUniqueString(), (k, existing) -> {
                    IdempotencyRecord base = existing != null
                            ? existing
                            : IdempotencyRecord.reserve(key, Instant.now(), java.time.Duration.ofHours(24));
                    return base.withResult(result);
                });
                return Mono.just(completed);
            });
        }

        @Override
        public Mono<Void> markFailedOrRelease(IdempotencyKey key) {
            return Mono.fromRunnable(() -> records.computeIfPresent(key.toUniqueString(),
                    (k, existing) -> existing.getStatus() == RecordStatus.COMPLETED ? existing : existing.asFailed()));
        }

        @Override
        public Mono<Long> deleteExpired(Instant now) {
            return Mono.fromCallable(() -> {
                long[] removed = {0L};
                records.entrySet().removeIf(e -> {
                    boolean expired = e.getValue().isExpired(now);
                    if (expired) {
                        removed[0]++;
                    }
                    return expired;
                });
                return removed[0];
            });
        }
    }

    // ---- Generadores -------------------------------------------------------

    // operationNumber conforme a ^[A-Za-z0-9\-]+$ (1..50)
    @Provide
    Arbitrary<String> operationNumbers() {
        return Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1).ofMaxLength(50);
    }

    // channel conforme a ^[A-Za-z0-9_]+$ (1..20)
    @Provide
    Arbitrary<String> channels() {
        return Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")
                .ofMinLength(1).ofMaxLength(20);
    }
}
