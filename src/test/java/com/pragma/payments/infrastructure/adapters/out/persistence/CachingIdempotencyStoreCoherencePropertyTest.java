package com.pragma.payments.infrastructure.adapters.out.persistence;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.Size;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 3: Coherencia de caché.
 *
 * <p>Validates: Requirements 4.1, 4.5</p>
 *
 * <p>Para toda secuencia aleatoria de operaciones {@code reserve/complete/fail/deleteExpired/find}
 * sobre {@link CachingIdempotencyStore}, se verifica que:</p>
 * <ul>
 *   <li>una lectura ({@code find}) nunca devuelve un registro cuyo estado real en el store sea
 *       {@code IN_PROGRESS} o {@code FAILED} <em>servido desde la caché</em> (la caché solo retiene
 *       {@code COMPLETED} inmutables, Requirement 4.1); y</li>
 *   <li>una lectura nunca devuelve un registro ya eliminado por el barrido de expiración
 *       ({@code deleteExpired}): tras la eliminación la entrada de caché queda invalidada
 *       (Requirement 4.5).</li>
 * </ul>
 *
 * <p>El store se modela con un stub en memoria ({@code InMemoryStore}) que refleja el estado real y
 * es la fuente de verdad. El decorator se construye sobre una caché Caffeine real. En cada paso de
 * {@code find} se compara el valor devuelto por el decorator con el estado real del store: deben
 * coincidir exactamente, de modo que la caché jamás introduce una incoherencia.</p>
 */
class CachingIdempotencyStoreCoherencePropertyTest {

    private static final Duration TTL = Duration.ofHours(24);

    enum Op { RESERVE, COMPLETE, FAIL, DELETE_EXPIRED, FIND }

    @Property(tries = 100)
    void cacheNeverServesMutableOrDeletedRecords(@ForAll("operationSequences") List<Op> operations) {
        IdempotencyKey key = new IdempotencyKey("OP-1", "web");

        InMemoryStore backing = new InMemoryStore();
        Cache<String, IdempotencyRecord> cache =
                Caffeine.newBuilder().maximumSize(1000).build();
        CachingIdempotencyStore store = new CachingIdempotencyStore(backing, cache);

        for (Op op : operations) {
            switch (op) {
                case RESERVE -> store.reserve(IdempotencyRecord.reserve(key, Instant.now(), TTL))
                        .onErrorResume(e -> Mono.empty())
                        .block();
                case COMPLETE -> {
                    // complete solo tiene sentido si hay una reserva previa; el stub lo maneja.
                    if (backing.has(key)) {
                        store.complete(key, completedResult(key)).block();
                    }
                }
                case FAIL -> store.markFailedOrRelease(key).block();
                case DELETE_EXPIRED -> store.deleteExpired(Instant.now().plus(Duration.ofDays(365))).block();
                case FIND -> {
                    IdempotencyRecord fromDecorator = store.find(key).block();
                    IdempotencyRecord fromStore = backing.rawFind(key);

                    // Invariante de coherencia: el decorator devuelve EXACTAMENTE el estado real.
                    assertThat(fromDecorator).isEqualTo(fromStore);

                    // La caché nunca sirve un estado mutable: si el decorator devolvió algo, o bien
                    // coincide con el store, o si vino de caché debe ser COMPLETED.
                    if (fromDecorator != null) {
                        IdempotencyRecord cached = cache.getIfPresent(key.toUniqueString());
                        if (cached != null) {
                            assertThat(cached.getStatus()).isEqualTo(RecordStatus.COMPLETED);
                        }
                    }
                    // La caché jamás retiene IN_PROGRESS o FAILED.
                    IdempotencyRecord cachedAny = cache.getIfPresent(key.toUniqueString());
                    if (cachedAny != null) {
                        assertThat(cachedAny.getStatus()).isEqualTo(RecordStatus.COMPLETED);
                    }
                }
            }

            // Tras CUALQUIER operación, si el store no tiene el registro, la caché no debe retenerlo
            // (invalidación coherente tras deleteExpired/fail/reserve).
            if (!backing.has(key)) {
                IdempotencyRecord leftover = cache.getIfPresent(key.toUniqueString());
                // Puede existir en caché solo si es COMPLETED y aún no se ha invalidado; pero si el
                // store ya no lo tiene (p.ej. deleteExpired), no debe quedar.
                if (leftover != null) {
                    // Solo es aceptable si el store sí lo tiene como COMPLETED (no es el caso aquí).
                    assertThat(backing.rawFind(key)).isNotNull();
                }
            }
        }
    }

    private Transaction completedResult(IdempotencyKey key) {
        return Transaction.builder()
                .id("tx-" + key.toUniqueString())
                .idempotencyKey(key)
                .accountId("acc-1")
                .channel("web")
                .status(TransactionStatus.COMPLETED)
                .build();
    }

    @Provide
    Arbitrary<List<Op>> operationSequences() {
        Arbitrary<Op> ops = Arbitraries.of(Op.values());
        return ops.list().ofMinSize(1).ofMaxSize(20);
    }

    /**
     * Stub en memoria de {@link IdempotencyStorePort} que refleja el estado real (fuente de verdad)
     * para comparar contra lo que el decorator devuelve.
     */
    private static final class InMemoryStore implements IdempotencyStorePort {
        private final Map<String, IdempotencyRecord> data = new ConcurrentHashMap<>();

        boolean has(IdempotencyKey key) {
            return data.containsKey(key.toUniqueString());
        }

        IdempotencyRecord rawFind(IdempotencyKey key) {
            return data.get(key.toUniqueString());
        }

        @Override
        public Mono<IdempotencyRecord> reserve(IdempotencyRecord record) {
            String k = record.getKey().toUniqueString();
            if (data.containsKey(k)) {
                // Clave ya reservada/presente: el stub simplemente no vuelve a insertar y emite vacío
                // (el decorator trata el error/empty sin cachear estados mutables).
                return Mono.empty();
            }
            data.put(k, record);
            return Mono.just(record);
        }

        @Override
        public Mono<IdempotencyRecord> find(IdempotencyKey key) {
            IdempotencyRecord r = data.get(key.toUniqueString());
            return r == null ? Mono.empty() : Mono.just(r);
        }

        @Override
        public Mono<IdempotencyRecord> complete(IdempotencyKey key, Transaction result) {
            String k = key.toUniqueString();
            IdempotencyRecord existing = data.get(k);
            if (existing == null) {
                return Mono.empty();
            }
            IdempotencyRecord completed = existing.withResult(result);
            data.put(k, completed);
            return Mono.just(completed);
        }

        @Override
        public Mono<Void> markFailedOrRelease(IdempotencyKey key) {
            String k = key.toUniqueString();
            IdempotencyRecord existing = data.get(k);
            if (existing != null) {
                data.put(k, existing.asFailed());
            }
            return Mono.empty();
        }

        @Override
        public Mono<Long> deleteExpired(Instant now) {
            long count = data.size();
            data.clear();
            return Mono.just(count);
        }
    }
}
