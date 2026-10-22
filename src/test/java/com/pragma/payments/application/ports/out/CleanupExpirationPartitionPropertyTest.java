package com.pragma.payments.application.ports.out;

import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test de propiedad para el particionado por expiración del proceso de limpieza.
 *
 * Ejercita el contrato de {@link IdempotencyStorePort#deleteExpired(Instant)} sobre un doble de
 * test en memoria que modela el borrado por expiración (equivalente al del almacenamiento
 * persistente): elimina SOLO los registros con {@code expiresAt < now}, conserva los que tienen
 * {@code expiresAt >= now} y devuelve el conteo de registros eliminados. Se usan conjuntos mixtos
 * de registros con {@code expiresAt} a ambos lados de {@code now} y claves distintas por registro.
 */
class CleanupExpirationPartitionPropertyTest {

    // Feature: implementacion-idempotencia, Property 9: La limpieza particiona por expiración
    // Validates: Requirements 6.3, 6.4
    //
    // Para cualquier conjunto de Idempotency_Record y cualquier instante now, ejecutar el
    // Cleanup_Process elimina exactamente los record con expiresAt < now y conserva exactamente
    // los record con expiresAt >= now.
    @Property(tries = 100)
    void deleteExpiredRemovesOnlyExpiredAndKeepsAllCurrent(
            @ForAll @IntRange(min = 0, max = 15) int expiredCount,
            @ForAll @IntRange(min = 0, max = 15) int currentCount) {

        IdempotencyStorePort store = new ExpirationPartitioningInMemoryStore();
        Instant now = Instant.now();

        // Conjunto mixto con expiresAt a ambos lados de now y claves distintas por registro.
        List<IdempotencyKey> expiredKeys = new ArrayList<>();
        List<IdempotencyKey> currentKeys = new ArrayList<>();

        int index = 0;
        // Registros expirados: expiresAt estrictamente anterior a now (expiresAt < now).
        for (int i = 0; i < expiredCount; i++, index++) {
            IdempotencyKey key = new IdempotencyKey("op-" + index, "ch" + index);
            Instant createdAt = now.minus(Duration.ofHours(48));
            Instant expiresAt = now.minusSeconds(i + 1L); // < now
            store.reserve(record(key, expiresAt, createdAt)).block();
            expiredKeys.add(key);
        }
        // Registros vigentes: expiresAt igual o posterior a now (expiresAt >= now).
        for (int i = 0; i < currentCount; i++, index++) {
            IdempotencyKey key = new IdempotencyKey("op-" + index, "ch" + index);
            Instant createdAt = now.minus(Duration.ofHours(1));
            Instant expiresAt = (i == 0) ? now : now.plusSeconds(i); // >= now (incluye el borde now)
            store.reserve(record(key, expiresAt, createdAt)).block();
            currentKeys.add(key);
        }

        // Ejecutar el Cleanup_Process.
        Long deleted = store.deleteExpired(now).block();

        // (1) el conteo devuelto == número de expirados.
        assertThat(deleted)
                .as("deleteExpired devuelve el número de registros expirados eliminados")
                .isEqualTo((long) expiredCount);

        // (2) todos los expirados desaparecen (find devuelve vacío).
        for (IdempotencyKey key : expiredKeys) {
            assertThat(store.find(key).blockOptional())
                    .as("el registro expirado con clave %s fue eliminado", key.toUniqueString())
                    .isEmpty();
        }

        // (3) todos los vigentes permanecen intactos (find los devuelve).
        for (IdempotencyKey key : currentKeys) {
            assertThat(store.find(key).blockOptional())
                    .as("el registro vigente con clave %s se conserva", key.toUniqueString())
                    .isPresent();
        }
    }

    private static IdempotencyRecord record(IdempotencyKey key, Instant expiresAt, Instant createdAt) {
        return new IdempotencyRecord(key, RecordStatus.IN_PROGRESS, null, createdAt, expiresAt);
    }

    /**
     * Doble de test del {@link IdempotencyStorePort} en memoria que modela el borrado por
     * expiración, igual que el {@code InMemoryIdempotencyStore} usado en los tests del caso de uso.
     * {@code deleteExpired} elimina exactamente los registros con {@code expiresAt < now} y
     * conserva los que tienen {@code expiresAt >= now}, devolviendo el conteo de eliminados.
     * Solo se implementan las operaciones relevantes para Property 9 ({@code reserve},
     * {@code find} y {@code deleteExpired}); las demás no se usan en este test.
     */
    private static final class ExpirationPartitioningInMemoryStore implements IdempotencyStorePort {

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
        public Mono<Long> deleteExpired(Instant now) {
            return Mono.defer(() -> {
                long[] removed = {0L};
                // Elimina SOLO los registros con expiresAt < now; conserva expiresAt >= now.
                store.values().removeIf(record -> {
                    boolean expired = record.getExpiresAt().isBefore(now);
                    if (expired) {
                        removed[0]++;
                    }
                    return expired;
                });
                return Mono.just(removed[0]);
            });
        }

        @Override
        public Mono<IdempotencyRecord> complete(IdempotencyKey key, Transaction result) {
            return Mono.error(new UnsupportedOperationException("no usado en Property 9"));
        }

        @Override
        public Mono<Void> markFailedOrRelease(IdempotencyKey key) {
            return Mono.error(new UnsupportedOperationException("no usado en Property 9"));
        }

        private String canonical(IdempotencyKey key) {
            return key.getOperationNumber() + "_" + key.getChannel();
        }
    }
}
