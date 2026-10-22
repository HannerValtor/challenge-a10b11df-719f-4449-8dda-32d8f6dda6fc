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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test de propiedad para la reserva atómica única por el camino secuencial.
 *
 * Ejercita el contrato de {@link IdempotencyStorePort#reserve} sobre un doble de test en memoria
 * que respeta la restricción UNIQUE de forma atómica (equivalente a la del almacenamiento
 * persistente): la primera reserva de una clave gana y deja el registro en {@code IN_PROGRESS};
 * cualquier reserva posterior de la misma clave falla con {@link DuplicateReservationException}
 * y se trata como solicitud duplicada.
 */
class AtomicReservationPropertyTest {

    // Feature: implementacion-idempotencia, Property 6: Reserva atómica única (camino secuencial)
    // Validates: Requirements 4.1, 4.3
    //
    // Para cualquier clave inexistente en el store, una operación reserve crea exactamente un
    // Idempotency_Record en estado IN_PROGRESS; un segundo (y cualquier posterior) reserve sobre
    // la misma clave falla por violación de unicidad y se trata como Duplicate_Request.
    @Property(tries = 100)
    void firstReserveWinsAndSubsequentReservesOnSameKeyFailAsDuplicate(
            @ForAll("idempotencyKeys") IdempotencyKey key,
            @ForAll @IntRange(min = 1, max = 10) int subsequentAttempts) {

        IdempotencyStorePort store = new UniqueConstrainedInMemoryStore();
        Instant now = Instant.now();
        Duration ttl = Duration.ofHours(24);

        // 1) La PRIMERA reserve sobre una clave inexistente tiene éxito y emite IN_PROGRESS.
        IdempotencyRecord first = IdempotencyRecord.reserve(key, now, ttl);
        IdempotencyRecord reserved = store.reserve(first).block();

        assertThat(reserved).as("la primera reserva emite el registro reservado").isNotNull();
        assertThat(reserved.getStatus())
                .as("el registro reservado queda en IN_PROGRESS")
                .isEqualTo(RecordStatus.IN_PROGRESS);
        assertThat(reserved.getKey()).isEqualTo(key);

        // Se creó exactamente un registro para la clave.
        IdempotencyRecord stored = store.find(key).block();
        assertThat(stored).as("existe exactamente un registro tras la primera reserva").isNotNull();
        assertThat(stored.getStatus()).isEqualTo(RecordStatus.IN_PROGRESS);

        // 2) CUALQUIER reserve posterior con la misma clave falla por violación de unicidad
        //    (se trata como Duplicate_Request) y no altera el registro ganador.
        for (int attempt = 0; attempt < subsequentAttempts; attempt++) {
            IdempotencyRecord duplicate = IdempotencyRecord.reserve(key, now.plusSeconds(attempt + 1L), ttl);
            assertThatThrownBy(() -> store.reserve(duplicate).block())
                    .as("la reserva duplicada nº %d se rechaza como Duplicate_Request", attempt + 1)
                    .isInstanceOf(DuplicateReservationException.class);

            // El registro persistido sigue siendo el de la PRIMERA reserva (el ganador no cambia).
            IdempotencyRecord afterDuplicate = store.find(key).block();
            assertThat(afterDuplicate)
                    .as("el ganador original se preserva tras la reserva duplicada")
                    .isEqualTo(reserved);
        }
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
     * {@code InMemoryIdempotencyStore} usado en los tests del caso de uso. Solo se implementan
     * las operaciones relevantes para Property 6 ({@code reserve} y {@code find}); las demás no
     * se usan en este test secuencial.
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
            return Mono.error(new UnsupportedOperationException("no usado en Property 6"));
        }

        @Override
        public Mono<Void> markFailedOrRelease(IdempotencyKey key) {
            return Mono.error(new UnsupportedOperationException("no usado en Property 6"));
        }

        @Override
        public Mono<Long> deleteExpired(Instant now) {
            return Mono.error(new UnsupportedOperationException("no usado en Property 6"));
        }

        private String canonical(IdempotencyKey key) {
            return key.getOperationNumber() + "_" + key.getChannel();
        }
    }
}
