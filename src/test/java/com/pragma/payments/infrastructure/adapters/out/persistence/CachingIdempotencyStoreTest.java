package com.pragma.payments.infrastructure.adapters.out.persistence;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests unitarios (basados en ejemplos) de {@link CachingIdempotencyStore}, el decorator de caché
 * de lectura sobre {@link IdempotencyStoreAdapter} (D3, H6).
 *
 * <p>Cubre:</p>
 * <ul>
 *   <li>Hit/miss de caché en {@code find} (Requirements 4.2, 4.3).</li>
 *   <li>Cachear únicamente registros {@code COMPLETED}; nunca {@code IN_PROGRESS}/{@code FAILED}
 *       (Requirement 4.1).</li>
 *   <li>Poblado de caché en {@code complete} (Requirement 4.4).</li>
 *   <li>Invalidación en {@code reserve}, {@code markFailedOrRelease} y {@code deleteExpired}
 *       (Requirement 4.5).</li>
 *   <li>Degradación ante error de caché: se delega en el store sin propagar el error
 *       (Requirement 4.7).</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("CachingIdempotencyStore - decorator de cache de lectura de idempotencia")
class CachingIdempotencyStoreTest {

    private static final Duration TTL = Duration.ofHours(24);

    @Mock
    private IdempotencyStorePort delegate;

    private IdempotencyKey key;

    @BeforeEach
    void setUp() {
        key = new IdempotencyKey("OP-1", "web");
    }

    private Cache<String, IdempotencyRecord> realCache() {
        return Caffeine.newBuilder().maximumSize(1000).expireAfterWrite(Duration.ofHours(1)).build();
    }

    private Transaction completedResult() {
        return Transaction.builder()
                .id("tx-1")
                .idempotencyKey(key)
                .accountId("acc-1")
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .channel("web")
                .status(TransactionStatus.COMPLETED)
                .build();
    }

    private IdempotencyRecord completedRecord() {
        Instant now = Instant.now();
        return new IdempotencyRecord(key, RecordStatus.IN_PROGRESS, null, now, now.plus(TTL))
                .withResult(completedResult());
    }

    private IdempotencyRecord inProgressRecord() {
        return IdempotencyRecord.reserve(key, Instant.now(), TTL);
    }

    private IdempotencyRecord failedRecord() {
        return IdempotencyRecord.reserve(key, Instant.now(), TTL).asFailed();
    }

    @Nested
    @DisplayName("find - lectura con cache")
    class Find {

        @Test
        @DisplayName("miss -> delega en el store y cachea el registro COMPLETED")
        void missDelegatesAndCachesCompleted() {
            Cache<String, IdempotencyRecord> cache = realCache();
            IdempotencyRecord completed = completedRecord();
            when(delegate.find(key)).thenReturn(Mono.just(completed));
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, cache);

            StepVerifier.create(store.find(key))
                    .assertNext(r -> assertThat(r).isSameAs(completed))
                    .verifyComplete();

            // La entrada quedó cacheada (se pobló en el miss por ser COMPLETED).
            assertThat(cache.getIfPresent(key.toUniqueString())).isSameAs(completed);
            verify(delegate).find(key);
        }

        @Test
        @DisplayName("hit COMPLETED -> devuelve desde cache SIN consultar el store")
        void hitReturnsFromCacheWithoutDelegating() {
            Cache<String, IdempotencyRecord> cache = realCache();
            IdempotencyRecord completed = completedRecord();
            cache.put(key.toUniqueString(), completed);
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, cache);

            StepVerifier.create(store.find(key))
                    .assertNext(r -> assertThat(r).isSameAs(completed))
                    .verifyComplete();

            verify(delegate, never()).find(any());
        }

        @Test
        @DisplayName("miss IN_PROGRESS -> delega pero NO cachea (estado mutable)")
        void missInProgressDelegatesButDoesNotCache() {
            Cache<String, IdempotencyRecord> cache = realCache();
            when(delegate.find(key)).thenReturn(Mono.just(inProgressRecord()));
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, cache);

            StepVerifier.create(store.find(key))
                    .assertNext(r -> assertThat(r.getStatus()).isEqualTo(RecordStatus.IN_PROGRESS))
                    .verifyComplete();

            assertThat(cache.getIfPresent(key.toUniqueString())).isNull();
        }

        @Test
        @DisplayName("miss FAILED -> delega pero NO cachea (estado mutable)")
        void missFailedDelegatesButDoesNotCache() {
            Cache<String, IdempotencyRecord> cache = realCache();
            when(delegate.find(key)).thenReturn(Mono.just(failedRecord()));
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, cache);

            StepVerifier.create(store.find(key))
                    .assertNext(r -> assertThat(r.getStatus()).isEqualTo(RecordStatus.FAILED))
                    .verifyComplete();

            assertThat(cache.getIfPresent(key.toUniqueString())).isNull();
        }

        @Test
        @DisplayName("miss vacio (ausente) -> delega y no cachea nada")
        void missEmptyDelegatesAndCachesNothing() {
            Cache<String, IdempotencyRecord> cache = realCache();
            when(delegate.find(key)).thenReturn(Mono.empty());
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, cache);

            StepVerifier.create(store.find(key)).verifyComplete();

            assertThat(cache.getIfPresent(key.toUniqueString())).isNull();
        }
    }

    @Nested
    @DisplayName("complete - poblado de cache")
    class Complete {

        @Test
        @DisplayName("complete -> delega y puebla la cache con el registro COMPLETED")
        void completeDelegatesAndPopulatesCache() {
            Cache<String, IdempotencyRecord> cache = realCache();
            IdempotencyRecord completed = completedRecord();
            Transaction result = completedResult();
            when(delegate.complete(key, result)).thenReturn(Mono.just(completed));
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, cache);

            StepVerifier.create(store.complete(key, result))
                    .assertNext(r -> assertThat(r).isSameAs(completed))
                    .verifyComplete();

            assertThat(cache.getIfPresent(key.toUniqueString())).isSameAs(completed);
        }
    }

    @Nested
    @DisplayName("invalidacion de cache (coherencia)")
    class Invalidation {

        @Test
        @DisplayName("reserve -> delega e invalida la entrada previa en cache")
        void reserveInvalidatesCacheEntry() {
            Cache<String, IdempotencyRecord> cache = realCache();
            cache.put(key.toUniqueString(), completedRecord());
            IdempotencyRecord reservation = inProgressRecord();
            when(delegate.reserve(reservation)).thenReturn(Mono.just(reservation));
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, cache);

            StepVerifier.create(store.reserve(reservation))
                    .assertNext(r -> assertThat(r.getStatus()).isEqualTo(RecordStatus.IN_PROGRESS))
                    .verifyComplete();

            assertThat(cache.getIfPresent(key.toUniqueString())).isNull();
        }

        @Test
        @DisplayName("markFailedOrRelease -> delega e invalida la entrada en cache")
        void markFailedInvalidatesCacheEntry() {
            Cache<String, IdempotencyRecord> cache = realCache();
            cache.put(key.toUniqueString(), completedRecord());
            when(delegate.markFailedOrRelease(key)).thenReturn(Mono.empty());
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, cache);

            StepVerifier.create(store.markFailedOrRelease(key)).verifyComplete();

            assertThat(cache.getIfPresent(key.toUniqueString())).isNull();
        }

        @Test
        @DisplayName("deleteExpired -> delega e invalida TODA la cache")
        void deleteExpiredInvalidatesAll() {
            Cache<String, IdempotencyRecord> cache = realCache();
            cache.put(key.toUniqueString(), completedRecord());
            cache.put("OTHER_web", completedRecord());
            Instant now = Instant.now();
            when(delegate.deleteExpired(now)).thenReturn(Mono.just(2L));
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, cache);

            StepVerifier.create(store.deleteExpired(now))
                    .assertNext(count -> assertThat(count).isEqualTo(2L))
                    .verifyComplete();

            cache.cleanUp();
            assertThat(cache.estimatedSize()).isZero();
        }
    }

    @Nested
    @DisplayName("degradacion ante error de cache (Requirement 4.7)")
    class Degradation {

        @Test
        @DisplayName("find con cache que falla al leer -> delega en el store sin propagar el error")
        void findFallsBackToStoreWhenCacheReadFails() {
            @SuppressWarnings("unchecked")
            Cache<String, IdempotencyRecord> failingCache = mock(Cache.class);
            doThrow(new RuntimeException("cache boom")).when(failingCache).getIfPresent(anyString());
            IdempotencyRecord completed = completedRecord();
            when(delegate.find(key)).thenReturn(Mono.just(completed));
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, failingCache);

            // El error de la caché no se propaga: la lectura se resuelve desde el store.
            StepVerifier.create(store.find(key))
                    .assertNext(r -> assertThat(r).isSameAs(completed))
                    .verifyComplete();

            verify(delegate).find(key);
        }

        @Test
        @DisplayName("find con cache que falla al escribir -> devuelve el resultado del store igualmente")
        void findFallsBackWhenCacheWriteFails() {
            @SuppressWarnings("unchecked")
            Cache<String, IdempotencyRecord> failingCache = mock(Cache.class);
            when(failingCache.getIfPresent(anyString())).thenReturn(null);
            doThrow(new RuntimeException("cache write boom")).when(failingCache).put(anyString(), any());
            IdempotencyRecord completed = completedRecord();
            when(delegate.find(key)).thenReturn(Mono.just(completed));
            CachingIdempotencyStore store = new CachingIdempotencyStore(delegate, failingCache);

            StepVerifier.create(store.find(key))
                    .assertNext(r -> assertThat(r).isSameAs(completed))
                    .verifyComplete();
        }
    }
}
