package com.pragma.payments.infrastructure.adapters.out.fraud;

import com.github.benmanes.caffeine.cache.Cache;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.config.ExternalTimeoutProperties;
import com.pragma.payments.infrastructure.config.FraudCacheProperties;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests unitarios de la caché determinista y acotada de {@link FraudDetectionAdapter} (tarea 3.4).
 *
 * <p>Cubren los Requirements 5.2, 5.4 y 5.5:</p>
 * <ul>
 *   <li><b>5.2 (clave determinista):</b> dos transacciones con los mismos atributos de entrada
 *       (cuenta, monto, moneda, canal) producen siempre la misma clave, independientemente de ids
 *       por solicitud o marcas de tiempo; atributos distintos producen claves distintas.</li>
 *   <li><b>5.5 (acotamiento):</b> el número de entradas nunca supera el {@code maximumSize}
 *       configurado (desalojo del menos usado al insertar sobre el máximo).</li>
 *   <li><b>5.4 (expiración):</b> una entrada vencida por {@code expireAfterWrite} se descarta y deja
 *       de estar presente, forzando el recálculo posterior.</li>
 * </ul>
 */
@DisplayName("FraudDetectionAdapter - Caché determinista y acotada")
class FraudDetectionAdapterTest {

    private FraudDetectionAdapter buildAdapter(FraudCacheProperties cacheProperties) {
        return new FraudDetectionAdapter(
                WebClient.builder().build(),
                BulkheadRegistry.ofDefaults(),
                new ExternalTimeoutProperties(),
                cacheProperties);
    }

    private Transaction transaction(String id, String account, String amount, String currency, String channel) {
        return Transaction.builder()
                .id(id)
                .accountId(account)
                .amount(new BigDecimal(amount))
                .currency(currency)
                .channel(channel)
                .idempotencyKey(new IdempotencyKey("OP-" + id, channel))
                .build();
    }

    private String buildCacheKey(FraudDetectionAdapter adapter, Transaction tx) throws Exception {
        Method m = FraudDetectionAdapter.class.getDeclaredMethod("buildCacheKey", Transaction.class);
        m.setAccessible(true);
        return (String) m.invoke(adapter, tx);
    }

    @SuppressWarnings("unchecked")
    private Cache<String, Boolean> fraudCache(FraudDetectionAdapter adapter) throws Exception {
        Field f = FraudDetectionAdapter.class.getDeclaredField("fraudCache");
        f.setAccessible(true);
        return (Cache<String, Boolean>) f.get(adapter);
    }

    @Nested
    @DisplayName("Clave determinista (Requirement 5.2)")
    class DeterministicKey {

        @Test
        @DisplayName("Mismos atributos de entrada -> misma clave, pese a id y timestamp distintos")
        void sameAttributesProduceSameKey() throws Exception {
            FraudDetectionAdapter adapter = buildAdapter(new FraudCacheProperties());

            Transaction a = transaction(UUID.randomUUID().toString(), "ACC-1", "100.00", "USD", "WEB");
            Transaction b = transaction(UUID.randomUUID().toString(), "ACC-1", "100.00", "USD", "WEB");

            String keyA = buildCacheKey(adapter, a);
            String keyB = buildCacheKey(adapter, b);

            assertThat(keyA).isEqualTo(keyB);
            // La clave no debe depender de datos por solicitud ni del tiempo del sistema.
            assertThat(keyA).doesNotContain(a.getId());
        }

        @Test
        @DisplayName("Atributos distintos -> claves distintas")
        void differentAttributesProduceDifferentKeys() throws Exception {
            FraudDetectionAdapter adapter = buildAdapter(new FraudCacheProperties());

            Transaction base = transaction("1", "ACC-1", "100.00", "USD", "WEB");
            Transaction diffAccount = transaction("2", "ACC-2", "100.00", "USD", "WEB");
            Transaction diffAmount = transaction("3", "ACC-1", "200.00", "USD", "WEB");
            Transaction diffCurrency = transaction("4", "ACC-1", "100.00", "EUR", "WEB");
            Transaction diffChannel = transaction("5", "ACC-1", "100.00", "USD", "MOBILE");

            String baseKey = buildCacheKey(adapter, base);
            assertThat(baseKey).isNotEqualTo(buildCacheKey(adapter, diffAccount));
            assertThat(baseKey).isNotEqualTo(buildCacheKey(adapter, diffAmount));
            assertThat(baseKey).isNotEqualTo(buildCacheKey(adapter, diffCurrency));
            assertThat(baseKey).isNotEqualTo(buildCacheKey(adapter, diffChannel));
        }
    }

    @Nested
    @DisplayName("Acotamiento por maximumSize (Requirement 5.5)")
    class BoundedCache {

        @Test
        @DisplayName("El número de entradas nunca supera el maximumSize configurado")
        void sizeNeverExceedsMaximum() throws Exception {
            FraudCacheProperties props = new FraudCacheProperties();
            props.setMaximumSize(5);
            FraudDetectionAdapter adapter = buildAdapter(props);
            Cache<String, Boolean> cache = fraudCache(adapter);

            for (int i = 0; i < 100; i++) {
                cache.put("key-" + i, i % 2 == 0);
            }
            // Caffeine aplica el desalojo de forma asíncrona; cleanUp fuerza el mantenimiento.
            await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> {
                        cache.cleanUp();
                        assertThat(cache.estimatedSize()).isLessThanOrEqualTo(5L);
                    });
        }
    }

    @Nested
    @DisplayName("Expiración por expireAfterWrite (Requirement 5.4)")
    class ExpiringCache {

        @Test
        @DisplayName("Una entrada vencida se descarta y deja de estar presente")
        void entryExpiresAfterWrite() throws Exception {
            FraudCacheProperties props = new FraudCacheProperties();
            props.setExpireAfterWrite(Duration.ofMillis(100));
            FraudDetectionAdapter adapter = buildAdapter(props);
            Cache<String, Boolean> cache = fraudCache(adapter);

            cache.put("ACC-1_100.00_USD_WEB", Boolean.FALSE);
            assertThat(cache.getIfPresent("ACC-1_100.00_USD_WEB")).isNotNull();

            // Tras superar expireAfterWrite la entrada debe descartarse.
            await().atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> {
                        cache.cleanUp();
                        assertThat(cache.getIfPresent("ACC-1_100.00_USD_WEB")).isNull();
                    });
        }
    }
}
