package com.pragma.payments.infrastructure.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.pragma.payments.domain.model.IdempotencyRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Configuración de la caché de lectura de idempotencia (Fase 3, D3, H6).
 *
 * <p>Expone una instancia Caffeine acotada ({@code maximumSize}) y con expiración por escritura
 * ({@code expireAfterWrite}) que el decorator {@code CachingIdempotencyStore} usa para servir
 * lecturas de registros {@code COMPLETED} sin golpear la base de datos (Requirements 4.1, 4.2).</p>
 *
 * <p><strong>Invariante de coherencia (Requirement 4.6):</strong> el TTL de la caché debe ser
 * menor o igual que el TTL de idempotencia ({@code payments.idempotency.ttl}) para que ninguna
 * entrada sobreviva a la expiración de su Idempotency_Record. Si la configuración declara un TTL
 * de caché mayor, aquí se acota al TTL de idempotencia y se registra una advertencia, en lugar de
 * fallar el arranque: así la coherencia queda garantizada de forma defensiva.</p>
 *
 * <p>La clave de caché es la forma canónica {@code operationNumber + "_" + channel}
 * ({@code IdempotencyKey.toUniqueString()}); el valor es un {@link IdempotencyRecord} en estado
 * {@code COMPLETED} (inmutable).</p>
 */
@Configuration
public class CacheConfig {

    private static final Logger log = LoggerFactory.getLogger(CacheConfig.class);

    /**
     * Instancia Caffeine para la caché de lectura de idempotencia. El TTL efectivo es
     * {@code min(cache.ttl, idempotency.ttl)} para preservar el invariante del Requirement 4.6.
     *
     * @param cacheProperties       parámetros de la caché de idempotencia ({@code maximumSize}, {@code ttl})
     * @param idempotencyProperties parámetros de idempotencia, de los que se toma el TTL de los registros
     * @return una caché Caffeine acotada y con expiración por escritura coherente con el TTL de idempotencia
     */
    @Bean
    public Cache<String, IdempotencyRecord> idempotencyRecordCache(
            IdempotencyCacheProperties cacheProperties,
            IdempotencyProperties idempotencyProperties) {

        Duration idempotencyTtl = idempotencyProperties.getTtl();
        Duration configuredCacheTtl = cacheProperties.getTtl();

        // Invariante 4.6: TTL de caché <= TTL de idempotencia. Se acota de forma defensiva.
        Duration effectiveTtl = configuredCacheTtl;
        if (configuredCacheTtl.compareTo(idempotencyTtl) > 0) {
            log.warn("El TTL de la caché de idempotencia ({}) supera el TTL de idempotencia ({}); "
                            + "se acota a {} para preservar la coherencia (Requirement 4.6).",
                    configuredCacheTtl, idempotencyTtl, idempotencyTtl);
            effectiveTtl = idempotencyTtl;
        }

        log.info("Caché de lectura de idempotencia inicializada: maximumSize={}, expireAfterWrite={}",
                cacheProperties.getMaximumSize(), effectiveTtl);

        return Caffeine.newBuilder()
                .maximumSize(cacheProperties.getMaximumSize())
                .expireAfterWrite(effectiveTtl)
                .build();
    }
}
