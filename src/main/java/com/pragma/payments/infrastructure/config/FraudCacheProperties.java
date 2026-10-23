package com.pragma.payments.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Propiedades de configuración externalizadas de la caché determinista de resultados de fraude
 * (Fase 3, D4, H5).
 *
 * <p>Se enlazan desde {@code application.yml} bajo el prefijo {@code fraud.cache}. El registro de
 * esta clase lo realiza {@code @ConfigurationPropertiesScan} declarado en {@code Application}, por
 * lo que no requiere una declaración adicional de {@code @EnableConfigurationProperties}.</p>
 *
 * <p>Reemplaza el dimensionamiento ad-hoc del antiguo {@code ConcurrentHashMap} ilimitado por una
 * caché Caffeine acotada: un número máximo de entradas ({@code maximumSize}) y una expiración por
 * escritura ({@code expireAfterWrite}), ambos configurables externamente con valores por defecto
 * seguros (Requirements 5.1, 5.3).</p>
 *
 * <ul>
 *   <li><b>maximumSize</b>: número máximo de entradas de la caché. Por defecto {@code 10000}.</li>
 *   <li><b>expireAfterWrite</b>: expiración por escritura de cada entrada. Por defecto
 *       {@code 300s}.</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "fraud.cache")
public class FraudCacheProperties {

    /** Número máximo de entradas de la caché de fraude. Por defecto {@code 10000}. */
    private long maximumSize = 10_000L;

    /** Expiración por escritura de cada entrada de la caché de fraude. Por defecto {@code 300s}. */
    private Duration expireAfterWrite = Duration.ofSeconds(300);

    public long getMaximumSize() {
        return maximumSize;
    }

    public void setMaximumSize(long maximumSize) {
        this.maximumSize = maximumSize;
    }

    public Duration getExpireAfterWrite() {
        return expireAfterWrite;
    }

    public void setExpireAfterWrite(Duration expireAfterWrite) {
        this.expireAfterWrite = expireAfterWrite;
    }
}
