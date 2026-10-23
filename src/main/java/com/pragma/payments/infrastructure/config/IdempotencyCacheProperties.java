package com.pragma.payments.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Propiedades de configuración externalizadas de la caché de lectura de idempotencia
 * (Fase 3, D3, H6, Requirements 4.1, 4.6).
 *
 * <p>Se enlazan desde {@code application.yml} bajo el prefijo {@code payments.idempotency.cache}.
 * El registro de esta clase lo realiza {@code @ConfigurationPropertiesScan} declarado en
 * {@code Application}, por lo que no requiere {@code @EnableConfigurationProperties}.</p>
 *
 * <p>La caché almacena únicamente registros {@code COMPLETED} (inmutables) para evitar
 * re-consultar la base de datos ante solicitudes duplicadas y consultas repetidas. El TTL de la
 * caché debe ser menor o igual que el TTL de idempotencia ({@code payments.idempotency.ttl}) para
 * que ninguna entrada sobreviva a la expiración de su Idempotency_Record (Requirement 4.6); el
 * invariante se valida y acota en {@link CacheConfig}.</p>
 *
 * <ul>
 *   <li><b>maximumSize</b>: número máximo de entradas de la caché. Por defecto {@code 10000}.</li>
 *   <li><b>ttl</b>: expiración por escritura de cada entrada. Por defecto {@code PT1H}; se acota
 *       a {@code payments.idempotency.ttl} si lo supera (Requirement 4.6).</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "payments.idempotency.cache")
public class IdempotencyCacheProperties {

    /** Número máximo de entradas de la caché de idempotencia. Por defecto {@code 10000}. */
    private long maximumSize = 10_000L;

    /**
     * TTL (expiración por escritura) de cada entrada de la caché de idempotencia (ISO-8601).
     * Por defecto {@code PT1H}. Debe ser {@code <=} {@code payments.idempotency.ttl}.
     */
    private Duration ttl = Duration.ofHours(1);

    public long getMaximumSize() {
        return maximumSize;
    }

    public void setMaximumSize(long maximumSize) {
        this.maximumSize = maximumSize;
    }

    public Duration getTtl() {
        return ttl;
    }

    public void setTtl(Duration ttl) {
        this.ttl = ttl;
    }
}
