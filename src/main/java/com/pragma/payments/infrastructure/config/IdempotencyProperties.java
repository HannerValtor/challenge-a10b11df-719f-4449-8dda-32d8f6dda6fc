package com.pragma.payments.infrastructure.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Propiedades de configuración externalizadas de la idempotencia persistente (Fase 2).
 *
 * <p>Se enlazan desde {@code application.yml} bajo el prefijo {@code payments.idempotency}.
 * El registro de esta clase lo realiza {@code @ConfigurationPropertiesScan} declarado en
 * {@code Application}, por lo que no requiere {@code @EnableConfigurationProperties}.</p>
 *
 * <ul>
 *   <li><b>ttl</b>: periodo de vida de un Idempotency_Record antes de ser elegible para
 *       eliminación; se usa para calcular {@code expiresAt = createdAt + ttl}
 *       (Requirements 6.1, 6.2).</li>
 *   <li><b>cleanup.fixedDelay</b>: periodicidad con la que el Cleanup_Process barre y elimina
 *       los records expirados (Requirement 6.2).</li>
 * </ul>
 *
 * <p>Ambos valores se expresan como {@link Duration} en formato ISO-8601 (p. ej. {@code PT24H},
 * {@code PT1H}).</p>
 */
@ConfigurationProperties(prefix = "payments.idempotency")
public class IdempotencyProperties {

    /** TTL de un Idempotency_Record (ISO-8601). Por defecto {@code PT24H}. */
    private Duration ttl = Duration.ofHours(24);

    /** Configuración del proceso de limpieza por expiración. */
    private final Cleanup cleanup = new Cleanup();

    public Duration getTtl() {
        return ttl;
    }

    public void setTtl(Duration ttl) {
        this.ttl = ttl;
    }

    public Cleanup getCleanup() {
        return cleanup;
    }

    /**
     * Parámetros del Cleanup_Process programado.
     */
    public static class Cleanup {

        /** Periodicidad del barrido de expiración (ISO-8601). Por defecto {@code PT1H}. */
        private Duration fixedDelay = Duration.ofHours(1);

        public Duration getFixedDelay() {
            return fixedDelay;
        }

        public void setFixedDelay(Duration fixedDelay) {
            this.fixedDelay = fixedDelay;
        }
    }
}
