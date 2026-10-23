package com.pragma.payments.infrastructure.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Propiedades de configuración externalizadas de los timeouts por servicio externo (Fase 3, D2).
 *
 * <p>Se enlazan desde {@code application.yml} bajo el prefijo
 * {@code external.services.timeouts}. El registro de esta clase lo realiza
 * {@code @ConfigurationPropertiesScan} declarado en {@code Application}, por lo que no requiere
 * una declaración adicional de {@code @EnableConfigurationProperties}.</p>
 *
 * <p>Centralizar los timeouts aquí elimina el doble control de tiempo (H1) y establece un único
 * lugar de verdad para fraude, riesgo y core, garantizando que el buró de riesgo cumpla el
 * requisito del reto de un timeout estrictamente mayor que 2 segundos (Requirements 1.1, 1.2,
 * 1.3).</p>
 *
 * <ul>
 *   <li><b>fraud</b>: timeout del servicio de detección de fraude. Por defecto {@code 1000ms}.</li>
 *   <li><b>risk</b>: timeout del buró de riesgo. Por defecto {@code 2500ms} ({@literal >} 2s).</li>
 *   <li><b>core</b>: timeout del core bancario. Por defecto {@code 1000ms}.</li>
 * </ul>
 *
 * <p>Cada valor se expresa en milisegundos en {@code application.yml} (p. ej. {@code risk: 2500})
 * y se enlaza a un {@link Duration} para su uso directo por los adaptadores de salida.</p>
 */
@ConfigurationProperties(prefix = "external.services.timeouts")
public class ExternalTimeoutProperties {

    private static final Logger log = LoggerFactory.getLogger(ExternalTimeoutProperties.class);

    /**
     * Umbral mínimo (exclusivo) del timeout del buró de riesgo exigido por el reto: el timeout de
     * riesgo debe ser estrictamente mayor que 2 segundos (Requirement 1.4).
     */
    private static final Duration RISK_TIMEOUT_MINIMUM = Duration.ofMillis(2000);

    /** Timeout del servicio de fraude. Por defecto {@code 1000ms}. */
    private Duration fraud = Duration.ofMillis(1000);

    /** Timeout del buró de riesgo. Por defecto {@code 2500ms} (estrictamente {@literal >} 2s). */
    private Duration risk = Duration.ofMillis(2500);

    /** Timeout del core bancario. Por defecto {@code 1000ms}. */
    private Duration core = Duration.ofMillis(1000);

    /**
     * Valida el invariante de configuración del buró de riesgo en el arranque del contexto Spring
     * (Requirement 1.4): el timeout de riesgo DEBE ser estrictamente mayor que 2000 milisegundos.
     *
     * <p>Si la configuración provista incumple el invariante, se aborta el arranque lanzando una
     * {@link IllegalStateException} con un mensaje explícito que indica el valor inválido y el
     * mínimo exigido, de modo que el fallo sea diagnosticable de inmediato.</p>
     *
     * @throws IllegalStateException si {@code risk} es nulo o no es estrictamente mayor que 2000ms
     */
    @PostConstruct
    void validate() {
        if (risk == null || risk.compareTo(RISK_TIMEOUT_MINIMUM) <= 0) {
            String message = String.format(
                    "Configuración de timeout inválida: 'external.services.timeouts.risk' debe ser "
                            + "estrictamente mayor que %dms (requisito del buró de riesgo > 2s), "
                            + "pero se configuró %s. Ajuste el valor en application.yml.",
                    RISK_TIMEOUT_MINIMUM.toMillis(),
                    risk == null ? "null" : risk.toMillis() + "ms");
            log.error(message);
            throw new IllegalStateException(message);
        }
        log.info("Timeouts externos validados: fraud={}ms, risk={}ms, core={}ms",
                fraud != null ? fraud.toMillis() : null,
                risk.toMillis(),
                core != null ? core.toMillis() : null);
    }

    public Duration getFraud() {
        return fraud;
    }

    public void setFraud(Duration fraud) {
        this.fraud = fraud;
    }

    public Duration getRisk() {
        return risk;
    }

    public void setRisk(Duration risk) {
        this.risk = risk;
    }

    public Duration getCore() {
        return core;
    }

    public void setCore(Duration core) {
        this.core = core;
    }
}
