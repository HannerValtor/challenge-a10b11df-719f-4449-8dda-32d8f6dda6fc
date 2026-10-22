package com.pragma.payments.infrastructure.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.netty.resources.ConnectionProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Tests unitarios del pool de conexiones acotado (Fase 1).
 * Valida los Requirements 2.5 (maxConnections=100) y 2.6 (pendingAcquireMaxCount finito).
 */
@DisplayName("WebClientConfig - Pool de conexiones acotado")
class WebClientConfigTest {

    private WebClientConfig newConfigWithDefaults() {
        WebClientConfig config = new WebClientConfig();
        // Inyecta los valores @Value manualmente para un test unitario puro (sin contexto Spring).
        ReflectionTestUtils.setField(config, "connectionTimeoutMs", 5000);
        ReflectionTestUtils.setField(config, "responseTimeoutMs", 10000);
        ReflectionTestUtils.setField(config, "maxConnections", 100);
        return config;
    }

    @Test
    @DisplayName("El ConnectionProvider limita las conexiones máximas a 100")
    void connectionProviderLimitsMaxConnectionsTo100() {
        ConnectionProvider provider = newConfigWithDefaults().connectionProvider();

        assertThat(provider.maxConnections())
                .as("maxConnections del pool external-services-pool")
                .isEqualTo(100);
    }

    @Test
    @DisplayName("La cola de adquisición está acotada (pendingAcquireMaxCount finito > 0, no -1 ilimitado)")
    void connectionProviderUsesBoundedAcquireQueue() {
        // El valor es una decisión de diseño expuesta como constante verificable; la API de
        // ConnectionProvider no ofrece getter público para pendingAcquireMaxCount.
        assertThat(WebClientConfig.PENDING_ACQUIRE_MAX_COUNT)
                .as("pendingAcquireMaxCount finito y acotado (no -1 ilimitado)")
                .isGreaterThan(0)
                .isNotEqualTo(-1);

        // Y el bean se construye sin error con esa configuración.
        assertThatCode(() -> newConfigWithDefaults().connectionProvider())
                .doesNotThrowAnyException();
    }
}
