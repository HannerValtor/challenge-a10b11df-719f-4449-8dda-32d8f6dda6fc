package com.pragma.payments.infrastructure.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test de binding de la configuración de idempotencia (Tarea 6.2).
 *
 * <p>Verifica que las propiedades con prefijo {@code payments.idempotency}
 * ({@code ttl} y {@code cleanup.fixed-delay}) se enlazan correctamente como
 * {@link Duration} ISO-8601 en {@link IdempotencyProperties}, y que los valores
 * por defecto (PT24H / PT1H) aplican cuando no se especifican propiedades.</p>
 *
 * <p>Se usa {@link ApplicationContextRunner} para un arranque ligero del contexto
 * que habilita únicamente {@code @EnableConfigurationProperties(IdempotencyProperties.class)},
 * sin levantar el contexto completo de la aplicación.</p>
 *
 * Validates: Requirements 6.2
 */
class IdempotencyPropertiesBindingTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(
                    ConfigurationPropertiesAutoConfiguration.class))
            .withUserConfiguration(TestConfig.class);

    @Test
    void bindsTtlAndFixedDelayFromProperties() {
        contextRunner
                .withPropertyValues(
                        "payments.idempotency.ttl=PT12H",
                        "payments.idempotency.cleanup.fixed-delay=PT30M")
                .run(context -> {
                    assertThat(context).hasSingleBean(IdempotencyProperties.class);
                    IdempotencyProperties props = context.getBean(IdempotencyProperties.class);

                    assertThat(props.getTtl()).isEqualTo(Duration.ofHours(12));
                    assertThat(props.getCleanup().getFixedDelay()).isEqualTo(Duration.ofMinutes(30));
                });
    }

    @Test
    void appliesDefaultsWhenPropertiesAreAbsent() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(IdempotencyProperties.class);
            IdempotencyProperties props = context.getBean(IdempotencyProperties.class);

            assertThat(props.getTtl()).isEqualTo(Duration.ofHours(24));
            assertThat(props.getCleanup().getFixedDelay()).isEqualTo(Duration.ofHours(1));
        });
    }

    @EnableConfigurationProperties(IdempotencyProperties.class)
    static class TestConfig {
    }
}
