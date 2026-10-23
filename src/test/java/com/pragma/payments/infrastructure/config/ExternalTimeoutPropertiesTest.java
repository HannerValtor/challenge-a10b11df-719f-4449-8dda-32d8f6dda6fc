package com.pragma.payments.infrastructure.config;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests de propiedad (jqwik) para {@link ExternalTimeoutProperties}.
 *
 * <p>Feature: optimizacion-refactor-fase3, Property 5: Requisito de timeout del buró.</p>
 *
 * <p>Verifica el invariante de configuración del reto: el timeout del buró de riesgo debe ser
 * estrictamente mayor que 2 segundos. Se comprueban las dos caras del invariante: (1) para
 * cualquier configuración válida ({@code risk > 2000ms}) la propiedad enlazada satisface
 * {@code getRisk() > Duration.ofSeconds(2)} y la validación de arranque tiene éxito; (2) para
 * cualquier configuración inválida ({@code risk <= 2000ms}) la validación de arranque
 * ({@code @PostConstruct validate()}) falla con {@link IllegalStateException}.</p>
 */
class ExternalTimeoutPropertiesTest {

    /** Umbral exclusivo del reto: el timeout de riesgo debe ser estrictamente mayor que 2s. */
    private static final long RISK_MINIMUM_MILLIS = 2000L;

    /**
     * Timeouts de riesgo VÁLIDOS: estrictamente mayores que 2000ms (desde 2001ms hasta 60000ms),
     * cubriendo el espacio de configuraciones aceptables del buró.
     */
    @Provide
    Arbitrary<Duration> validRiskTimeouts() {
        return Arbitraries.longs()
                .between(RISK_MINIMUM_MILLIS + 1, 60_000L)
                .map(Duration::ofMillis);
    }

    /**
     * Timeouts de riesgo INVÁLIDOS: menores o iguales a 2000ms (desde 0ms hasta 2000ms inclusive),
     * cubriendo el borde del invariante (exactamente 2000ms también es inválido).
     */
    @Provide
    Arbitrary<Duration> invalidRiskTimeouts() {
        return Arbitraries.longs()
                .between(0L, RISK_MINIMUM_MILLIS)
                .map(Duration::ofMillis);
    }

    /**
     * Timeouts positivos arbitrarios para fraude y core (no afectan al invariante de riesgo pero
     * deben poder coexistir con cualquier configuración de riesgo).
     */
    @Provide
    Arbitrary<Duration> positiveTimeouts() {
        return Arbitraries.longs()
                .between(1L, 10_000L)
                .map(Duration::ofMillis);
    }

    // Property 5: Requisito de timeout del buró
    // Para cualquier configuración válida, el timeout de riesgo enlazado es estrictamente mayor que
    // 2 segundos y la validación de arranque tiene éxito.
    // Validates: Requirements 1.2, 1.4
    @Property(tries = 100)
    void validRiskTimeoutIsStrictlyGreaterThanTwoSecondsAndStartupSucceeds(
            @ForAll("validRiskTimeouts") Duration risk,
            @ForAll("positiveTimeouts") Duration fraud,
            @ForAll("positiveTimeouts") Duration core) {

        ExternalTimeoutProperties properties = new ExternalTimeoutProperties();
        properties.setRisk(risk);
        properties.setFraud(fraud);
        properties.setCore(core);

        // (1) risk() > 2s
        assertThat(properties.getRisk()).isGreaterThan(Duration.ofSeconds(2));

        // (2) el arranque (validación @PostConstruct) tiene éxito para configuración válida
        properties.validate();
    }

    // Property 5: Requisito de timeout del buró
    // Para cualquier configuración inválida (risk <= 2000ms), la validación de arranque falla con
    // IllegalStateException indicando configuración de timeout inválida.
    // Validates: Requirements 1.4
    @Property(tries = 100)
    void invalidRiskTimeoutFailsStartupValidation(
            @ForAll("invalidRiskTimeouts") Duration risk,
            @ForAll("positiveTimeouts") Duration fraud,
            @ForAll("positiveTimeouts") Duration core) {

        ExternalTimeoutProperties properties = new ExternalTimeoutProperties();
        properties.setRisk(risk);
        properties.setFraud(fraud);
        properties.setCore(core);

        // risk <= 2s no satisface el invariante del reto
        assertThat(properties.getRisk()).isLessThanOrEqualTo(Duration.ofSeconds(2));

        // el arranque debe fallar con un error de configuración inválida
        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalStateException.class);
    }
}
