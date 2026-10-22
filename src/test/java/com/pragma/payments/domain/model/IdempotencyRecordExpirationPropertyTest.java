package com.pragma.payments.domain.model;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.LongRange;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests de propiedad (jqwik) para el cálculo de la marca de expiración de
 * {@link IdempotencyRecord}.
 *
 * Verifica el invariante de dominio: {@code expiresAt = createdAt + TTL} y,
 * para un TTL positivo, {@code expiresAt > createdAt}.
 */
class IdempotencyRecordExpirationPropertyTest {

    /**
     * Instantes de creación razonables: desde el epoch (1970-01-01) hasta el año
     * ~2262, acotando el rango para evitar desbordamientos al sumar el TTL.
     */
    @Provide
    Arbitrary<Instant> createdAtInstants() {
        return Arbitraries.longs()
                .between(0L, 9_000_000_000L) // segundos desde epoch (~hasta el año 2255)
                .map(Instant::ofEpochSecond);
    }

    /**
     * TTL estrictamente positivo (entre 1 segundo y 365 días), expresado como
     * {@link Duration}, cubriendo el espacio de TTL válidos del dominio.
     */
    @Provide
    Arbitrary<Duration> positiveTtls() {
        return Arbitraries.longs()
                .between(1L, Duration.ofDays(365).getSeconds())
                .map(Duration::ofSeconds);
    }

    // Feature: implementacion-idempotencia, Property 8: Cálculo de la marca de expiración
    // Para cualquier instante de creación y cualquier TTL positivo configurado, el
    // Idempotency_Record creado cumple expiresAt = createdAt + TTL y expiresAt > createdAt.
    // Validates: Requirements 6.1
    @Property(tries = 100)
    void expirationMarkEqualsCreatedAtPlusTtlAndIsStrictlyAfterCreatedAt(
            @ForAll("createdAtInstants") Instant createdAt,
            @ForAll("positiveTtls") Duration ttl) {

        IdempotencyKey key = IdempotencyKey.fromTransaction("OP-123", "web");

        IdempotencyRecord record = IdempotencyRecord.reserve(key, createdAt, ttl);

        // (1) expiresAt == createdAt + ttl
        assertThat(record.getExpiresAt()).isEqualTo(createdAt.plus(ttl));

        // (2) expiresAt > createdAt para TTL positivo
        assertThat(record.getExpiresAt()).isAfter(createdAt);
    }

    // Feature: implementacion-idempotencia, Property 8: Cálculo de la marca de expiración
    // Robustez: un TTL cero o negativo rompe el invariante expiresAt > createdAt y debe ser rechazado.
    // Validates: Requirements 6.1
    @Property(tries = 100)
    void rejectsNonPositiveTtl(
            @ForAll("createdAtInstants") Instant createdAt,
            @ForAll @LongRange(min = -365L * 24 * 3600, max = 0L) long nonPositiveSeconds) {

        IdempotencyKey key = IdempotencyKey.fromTransaction("OP-123", "web");
        Duration nonPositiveTtl = Duration.ofSeconds(nonPositiveSeconds);

        assertThatThrownBy(() -> IdempotencyRecord.reserve(key, createdAt, nonPositiveTtl))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
