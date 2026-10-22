package com.pragma.payments.domain.model;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests de propiedad para la identidad canónica de la {@link IdempotencyKey}.
 *
 * Feature: implementacion-idempotencia, Property 1: Identidad canónica de la clave
 * Para cualquier par válido (operationNumber, channel), la forma canónica persistida
 * del Idempotency_Record es exactamente operationNumber + "_" + channel; en consecuencia,
 * dos solicitudes con el mismo operationNumber pero distinto channel producen claves
 * distintas y por tanto Idempotency_Record independientes.
 *
 * Validates: Requirements 1.3, 1.4
 */
class IdempotencyKeyCanonicalIdentityPropertyTest {

    // operationNumber conforme a ^[A-Za-z0-9\-]+$ (size 1-50)
    @Provide
    Arbitrary<String> operationNumbers() {
        return Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1)
                .ofMaxLength(50);
    }

    // channel conforme a ^[A-Za-z0-9_]+$ (size 1-20)
    @Provide
    Arbitrary<String> channels() {
        return Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")
                .ofMinLength(1)
                .ofMaxLength(20);
    }

    // Feature: implementacion-idempotencia, Property 1: Identidad canónica de la clave
    @Property(tries = 100)
    void canonicalFormIsOperationNumberUnderscoreChannel(
            @ForAll("operationNumbers") String operationNumber,
            @ForAll("channels") String channel) {

        IdempotencyKey key = new IdempotencyKey(operationNumber, channel);

        assertThat(key.toUniqueString()).isEqualTo(operationNumber + "_" + channel);
    }

    // Feature: implementacion-idempotencia, Property 1: Identidad canónica de la clave
    @Property(tries = 100)
    void sameOperationNumberAndChannelProduceEqualKeys(
            @ForAll("operationNumbers") String operationNumber,
            @ForAll("channels") String channel) {

        IdempotencyKey a = new IdempotencyKey(operationNumber, channel);
        IdempotencyKey b = new IdempotencyKey(operationNumber, channel);

        assertThat(a.toUniqueString()).isEqualTo(b.toUniqueString());
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    // Feature: implementacion-idempotencia, Property 1: Identidad canónica de la clave
    @Property(tries = 100)
    void sameOperationNumberDifferentChannelProduceIndependentKeys(
            @ForAll("operationNumbers") String operationNumber,
            @ForAll("channels") String channelA,
            @ForAll("channels") String channelB) {

        // Restringir al subespacio en el que los canales realmente difieren.
        net.jqwik.api.Assume.that(!channelA.equals(channelB));

        IdempotencyKey keyA = new IdempotencyKey(operationNumber, channelA);
        IdempotencyKey keyB = new IdempotencyKey(operationNumber, channelB);

        assertThat(keyA.toUniqueString()).isNotEqualTo(keyB.toUniqueString());
        assertThat(keyA).isNotEqualTo(keyB);
    }

    // Feature: implementacion-idempotencia, Property 1: Identidad canónica de la clave
    @Property(tries = 100)
    void differentOperationNumberSameChannelProduceIndependentKeys(
            @ForAll("operationNumbers") String operationNumberA,
            @ForAll("operationNumbers") String operationNumberB,
            @ForAll("channels") String channel) {

        net.jqwik.api.Assume.that(!operationNumberA.equals(operationNumberB));

        IdempotencyKey keyA = new IdempotencyKey(operationNumberA, channel);
        IdempotencyKey keyB = new IdempotencyKey(operationNumberB, channel);

        assertThat(keyA.toUniqueString()).isNotEqualTo(keyB.toUniqueString());
        assertThat(keyA).isNotEqualTo(keyB);
    }
}
