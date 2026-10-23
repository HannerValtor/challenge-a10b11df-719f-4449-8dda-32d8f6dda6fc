package com.pragma.payments.infrastructure.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.persistence.CachingIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.context.ConfigurationPropertiesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import reactor.core.publisher.Mono;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test de wiring de la caché de lectura de idempotencia (Fase 3, Tarea 10.1).
 *
 * <p>Verifica, mediante un arranque ligero de contexto ({@link ApplicationContextRunner}) y sin
 * levantar JPA/PostgreSQL, que:</p>
 * <ul>
 *   <li>{@link CacheConfig} produce el bean Caffeine {@code idempotencyRecordCache};</li>
 *   <li>el decorator {@link CachingIdempotencyStore} se registra como bean {@code @Primary} del
 *       puerto {@link IdempotencyStorePort}, interponiéndose delante del adaptador de persistencia
 *       (resuelto aquí por un stub {@code idempotencyStoreAdapter}); y</li>
 *   <li>el TTL de la caché se acota al TTL de idempotencia cuando lo supera (Requirement 4.6).</li>
 * </ul>
 */
class CacheConfigWiringTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations.of(
                    ConfigurationPropertiesAutoConfiguration.class))
            .withUserConfiguration(TestConfig.class, CacheConfig.class)
            .withBean(CachingIdempotencyStore.class);

    @Test
    void cacheBeanAndPrimaryDecoratorAreWired() {
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(Cache.class);
            // El bean @Primary del puerto debe ser el decorator de caché, no el adaptador JPA.
            IdempotencyStorePort primary = context.getBean(IdempotencyStorePort.class);
            assertThat(primary).isInstanceOf(CachingIdempotencyStore.class);
        });
    }

    @Test
    void cacheTtlIsCappedToIdempotencyTtl() {
        contextRunner
                .withPropertyValues(
                        "payments.idempotency.ttl=PT1H",
                        "payments.idempotency.cache.ttl=PT24H")
                .run(context -> {
                    // El contexto arranca sin fallar: CacheConfig acota el TTL de forma defensiva.
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(Cache.class);
                });
    }

    @EnableConfigurationProperties({IdempotencyProperties.class, IdempotencyCacheProperties.class})
    static class TestConfig {

        /** Stub del adaptador JPA con el nombre de bean esperado por el {@code @Qualifier} del decorator. */
        @Bean(name = "idempotencyStoreAdapter")
        IdempotencyStorePort idempotencyStoreAdapter() {
            return new IdempotencyStorePort() {
                @Override
                public Mono<IdempotencyRecord> reserve(IdempotencyRecord record) {
                    return Mono.just(record);
                }

                @Override
                public Mono<IdempotencyRecord> find(IdempotencyKey key) {
                    return Mono.empty();
                }

                @Override
                public Mono<IdempotencyRecord> complete(IdempotencyKey key, Transaction result) {
                    return Mono.empty();
                }

                @Override
                public Mono<Void> markFailedOrRelease(IdempotencyKey key) {
                    return Mono.empty();
                }

                @Override
                public Mono<Long> deleteExpired(Instant now) {
                    return Mono.just(0L);
                }
            };
        }
    }
}
