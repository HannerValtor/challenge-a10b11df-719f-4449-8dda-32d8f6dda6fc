package com.pragma.payments.infrastructure.adapters.out.risk;

import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.config.ExternalTimeoutProperties;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests unitarios de resiliencia de {@link RiskBureauAdapter} (tarea 3.4).
 *
 * <p>Verifican la distinción fallback vs. fallo transitorio (Requirements 2.5, 2.6):</p>
 * <ul>
 *   <li>Con el Circuit Breaker <b>abierto</b> ({@link CallNotPermittedException}) el fallback degrada
 *       al score por defecto 50, dentro del rango 0..100, sin invocar el servicio externo.</li>
 *   <li>Con el Circuit Breaker <b>cerrado</b> y un fallo transitorio (timeout {@literal >} 2500ms o
 *       5xx) el fallback <b>re-propaga</b> el error sin devolver el score degradado, de modo que la
 *       clave de idempotencia quede elegible para reintento.</li>
 * </ul>
 */
@DisplayName("RiskBureauAdapter - Tests de resiliencia (fallback vs. fallo transitorio)")
class RiskBureauAdapterTest {

    private RiskBureauAdapter adapter;

    private Transaction createTransaction() {
        return Transaction.builder()
                .accountId("ACC-123456")
                .amount(new BigDecimal("5000.00"))
                .currency("USD")
                .channel("WEB")
                .idempotencyKey(new IdempotencyKey("OP-001", "WEB"))
                .build();
    }

    @BeforeEach
    void setUp() {
        ExternalTimeoutProperties timeoutProperties = new ExternalTimeoutProperties();
        // risk por defecto = 2500ms (> 2s, invariante del reto).
        adapter = new RiskBureauAdapter(
                WebClient.builder().build(),
                BulkheadRegistry.ofDefaults(),
                timeoutProperties,
                "http://localhost:8083");
    }

    @Nested
    @DisplayName("Circuit Breaker ABIERTO (Requirement 2.5)")
    class CircuitBreakerOpen {

        @Test
        @DisplayName("El fallback degrada a score 50 (rango 0..100) cuando el CB está abierto")
        void fallbackReturnsScore50_whenCircuitBreakerOpen() {
            Transaction transaction = createTransaction();
            CallNotPermittedException cbOpen = buildCallNotPermitted();

            StepVerifier.create(adapter.getRiskScoreFallback(transaction, cbOpen))
                    .assertNext(score -> {
                        assertThat(score).isEqualTo(50);
                        assertThat(score).isBetween(0, 100);
                    })
                    .verifyComplete();
        }
    }

    @Nested
    @DisplayName("Circuit Breaker CERRADO - fallo transitorio (Requirement 2.6)")
    class CircuitBreakerClosedTransientFailure {

        @Test
        @DisplayName("El fallback re-propaga un timeout (> 2500ms) sin devolver el score degradado")
        void fallbackPropagatesTimeout_whenCircuitBreakerClosed() {
            Transaction transaction = createTransaction();
            TimeoutException timeout = new TimeoutException("risk timeout > 2500ms");

            StepVerifier.create(adapter.getRiskScoreFallback(transaction, timeout))
                    .expectErrorMatches(err -> err == timeout)
                    .verify();
        }

        @Test
        @DisplayName("El fallback re-propaga un fallo transitorio genérico (p. ej. 5xx) sin fallback")
        void fallbackPropagatesTransientError_whenCircuitBreakerClosed() {
            Transaction transaction = createTransaction();
            RuntimeException transient5xx = new RuntimeException("Error 5xx del buró de riesgo");

            StepVerifier.create(adapter.getRiskScoreFallback(transaction, transient5xx))
                    .expectErrorMatches(err -> err == transient5xx)
                    .verify();
        }
    }

    @Nested
    @DisplayName("isAccountBlacklisted - resiliencia")
    class BlacklistResilience {

        @Test
        @DisplayName("Degrada a false cuando el CB está abierto")
        void fallbackReturnsFalse_whenCircuitBreakerOpen() {
            StepVerifier.create(adapter.isAccountBlacklistedFallback("ACC-123456", buildCallNotPermitted()))
                    .expectNext(false)
                    .verifyComplete();
        }

        @Test
        @DisplayName("Re-propaga el error ante fallo transitorio con CB cerrado")
        void fallbackPropagates_whenCircuitBreakerClosed() {
            RuntimeException transientError = new RuntimeException("timeout blacklist");
            StepVerifier.create(adapter.isAccountBlacklistedFallback("ACC-123456", transientError))
                    .expectErrorMatches(err -> err == transientError)
                    .verify();
        }
    }

    /** Construye una {@link CallNotPermittedException} real como la que emite un CB abierto. */
    private static CallNotPermittedException buildCallNotPermitted() {
        CircuitBreaker cb = CircuitBreaker.of("riskBureauCircuitBreaker",
                CircuitBreakerConfig.custom()
                        .waitDurationInOpenState(Duration.ofMinutes(1))
                        .build());
        cb.transitionToOpenState();
        return CallNotPermittedException.createCallNotPermittedException(cb);
    }
}
