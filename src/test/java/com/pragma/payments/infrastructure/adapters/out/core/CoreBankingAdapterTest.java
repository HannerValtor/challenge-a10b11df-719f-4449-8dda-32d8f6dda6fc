package com.pragma.payments.infrastructure.adapters.out.core;

import com.pragma.payments.domain.model.Transaction;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.time.Duration;

/**
 * Tests unitarios de resiliencia de {@link CoreBankingAdapter} (tarea 3.4).
 *
 * <p>Verifican que el fallback del core (invocado cuando el Circuit Breaker está abierto o tras
 * agotar reintentos) devuelve la referencia {@code "DECLINED"} sin invocar el servicio externo,
 * de modo que el sistema siga operando en modo degradado (Requirement 2.3).</p>
 *
 * <p>El método de fallback es privado por diseño, por lo que se invoca por reflexión para
 * verificar su contrato de forma aislada, sin levantar el contexto Spring ni el proxy AOP.</p>
 */
@DisplayName("CoreBankingAdapter - Tests de resiliencia (fallback DECLINED)")
class CoreBankingAdapterTest {

    private CoreBankingAdapter adapter;

    private Transaction createTransaction() {
        return Transaction.builder()
                .accountId("ACC-12345")
                .amount(new BigDecimal("1000.00"))
                .currency("USD")
                .channel("WEB")
                .build();
    }

    @BeforeEach
    void setUp() {
        // Constructor de conveniencia para pruebas unitarias (sin contexto Spring).
        adapter = new CoreBankingAdapter("http://localhost:8081");
    }

    @Test
    @DisplayName("El fallback devuelve DECLINED cuando el Circuit Breaker está abierto")
    void fallbackReturnsDeclined_whenCircuitBreakerOpen() throws Exception {
        Transaction transaction = createTransaction();
        CallNotPermittedException cbOpen = buildCallNotPermitted();

        StepVerifier.create(invokeFallback(transaction, cbOpen))
                .expectNext("DECLINED")
                .verifyComplete();
    }

    @Test
    @DisplayName("El fallback devuelve DECLINED ante un fallo tras agotar reintentos (5xx)")
    void fallbackReturnsDeclined_onExhaustedRetries() throws Exception {
        Transaction transaction = createTransaction();
        RuntimeException serverError = new RuntimeException("Core Banking no disponible: 503");

        StepVerifier.create(invokeFallback(transaction, serverError))
                .expectNext("DECLINED")
                .verifyComplete();
    }

    @SuppressWarnings("unchecked")
    private Mono<String> invokeFallback(Transaction transaction, Throwable cause) throws Exception {
        Method fallback = CoreBankingAdapter.class
                .getDeclaredMethod("processTransactionFallback", Transaction.class, Throwable.class);
        fallback.setAccessible(true);
        return (Mono<String>) fallback.invoke(adapter, transaction, cause);
    }

    /** Construye una {@link CallNotPermittedException} real como la que emite un CB abierto. */
    private static CallNotPermittedException buildCallNotPermitted() {
        CircuitBreaker cb = CircuitBreaker.of("coreBankingCircuitBreaker",
                CircuitBreakerConfig.custom()
                        .waitDurationInOpenState(Duration.ofMinutes(1))
                        .build());
        cb.transitionToOpenState();
        return CallNotPermittedException.createCallNotPermittedException(cb);
    }
}
