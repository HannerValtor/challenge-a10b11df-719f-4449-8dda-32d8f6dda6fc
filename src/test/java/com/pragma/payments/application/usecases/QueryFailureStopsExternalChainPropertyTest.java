package com.pragma.payments.application.usecases;

import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.core.CoreBankingAdapter;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Builders;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test de propiedad para el fallo de consulta que impide ejecutar la cadena de servicios externos.
 *
 * <p>Ejercita {@link PaymentUseCase#processPayment(Mono)} cuando la consulta inicial al
 * Idempotency_Store ({@link IdempotencyStorePort#find(IdempotencyKey)}) falla con un error
 * técnico (p. ej. un fallo de persistencia simulado con {@link RuntimeException}). En ese
 * escenario el caso de uso DEBE propagar el error (fail-closed) y NUNCA invocar la cadena de
 * servicios externos (antifraude → buró de riesgo → core bancario), pues la idempotencia no
 * pudo verificarse (Req 2.2).</p>
 *
 * <p>Nota sobre Resilience4j: {@code processPayment} lleva {@code @CircuitBreaker} (con
 * {@code fallbackMethod}) y {@code @Retry}, pero esas anotaciones solo actúan a través del
 * proxy de Spring. Al instanciar {@link PaymentUseCase} con {@code new} en este test unitario
 * NO se aplica ningún aspecto, por lo que el error técnico de {@code find} se propaga
 * directamente, que es exactamente lo que esta propiedad verifica.</p>
 *
 * <p>Los mocks de Mockito se crean y verifican DENTRO del método {@code @Property} para evitar
 * estado compartido entre iteraciones (tries) de jqwik.</p>
 */
class QueryFailureStopsExternalChainPropertyTest {

    // Feature: implementacion-idempotencia, Property 4: Un fallo de consulta impide ejecutar la cadena externa
    // Validates: Requirements 2.2
    //
    // Para cualquier solicitud de pago, si la consulta al Idempotency_Store falla con un error
    // técnico, el Payment_Service propaga el error y no invoca ningún servicio externo.
    @Property(tries = 100)
    void queryTechnicalFailurePropagatesAndStopsExternalChain(
            @ForAll("transactions") Transaction transaction) {

        // Mocks aislados por iteración (sin estado compartido entre tries).
        FraudDetectionPort fraudDetectionPort = mock(FraudDetectionPort.class);
        RiskBureauPort riskBureauPort = mock(RiskBureauPort.class);
        CoreBankingAdapter coreBankingAdapter = mock(CoreBankingAdapter.class);
        IdempotencyStorePort idempotencyStore = mock(IdempotencyStorePort.class);
        IdempotencyProperties properties = new IdempotencyProperties();

        // La consulta al store falla con un error TÉCNICO (fallo de persistencia simulado).
        RuntimeException technicalFailure =
                new RuntimeException("fallo técnico de persistencia al consultar el Idempotency_Store");
        when(idempotencyStore.find(any(IdempotencyKey.class)))
                .thenReturn(Mono.error(technicalFailure));

        PaymentUseCase useCase = new PaymentUseCase(
                fraudDetectionPort, riskBureauPort, coreBankingAdapter, idempotencyStore, properties);

        // 1) El error técnico se propaga: processPayment emite exactamente ese error.
        StepVerifier.create(useCase.processPayment(Mono.just(transaction)))
                .expectErrorMatches(error -> error == technicalFailure)
                .verify();

        // 2) Se consultó el store antes de cualquier ejecución externa.
        verify(idempotencyStore).find(any(IdempotencyKey.class));

        // 3) CERO invocaciones a la cadena de servicios externos (fraude → riesgo → core).
        verify(fraudDetectionPort, never()).evaluateTransaction(any(Transaction.class));
        verify(riskBureauPort, never()).getRiskScore(any(Transaction.class));
        verify(riskBureauPort, never()).isAccountBlacklisted(anyString());
        verify(coreBankingAdapter, never()).processTransaction(any(Transaction.class));

        // 4) Tampoco se reserva ni completa: no se progresa más allá de la verificación fallida.
        verify(idempotencyStore, never()).reserve(any());
        verify(idempotencyStore, never()).complete(any(IdempotencyKey.class), any(Transaction.class));
    }

    // ---- Generadores -------------------------------------------------------

    // operationNumber conforme a ^[A-Za-z0-9\-]+$ (1..50)
    @Provide
    Arbitrary<String> operationNumbers() {
        return Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1).ofMaxLength(50);
    }

    // channel conforme a ^[A-Za-z0-9_]+$ (1..20)
    @Provide
    Arbitrary<String> channels() {
        return Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")
                .ofMinLength(1).ofMaxLength(20);
    }

    /**
     * Transacciones válidas y variadas con su {@link IdempotencyKey} asociada. La clave de
     * idempotencia debe estar presente porque {@code processPayment} la obtiene de la transacción
     * antes de consultar el store.
     */
    @Provide
    Arbitrary<Transaction> transactions() {
        Arbitrary<IdempotencyKey> keys =
                Combinators.combine(operationNumbers(), channels()).as(IdempotencyKey::new);
        Arbitrary<BigDecimal> amounts = Arbitraries.longs().between(1L, 1_000_000_00L)
                .map(cents -> BigDecimal.valueOf(cents, 2));
        Arbitrary<String> currencies = Arbitraries.of("USD", "EUR", "COP", "MXN", "GBP");
        Arbitrary<String> channelsForTx = Arbitraries.of("web", "mobile", "api");
        Arbitrary<String> accountIds = Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1).ofMaxLength(30);
        Arbitrary<String> descriptions = Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ")
                .ofMinLength(0).ofMaxLength(80);
        Arbitrary<Instant> createdAts = Arbitraries.longs()
                .between(0L, Duration.ofDays(3650).toSeconds())
                .map(Instant::ofEpochSecond);

        return Builders.withBuilder(Transaction::builder)
                .use(keys).in(Transaction.Builder::idempotencyKey)
                .use(accountIds).in(Transaction.Builder::accountId)
                .use(amounts).in(Transaction.Builder::amount)
                .use(currencies).in(Transaction.Builder::currency)
                .use(channelsForTx).in(Transaction.Builder::channel)
                .use(descriptions).in(Transaction.Builder::description)
                .use(createdAts).in(Transaction.Builder::createdAt)
                .build(Transaction.Builder::build);
    }
}
