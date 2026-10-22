package com.pragma.payments.application.usecases;

import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test de propiedad para la transición a FAILED / liberación de la reserva ante un fallo transitorio
 * de la cadena de servicios externos.
 *
 * <p>Ejercita {@link PaymentUseCase#processPayment(Mono)} cuando la clave está libre
 * ({@code find → Mono.empty()}), la reserva gana la carrera ({@code reserve → Mono.just(arg)}) y a
 * continuación un eslabón de la cadena externa falla con un error transitorio (no un duplicado y no
 * {@code TransactionAlreadyProcessedException}). En ese escenario el caso de uso DEBE invocar
 * {@link IdempotencyStorePort#markFailedOrRelease(IdempotencyKey)} exactamente una vez para esa clave
 * —dejando el record FAILED/liberado y la clave elegible a un reintento posterior (Req 7.3)— y
 * propagar el error, sin completar nunca el record.</p>
 *
 * <p>Nota sobre el envoltorio de errores: {@code PaymentUseCase.executeFraudDetection} captura
 * cualquier excepción del {@code FraudDetectionPort} y la reemplaza por una
 * {@link ExternalServiceTimeoutException}. Por eso el fallo transitorio se inyecta en el motor
 * antifraude y el error propagado que se verifica es {@code ExternalServiceTimeoutException}.</p>
 *
 * <p>Nota sobre Resilience4j: {@code processPayment} lleva {@code @CircuitBreaker}/{@code @Retry},
 * pero esas anotaciones solo actúan a través del proxy de Spring. Al instanciar
 * {@link PaymentUseCase} con {@code new} en este test unitario NO se aplica ningún aspecto, por lo
 * que el fallo transitorio se propaga directamente tras liberar la reserva.</p>
 *
 * <p>Los mocks se crean y verifican DENTRO del método {@code @Property} para evitar estado
 * compartido entre iteraciones (tries) de jqwik.</p>
 */
class TransientFailureReleasesReservationPropertyTest {

    // Feature: implementacion-idempotencia, Property 11: Transición a FAILED / liberación ante fallo transitorio
    //
    // Validates: Requirements 7.3
    //
    // Para cualquier reserva cuyo procesamiento falla por un fallo transitorio (no por duplicado),
    // el Idempotency_Record queda en estado FAILED o es liberado, de modo que la clave queda
    // elegible para un reintento posterior.
    @Property(tries = 100)
    void transientChainFailureReleasesReservationAndPropagates(
            @ForAll("transactions") Transaction transaction) {

        // Mocks aislados por iteración (sin estado compartido entre tries).
        FraudDetectionPort fraudDetectionPort = mock(FraudDetectionPort.class);
        RiskBureauPort riskBureauPort = mock(RiskBureauPort.class);
        CoreBankingAdapter coreBankingAdapter = mock(CoreBankingAdapter.class);
        IdempotencyStorePort idempotencyStore = mock(IdempotencyStorePort.class);
        IdempotencyProperties properties = new IdempotencyProperties();

        IdempotencyKey key = transaction.getIdempotencyKey();

        // Clave libre: el flujo continúa a la reserva.
        when(idempotencyStore.find(any(IdempotencyKey.class))).thenReturn(Mono.empty());
        // La reserva gana la carrera: emite el propio record reservado.
        when(idempotencyStore.reserve(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        // La liberación de la reserva completa sin error.
        when(idempotencyStore.markFailedOrRelease(any(IdempotencyKey.class))).thenReturn(Mono.empty());

        // Fallo TRANSITORIO en un eslabón de la cadena externa (motor antifraude). El use case lo
        // envuelve en ExternalServiceTimeoutException dentro de executeFraudDetection.
        RuntimeException transientFailure =
                new RuntimeException("fallo transitorio del motor antifraude");
        when(fraudDetectionPort.evaluateTransaction(any(Transaction.class)))
                .thenReturn(Mono.error(transientFailure));

        PaymentUseCase useCase = new PaymentUseCase(
                fraudDetectionPort, riskBureauPort, coreBankingAdapter, idempotencyStore, properties);

        // 1) El error transitorio se propaga (envuelto como ExternalServiceTimeoutException).
        StepVerifier.create(useCase.processPayment(Mono.just(transaction)))
                .expectError(ExternalServiceTimeoutException.class)
                .verify();

        // 2) Se reservó la clave y luego se liberó/marcó FAILED exactamente una vez PARA ESA clave.
        verify(idempotencyStore, times(1)).reserve(any());
        verify(idempotencyStore, times(1)).markFailedOrRelease(eq(key));

        // 3) El record NUNCA se completa: un fallo transitorio no deja el registro en COMPLETED.
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
     * antes de reservar en el store.
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
