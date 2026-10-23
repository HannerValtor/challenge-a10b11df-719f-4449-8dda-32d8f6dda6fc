package com.pragma.payments.application.orchestrator;

import com.pragma.payments.application.chain.PaymentProcessingChain;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Property 7: Liberación ante fallo transitorio.
 *
 * <p>Validates: Requirements 7.9, 8.5</p>
 *
 * <p>Para toda transacción cuya clave está libre (gana la reserva) y cuya cadena de servicios
 * externos falla con un fallo transitorio —incluido un timeout del buró de riesgo superior a 2s
 * ({@link ExternalServiceTimeoutException})— el {@link IdempotencyOrchestratorImpl}:</p>
 * <ul>
 *   <li>invoca {@link IdempotencyStorePort#markFailedOrRelease(IdempotencyKey)} exactamente una vez
 *       para esa clave, dejándola {@code FAILED}/liberada y elegible a reintento (Requirement 7.9);</li>
 *   <li>propaga el error transitorio (que la capa web mapea a HTTP 503, Requirement 8.5); y</li>
 *   <li>nunca completa el registro (un fallo transitorio no deja {@code COMPLETED}).</li>
 * </ul>
 *
 * <p>Se ejercita el orquestador directamente (que es el propietario de la decisión de idempotencia
 * tras el refactor SRP, D1) con el store y la cadena mockeados. Los mocks se crean y verifican
 * DENTRO del método {@code @Property} para aislar cada iteración.</p>
 */
class TransientFailureReleasePropertyTest {

    @Property(tries = 100)
    void transientChainFailureReleasesReservationAndPropagates(
            @ForAll("transactions") Transaction transaction) {

        IdempotencyStorePort idempotencyStore = mock(IdempotencyStorePort.class);
        PaymentProcessingChain processingChain = mock(PaymentProcessingChain.class);
        IdempotencyProperties properties = new IdempotencyProperties();

        IdempotencyKey key = transaction.getIdempotencyKey();

        // Clave libre -> continúa a reservar; la reserva gana la carrera.
        when(idempotencyStore.find(any(IdempotencyKey.class))).thenReturn(Mono.empty());
        when(idempotencyStore.reserve(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(idempotencyStore.markFailedOrRelease(any(IdempotencyKey.class))).thenReturn(Mono.empty());

        // Fallo transitorio: timeout del buró de riesgo > 2s (ExternalServiceTimeoutException -> 503).
        ExternalServiceTimeoutException transientFailure = new ExternalServiceTimeoutException(
                "RISK_BUREAU", "/score", Duration.ofMillis(2500), Duration.ofMillis(3000));
        when(processingChain.execute(any(Transaction.class))).thenReturn(Mono.error(transientFailure));

        IdempotencyOrchestratorImpl orchestrator =
                new IdempotencyOrchestratorImpl(idempotencyStore, processingChain, properties);

        // 1) El error transitorio se propaga (la capa web lo mapea a 503).
        StepVerifier.create(orchestrator.resolve(key, transaction))
                .expectError(ExternalServiceTimeoutException.class)
                .verify();

        // 2) Se reservó y luego se liberó/marcó FAILED exactamente una vez para esa clave.
        verify(idempotencyStore, times(1)).reserve(any());
        verify(idempotencyStore, times(1)).markFailedOrRelease(eq(key));

        // 3) El registro nunca se completa.
        verify(idempotencyStore, never()).complete(any(IdempotencyKey.class), any(Transaction.class));
    }

    @Provide
    Arbitrary<Transaction> transactions() {
        Arbitrary<String> operationNumbers = Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1).ofMaxLength(50);
        Arbitrary<String> channels = Arbitraries.of("web", "mobile", "api");
        Arbitrary<BigDecimal> amounts = Arbitraries.longs().between(1L, 1_000_000_00L)
                .map(cents -> BigDecimal.valueOf(cents, 2));
        Arbitrary<String> currencies = Arbitraries.of("USD", "EUR", "COP", "MXN", "GBP");

        return Combinators.combine(operationNumbers, channels, amounts, currencies)
                .as((operationNumber, channel, amount, currency) -> {
                    IdempotencyKey key = new IdempotencyKey(operationNumber, channel);
                    return Transaction.builder()
                            .idempotencyKey(key)
                            .accountId("ACC-1")
                            .amount(amount)
                            .currency(currency)
                            .channel(channel)
                            .status(TransactionStatus.PENDING)
                            .build();
                });
    }
}
