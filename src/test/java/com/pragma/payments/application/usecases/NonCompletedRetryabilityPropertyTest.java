package com.pragma.payments.application.usecases;

import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.core.CoreBankingAdapter;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test de propiedad para la reintentabilidad de estados no-COMPLETED.
 *
 * <p>Ejercita {@link PaymentUseCase#processPayment(Mono)} cuando el registro de idempotencia para
 * la clave está ausente (equivalente a eliminado por expiración / expirado, modelado como
 * {@code find} → {@link Mono#empty()}) o en estado {@code FAILED}. En ambos casos una nueva
 * solicitud debe reservar de nuevo con éxito y ejecutar la cadena de servicios externos
 * (antifraude → buró de riesgo → core bancario) como si fuese una solicitud nueva, produciendo un
 * resultado {@code COMPLETED} (Req 6.5, 7.4).</p>
 *
 * <p>Los mocks de Mockito se crean y verifican DENTRO de cada método {@code @Property} para evitar
 * estado compartido entre iteraciones (tries) de jqwik que contaminaría {@code verify(...)}.</p>
 */
class NonCompletedRetryabilityPropertyTest {

    // Feature: implementacion-idempotencia, Property 12: Reintentabilidad de estados no-COMPLETED
    //
    // Validates: Requirements 6.5, 7.4
    //
    // Para cualquier clave cuyo Idempotency_Record está ausente (eliminado por expiración),
    // expirado o en estado FAILED, una nueva solicitud reserva de nuevo con éxito y ejecuta la
    // cadena de servicios externos como si fuera una solicitud nueva.

    /**
     * Selector del estado inicial del registro para la clave de la solicitud:
     * <ul>
     *     <li>{@code ABSENT}: {@code find} → {@link Mono#empty()} (ausente / expirado / eliminado por TTL).</li>
     *     <li>{@code FAILED}: {@code find} → registro en estado {@code FAILED}.</li>
     * </ul>
     * Ambos son estados no-COMPLETED y deben conducir a una nueva reserva + ejecución de la cadena.
     */
    enum InitialState {
        ABSENT,
        FAILED
    }

    @Property(tries = 100)
    void nonCompletedStatesReserveAgainAndExecuteExternalChain(
            @ForAll("transactions") Transaction transaction,
            @ForAll("initialStates") InitialState initialState) {

        // Mocks aislados por iteración (sin estado compartido entre tries).
        FraudDetectionPort fraudDetectionPort = mock(FraudDetectionPort.class);
        RiskBureauPort riskBureauPort = mock(RiskBureauPort.class);
        CoreBankingAdapter coreBankingAdapter = mock(CoreBankingAdapter.class);
        IdempotencyStorePort idempotencyStore = mock(IdempotencyStorePort.class);
        IdempotencyProperties properties = new IdempotencyProperties();

        IdempotencyKey key = transaction.getIdempotencyKey();

        // Estado inicial no-COMPLETED: ausente/expirado (empty) o FAILED.
        if (initialState == InitialState.ABSENT) {
            // Record ausente/expirado/eliminado por TTL: a este nivel se modela como find vacío.
            when(idempotencyStore.find(any(IdempotencyKey.class))).thenReturn(Mono.empty());
        } else {
            IdempotencyRecord failed = IdempotencyRecord
                    .reserve(key, Instant.now(), Duration.ofHours(24))
                    .asFailed();
            when(idempotencyStore.find(any(IdempotencyKey.class))).thenReturn(Mono.just(failed));
        }

        // La reserva vuelve a ganar (clave elegible a reintento): emite el record IN_PROGRESS recibido.
        when(idempotencyStore.reserve(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        // complete devuelve el record completado asociando el Processing_Result.
        when(idempotencyStore.complete(any(IdempotencyKey.class), any(Transaction.class)))
                .thenAnswer(inv -> {
                    Transaction result = inv.getArgument(1);
                    return Mono.just(IdempotencyRecord
                            .reserve(key, Instant.now(), Duration.ofHours(24))
                            .withResult(result));
                });

        // Cadena externa feliz: no fraude, riesgo bajo, core OK.
        when(fraudDetectionPort.evaluateTransaction(any(Transaction.class))).thenReturn(Mono.just(false));
        when(riskBureauPort.getRiskScore(any(Transaction.class))).thenReturn(Mono.just(10));
        when(coreBankingAdapter.processTransaction(any(Transaction.class))).thenReturn(Mono.just("CORE-REF-OK"));

        PaymentUseCase useCase = new PaymentUseCase(
                fraudDetectionPort, riskBureauPort, coreBankingAdapter, idempotencyStore, properties);

        Transaction result = useCase.processPayment(Mono.just(transaction)).block();

        // La nueva solicitud produce un resultado COMPLETED.
        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isEqualTo(Transaction.TransactionStatus.COMPLETED.name());
        assertThat(result.getExternalReference()).isEqualTo("CORE-REF-OK");

        // Se reservó de nuevo exactamente una vez.
        verify(idempotencyStore, times(1)).reserve(any());

        // La cadena de servicios externos se invocó exactamente una vez por servicio.
        verify(fraudDetectionPort, times(1)).evaluateTransaction(any(Transaction.class));
        verify(riskBureauPort, times(1)).getRiskScore(any(Transaction.class));
        verify(coreBankingAdapter, times(1)).processTransaction(any(Transaction.class));

        // Y se completó el registro exactamente una vez (persistencia del Processing_Result).
        verify(idempotencyStore, times(1)).complete(any(IdempotencyKey.class), any(Transaction.class));
    }

    // ---- Generadores -------------------------------------------------------

    @Provide
    Arbitrary<InitialState> initialStates() {
        return Arbitraries.of(InitialState.ABSENT, InitialState.FAILED);
    }

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

        return Combinators.combine(keys, amounts, currencies, channelsForTx, accountIds, descriptions)
                .as((key, amount, currency, channel, accountId, description) ->
                        Transaction.builder()
                                .idempotencyKey(key)
                                .accountId(accountId)
                                .amount(amount)
                                .currency(currency)
                                .channel(channel)
                                .description(description)
                                .build());
    }
}
