package com.pragma.payments.application.usecases;

import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test de propiedad para el rechazo de una solicitud duplicada cuyo Idempotency_Record está en
 * estado {@link RecordStatus#IN_PROGRESS}.
 *
 * <p>Ejercita {@link PaymentUseCase#processPayment(Mono)} cuando la consulta inicial al
 * Idempotency_Store ({@link IdempotencyStorePort#find(IdempotencyKey)}) devuelve un registro en
 * estado {@code IN_PROGRESS} (una reserva aún en curso para la misma clave). En ese escenario el
 * caso de uso DEBE rechazar la solicitud duplicada emitiendo
 * {@link TransactionAlreadyProcessedException} (señal mapeable a HTTP 409) y NUNCA invocar la
 * cadena de servicios externos (antifraude → buró de riesgo → core bancario) ni reservar/completar
 * un nuevo registro (Req 7.1).</p>
 *
 * <p>El registro {@code IN_PROGRESS} se construye con
 * {@link IdempotencyRecord#reserve(IdempotencyKey, Instant, Duration)}, que produce exactamente un
 * record {@code IN_PROGRESS} sin resultado asociado.</p>
 *
 * <p>Nota sobre Resilience4j: {@code processPayment} lleva {@code @CircuitBreaker} (con
 * {@code fallbackMethod}) y {@code @Retry}, pero esas anotaciones solo actúan a través del proxy de
 * Spring. Al instanciar {@link PaymentUseCase} con {@code new} en este test unitario NO se aplica
 * ningún aspecto, por lo que el error se propaga directamente, que es lo que esta propiedad
 * verifica.</p>
 *
 * <p>Los mocks de Mockito se crean y verifican DENTRO del método {@code @Property} para evitar
 * estado compartido entre iteraciones (tries) de jqwik.</p>
 */
class InProgressDuplicateRejectionPropertyTest {

    // Feature: implementacion-idempotencia, Property 10: Rechazo de duplicado en estado IN_PROGRESS
    // Validates: Requirements 7.1
    //
    // Para cualquier clave cuyo Idempotency_Record está en IN_PROGRESS, una solicitud duplicada se
    // rechaza (señal mapeable a HTTP 409) sin invocar la cadena de servicios externos.
    @Property(tries = 100)
    void inProgressRecordRejectsDuplicateWithoutExternalChain(
            @ForAll("transactions") Transaction transaction) {

        // Mocks aislados por iteración (sin estado compartido entre tries).
        FraudDetectionPort fraudDetectionPort = mock(FraudDetectionPort.class);
        RiskBureauPort riskBureauPort = mock(RiskBureauPort.class);
        CoreBankingAdapter coreBankingAdapter = mock(CoreBankingAdapter.class);
        IdempotencyStorePort idempotencyStore = mock(IdempotencyStorePort.class);
        IdempotencyProperties properties = new IdempotencyProperties();

        IdempotencyKey key = transaction.getIdempotencyKey();

        // Record IN_PROGRESS para la clave: reserva aún en curso, sin resultado asociado.
        IdempotencyRecord inProgress =
                IdempotencyRecord.reserve(key, Instant.now(), Duration.ofHours(24));
        assertThat(inProgress.getStatus()).isEqualTo(RecordStatus.IN_PROGRESS);
        assertThat(inProgress.getResult()).isNull();

        when(idempotencyStore.find(any(IdempotencyKey.class)))
                .thenReturn(Mono.just(inProgress));

        PaymentUseCase useCase = new PaymentUseCase(
                fraudDetectionPort, riskBureauPort, coreBankingAdapter, idempotencyStore, properties);

        // 1) La solicitud duplicada se rechaza con TransactionAlreadyProcessedException (→ 409).
        StepVerifier.create(useCase.processPayment(Mono.just(transaction)))
                .expectError(TransactionAlreadyProcessedException.class)
                .verify();

        // 2) Se consultó el store antes de cualquier ejecución externa.
        verify(idempotencyStore).find(any(IdempotencyKey.class));

        // 3) CERO invocaciones a la cadena de servicios externos (fraude → riesgo → core).
        verify(fraudDetectionPort, never()).evaluateTransaction(any(Transaction.class));
        verify(riskBureauPort, never()).getRiskScore(any(Transaction.class));
        verify(riskBureauPort, never()).isAccountBlacklisted(anyString());
        verify(coreBankingAdapter, never()).processTransaction(any(Transaction.class));

        // 4) Tampoco se reserva ni completa: un duplicado IN_PROGRESS no progresa.
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
     * Transacciones válidas y variadas con su {@link IdempotencyKey} asociada. La clave debe estar
     * presente porque {@code processPayment} la obtiene de la transacción antes de consultar el store.
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
