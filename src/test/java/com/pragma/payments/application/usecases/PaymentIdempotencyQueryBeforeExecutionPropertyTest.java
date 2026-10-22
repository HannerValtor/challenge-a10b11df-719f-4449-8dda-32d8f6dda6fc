package com.pragma.payments.application.usecases;

import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.core.CoreBankingAdapter;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;
import org.mockito.InOrder;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test de propiedad para la consulta de idempotencia previa a la ejecución de la cadena externa.
 *
 * <p>Los mocks se crean y configuran DENTRO de cada método {@code @Property} para garantizar
 * verificación limpia por cada iteración (try) de jqwik, evitando estado compartido entre
 * ejecuciones que contaminaría {@code verify(...)}.</p>
 */
class PaymentIdempotencyQueryBeforeExecutionPropertyTest {

    // Feature: implementacion-idempotencia, Property 3: Consulta de idempotencia previa a la ejecución externa
    //
    // Validates: Requirements 2.1, 3.1
    //
    // Para cualquier solicitud de pago, el Payment_Service consulta el Idempotency_Store por la
    // clave ANTES de invocar la cadena de servicios externos; si existe un record COMPLETED, la
    // cadena externa no se invoca en ninguna ejecución.

    /**
     * Caso ausente (continúa el flujo): {@code find(key)} se invoca ANTES que cualquier servicio
     * externo (fraude, riesgo, core). Se verifica el orden con {@link InOrder}.
     */
    @Property(tries = 100)
    void findIsQueriedBeforeTheExternalChain(@ForAll("transactions") Transaction transaction) {
        FraudDetectionPort fraudDetectionPort = Mockito.mock(FraudDetectionPort.class);
        RiskBureauPort riskBureauPort = Mockito.mock(RiskBureauPort.class);
        CoreBankingAdapter coreBankingAdapter = Mockito.mock(CoreBankingAdapter.class);
        IdempotencyStorePort idempotencyStore = Mockito.mock(IdempotencyStorePort.class);

        IdempotencyKey key = transaction.getIdempotencyKey();

        // No existe registro previo: el flujo continúa a reserva + cadena externa.
        when(idempotencyStore.find(any())).thenReturn(Mono.empty());
        when(idempotencyStore.reserve(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(idempotencyStore.complete(any(), any())).thenAnswer(inv -> {
            Transaction result = inv.getArgument(1);
            return Mono.just(
                    IdempotencyRecord.reserve(key, Instant.now(), Duration.ofHours(24)).withResult(result));
        });

        // Cadena externa feliz: no fraude, riesgo bajo, core aprueba.
        when(fraudDetectionPort.evaluateTransaction(any())).thenReturn(Mono.just(false));
        when(riskBureauPort.getRiskScore(any())).thenReturn(Mono.just(10));
        when(coreBankingAdapter.processTransaction(any())).thenReturn(Mono.just("CORE-REF-OK"));

        PaymentUseCase useCase = new PaymentUseCase(
                fraudDetectionPort, riskBureauPort, coreBankingAdapter,
                idempotencyStore, new IdempotencyProperties());

        Transaction result = useCase.processPayment(Mono.just(transaction)).block();

        assertThat(result).isNotNull();

        // La consulta de idempotencia ocurre ANTES de invocar la cadena de servicios externos.
        InOrder inOrder = Mockito.inOrder(idempotencyStore, fraudDetectionPort, riskBureauPort, coreBankingAdapter);
        inOrder.verify(idempotencyStore).find(any());
        inOrder.verify(fraudDetectionPort).evaluateTransaction(any());
        inOrder.verify(riskBureauPort).getRiskScore(any());
        inOrder.verify(coreBankingAdapter).processTransaction(any());
    }

    /**
     * Caso COMPLETED cacheado: {@code find(key)} devuelve un record COMPLETED, por lo que la cadena
     * externa NO se invoca en ninguna ejecución y se retorna el resultado cacheado.
     */
    @Property(tries = 100)
    void completedRecordSkipsTheExternalChainEntirely(@ForAll("transactions") Transaction transaction) {
        FraudDetectionPort fraudDetectionPort = Mockito.mock(FraudDetectionPort.class);
        RiskBureauPort riskBureauPort = Mockito.mock(RiskBureauPort.class);
        CoreBankingAdapter coreBankingAdapter = Mockito.mock(CoreBankingAdapter.class);
        IdempotencyStorePort idempotencyStore = Mockito.mock(IdempotencyStorePort.class);

        IdempotencyKey key = transaction.getIdempotencyKey();

        // Resultado previamente cacheado para la misma clave.
        Transaction cachedResult = Transaction.builder()
                .idempotencyKey(key)
                .accountId(transaction.getAccountId())
                .amount(transaction.getAmount())
                .currency(transaction.getCurrency())
                .channel(transaction.getChannel())
                .status(Transaction.TransactionStatus.COMPLETED)
                .build();
        IdempotencyRecord completed = IdempotencyRecord
                .reserve(key, Instant.now(), Duration.ofHours(24))
                .withResult(cachedResult);

        when(idempotencyStore.find(any())).thenReturn(Mono.just(completed));

        PaymentUseCase useCase = new PaymentUseCase(
                fraudDetectionPort, riskBureauPort, coreBankingAdapter,
                idempotencyStore, new IdempotencyProperties());

        Transaction result = useCase.processPayment(Mono.just(transaction)).block();

        // Se retorna el resultado cacheado...
        assertThat(result).isNotNull();
        assertThat(result.getId()).isEqualTo(cachedResult.getId());
        assertThat(result.getStatus()).isEqualTo(RecordStatus.COMPLETED.name());

        // ...tras consultar el store...
        verify(idempotencyStore, times(1)).find(any());

        // ...y SIN invocar la cadena de servicios externos ni reservar/completar nada.
        verify(fraudDetectionPort, never()).evaluateTransaction(any());
        verify(riskBureauPort, never()).getRiskScore(any());
        verify(coreBankingAdapter, never()).processTransaction(any());
        verify(idempotencyStore, never()).reserve(any());
        verify(idempotencyStore, never()).complete(any(), any());
    }

    /**
     * Refuerzo de la propiedad: para K >= 1 solicitudes duplicadas posteriores sobre un record
     * COMPLETED, en NINGUNA ejecución se invoca la cadena externa.
     */
    @Property(tries = 100)
    void noExternalInvocationAcrossKDuplicateCompletedRequests(
            @ForAll("transactions") Transaction transaction,
            @ForAll @IntRange(min = 1, max = 8) int duplicates) {

        FraudDetectionPort fraudDetectionPort = Mockito.mock(FraudDetectionPort.class);
        RiskBureauPort riskBureauPort = Mockito.mock(RiskBureauPort.class);
        CoreBankingAdapter coreBankingAdapter = Mockito.mock(CoreBankingAdapter.class);
        IdempotencyStorePort idempotencyStore = Mockito.mock(IdempotencyStorePort.class);

        IdempotencyKey key = transaction.getIdempotencyKey();
        Transaction cachedResult = Transaction.builder()
                .idempotencyKey(key)
                .channel(transaction.getChannel())
                .status(Transaction.TransactionStatus.COMPLETED)
                .build();
        IdempotencyRecord completed = IdempotencyRecord
                .reserve(key, Instant.now(), Duration.ofHours(24))
                .withResult(cachedResult);

        when(idempotencyStore.find(any())).thenReturn(Mono.just(completed));

        PaymentUseCase useCase = new PaymentUseCase(
                fraudDetectionPort, riskBureauPort, coreBankingAdapter,
                idempotencyStore, new IdempotencyProperties());

        for (int i = 0; i < duplicates; i++) {
            Transaction result = useCase.processPayment(Mono.just(transaction)).block();
            assertThat(result).isNotNull();
            assertThat(result.getId()).isEqualTo(cachedResult.getId());
        }

        // En ninguna de las K ejecuciones se tocó la cadena externa.
        verify(fraudDetectionPort, never()).evaluateTransaction(any());
        verify(riskBureauPort, never()).getRiskScore(any());
        verify(coreBankingAdapter, never()).processTransaction(any());
        // find se consultó en cada una de las K solicitudes (previo a cualquier ejecución).
        verify(idempotencyStore, times(duplicates)).find(any());
    }

    /**
     * Generador de transacciones válidas con claves de idempotencia conformes a los patrones
     * {@code operationNumber (^[A-Za-z0-9\-]+$)} y {@code channel (^[A-Za-z0-9_]+$)}.
     */
    @Provide
    Arbitrary<Transaction> transactions() {
        Arbitrary<String> operationNumbers = Arbitraries.strings()
                .withCharRange('A', 'Z').withCharRange('a', 'z').withCharRange('0', '9')
                .ofMinLength(1).ofMaxLength(20);
        Arbitrary<String> channels = Arbitraries.of("web", "mobile", "api");
        Arbitrary<BigDecimal> amounts = Arbitraries.longs().between(1, 10_000_000)
                .map(cents -> new BigDecimal(cents).movePointLeft(2));
        Arbitrary<String> currencies = Arbitraries.of("COP", "USD", "EUR");
        Arbitrary<String> descriptions = Arbitraries.strings()
                .withCharRange('a', 'z').ofMinLength(0).ofMaxLength(30);

        return Combinators.combine(operationNumbers, channels, amounts, currencies, descriptions)
                .as((operationNumber, channel, amount, currency, description) ->
                        Transaction.builder()
                                .idempotencyKey(new IdempotencyKey(operationNumber, channel))
                                .accountId("ACC-1")
                                .amount(amount)
                                .currency(currency)
                                .channel(channel)
                                .description(description)
                                .build());
    }
}
