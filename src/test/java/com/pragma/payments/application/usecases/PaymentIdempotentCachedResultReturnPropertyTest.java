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
import net.jqwik.api.constraints.IntRange;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test de propiedad para el retorno idempotente del resultado cacheado.
 *
 * <p>A diferencia del test hermano de la tarea 8.2
 * ({@code PaymentIdempotencyQueryBeforeExecutionPropertyTest}), que se centra en el ORDEN de
 * consulta y la AUSENCIA de ejecución de la cadena externa, este test se centra en la IGUALDAD
 * del Processing_Result a través de K solicitudes duplicadas: todas las respuestas deben ser
 * equivalentes entre sí y equivalentes al resultado cacheado de la solicitud original.</p>
 *
 * <p>Los mocks se crean y configuran DENTRO de cada método {@code @Property} para garantizar
 * una verificación limpia por cada iteración (try) de jqwik, evitando estado compartido entre
 * ejecuciones.</p>
 */
class PaymentIdempotentCachedResultReturnPropertyTest {

    // Feature: implementacion-idempotencia, Property 5: Retorno idempotente del resultado cacheado
    //
    // Validates: Requirements 3.1, 3.2
    //
    // Para cualquier transacción original completada y para cualquier número K >= 1 de solicitudes
    // duplicadas posteriores con la misma clave, todas las respuestas devuelven un Processing_Result
    // equivalente al de la solicitud original y ninguna reejecuta la cadena de servicios externos.

    /**
     * Dado un record COMPLETED con un resultado cacheado, K >= 1 solicitudes duplicadas devuelven
     * SIEMPRE el MISMO resultado (igualdad entre las K respuestas y con el resultado cacheado por
     * id y campos relevantes) y NUNCA reejecutan la cadena de servicios externos ni vuelven a
     * reservar/completar.
     */
    @Property(tries = 100)
    void kDuplicatesOverCompletedRecordReturnTheSameCachedResultWithoutReexecution(
            @ForAll("transactions") Transaction request,
            @ForAll @IntRange(min = 1, max = 8) int duplicates) {

        FraudDetectionPort fraudDetectionPort = Mockito.mock(FraudDetectionPort.class);
        RiskBureauPort riskBureauPort = Mockito.mock(RiskBureauPort.class);
        CoreBankingAdapter coreBankingAdapter = Mockito.mock(CoreBankingAdapter.class);
        IdempotencyStorePort idempotencyStore = Mockito.mock(IdempotencyStorePort.class);

        IdempotencyKey key = request.getIdempotencyKey();

        // Resultado de la solicitud ORIGINAL ya completado y cacheado para esta clave.
        Transaction cachedResult = Transaction.builder()
                .idempotencyKey(key)
                .accountId(request.getAccountId())
                .amount(request.getAmount())
                .currency(request.getCurrency())
                .channel(request.getChannel())
                .description(request.getDescription())
                .status(Transaction.TransactionStatus.COMPLETED)
                .build();
        cachedResult.setExternalReference("CORE-REF-" + key.toUniqueString());

        IdempotencyRecord completed = IdempotencyRecord
                .reserve(key, Instant.now(), Duration.ofHours(24))
                .withResult(cachedResult);

        // find SIEMPRE devuelve el record COMPLETED (Mono.just(recordCompleted)).
        when(idempotencyStore.find(any())).thenReturn(Mono.just(completed));

        PaymentUseCase useCase = new PaymentUseCase(
                fraudDetectionPort, riskBureauPort, coreBankingAdapter,
                idempotencyStore, new IdempotencyProperties());

        // Ejecutar las K solicitudes duplicadas y recolectar sus resultados.
        List<Transaction> responses = new ArrayList<>();
        for (int i = 0; i < duplicates; i++) {
            Transaction response = useCase.processPayment(Mono.just(request)).block();
            assertThat(response).isNotNull();
            responses.add(response);
        }

        // Igualdad: cada respuesta es equivalente al resultado cacheado por id y campos relevantes...
        for (Transaction response : responses) {
            assertSameResult(response, cachedResult);
        }
        // ...y todas las K respuestas son equivalentes entre sí (misma transacción cacheada).
        Transaction first = responses.get(0);
        for (Transaction response : responses) {
            assertSameResult(response, first);
        }

        // Cero reejecuciones: la cadena de servicios externos no se invoca en ninguna de las K.
        verify(fraudDetectionPort, never()).evaluateTransaction(any());
        verify(riskBureauPort, never()).getRiskScore(any());
        verify(coreBankingAdapter, never()).processTransaction(any());
        // Tampoco se reserva ni se completa: el resultado proviene exclusivamente del caché.
        verify(idempotencyStore, never()).reserve(any());
        verify(idempotencyStore, never()).complete(any(), any());
    }

    /**
     * Compara dos resultados de procesamiento por su identidad y campos de negocio relevantes,
     * confirmando la equivalencia exigida por el Requirement 3.2.
     */
    private void assertSameResult(Transaction actual, Transaction expected) {
        assertThat(actual.getId()).isEqualTo(expected.getId());
        assertThat(actual.getStatus()).isEqualTo(expected.getStatus());
        assertThat(actual.getAmount()).isEqualByComparingTo(expected.getAmount());
        assertThat(actual.getCurrency()).isEqualTo(expected.getCurrency());
        assertThat(actual.getChannel()).isEqualTo(expected.getChannel());
        assertThat(actual.getExternalReference()).isEqualTo(expected.getExternalReference());
    }

    /**
     * Generador de transacciones válidas con claves de idempotencia conformes a los patrones
     * {@code operationNumber (^[A-Za-z0-9\-]+$)} y {@code channel (^[A-Za-z0-9_]+$)}, con montos,
     * monedas y descripciones variados.
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
