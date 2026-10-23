package com.pragma.payments.application.orchestrator;

import com.pragma.payments.application.chain.PaymentProcessingChain;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.Transaction;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Property 2: Determinismo del resultado duplicado
 *
 * <p>Validates: Requirements 7.3</p>
 *
 * <p>Para una {@link IdempotencyKey} cuyo registro de idempotencia ya está en estado
 * {@code COMPLETED}, toda repetición de {@code resolve(...)} devuelve el <strong>mismo</strong>
 * {@code Processing_Result} almacenado, sin ejecutar nunca la cadena de servicios externos
 * ({@link PaymentProcessingChain#execute(Transaction)}).</p>
 *
 * <p>El test ejercita directamente {@link IdempotencyOrchestratorImpl#resolve}, que es el
 * colaborador responsable de la decisión por estado tras el refactor SRP (D1). El almacén de
 * idempotencia ({@link IdempotencyStorePort}) se sustituye por un stub de Mockito que, para la
 * clave sembrada, siempre devuelve el registro {@code COMPLETED} con su resultado conocido. La
 * cadena de procesamiento se instrumenta con un {@link AtomicInteger} que debe permanecer en 0:
 * ninguna de las N repeticiones puede volver a ejecutar la cadena externa.</p>
 *
 * <p>Los mocks se crean y configuran DENTRO de cada iteración ({@code @Property} try) para
 * garantizar una verificación aislada, sin estado compartido entre ejecuciones.</p>
 */
class IdempotencyOrchestratorDuplicateResultPropertyTest {

    /**
     * Para una clave ya {@code COMPLETED} (sembrada en el store con un resultado conocido),
     * N repeticiones aleatorias de {@code resolve(...)} devuelven SIEMPRE el MISMO resultado
     * almacenado (igualdad entre las N respuestas y con el resultado sembrado) y NUNCA invocan
     * la cadena de servicios externos (contador de invocaciones == 0, verify never()).
     */
    @Property(tries = 100)
    void completedKeyReturnsSameStoredResultForEveryRepetitionWithoutExecutingChain(
            @ForAll("storedResults") Transaction storedResult,
            @ForAll @IntRange(min = 2, max = 16) int repetitions) {

        IdempotencyStorePort idempotencyStore = Mockito.mock(IdempotencyStorePort.class);
        PaymentProcessingChain processingChain = Mockito.mock(PaymentProcessingChain.class);

        IdempotencyKey key = storedResult.getIdempotencyKey();

        // Registro ya COMPLETED que retiene el Processing_Result conocido para esta clave.
        IdempotencyRecord completed = IdempotencyRecord
                .reserve(key, Instant.now(), Duration.ofHours(24))
                .withResult(storedResult);

        // El store siempre devuelve el record COMPLETED para esta clave (hit idempotente).
        when(idempotencyStore.find(any())).thenReturn(Mono.just(completed));

        // Instrumentar la cadena: si se invocara, incrementaría el contador (debe quedar en 0).
        AtomicInteger chainInvocations = new AtomicInteger(0);
        when(processingChain.execute(any())).thenAnswer(invocation -> {
            chainInvocations.incrementAndGet();
            return Mono.just((Transaction) invocation.getArgument(0));
        });

        IdempotencyOrchestrator orchestrator = new IdempotencyOrchestratorImpl(
                idempotencyStore, processingChain, new IdempotencyProperties());

        // Una transacción entrante "nueva" con la misma clave (simula la solicitud duplicada).
        Transaction incomingRequest = Transaction.builder()
                .idempotencyKey(key)
                .accountId(storedResult.getAccountId())
                .amount(storedResult.getAmount())
                .currency(storedResult.getCurrency())
                .channel(storedResult.getChannel())
                .description(storedResult.getDescription())
                .build();

        // Ejecutar N repeticiones de resolve(...) y recolectar sus resultados.
        List<Transaction> responses = new ArrayList<>();
        for (int i = 0; i < repetitions; i++) {
            Transaction response = orchestrator.resolve(key, incomingRequest).block();
            assertThat(response).isNotNull();
            responses.add(response);
        }

        // Determinismo: cada respuesta es el MISMO resultado almacenado...
        for (Transaction response : responses) {
            assertSameResult(response, storedResult);
        }
        // ...y todas las N respuestas son equivalentes entre sí.
        Transaction first = responses.get(0);
        for (Transaction response : responses) {
            assertSameResult(response, first);
        }

        // La cadena de servicios externos NUNCA se ejecuta para una clave ya COMPLETED.
        assertThat(chainInvocations.get()).isZero();
        verify(processingChain, never()).execute(any());
        // Tampoco se reserva ni se completa: el resultado proviene exclusivamente del registro COMPLETED.
        verify(idempotencyStore, never()).reserve(any());
        verify(idempotencyStore, never()).complete(any(), any());
    }

    /**
     * Compara dos resultados de procesamiento por su identidad y campos de negocio relevantes,
     * confirmando el determinismo exigido por el Requirement 7.3.
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
     * Generador de resultados de procesamiento ya {@code COMPLETED}, con claves de idempotencia
     * conformes a los patrones {@code operationNumber (^[A-Za-z0-9\-]+$)} y
     * {@code channel (^[A-Za-z0-9_]+$)}, variando monto, moneda, descripción y referencia externa
     * para cubrir distintos resultados almacenados a través de las iteraciones.
     */
    @Provide
    Arbitrary<Transaction> storedResults() {
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
                .as((operationNumber, channel, amount, currency, description) -> {
                    IdempotencyKey key = new IdempotencyKey(operationNumber, channel);
                    Transaction result = Transaction.builder()
                            .idempotencyKey(key)
                            .accountId("ACC-1")
                            .amount(amount)
                            .currency(currency)
                            .channel(channel)
                            .description(description)
                            .status(Transaction.TransactionStatus.COMPLETED)
                            .build();
                    result.setExternalReference("CORE-REF-" + key.toUniqueString());
                    return result;
                });
    }
}
