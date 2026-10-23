package com.pragma.payments.application.orchestrator;

import com.pragma.payments.application.chain.PaymentProcessingChain;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.mockito.InOrder;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Propiedades de consulta de idempotencia previa a la ejecución de la cadena externa, verificadas
 * sobre el {@link IdempotencyOrchestratorImpl}, que es el colaborador responsable del ciclo de vida
 * de idempotencia tras el refactor SRP (D1).
 *
 * <p>Cubre dos propiedades universales que antes se verificaban sobre el {@code PaymentUseCase}
 * monolítico y que ahora pertenecen al orquestador:</p>
 * <ul>
 *   <li><b>Consulta antes de la cadena (Req 2.1, 3.1, 3.2):</b> {@code find(key)} se invoca ANTES
 *       que la cadena de servicios externos; si existe un registro {@code COMPLETED}, la cadena no
 *       se invoca en ninguna ejecución.</li>
 *   <li><b>Fail-closed ante fallo de consulta (Req 2.2):</b> si {@code find(key)} falla con un error
 *       técnico, el error se propaga y la cadena externa NUNCA se ejecuta (idempotencia no
 *       verificable ⇒ no se procesa).</li>
 * </ul>
 *
 * <p>La cadena de servicios externos se representa por el puerto {@link PaymentProcessingChain}
 * (colaborador único del orquestador para la ejecución externa), mockeado por iteración.</p>
 */
class QueryBeforeChainPropertyTest {

    private static final Duration TTL = Duration.ofHours(24);

    /** Req 2.1/3.1: con registro ausente, find ocurre ANTES de ejecutar la cadena externa. */
    @Property(tries = 100)
    void findIsQueriedBeforeTheExternalChain(@ForAll("transactions") Transaction transaction) {
        IdempotencyStorePort store = Mockito.mock(IdempotencyStorePort.class);
        PaymentProcessingChain chain = Mockito.mock(PaymentProcessingChain.class);
        IdempotencyKey key = transaction.getIdempotencyKey();

        when(store.find(any())).thenReturn(Mono.empty());
        when(store.reserve(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(chain.execute(any())).thenReturn(Mono.just(completed(transaction)));
        when(store.complete(any(), any())).thenAnswer(inv -> Mono.just(
                IdempotencyRecord.reserve(key, Instant.now(), TTL).withResult(inv.getArgument(1))));

        IdempotencyOrchestratorImpl orchestrator =
                new IdempotencyOrchestratorImpl(store, chain, new IdempotencyProperties());

        Transaction result = orchestrator.resolve(key, transaction).block();
        assertThat(result).isNotNull();

        // find se consulta ANTES de ejecutar la cadena externa.
        InOrder inOrder = Mockito.inOrder(store, chain);
        inOrder.verify(store).find(any());
        inOrder.verify(chain).execute(any());
    }

    /** Req 3.2: con registro COMPLETED, la cadena externa no se invoca y se retorna el cacheado. */
    @Property(tries = 100)
    void completedRecordSkipsTheExternalChainEntirely(@ForAll("transactions") Transaction transaction) {
        IdempotencyStorePort store = Mockito.mock(IdempotencyStorePort.class);
        PaymentProcessingChain chain = Mockito.mock(PaymentProcessingChain.class);
        IdempotencyKey key = transaction.getIdempotencyKey();

        Transaction cached = completed(transaction);
        IdempotencyRecord completedRecord =
                IdempotencyRecord.reserve(key, Instant.now(), TTL).withResult(cached);
        when(store.find(any())).thenReturn(Mono.just(completedRecord));

        IdempotencyOrchestratorImpl orchestrator =
                new IdempotencyOrchestratorImpl(store, chain, new IdempotencyProperties());

        StepVerifier.create(orchestrator.resolve(key, transaction))
                .assertNext(result -> {
                    assertThat(result).isSameAs(cached);
                    assertThat(result.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
                })
                .verifyComplete();

        verify(chain, never()).execute(any());
        verify(store, never()).reserve(any());
        verify(store, never()).complete(any(), any());
    }

    /** Req 2.2: si find falla técnicamente, se propaga el error y NO se ejecuta la cadena (fail-closed). */
    @Property(tries = 100)
    void queryTechnicalFailurePropagatesAndStopsExternalChain(
            @ForAll("transactions") Transaction transaction) {
        IdempotencyStorePort store = Mockito.mock(IdempotencyStorePort.class);
        PaymentProcessingChain chain = Mockito.mock(PaymentProcessingChain.class);
        IdempotencyKey key = transaction.getIdempotencyKey();

        RuntimeException technicalFailure =
                new RuntimeException("fallo técnico de persistencia al consultar el Idempotency_Store");
        when(store.find(any())).thenReturn(Mono.error(technicalFailure));

        IdempotencyOrchestratorImpl orchestrator =
                new IdempotencyOrchestratorImpl(store, chain, new IdempotencyProperties());

        StepVerifier.create(orchestrator.resolve(key, transaction))
                .expectErrorMatches(error -> error == technicalFailure)
                .verify();

        verify(store).find(any());
        verify(chain, never()).execute(any());
        verify(store, never()).reserve(any());
        verify(store, never()).complete(any(), any());
    }

    private Transaction completed(Transaction request) {
        return Transaction.builder()
                .id("tx-" + request.getIdempotencyKey().toUniqueString())
                .idempotencyKey(request.getIdempotencyKey())
                .accountId(request.getAccountId())
                .amount(request.getAmount())
                .currency(request.getCurrency())
                .channel(request.getChannel())
                .status(TransactionStatus.COMPLETED)
                .build();
    }

    @Provide
    Arbitrary<Transaction> transactions() {
        Arbitrary<String> operationNumbers = Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1).ofMaxLength(50);
        Arbitrary<String> channels = Arbitraries.of("web", "mobile", "api");
        Arbitrary<BigDecimal> amounts = Arbitraries.longs().between(1L, 1_000_000_00L)
                .map(cents -> BigDecimal.valueOf(cents, 2));
        Arbitrary<String> currencies = Arbitraries.of("USD", "EUR", "COP");

        return Combinators.combine(operationNumbers, channels, amounts, currencies)
                .as((operationNumber, channel, amount, currency) -> Transaction.builder()
                        .idempotencyKey(new IdempotencyKey(operationNumber, channel))
                        .accountId("ACC-1")
                        .amount(amount)
                        .currency(currency)
                        .channel(channel)
                        .status(TransactionStatus.PENDING)
                        .build());
    }
}
