package com.pragma.payments.application.usecases;

import com.pragma.payments.application.orchestrator.IdempotencyOrchestrator;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests unitarios (basados en ejemplos) del coordinador delgado {@link PaymentUseCase}.
 *
 * <p>Tras el refactor de la tarea 7.1, {@code PaymentUseCase} dejó de contener la lógica
 * de orquestación de idempotencia y de la cadena de servicios externos: ahora delega por
 * completo en {@link IdempotencyOrchestrator} para el flujo principal y solo consulta el
 * {@link IdempotencyStorePort} para las operaciones de lectura ({@code isTransactionProcessed},
 * {@code getProcessedTransaction}).</p>
 *
 * <p>Estos tests verifican, con {@code JUnit 5 + Mockito + Reactor StepVerifier}:</p>
 * <ul>
 *   <li><b>Delegación pura</b> de {@code processPayment} en {@code orchestrator.resolve(key, tx)}:
 *       se invoca exactamente una vez con la clave y la transacción, y el {@code Mono} devuelto
 *       es el que produce el orquestador (Requirement 3.1).</li>
 *   <li><b>Ausencia de acceso directo a puertos externos/store en el flujo principal</b>: el
 *       coordinador ya no inyecta {@code FraudDetectionPort}/{@code RiskBureauPort}/
 *       {@code CoreBankingPort} (su constructor solo recibe orquestador + store) y no conduce la
 *       cadena de idempotencia desde el use case (no hay {@code reserve}/{@code complete}/
 *       {@code markFailedOrRelease}) (Requirement 2.4).</li>
 *   <li>Comportamiento de las consultas de solo lectura vía {@code IdempotencyStorePort}
 *       (COMPLETED &rarr; true/resultado; ausente &rarr; false/vacío; no-COMPLETED &rarr; false/vacío).</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentUseCase - coordinador delgado (delegación pura en el orquestador, sin acceso directo a puertos externos)")
class PaymentUseCaseTest {

    private static final Duration TTL = Duration.ofHours(24);

    @Mock
    private IdempotencyOrchestrator orchestrator;

    @Mock
    private IdempotencyStorePort idempotencyStore;

    private PaymentUseCase paymentUseCase;

    private IdempotencyKey key;
    private Transaction transaction;

    @BeforeEach
    void setUp() {
        paymentUseCase = new PaymentUseCase(orchestrator, idempotencyStore);
        key = new IdempotencyKey("OP-1", "web");
        transaction = Transaction.builder()
                .id("tx-1")
                .idempotencyKey(key)
                .accountId("acc-1")
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .channel("web")
                .status(TransactionStatus.PENDING)
                .build();
    }

    private IdempotencyRecord recordWithStatus(RecordStatus status, Transaction result) {
        Instant now = Instant.now();
        return new IdempotencyRecord(key, status, result, now, now.plus(TTL));
    }

    @Nested
    @DisplayName("processPayment - delegación pura en el orquestador (Req 3.1)")
    class ProcessPaymentDelegation {

        @Test
        @DisplayName("delega en orchestrator.resolve(key, tx) exactamente una vez y retorna su Mono")
        void delegatesToOrchestratorResolve() {
            Transaction resolved = Transaction.builder()
                    .id("tx-resolved")
                    .idempotencyKey(key)
                    .channel("web")
                    .status(TransactionStatus.COMPLETED)
                    .build();
            when(orchestrator.resolve(eq(key), eq(transaction))).thenReturn(Mono.just(resolved));

            StepVerifier.create(paymentUseCase.processPayment(Mono.just(transaction)))
                    .assertNext(result -> assertThat(result).isSameAs(resolved))
                    .verifyComplete();

            // Delegación pura: resolve invocado exactamente una vez con la clave y la transacción.
            verify(orchestrator).resolve(key, transaction);
            verifyNoMoreInteractions(orchestrator);
        }

        @Test
        @DisplayName("no accede al store ni conduce la cadena de idempotencia en el flujo principal (Req 2.4)")
        void mainFlowDoesNotDriveStoreChain() {
            Transaction resolved = Transaction.builder()
                    .id("tx-resolved")
                    .idempotencyKey(key)
                    .channel("web")
                    .status(TransactionStatus.COMPLETED)
                    .build();
            when(orchestrator.resolve(eq(key), eq(transaction))).thenReturn(Mono.just(resolved));

            StepVerifier.create(paymentUseCase.processPayment(Mono.just(transaction)))
                    .expectNext(resolved)
                    .verifyComplete();

            // El use case NO orquesta idempotencia directamente contra el store: esa
            // responsabilidad vive en el IdempotencyOrchestrator (D1).
            verify(idempotencyStore, never()).find(any());
            verify(idempotencyStore, never()).reserve(any());
            verify(idempotencyStore, never()).complete(any(), any());
            verify(idempotencyStore, never()).markFailedOrRelease(any());
            verify(idempotencyStore, never()).deleteExpired(any());
            verifyNoInteractions(idempotencyStore);
        }

        @Test
        @DisplayName("propaga el error emitido por el orquestador sin tocar el store")
        void propagatesOrchestratorError() {
            RuntimeException boom = new RuntimeException("resolve failed");
            when(orchestrator.resolve(eq(key), eq(transaction))).thenReturn(Mono.error(boom));

            StepVerifier.create(paymentUseCase.processPayment(Mono.just(transaction)))
                    .expectErrorMatches(err -> err == boom)
                    .verify();

            verify(orchestrator).resolve(key, transaction);
            verifyNoInteractions(idempotencyStore);
        }
    }

    @Nested
    @DisplayName("isTransactionProcessed - consulta de solo lectura vía store")
    class IsTransactionProcessed {

        @Test
        @DisplayName("COMPLETED -> true")
        void completedReturnsTrue() {
            Transaction result = Transaction.builder().id("tx-done").idempotencyKey(key).channel("web")
                    .status(TransactionStatus.COMPLETED).build();
            when(idempotencyStore.find(key)).thenReturn(Mono.just(recordWithStatus(RecordStatus.COMPLETED, result)));

            StepVerifier.create(paymentUseCase.isTransactionProcessed(key))
                    .expectNext(true)
                    .verifyComplete();

            verifyNoInteractions(orchestrator);
        }

        @Test
        @DisplayName("IN_PROGRESS -> false")
        void inProgressReturnsFalse() {
            when(idempotencyStore.find(key))
                    .thenReturn(Mono.just(recordWithStatus(RecordStatus.IN_PROGRESS, null)));

            StepVerifier.create(paymentUseCase.isTransactionProcessed(key))
                    .expectNext(false)
                    .verifyComplete();
        }

        @Test
        @DisplayName("ausente -> false")
        void absentReturnsFalse() {
            when(idempotencyStore.find(key)).thenReturn(Mono.empty());

            StepVerifier.create(paymentUseCase.isTransactionProcessed(key))
                    .expectNext(false)
                    .verifyComplete();
        }
    }

    @Nested
    @DisplayName("getProcessedTransaction - consulta de solo lectura vía store")
    class GetProcessedTransaction {

        @Test
        @DisplayName("COMPLETED -> resultado almacenado")
        void completedReturnsStoredResult() {
            Transaction result = Transaction.builder().id("tx-done").idempotencyKey(key).channel("web")
                    .status(TransactionStatus.COMPLETED).build();
            when(idempotencyStore.find(key)).thenReturn(Mono.just(recordWithStatus(RecordStatus.COMPLETED, result)));

            StepVerifier.create(paymentUseCase.getProcessedTransaction(key))
                    .assertNext(tx -> assertThat(tx).isSameAs(result))
                    .verifyComplete();

            verifyNoInteractions(orchestrator);
        }

        @Test
        @DisplayName("FAILED (no COMPLETED) -> vacío")
        void nonCompletedReturnsEmpty() {
            when(idempotencyStore.find(key))
                    .thenReturn(Mono.just(recordWithStatus(RecordStatus.FAILED, null)));

            StepVerifier.create(paymentUseCase.getProcessedTransaction(key))
                    .verifyComplete();
        }

        @Test
        @DisplayName("ausente -> vacío")
        void absentReturnsEmpty() {
            when(idempotencyStore.find(key)).thenReturn(Mono.empty());

            StepVerifier.create(paymentUseCase.getProcessedTransaction(key))
                    .verifyComplete();
        }
    }
}
