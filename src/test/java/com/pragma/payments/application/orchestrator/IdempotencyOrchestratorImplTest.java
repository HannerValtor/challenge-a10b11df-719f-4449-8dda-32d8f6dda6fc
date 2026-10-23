package com.pragma.payments.application.orchestrator;

import com.pragma.payments.application.chain.PaymentProcessingChain;
import com.pragma.payments.application.ports.out.DuplicateReservationException;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests unitarios (basados en ejemplos) de {@link IdempotencyOrchestratorImpl} con el puerto de
 * almacenamiento ({@link IdempotencyStorePort}) y la cadena externa ({@link PaymentProcessingChain})
 * mockeados (Mockito) y verificación reactiva (Reactor {@link StepVerifier}).
 *
 * <p>Cubre el ciclo de vida de idempotencia definido en el diseño (D1, D7):</p>
 * <ul>
 *   <li>{@code COMPLETED}: se retorna el resultado almacenado sin ejecutar la cadena externa
 *       (Requirements 3.2, 7.3).</li>
 *   <li>{@code IN_PROGRESS}: se rechaza con {@link TransactionAlreadyProcessedException} (&rarr; HTTP 409)
 *       sin ejecutar la cadena (Requirements 3.2, 7.4).</li>
 *   <li>{@code FAILED}: elegible a reintento &rarr; se reserva y ejecuta la cadena (Requirements 3.2, 7.4).</li>
 *   <li>Ausente: se reserva y ejecuta la cadena exactamente una vez, luego se completa
 *       (Requirements 3.3, 3.4).</li>
 *   <li>Pérdida de la carrera ({@link DuplicateReservationException}): se relee el registro y se decide
 *       sin ejecutar la cadena (Requirements 3.3, 7.4).</li>
 *   <li>Fallo transitorio de la cadena: se invoca {@code markFailedOrRelease} y se propaga el error,
 *       dejando la clave elegible a reintento (Requirement 7.9).</li>
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("IdempotencyOrchestratorImpl - orquestacion de idempotencia (find -> decidir -> reserve -> ejecutar -> complete/fail)")
class IdempotencyOrchestratorImplTest {

    private static final Duration TTL = Duration.ofHours(24);

    @Mock
    private IdempotencyStorePort idempotencyStore;

    @Mock
    private PaymentProcessingChain processingChain;

    @Mock
    private IdempotencyProperties idempotencyProperties;

    private IdempotencyOrchestratorImpl orchestrator;

    private IdempotencyKey key;
    private Transaction transaction;

    @BeforeEach
    void setUp() {
        orchestrator = new IdempotencyOrchestratorImpl(idempotencyStore, processingChain, idempotencyProperties);
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
        // El TTL solo es necesario en las rutas que reservan; se declara lenient para los
        // casos COMPLETED/IN_PROGRESS que nunca reservan.
        lenient().when(idempotencyProperties.getTtl()).thenReturn(TTL);
    }

    /** Construye un registro COMPLETED con un resultado asociado para la clave bajo prueba. */
    private IdempotencyRecord completedRecord(Transaction result) {
        Instant now = Instant.now();
        return new IdempotencyRecord(key, RecordStatus.IN_PROGRESS, null, now, now.plus(TTL))
                .withResult(result);
    }

    private IdempotencyRecord inProgressRecord() {
        return IdempotencyRecord.reserve(key, Instant.now(), TTL);
    }

    private IdempotencyRecord failedRecord() {
        return IdempotencyRecord.reserve(key, Instant.now(), TTL).asFailed();
    }

    @Nested
    @DisplayName("Decision por registro existente")
    class DecideByExistingRecord {

        @Test
        @DisplayName("COMPLETED -> retorna el resultado cacheado SIN ejecutar la cadena")
        void completedReturnsCachedResultWithoutExecutingChain() {
            Transaction storedResult = Transaction.builder()
                    .id("tx-original")
                    .idempotencyKey(key)
                    .channel("web")
                    .status(TransactionStatus.COMPLETED)
                    .build();
            when(idempotencyStore.find(key)).thenReturn(Mono.just(completedRecord(storedResult)));

            StepVerifier.create(orchestrator.resolve(key, transaction))
                    .assertNext(result -> {
                        assertThat(result).isSameAs(storedResult);
                        assertThat(result.getId()).isEqualTo("tx-original");
                    })
                    .verifyComplete();

            verify(processingChain, never()).execute(any());
            verify(idempotencyStore, never()).reserve(any());
            verify(idempotencyStore, never()).complete(any(), any());
        }

        @Test
        @DisplayName("IN_PROGRESS -> error TransactionAlreadyProcessedException (409) SIN ejecutar la cadena")
        void inProgressErrorsWithAlreadyProcessed() {
            when(idempotencyStore.find(key)).thenReturn(Mono.just(inProgressRecord()));

            StepVerifier.create(orchestrator.resolve(key, transaction))
                    .expectError(TransactionAlreadyProcessedException.class)
                    .verify();

            verify(processingChain, never()).execute(any());
            verify(idempotencyStore, never()).reserve(any());
        }

        @Test
        @DisplayName("FAILED -> reserva y ejecuta la cadena, luego completa (reintento)")
        void failedProceedsToReserveAndProcess() {
            when(idempotencyStore.find(key)).thenReturn(Mono.just(failedRecord()));
            when(idempotencyStore.reserve(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
            Transaction processed = Transaction.builder()
                    .id("tx-retry")
                    .idempotencyKey(key)
                    .channel("web")
                    .status(TransactionStatus.COMPLETED)
                    .build();
            when(processingChain.execute(transaction)).thenReturn(Mono.just(processed));
            when(idempotencyStore.complete(eq(key), eq(processed)))
                    .thenReturn(Mono.just(completedRecord(processed)));

            StepVerifier.create(orchestrator.resolve(key, transaction))
                    .assertNext(result -> assertThat(result).isSameAs(processed))
                    .verifyComplete();

            verify(idempotencyStore).reserve(any());
            verify(processingChain).execute(transaction);
            verify(idempotencyStore).complete(eq(key), eq(processed));
        }
    }

    @Nested
    @DisplayName("Reserva y procesamiento (registro ausente)")
    class ReserveAndProcess {

        @Test
        @DisplayName("Ausente -> reserva OK -> ejecuta la cadena una vez -> completa -> retorna resultado")
        void absentReservesExecutesOnceAndCompletes() {
            when(idempotencyStore.find(key)).thenReturn(Mono.empty());
            when(idempotencyStore.reserve(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
            Transaction processed = Transaction.builder()
                    .id("tx-new")
                    .idempotencyKey(key)
                    .channel("web")
                    .status(TransactionStatus.COMPLETED)
                    .build();
            when(processingChain.execute(transaction)).thenReturn(Mono.just(processed));
            when(idempotencyStore.complete(eq(key), eq(processed)))
                    .thenReturn(Mono.just(completedRecord(processed)));

            StepVerifier.create(orchestrator.resolve(key, transaction))
                    .assertNext(result -> assertThat(result).isSameAs(processed))
                    .verifyComplete();

            verify(idempotencyStore).reserve(any());
            verify(processingChain).execute(transaction);
            verify(idempotencyStore).complete(eq(key), eq(processed));
            verify(idempotencyStore, never()).markFailedOrRelease(any());
        }
    }

    @Nested
    @DisplayName("Perdida de la carrera (DuplicateReservationException)")
    class RaceLoss {

        @Test
        @DisplayName("reserve -> DuplicateReservationException + re-read COMPLETED -> resultado cacheado SIN ejecutar la cadena")
        void raceLossReReadsCompletedWithoutExecutingChain() {
            Transaction storedResult = Transaction.builder()
                    .id("tx-winner")
                    .idempotencyKey(key)
                    .channel("web")
                    .status(TransactionStatus.COMPLETED)
                    .build();
            // 1) find inicial: ausente -> intenta reservar.
            // 2) reserve: pierde la carrera -> DuplicateReservationException.
            // 3) find de relectura: ya COMPLETED por el ganador.
            when(idempotencyStore.find(key))
                    .thenReturn(Mono.empty())
                    .thenReturn(Mono.just(completedRecord(storedResult)));
            when(idempotencyStore.reserve(any()))
                    .thenReturn(Mono.error(new DuplicateReservationException(key.toUniqueString(), null)));

            StepVerifier.create(orchestrator.resolve(key, transaction))
                    .assertNext(result -> assertThat(result).isSameAs(storedResult))
                    .verifyComplete();

            verify(processingChain, never()).execute(any());
            verify(idempotencyStore, never()).complete(any(), any());
            verify(idempotencyStore, never()).markFailedOrRelease(any());
        }
    }

    @Nested
    @DisplayName("Fallo transitorio de la cadena")
    class TransientFailure {

        @Test
        @DisplayName("chain.execute -> error transitorio -> markFailedOrRelease y se propaga el error")
        void transientChainFailureMarksFailedAndPropagates() {
            RuntimeException transient_ = new RuntimeException("transient downstream failure");
            when(idempotencyStore.find(key)).thenReturn(Mono.empty());
            when(idempotencyStore.reserve(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
            when(processingChain.execute(transaction)).thenReturn(Mono.error(transient_));
            when(idempotencyStore.markFailedOrRelease(key)).thenReturn(Mono.empty());

            StepVerifier.create(orchestrator.resolve(key, transaction))
                    .expectErrorMatches(err -> err == transient_)
                    .verify();

            verify(idempotencyStore).markFailedOrRelease(key);
            verify(idempotencyStore, never()).complete(any(), any());
        }
    }
}
