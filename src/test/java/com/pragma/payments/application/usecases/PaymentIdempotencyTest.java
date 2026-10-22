package com.pragma.payments.application.usecases;

import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.core.CoreBankingAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests de idempotencia atómica del PaymentUseCase (Fase 1).
 *
 * Valida las Correctness Properties del diseño:
 * - Property 4: Exactamente-una-ejecución por clave de idempotencia bajo carrera (Req 3.1, 3.4, 4.3)
 * - Property 5: El duplicado se rechaza preservando la transacción previa (Req 3.2)
 * - Property 6: Construcción determinista de la clave con el canal incluido (Req 3.3, 4.2)
 */
@DisplayName("PaymentUseCase - Idempotencia atómica thread-safe")
class PaymentIdempotencyTest {

    private FraudDetectionPort fraudDetectionPort;
    private RiskBureauPort riskBureauPort;
    private CoreBankingAdapter coreBankingAdapter;
    private PaymentUseCase paymentUseCase;

    @BeforeEach
    void setUp() {
        fraudDetectionPort = Mockito.mock(FraudDetectionPort.class);
        riskBureauPort = Mockito.mock(RiskBureauPort.class);
        coreBankingAdapter = Mockito.mock(CoreBankingAdapter.class);

        // Camino feliz: no fraude, riesgo bajo, core aprueba.
        when(fraudDetectionPort.evaluateTransaction(any())).thenReturn(Mono.just(false));
        when(riskBureauPort.getRiskScore(any())).thenReturn(Mono.just(10));
        when(coreBankingAdapter.processTransaction(any())).thenReturn(Mono.just("CORE-REF-OK"));

        paymentUseCase = new PaymentUseCase(fraudDetectionPort, riskBureauPort, coreBankingAdapter);
    }

    private Transaction newTransaction(String operationNumber, String channel) {
        return Transaction.builder()
                .idempotencyKey(new IdempotencyKey(operationNumber, channel))
                .accountId("ACC-1")
                .amount(new BigDecimal("1000.00"))
                .currency("COP")
                .channel(channel)
                .build();
    }

    // Feature: diseno-concurrencia-pagos, Property 6: Construcción determinista de la clave con el canal incluido
    @Test
    @DisplayName("Property 6: la clave es determinista e incluye el canal")
    void idempotencyKeyIsDeterministicAndChannelAware() {
        IdempotencyKey k1 = new IdempotencyKey("OP-1", "web");
        IdempotencyKey k2 = new IdempotencyKey("OP-1", "web");
        IdempotencyKey kMobile = new IdempotencyKey("OP-1", "mobile");

        // Determinismo: misma op+canal => misma clave
        assertThat(k1.toUniqueString()).isEqualTo(k2.toUniqueString()).isEqualTo("OP-1_web");
        assertThat(k1).isEqualTo(k2);
        assertThat(k1.hashCode()).isEqualTo(k2.hashCode());

        // Canal incluido: misma op, distinto canal => claves distintas
        assertThat(k1.toUniqueString()).isNotEqualTo(kMobile.toUniqueString());
        assertThat(k1).isNotEqualTo(kMobile);
    }

    // Feature: diseno-concurrencia-pagos, Property 5: El duplicado se rechaza preservando la transacción previa
    @Test
    @DisplayName("Property 5: un segundo pago con la misma clave se rechaza como duplicado")
    void duplicatePaymentIsRejected() {
        Transaction first = newTransaction("OP-DUP", "web");
        Transaction second = newTransaction("OP-DUP", "web");

        Transaction firstResult = paymentUseCase.processPayment(Mono.just(first)).block();
        assertThat(firstResult).isNotNull();
        assertThat(firstResult.getStatus()).isEqualTo(Transaction.TransactionStatus.COMPLETED.name());

        // El segundo con la misma clave debe fallar como duplicado.
        assertThat(paymentUseCase.isTransactionProcessed(second.getIdempotencyKey()).block()).isTrue();

        Throwable error = catchError(() -> paymentUseCase.processPayment(Mono.just(second)).block());
        assertThat(error).isInstanceOf(TransactionAlreadyProcessedException.class);

        // La transacción previa se preserva (misma id registrada).
        Transaction stored = paymentUseCase.getProcessedTransaction(second.getIdempotencyKey()).block();
        assertThat(stored).isNotNull();
        assertThat(stored.getId()).isEqualTo(first.getId());
    }

    // Feature: diseno-concurrencia-pagos, Property 4: Exactamente-una-ejecución por clave de idempotencia bajo carrera
    @Test
    @DisplayName("Property 4: ante K solicitudes concurrentes con la misma clave, exactamente una se procesa")
    void exactlyOnceUnderConcurrency() throws InterruptedException {
        final int K = 16;
        final String operationNumber = "OP-RACE";
        final String channel = "web";

        ExecutorService pool = Executors.newFixedThreadPool(K);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(K);
        AtomicInteger successes = new AtomicInteger(0);
        AtomicInteger duplicates = new AtomicInteger(0);
        ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < K; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    Transaction tx = newTransaction(operationNumber, channel);
                    try {
                        paymentUseCase.processPayment(Mono.just(tx)).block();
                        successes.incrementAndGet();
                    } catch (TransactionAlreadyProcessedException dup) {
                        duplicates.incrementAndGet();
                    }
                } catch (Throwable t) {
                    unexpected.add(t);
                } finally {
                    done.countDown();
                }
            });
        }

        startGate.countDown(); // libera todos los hilos a la vez
        done.await();
        pool.shutdownNow();

        assertThat(unexpected).as("sin errores inesperados").isEmpty();
        assertThat(successes.get()).as("exactamente una ejecución exitosa").isEqualTo(1);
        assertThat(duplicates.get()).as("las K-1 restantes rechazadas como duplicado").isEqualTo(K - 1);
    }

    private Throwable catchError(Runnable r) {
        try {
            r.run();
            return null;
        } catch (Throwable t) {
            return t;
        }
    }
}
