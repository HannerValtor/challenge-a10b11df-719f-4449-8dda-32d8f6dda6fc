package com.pragma.payments.application.usecases;

import com.pragma.payments.application.ports.out.DuplicateReservationException;
import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.core.CoreBankingAdapter;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private IdempotencyStorePort idempotencyStore;
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

        // Store de idempotencia en memoria que respeta la restricción UNIQUE de forma atómica,
        // sustituto fiel del adaptador persistente para ejercitar las propiedades de correctitud.
        idempotencyStore = new InMemoryIdempotencyStore();

        IdempotencyProperties properties = new IdempotencyProperties();

        paymentUseCase = new PaymentUseCase(
                fraudDetectionPort, riskBureauPort, coreBankingAdapter, idempotencyStore, properties);
    }

    /**
     * Doble de test del {@link IdempotencyStorePort} en memoria. La atomicidad de la reserva se
     * apoya en {@link ConcurrentHashMap#putIfAbsent}, equivalente a la restricción UNIQUE del
     * almacenamiento persistente: ante K reservas concurrentes de la misma clave, solo una gana
     * y el resto recibe {@link DuplicateReservationException}.
     */
    private static final class InMemoryIdempotencyStore implements IdempotencyStorePort {

        private final Map<String, IdempotencyRecord> store = new ConcurrentHashMap<>();

        @Override
        public Mono<IdempotencyRecord> reserve(IdempotencyRecord record) {
            return Mono.defer(() -> {
                String key = canonical(record.getKey());
                IdempotencyRecord previous = store.putIfAbsent(key, record);
                if (previous != null) {
                    return Mono.error(new DuplicateReservationException(key, null));
                }
                return Mono.just(record);
            });
        }

        @Override
        public Mono<IdempotencyRecord> find(IdempotencyKey key) {
            return Mono.defer(() -> Mono.justOrEmpty(store.get(canonical(key))));
        }

        @Override
        public Mono<IdempotencyRecord> complete(IdempotencyKey key, Transaction result) {
            return Mono.defer(() -> {
                String canonical = canonical(key);
                IdempotencyRecord current = store.get(canonical);
                IdempotencyRecord base = current != null
                        ? current
                        : IdempotencyRecord.reserve(key, Instant.now(), java.time.Duration.ofHours(24));
                IdempotencyRecord completed = base.withResult(result);
                store.put(canonical, completed);
                return Mono.just(completed);
            });
        }

        @Override
        public Mono<Void> markFailedOrRelease(IdempotencyKey key) {
            return Mono.<Void>fromRunnable(() -> store.remove(canonical(key)));
        }

        @Override
        public Mono<Long> deleteExpired(Instant now) {
            return Mono.defer(() -> {
                long removed = store.values().removeIf(r -> r.isExpired(now)) ? 1L : 0L;
                return Mono.just(removed);
            });
        }

        private String canonical(IdempotencyKey key) {
            return key == null ? "unknown" : key.getOperationNumber() + "_" + key.getChannel();
        }
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

    // Feature: implementacion-idempotencia, Property 5: Retorno idempotente del resultado cacheado (Req 3.1, 3.2)
    @Test
    @DisplayName("Fase 2: un segundo pago con la misma clave (COMPLETED) retorna el resultado cacheado sin reejecutar")
    void duplicatePaymentReturnsCachedResult() {
        Transaction first = newTransaction("OP-DUP", "web");
        Transaction second = newTransaction("OP-DUP", "web");

        Transaction firstResult = paymentUseCase.processPayment(Mono.just(first)).block();
        assertThat(firstResult).isNotNull();
        assertThat(firstResult.getStatus()).isEqualTo(Transaction.TransactionStatus.COMPLETED.name());

        // El registro queda COMPLETED tras el primer procesamiento.
        assertThat(paymentUseCase.isTransactionProcessed(second.getIdempotencyKey()).block()).isTrue();

        // Fase 2 (Req 3.1): el duplicado COMPLETED retorna el resultado previo cacheado (HTTP 200),
        // sin reejecutar la cadena de servicios externos.
        Transaction secondResult = paymentUseCase.processPayment(Mono.just(second)).block();
        assertThat(secondResult).isNotNull();
        assertThat(secondResult.getId()).isEqualTo(first.getId());
        assertThat(secondResult.getStatus()).isEqualTo(Transaction.TransactionStatus.COMPLETED.name());

        // La cadena externa solo se ejecutó una vez (la original), no para el duplicado.
        Mockito.verify(coreBankingAdapter, Mockito.times(1)).processTransaction(any());

        // La transacción previa se preserva (misma id registrada).
        Transaction stored = paymentUseCase.getProcessedTransaction(second.getIdempotencyKey()).block();
        assertThat(stored).isNotNull();
        assertThat(stored.getId()).isEqualTo(first.getId());
    }

    // Feature: implementacion-idempotencia, Property 7: Exactamente una ejecución bajo concurrencia (Req 5.1, 5.2, 5.3)
    @Test
    @DisplayName("Property 7: ante K solicitudes concurrentes con la misma clave, la cadena externa se ejecuta una sola vez")
    void exactlyOnceUnderConcurrency() throws InterruptedException {
        final int K = 16;
        final String operationNumber = "OP-RACE";
        final String channel = "web";

        ExecutorService pool = Executors.newFixedThreadPool(K);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(K);
        // Solicitudes que pierden la carrera: o bien reciben 409 (record aún IN_PROGRESS),
        // o bien el resultado cacheado (record ya COMPLETED). Ambos son resultados de duplicado
        // válidos en Fase 2 y, en cualquier caso, NO reejecutan la cadena externa.
        AtomicInteger rejectedInProgress = new AtomicInteger(0);
        AtomicInteger cachedResults = new AtomicInteger(0);
        ConcurrentLinkedQueue<Throwable> unexpected = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < K; i++) {
            pool.submit(() -> {
                try {
                    startGate.await();
                    Transaction tx = newTransaction(operationNumber, channel);
                    try {
                        Transaction result = paymentUseCase.processPayment(Mono.just(tx)).block();
                        if (result != null) {
                            cachedResults.incrementAndGet();
                        }
                    } catch (TransactionAlreadyProcessedException dup) {
                        rejectedInProgress.incrementAndGet();
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
        // Invariante central de "exactamente-una-ejecución": la cadena externa (core bancario)
        // se invoca una sola vez pese a las K solicitudes concurrentes idénticas (Req 5.1).
        Mockito.verify(coreBankingAdapter, Mockito.times(1)).processTransaction(any());
        // Cada una de las K solicitudes obtuvo una respuesta de duplicado (409 o resultado cacheado).
        assertThat(rejectedInProgress.get() + cachedResults.get())
                .as("las K solicitudes resuelven como ejecución única + duplicados")
                .isEqualTo(K);
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
