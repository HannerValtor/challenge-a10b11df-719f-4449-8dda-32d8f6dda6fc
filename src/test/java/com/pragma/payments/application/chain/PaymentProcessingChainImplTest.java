package com.pragma.payments.application.chain;

import com.pragma.payments.application.ports.out.CoreBankingPort;
import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.application.ports.out.RiskBureauPort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests unitarios de {@link PaymentProcessingChainImpl} con puertos mockeados (Mockito) y
 * verificación reactiva (Reactor {@link StepVerifier}).
 *
 * <p>Cubre el comportamiento definido en el diseño (D1, corrección de H3):</p>
 * <ul>
 *   <li>Short-circuit en {@link TransactionStatus#REJECTED}: cuando la transacción se rechaza en
 *       fraude, no se invoca riesgo ni core; cuando se rechaza por riesgo alto, no se invoca core
 *       (Requirements 3.6, 3.7).</li>
 *   <li>Umbral de riesgo: un score por debajo de {@code 70} continúa hacia el core; un score
 *       {@code >= 70} rechaza (Requirement 3.7).</li>
 *   <li>Happy path: sin fraude y con riesgo bajo, el core se invoca y la transacción se completa
 *       (Requirement 3.5).</li>
 *   <li>Propagación de errores: un error emitido por un puerto se propaga por la cadena
 *       (Requirement 3.5).</li>
 * </ul>
 *
 * <p>El estado se verifica mediante la convención del enum {@link TransactionStatus#name()},
 * coherente con la implementación que compara por enum y nunca contra literales String
 * (Requirement 3.6).</p>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentProcessingChainImpl - cadena externa fraude -> riesgo -> core")
class PaymentProcessingChainImplTest {

    /** Umbral de riesgo alto preservado del flujo original (score >= 70 rechaza). */
    private static final int HIGH_RISK_THRESHOLD = 70;

    @Mock
    private FraudDetectionPort fraudDetectionPort;

    @Mock
    private RiskBureauPort riskBureauPort;

    @Mock
    private CoreBankingPort coreBankingPort;

    @InjectMocks
    private PaymentProcessingChainImpl chain;

    private Transaction transaction;

    @BeforeEach
    void setUp() {
        transaction = Transaction.builder()
                .id("tx-1")
                .idempotencyKey(new IdempotencyKey("OP-1", "web"))
                .accountId("acc-1")
                .amount(new BigDecimal("100.00"))
                .currency("USD")
                .channel("web")
                .status(TransactionStatus.PENDING)
                .build();
    }

    @Nested
    @DisplayName("Short-circuit por REJECTED (verifica H3)")
    class ShortCircuitOnRejected {

        @Test
        @DisplayName("Fraude detectado -> REJECTED y NO se invoca riesgo ni core")
        void fraudRejectedSkipsRiskAndCore() {
            when(fraudDetectionPort.evaluateTransaction(any())).thenReturn(Mono.just(true));

            StepVerifier.create(chain.execute(transaction))
                    .assertNext(result -> {
                        assertThat(result.getStatus()).isEqualTo(TransactionStatus.REJECTED);
                        assertThat(result.getFraudDetected()).isTrue();
                    })
                    .verifyComplete();

            verify(riskBureauPort, never()).getRiskScore(any());
            verify(coreBankingPort, never()).sendTransaction(any());
        }

        @Test
        @DisplayName("Riesgo alto (score >= 70) -> REJECTED y NO se invoca core")
        void highRiskRejectedSkipsCore() {
            when(fraudDetectionPort.evaluateTransaction(any())).thenReturn(Mono.just(false));
            when(riskBureauPort.getRiskScore(any())).thenReturn(Mono.just(HIGH_RISK_THRESHOLD));

            StepVerifier.create(chain.execute(transaction))
                    .assertNext(result -> {
                        assertThat(result.getStatus()).isEqualTo(TransactionStatus.REJECTED);
                        assertThat(result.getRiskScore()).isEqualTo(HIGH_RISK_THRESHOLD);
                    })
                    .verifyComplete();

            verify(coreBankingPort, never()).sendTransaction(any());
        }
    }

    @Nested
    @DisplayName("Umbral de riesgo (boundary en 70)")
    class RiskThresholdBoundary {

        @Test
        @DisplayName("Score justo por debajo del umbral (69) continua hacia el core")
        void scoreJustBelowThresholdProceedsToCore() {
            when(fraudDetectionPort.evaluateTransaction(any())).thenReturn(Mono.just(false));
            when(riskBureauPort.getRiskScore(any())).thenReturn(Mono.just(HIGH_RISK_THRESHOLD - 1));
            when(coreBankingPort.sendTransaction(any())).thenAnswer(invocation -> {
                Transaction tx = invocation.getArgument(0);
                tx.markAsCompleted("CORE-REF-1");
                return Mono.just(tx);
            });

            StepVerifier.create(chain.execute(transaction))
                    .assertNext(result -> {
                        assertThat(result.getRiskScore()).isEqualTo(HIGH_RISK_THRESHOLD - 1);
                        assertThat(result.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
                    })
                    .verifyComplete();

            verify(coreBankingPort).sendTransaction(any());
        }

        @Test
        @DisplayName("Score en el umbral (70) rechaza sin invocar el core")
        void scoreAtThresholdRejects() {
            when(fraudDetectionPort.evaluateTransaction(any())).thenReturn(Mono.just(false));
            when(riskBureauPort.getRiskScore(any())).thenReturn(Mono.just(HIGH_RISK_THRESHOLD));

            StepVerifier.create(chain.execute(transaction))
                    .assertNext(result ->
                            assertThat(result.getStatus()).isEqualTo(TransactionStatus.REJECTED))
                    .verifyComplete();

            verify(coreBankingPort, never()).sendTransaction(any());
        }
    }

    @Nested
    @DisplayName("Happy path (fraude ok + riesgo bajo -> core)")
    class HappyPath {

        @Test
        @DisplayName("Sin fraude y con riesgo bajo -> se invoca el core y la transaccion se completa")
        void notFraudulentLowRiskReachesCoreAndCompletes() {
            when(fraudDetectionPort.evaluateTransaction(any())).thenReturn(Mono.just(false));
            when(riskBureauPort.getRiskScore(any())).thenReturn(Mono.just(10));
            when(coreBankingPort.sendTransaction(any())).thenAnswer(invocation -> {
                Transaction tx = invocation.getArgument(0);
                tx.markAsCompleted("CORE-REF-OK");
                return Mono.just(tx);
            });

            StepVerifier.create(chain.execute(transaction))
                    .assertNext(result -> {
                        assertThat(result.getFraudDetected()).isFalse();
                        assertThat(result.getRiskScore()).isEqualTo(10);
                        assertThat(result.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
                        assertThat(result.getExternalReference()).isEqualTo("CORE-REF-OK");
                    })
                    .verifyComplete();

            verify(fraudDetectionPort).evaluateTransaction(any());
            verify(riskBureauPort).getRiskScore(any());
            verify(coreBankingPort).sendTransaction(any());
        }
    }

    @Nested
    @DisplayName("Propagacion de errores por la cadena")
    class ErrorPropagation {

        @Test
        @DisplayName("Error en fraude se propaga y NO se invoca riesgo ni core")
        void fraudErrorPropagates() {
            when(fraudDetectionPort.evaluateTransaction(any()))
                    .thenReturn(Mono.error(new RuntimeException("fraud failure")));

            StepVerifier.create(chain.execute(transaction))
                    .expectErrorMatches(err -> err instanceof RuntimeException
                            && "fraud failure".equals(err.getMessage()))
                    .verify();

            verify(riskBureauPort, never()).getRiskScore(any());
            verify(coreBankingPort, never()).sendTransaction(any());
        }

        @Test
        @DisplayName("Error en riesgo se propaga y NO se invoca core")
        void riskErrorPropagates() {
            when(fraudDetectionPort.evaluateTransaction(any())).thenReturn(Mono.just(false));
            when(riskBureauPort.getRiskScore(any()))
                    .thenReturn(Mono.error(new RuntimeException("risk failure")));

            StepVerifier.create(chain.execute(transaction))
                    .expectErrorMatches(err -> err instanceof RuntimeException
                            && "risk failure".equals(err.getMessage()))
                    .verify();

            verify(coreBankingPort, never()).sendTransaction(any());
        }

        @Test
        @DisplayName("Error en core se propaga hacia el llamador")
        void coreErrorPropagates() {
            when(fraudDetectionPort.evaluateTransaction(any())).thenReturn(Mono.just(false));
            when(riskBureauPort.getRiskScore(any())).thenReturn(Mono.just(5));
            when(coreBankingPort.sendTransaction(any()))
                    .thenReturn(Mono.error(new RuntimeException("core failure")));

            StepVerifier.create(chain.execute(transaction))
                    .expectErrorMatches(err -> err instanceof RuntimeException
                            && "core failure".equals(err.getMessage()))
                    .verify();

            verify(coreBankingPort).sendTransaction(any());
        }
    }
}
