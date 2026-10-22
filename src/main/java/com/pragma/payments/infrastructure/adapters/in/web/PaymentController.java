package com.pragma.payments.infrastructure.adapters.in.web;



import com.pragma.payments.domain.exception.ExternalServiceTimeoutException;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import com.pragma.payments.application.ports.in.PaymentServicePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.domain.model.Transaction.TransactionStatus;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payments")
@Tag(name = "Pagos", description = "Endpoints para procesamiento de transacciones de pago")
public class PaymentController {

    private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

    private final PaymentServicePort paymentServicePort;

    public PaymentController(PaymentServicePort paymentServicePort) {
        this.paymentServicePort = paymentServicePort;
    }

    @PostMapping
    @Operation(summary = "Procesar pago", description = "Procesa una solicitud de pago de forma asíncrona")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Pago procesado exitosamente"),
            @ApiResponse(responseCode = "400", description = "Solicitud inválida"),
            @ApiResponse(responseCode = "409", description = "Transacción duplicada"),
            @ApiResponse(responseCode = "503", description = "Servicio no disponible")
    })
    public Mono<ResponseEntity<PaymentResponse>> processPayment(
            @Parameter(description = "Datos de la solicitud de pago") @RequestBody PaymentRequest request) {

        log.info("Recibida solicitud de pago para operación: {} desde canal: {}",
                request.operationNumber(), request.channel());

        // Validación de canal en el borde (Requirement 4.4): un canal no admitido se rechaza
        // ANTES de crear o tocar cualquier transacción. El GlobalExceptionHandler lo mapea a 400.
        validateChannel(request.channel());

        IdempotencyKey idempotencyKey = new IdempotencyKey(
                request.operationNumber(),
                request.channel()
        );

        Transaction transaction = Transaction.builder()
                .id(UUID.randomUUID().toString())
                .idempotencyKey(idempotencyKey)
                .accountId(request.accountId())
                .amount(request.amount())
                .currency(request.currency())
                .description(request.description())
                .status(TransactionStatus.PENDING)
                .createdAt(Instant.now())
                .build();

        // El mapeo de errores a HTTP está centralizado en GlobalExceptionHandler (sin onErrorResume
        // por tipo aquí): las señales de error se propagan y se traducen de forma no bloqueante.
        return paymentServicePort.processPayment(Mono.just(transaction))
                .map(this::toResponse)
                .map(ResponseEntity::ok);
    }

    private static final java.util.Set<String> VALID_CHANNELS = java.util.Set.of("web", "mobile", "api");

    private void validateChannel(String channel) {
        if (channel == null || !VALID_CHANNELS.contains(channel.toLowerCase(java.util.Locale.ROOT))) {
            throw new com.pragma.payments.domain.exception.InvalidChannelException(channel);
        }
    }

    @GetMapping("/status/{operationNumber}/{channel}")
    @Operation(summary = "Consultar estado de pago",
            description = "Verifica si una transacción fue procesada usando clave de idempotencia")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Consulta exitosa"),
            @ApiResponse(responseCode = "404", description = "Transacción no encontrada")
    })
    public Mono<ResponseEntity<PaymentResponse>> getPaymentStatus(
            @Parameter(description = "Número de operación") @PathVariable String operationNumber,
            @Parameter(description = "Canal de la transacción") @PathVariable String channel) {

        log.info("Consultando estado de pago para operación: {} canal: {}", operationNumber, channel);

        IdempotencyKey idempotencyKey = new IdempotencyKey(operationNumber, channel);

        return paymentServicePort.getProcessedTransaction(idempotencyKey)
                .map(this::toResponse)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @RequestMapping(method = RequestMethod.HEAD, value = "/exists/{operationNumber}/{channel}")
    @Operation(summary = "Verificar existencia de transacción",
            description = "Verifica rápidamente si una transacción fue procesada")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Transacción existe"),
            @ApiResponse(responseCode = "404", description = "Transacción no encontrada")
    })
    public Mono<ResponseEntity<Void>> checkTransactionExists(
            @Parameter(description = "Número de operación") @PathVariable String operationNumber,
            @Parameter(description = "Canal de la transacción") @PathVariable String channel) {

        IdempotencyKey idempotencyKey = new IdempotencyKey(operationNumber, channel);

        return paymentServicePort.isTransactionProcessed(idempotencyKey)
                .flatMap(exists -> exists
                        ? Mono.just(ResponseEntity.noContent().build())
                        : Mono.just(ResponseEntity.notFound().build()));
    }

    private PaymentResponse toResponse(Transaction transaction) {
        return PaymentResponse.builder()
                .transactionId(transaction.getId())
                .status(transaction.getStatus())
                .amount(transaction.getAmount())
                .currency(transaction.getCurrency())
                .processedAt(transaction.getProcessedAt())
                .failureReason(transaction.getFailureReason())
                .riskScore(transaction.getRiskScore())
                .build();
    }

    public record PaymentRequest(
            String operationNumber,
            String channel,
            String accountId,
            BigDecimal amount,
            String currency,
            String description
    ) {}

    public record PaymentResponse(
            String transactionId,
            String status,
            BigDecimal amount,
            String currency,
            Instant processedAt,
            String failureReason,
            Integer riskScore
    ) {
        public static PaymentResponse error(String message) {
            return new PaymentResponse(null, "ERROR", null, null, null, message, null);
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String transactionId;
            private String status;
            private BigDecimal amount;
            private String currency;
            private Instant processedAt;
            private String failureReason;
            private Integer riskScore;

            public Builder transactionId(String transactionId) {
                this.transactionId = transactionId;
                return this;
            }

            public Builder status(String status) {
                this.status = status;
                return this;
            }

            public Builder amount(BigDecimal amount) {
                this.amount = amount;
                return this;
            }

            public Builder currency(String currency) {
                this.currency = currency;
                return this;
            }

            public Builder processedAt(Instant processedAt) {
                this.processedAt = processedAt;
                return this;
            }

            public Builder failureReason(String failureReason) {
                this.failureReason = failureReason;
                return this;
            }

            public Builder riskScore(Integer riskScore) {
                this.riskScore = riskScore;
                return this;
            }

            public PaymentResponse build() {
                return new PaymentResponse(
                        transactionId, status, amount, currency, processedAt, failureReason, riskScore);
            }
        }
    }
}