package com.pragma.payments.domain.model;

import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Entidad de dominio que representa una transacción financiera.
 * Encapsula toda la información necesaria para procesar un pago,
 * incluyendo el manejo de idempotencia mediante la clave compuesta
 * formada por el número de operación y el canal de origen.
 */
public class Transaction {

    private String id;
    private IdempotencyKey idempotencyKey;
    private String accountId;
    private BigDecimal amount;
    private String currency;
    private String channel;
    private String description;
    private String status;
    private String failureReason;
    private Integer riskScore;
    private Boolean fraudDetected;
    private String externalReference;
    private Instant createdAt;
    private Instant processedAt;

    public Transaction() {
        this.id = UUID.randomUUID().toString();
        this.status = TransactionStatus.PENDING.name();
        this.createdAt = Instant.now();
    }

    public enum TransactionStatus {
        PENDING,
        VALIDATING,
        PROCESSING,
        APPROVED,
        COMPLETED,
        FAILED,
        REJECTED,
        DUPLICATE
    }

    public void validateNotAlreadyProcessed() {
        if (TransactionStatus.COMPLETED.name().equals(this.status)
                || TransactionStatus.DUPLICATE.name().equals(this.status)) {
            throw new TransactionAlreadyProcessedException(
                    this.idempotencyKey != null ? this.idempotencyKey.toUniqueString() : "unknown",
                    this.channel,
                    this.id,
                    this.processedAt,
                    this.idempotencyKey != null ? this.idempotencyKey.getOperationNumber() : "unknown");
        }
    }

    public void markAsCompleted(String externalReference) {
        this.status = TransactionStatus.COMPLETED.name();
        this.externalReference = externalReference;
        this.processedAt = Instant.now();
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getId() {
        return id;
    }

    public IdempotencyKey getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getAccountId() {
        return accountId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getChannel() {
        return channel;
    }

    public String getDescription() {
        return description;
    }

    public String getStatus() {
        return status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public Integer getRiskScore() {
        return riskScore;
    }

    public Boolean getFraudDetected() {
        return fraudDetected;
    }

    public String getExternalReference() {
        return externalReference;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setIdempotencyKey(IdempotencyKey idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setStatus(TransactionStatus status) {
        this.status = status != null ? status.name() : null;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public void setRiskScore(Integer riskScore) {
        this.riskScore = riskScore;
    }

    public void setFraudDetected(Boolean fraudDetected) {
        this.fraudDetected = fraudDetected;
    }

    public void setExternalReference(String externalReference) {
        this.externalReference = externalReference;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public void setProcessedAt(Instant processedAt) {
        this.processedAt = processedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "id='" + id + '\'' +
                ", idempotencyKey=" + idempotencyKey +
                ", accountId='" + accountId + '\'' +
                ", amount=" + amount +
                ", channel='" + channel + '\'' +
                ", status='" + status + '\'' +
                ", createdAt=" + createdAt +
                '}';
    }

    public static class Builder {
        private final Transaction transaction = new Transaction();

        public Builder id(String id) {
            transaction.id = id;
            return this;
        }

        public Builder idempotencyKey(IdempotencyKey idempotencyKey) {
            transaction.idempotencyKey = idempotencyKey;
            return this;
        }

        public Builder accountId(String accountId) {
            transaction.accountId = accountId;
            return this;
        }

        public Builder amount(BigDecimal amount) {
            transaction.amount = amount;
            return this;
        }

        public Builder currency(String currency) {
            transaction.currency = currency;
            return this;
        }

        public Builder channel(String channel) {
            transaction.channel = channel;
            return this;
        }

        public Builder description(String description) {
            transaction.description = description;
            return this;
        }

        public Builder status(String status) {
            transaction.status = status;
            return this;
        }

        public Builder status(TransactionStatus status) {
            transaction.status = status != null ? status.name() : null;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            transaction.createdAt = createdAt;
            return this;
        }

        public Transaction build() {
            return transaction;
        }
    }
}
