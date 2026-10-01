package com.pragma.payments.domain.model;

import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Representa la clave de idempotencia para el procesamiento de transacciones.
 * 
 * La clave está compuesta por el número de operación y el canal de origen,
 * formando un identificador único que garantiza que una transacción no se
 * procese más de una vez, incluso bajo condiciones de concurrencia.
 * 
 * Esta clase encapsula la lógica de validación y generación de claves de
 * idempotencia, asegurando que se cumplan las reglas de negocio definidas.
 */
public class IdempotencyKey {
    
    private static final Pattern OPERATION_NUMBER_PATTERN = Pattern.compile("^[A-Za-z0-9\\-]+$");
    private static final Pattern CHANNEL_PATTERN = Pattern.compile("^[A-Za-z0-9_]+$");
    
    @NotBlank(message = "El número de operación es obligatorio para la clave de idempotencia")
    @Size(min = 1, max = 50, message = "El número de operación debe tener entre 1 y 50 caracteres")
    private String operationNumber;
    
    @NotBlank(message = "El canal es obligatorio para la clave de idempotencia")
    @Size(min = 1, max = 20, message = "El canal debe tener entre 1 y 20 caracteres")
    private String channel;
    
    private LocalDateTime generatedAt;
    
    public IdempotencyKey() {
        this.generatedAt = LocalDateTime.now();
    }
    
    public IdempotencyKey(String operationNumber, String channel) {
        this();
        validateOperationNumber(operationNumber);
        validateChannel(channel);
        this.operationNumber = operationNumber;
        this.channel = channel;
    }
    
    private void validateOperationNumber(String operationNumber) {
        if (operationNumber == null || operationNumber.isBlank()) {
            throw new IllegalArgumentException("El número de operación no puede ser nulo o vacío");
        }
        if (!OPERATION_NUMBER_PATTERN.matcher(operationNumber).matches()) {
            throw new IllegalArgumentException(
                "El número de operación contiene caracteres inválidos. Solo se permiten letras, números y guiones"
            );
        }
    }
    
    private void validateChannel(String channel) {
        if (channel == null || channel.isBlank()) {
            throw new IllegalArgumentException("El canal no puede ser nulo o vacío");
        }
        if (!CHANNEL_PATTERN.matcher(channel).matches()) {
            throw new IllegalArgumentException(
                "El canal contiene caracteres inválidos. Solo se permiten letras, números y guiones bajos"
            );
        }
    }
    
    public void validateNotDuplicate(IdempotencyKey existingKey) {
        if (existingKey != null && this.equals(existingKey)) {
            throw new TransactionAlreadyProcessedException(
                this.toUniqueString(),
                this.channel,
                null,
                null,
                this.operationNumber
            );
        }
    }
    
    public static IdempotencyKey fromTransaction(String operationNumber, String channel) {
        return new IdempotencyKey(operationNumber, channel);
    }
    
    public String toUniqueString() {
        return operationNumber + "_" + channel;
    }
    
    public String getOperationNumber() {
        return operationNumber;
    }
    
    public String getChannel() {
        return channel;
    }
    
    public LocalDateTime getGeneratedAt() {
        return generatedAt;
    }
    
    public void setOperationNumber(String operationNumber) {
        validateOperationNumber(operationNumber);
        this.operationNumber = operationNumber;
    }
    
    public void setChannel(String channel) {
        validateChannel(channel);
        this.channel = channel;
    }
    
    public void setGeneratedAt(LocalDateTime generatedAt) {
        this.generatedAt = generatedAt;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdempotencyKey that = (IdempotencyKey) o;
        return Objects.equals(operationNumber, that.operationNumber) &&
               Objects.equals(channel, that.channel);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(operationNumber, channel);
    }
    
    @Override
    public String toString() {
        return "IdempotencyKey{" +
                "operationNumber='" + operationNumber + '\'' +
                ", channel='" + channel + '\'' +
                ", generatedAt=" + generatedAt +
                '}';
    }
}