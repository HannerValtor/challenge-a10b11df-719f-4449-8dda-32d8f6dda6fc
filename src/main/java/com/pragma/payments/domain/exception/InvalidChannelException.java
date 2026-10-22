package com.pragma.payments.domain.exception;

/**
 * Señala que el canal de una solicitud de pago no pertenece al conjunto admitido
 * ({@code web}, {@code mobile}, {@code api}). Se mapea a HTTP 400 (Requirement 4.4).
 */
public class InvalidChannelException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String channel;

    public InvalidChannelException(String channel) {
        super("Canal inválido: '" + channel + "'. Canales admitidos: web, mobile, api.");
        this.channel = channel;
    }

    public String getChannel() {
        return channel;
    }
}
