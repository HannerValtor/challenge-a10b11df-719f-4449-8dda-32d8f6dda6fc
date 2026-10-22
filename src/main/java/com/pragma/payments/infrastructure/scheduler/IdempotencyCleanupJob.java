package com.pragma.payments.infrastructure.scheduler;

import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Cleanup_Process programado que elimina periódicamente los Idempotency_Record expirados
 * del almacenamiento de idempotencia, acotando su crecimiento (Requirements 6.3, 6.4).
 *
 * <p>En cada ejecución invoca {@link IdempotencyStorePort#deleteExpired(Instant)} con el
 * instante actual y registra con SLF4J el número de registros eliminados. La periodicidad
 * del barrido se externaliza en {@link IdempotencyProperties} bajo
 * {@code payments.idempotency.cleanup.fixed-delay} (Requirement 6.2).</p>
 *
 * <p>Como {@code deleteExpired} devuelve un {@link reactor.core.publisher.Mono}, el job se
 * suscribe de forma no bloqueante dentro del contexto del scheduler, preservando el modelo
 * reactivo de WebFlux sin bloquear el event-loop.</p>
 */
@Component
public class IdempotencyCleanupJob {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyCleanupJob.class);

    private final IdempotencyStorePort store;
    private final IdempotencyProperties properties;

    public IdempotencyCleanupJob(IdempotencyStorePort store, IdempotencyProperties properties) {
        this.store = store;
        this.properties = properties;
    }

    /**
     * Ejecuta el barrido de expiración con la periodicidad configurada
     * ({@code payments.idempotency.cleanup.fixed-delay}, en milisegundos).
     *
     * <p>Se suscribe de forma no bloqueante al resultado reactivo y registra el número de
     * registros eliminados, o el error en caso de fallo del barrido.</p>
     */
    @Scheduled(fixedDelayString = "${payments.idempotency.cleanup.fixed-delay}")
    public void cleanupExpiredRecords() {
        Instant now = Instant.now();
        log.debug("Iniciando limpieza de Idempotency_Record expirados en now={}", now);
        store.deleteExpired(now)
                .subscribe(
                        deletedCount -> log.info(
                                "Cleanup_Process completado: {} Idempotency_Record expirados eliminados",
                                deletedCount),
                        error -> log.error(
                                "Error durante la limpieza de Idempotency_Record expirados",
                                error));
    }
}
