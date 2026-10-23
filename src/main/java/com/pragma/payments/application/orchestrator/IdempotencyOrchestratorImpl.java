package com.pragma.payments.application.orchestrator;

import com.pragma.payments.application.chain.PaymentProcessingChain;
import com.pragma.payments.application.ports.out.DuplicateReservationException;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.exception.TransactionAlreadyProcessedException;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.config.IdempotencyProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Implementación por defecto de {@link IdempotencyOrchestrator} que encapsula el
 * ciclo de vida de idempotencia (find &rarr; decidir &rarr; reserve &rarr; ejecutar
 * cadena &rarr; complete/fail) garantizando la semántica de
 * <em>exactamente-una-ejecución</em>.
 *
 * <p>Responsabilidades (D1, D7):</p>
 * <ul>
 *   <li>Consultar el {@link IdempotencyStorePort} por la clave y decidir según el
 *       {@link RecordStatus} obtenido: {@code COMPLETED} retorna el resultado
 *       almacenado sin ejecutar la cadena (Req 3.3, 7.3); {@code IN_PROGRESS}
 *       produce un conflicto (Req 3.4, 7.6 &rarr; HTTP 409); {@code FAILED} o
 *       ausente conducen a reservar y procesar (Req 7.4).</li>
 *   <li>Reservar atómicamente la clave mediante inserción condicional
 *       ({@link IdempotencyStorePort#reserve}); ante
 *       {@link DuplicateReservationException} (pérdida de la carrera), releer el
 *       registro y decidir <strong>sin</strong> ejecutar la cadena (Req 3.8, 7.1,
 *       7.2).</li>
 *   <li>Ejecutar la cadena externa exactamente una vez vía
 *       {@link PaymentProcessingChain}; completar el registro en éxito o marcarlo
 *       {@code FAILED}/liberarlo ante un fallo transitorio (Req 7.9).</li>
 * </ul>
 *
 * <p>No emplea {@code synchronized} ni {@code ReentrantLock}: la atomicidad de la
 * reserva se delega exclusivamente en la restricción de unicidad del almacenamiento
 * persistente (D7, Req 3.8). Tampoco envuelve las llamadas al store en
 * {@code subscribeOn(boundedElastic)}: el salto de scheduler lo posee el adaptador
 * de persistencia (H7).</p>
 */
@Component
public class IdempotencyOrchestratorImpl implements IdempotencyOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyOrchestratorImpl.class);

    private final IdempotencyStorePort idempotencyStore;
    private final PaymentProcessingChain processingChain;
    private final IdempotencyProperties idempotencyProperties;

    public IdempotencyOrchestratorImpl(IdempotencyStorePort idempotencyStore,
                                       PaymentProcessingChain processingChain,
                                       IdempotencyProperties idempotencyProperties) {
        this.idempotencyStore = idempotencyStore;
        this.processingChain = processingChain;
        this.idempotencyProperties = idempotencyProperties;
    }

    @Override
    public Mono<Transaction> resolve(IdempotencyKey key, Transaction transaction) {
        // Se consulta el store ANTES de ejecutar la cadena de servicios externos (Req 3.2).
        // Si find falla técnicamente, el error se propaga y nunca se ejecuta la cadena.
        return idempotencyStore.find(key)
                .flatMap(existing -> decideByExistingRecord(existing, transaction))
                // Record ausente/expirado: continuar a la reserva (Req 7.4).
                .switchIfEmpty(Mono.defer(() -> reserveAndProcess(key, transaction)));
    }

    /**
     * Aplica la decisión de idempotencia a partir de un registro existente (Req 3.2, 7.3, 7.4, 7.6):
     * <ul>
     *     <li>{@code COMPLETED} &rarr; retorna el resultado almacenado sin ejecutar la cadena externa.</li>
     *     <li>{@code IN_PROGRESS} &rarr; lanza {@link TransactionAlreadyProcessedException} (&rarr; HTTP 409).</li>
     *     <li>{@code FAILED} &rarr; continúa a una nueva reserva para permitir el reintento.</li>
     * </ul>
     */
    private Mono<Transaction> decideByExistingRecord(IdempotencyRecord existing, Transaction transaction) {
        RecordStatus status = existing.getStatus();
        if (status == RecordStatus.COMPLETED) {
            log.info("Solicitud duplicada COMPLETED para clave {}: se retorna el resultado almacenado",
                    canonicalKey(existing.getKey()));
            return Mono.just(existing.getResult());
        }
        if (status == RecordStatus.IN_PROGRESS) {
            log.warn("Solicitud duplicada IN_PROGRESS para clave {}: se rechaza (409)",
                    canonicalKey(existing.getKey()));
            return Mono.error(toAlreadyProcessed(existing));
        }
        // FAILED: elegible para reintento (Req 7.4).
        log.info("Registro FAILED para clave {}: se reintenta reservando de nuevo",
                canonicalKey(existing.getKey()));
        return reserveAndProcess(transaction.getIdempotencyKey(), transaction);
    }

    /**
     * Reserva atómicamente la clave en estado {@code IN_PROGRESS} (Req 3.3) y, si gana la carrera,
     * ejecuta la cadena de servicios externos exactamente una vez vía {@link PaymentProcessingChain}
     * y luego completa el registro (Req 3.4). Si pierde la carrera por un duplicado (violación de
     * unicidad traducida a {@link DuplicateReservationException}), relee el registro y aplica la misma
     * decisión sin ejecutar la cadena (Req 3.8, 7.1, 7.2). Ante un fallo transitorio de la cadena,
     * marca el registro como {@code FAILED} o libera la reserva para habilitar un reintento posterior
     * (Req 7.9).
     */
    private Mono<Transaction> reserveAndProcess(IdempotencyKey key, Transaction transaction) {
        IdempotencyRecord reservation =
                IdempotencyRecord.reserve(key, Instant.now(), idempotencyProperties.getTtl());

        return idempotencyStore.reserve(reservation)
                .flatMap(reserved -> executeChainAndComplete(key, transaction))
                // Pérdida de la carrera: relee y aplica la decisión de Duplicate_Request (Req 3.8, 7.2).
                .onErrorResume(DuplicateReservationException.class, dup -> {
                    log.warn("Reserva perdida por carrera para clave {}: se relee el registro",
                            canonicalKey(key));
                    return idempotencyStore.find(key)
                            .flatMap(existing -> decideByExistingRecord(existing, transaction))
                            .switchIfEmpty(Mono.error(dup));
                });
    }

    /**
     * Ejecuta la cadena de servicios externos exactamente una vez vía {@link PaymentProcessingChain}
     * y, al terminar con éxito, persiste el resultado completando el registro de idempotencia. Ante un
     * fallo transitorio (cualquier error que no sea duplicado ni already-processed), marca el registro
     * como {@code FAILED}/lo libera y propaga el error (Req 7.9).
     */
    private Mono<Transaction> executeChainAndComplete(IdempotencyKey key, Transaction transaction) {
        return processingChain.execute(transaction)
                .flatMap(result -> idempotencyStore.complete(key, result).thenReturn(result))
                .onErrorResume(error -> {
                    if (error instanceof TransactionAlreadyProcessedException
                            || error instanceof DuplicateReservationException) {
                        return Mono.error(error);
                    }
                    // Fallo transitorio de la cadena externa: dejar la clave elegible a reintento (Req 7.9).
                    log.warn("Fallo transitorio procesando la clave {}: se marca FAILED/libera la reserva",
                            canonicalKey(key));
                    return idempotencyStore.markFailedOrRelease(key)
                            .then(Mono.error(error));
                });
    }

    private TransactionAlreadyProcessedException toAlreadyProcessed(IdempotencyRecord record) {
        IdempotencyKey key = record.getKey();
        Transaction result = record.getResult();
        return new TransactionAlreadyProcessedException(
                canonicalKey(key),
                key != null ? key.getChannel() : "unknown",
                result != null ? result.getId() : null,
                result != null ? result.getProcessedAt() : null,
                key != null ? key.getOperationNumber() : "unknown");
    }

    private String canonicalKey(IdempotencyKey key) {
        if (key == null) {
            return "unknown";
        }
        return key.getOperationNumber() + "_" + key.getChannel();
    }
}
