package com.pragma.payments.infrastructure.adapters.out.persistence;

import com.github.benmanes.caffeine.cache.Cache;
import com.pragma.payments.application.ports.out.IdempotencyStorePort;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Decorator de {@link IdempotencyStorePort} que añade una caché de lectura para registros
 * {@code COMPLETED} (Fase 3, D3, H6, Requirements 4.1–4.8).
 *
 * <p>Envuelve al {@link IdempotencyStoreAdapter} (acceso JPA/Hikari en {@code boundedElastic}) y
 * sirve desde una caché Caffeine ({@link Cache}) únicamente los registros inmutables en estado
 * {@code COMPLETED}. Los estados mutables ({@code IN_PROGRESS}, {@code FAILED}) nunca se cachean
 * (Requirement 4.1), de modo que la caché jamás devuelve un estado que pueda cambiar en la base de
 * datos.</p>
 *
 * <p>Se registra como {@link Primary}: la capa de aplicación (p. ej. el orquestador de idempotencia)
 * depende del puerto {@code IdempotencyStorePort} y recibe este decorator en lugar del adaptador
 * JPA directo. El delegate se declara como el puerto {@link IdempotencyStorePort} (respetando el
 * principio de inversión de dependencias) y se resuelve con {@link Qualifier} al bean concreto
 * {@code idempotencyStoreAdapter}, evitando tanto la auto-referencia del decorator (que es
 * {@code @Primary}) como el acoplamiento a una clase concreta.</p>
 *
 * <p><strong>Coherencia (Requirement 4.5):</strong> {@code reserve}, {@code markFailedOrRelease} y
 * {@code deleteExpired} invalidan la entrada de caché correspondiente (o toda la caché, en el
 * barrido de expiración) para que la caché nunca sirva un registro obsoleto o ya eliminado.
 * {@code complete} puebla la caché con el registro {@code COMPLETED} resultante (Requirement 4.4).</p>
 *
 * <p><strong>Degradación (Requirement 4.7):</strong> si una operación sobre la caché falla por un
 * error técnico, se registra y se continúa delegando en el store sin propagar el error al llamador.
 * El acceso a la caché Caffeine es en memoria y no bloqueante; el único acceso bloqueante (JPA) lo
 * aísla el adaptador delegado en {@code boundedElastic} (Requirement 4.8, H7): este decorator no
 * introduce un segundo salto de scheduler.</p>
 */
@Primary
@Component
public class CachingIdempotencyStore implements IdempotencyStorePort {

    private static final Logger log = LoggerFactory.getLogger(CachingIdempotencyStore.class);

    private final IdempotencyStorePort delegate;
    private final Cache<String, IdempotencyRecord> cache;

    public CachingIdempotencyStore(@Qualifier("idempotencyStoreAdapter") IdempotencyStorePort delegate,
                                   Cache<String, IdempotencyRecord> idempotencyRecordCache) {
        this.delegate = delegate;
        this.cache = idempotencyRecordCache;
    }

    /**
     * Reserva atómica delegada. La reserva deja la clave en estado {@code IN_PROGRESS} (mutable), por
     * lo que se invalida cualquier entrada de caché previa para esa clave y nunca se cachea el
     * resultado de la reserva (Requirements 4.1, 4.5).
     */
    @Override
    public Mono<IdempotencyRecord> reserve(IdempotencyRecord record) {
        String cacheKey = canonical(record.getKey());
        return delegate.reserve(record)
                .doOnSubscribe(s -> invalidateQuietly(cacheKey))
                .doOnError(e -> invalidateQuietly(cacheKey));
    }

    /**
     * Lectura con caché: si hay un hit {@code COMPLETED} en caché, se devuelve sin consultar el store
     * (Requirement 4.2). En caso de miss, se delega en el store y, únicamente si el registro devuelto
     * es {@code COMPLETED}, se cachea antes de devolverlo (Requirements 4.1, 4.3).
     */
    @Override
    public Mono<IdempotencyRecord> find(IdempotencyKey key) {
        String cacheKey = canonical(key);
        IdempotencyRecord cached = getIfPresentQuietly(cacheKey);
        if (cached != null) {
            log.debug("Hit de caché de idempotencia para clave '{}' (COMPLETED)", cacheKey);
            return Mono.just(cached);
        }
        return delegate.find(key)
                .doOnNext(record -> cacheIfCompleted(cacheKey, record));
    }

    /**
     * Completa el registro delegando en el store y, con el resultado {@code COMPLETED} inmutable,
     * puebla la caché (Requirement 4.4).
     */
    @Override
    public Mono<IdempotencyRecord> complete(IdempotencyKey key, Transaction result) {
        String cacheKey = canonical(key);
        return delegate.complete(key, result)
                .doOnNext(record -> cacheIfCompleted(cacheKey, record));
    }

    /**
     * Marca el registro como {@code FAILED}/libera la reserva e invalida la entrada de caché, de modo
     * que una lectura posterior no sirva un estado obsoleto (Requirement 4.5).
     */
    @Override
    public Mono<Void> markFailedOrRelease(IdempotencyKey key) {
        String cacheKey = canonical(key);
        return delegate.markFailedOrRelease(key)
                .doFinally(signal -> invalidateQuietly(cacheKey));
    }

    /**
     * Elimina los registros expirados en el store e invalida toda la caché de forma conservadora, ya
     * que el barrido puede eliminar múltiples claves; así ninguna entrada sobrevive a su registro
     * (Requirement 4.5).
     */
    @Override
    public Mono<Long> deleteExpired(Instant now) {
        return delegate.deleteExpired(now)
                .doFinally(signal -> invalidateAllQuietly());
    }

    /**
     * Cachea el registro solo si su estado es {@code COMPLETED} (inmutable). Cualquier otro estado se
     * ignora para no servir estados mutables desde la caché (Requirements 4.1, 4.3).
     */
    private void cacheIfCompleted(String cacheKey, IdempotencyRecord record) {
        if (record != null && record.getStatus() == RecordStatus.COMPLETED) {
            putQuietly(cacheKey, record);
        }
    }

    // --- Accesos a la caché con degradación ante error técnico (Requirement 4.7) ---

    private IdempotencyRecord getIfPresentQuietly(String cacheKey) {
        try {
            return cache.getIfPresent(cacheKey);
        } catch (RuntimeException ex) {
            log.warn("Error leyendo la caché de idempotencia para '{}'; se delega en el store: {}",
                    cacheKey, ex.getMessage());
            return null;
        }
    }

    private void putQuietly(String cacheKey, IdempotencyRecord record) {
        try {
            cache.put(cacheKey, record);
            log.debug("Registro '{}' cacheado en estado COMPLETED", cacheKey);
        } catch (RuntimeException ex) {
            log.warn("Error escribiendo en la caché de idempotencia para '{}': {}",
                    cacheKey, ex.getMessage());
        }
    }

    private void invalidateQuietly(String cacheKey) {
        try {
            cache.invalidate(cacheKey);
        } catch (RuntimeException ex) {
            log.warn("Error invalidando la entrada de caché de idempotencia '{}': {}",
                    cacheKey, ex.getMessage());
        }
    }

    private void invalidateAllQuietly() {
        try {
            cache.invalidateAll();
            log.debug("Caché de idempotencia invalidada por completo tras el barrido de expiración");
        } catch (RuntimeException ex) {
            log.warn("Error invalidando por completo la caché de idempotencia: {}", ex.getMessage());
        }
    }

    private String canonical(IdempotencyKey key) {
        return key.toUniqueString();
    }
}
