package com.pragma.payments.infrastructure.adapters.out.fraud;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.pragma.payments.application.ports.out.FraudDetectionPort;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.config.ExternalTimeoutProperties;
import com.pragma.payments.infrastructure.config.FraudCacheProperties;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;

/**
 * Adaptador de salida hacia el servicio de detección de fraude (Fase 3, D4, H2, H5).
 *
 * <p>Refactorización de Fase 3:</p>
 * <ul>
 *   <li><b>Caché acotada y determinista (D4, H5, Requirements 5.1–5.5):</b> se reemplaza el
 *       {@code ConcurrentHashMap} ilimitado por una caché Caffeine con {@code maximumSize} y
 *       {@code expireAfterWrite} configurables externamente ({@link FraudCacheProperties}). La
 *       clave se deriva únicamente de atributos de entrada estables (cuenta, monto, moneda, canal),
 *       sin marcas de tiempo ni valores aleatorios, de modo que dos solicitudes con los mismos
 *       atributos produzcan siempre la misma clave.</li>
 *   <li><b>Resiliencia efectiva (D6, H2, Requirement 2.2):</b> las anotaciones
 *       {@code @CircuitBreaker}/{@code @Retry} se declaran sobre el método público
 *       {@link #evaluateTransaction(Transaction)}, invocado a través del límite del bean, para que
 *       el AOP de Resilience4j las intercepte realmente.</li>
 *   <li><b>Timeout coherente (D2, H1, Requirements 1.6, 1.8, 1.9):</b> el timeout de fraude se toma
 *       de {@link ExternalTimeoutProperties#getFraud()} en lugar de un valor hardcodeado.</li>
 * </ul>
 */
@Component
public class FraudDetectionAdapter implements FraudDetectionPort {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionAdapter.class);
    private static final String FRAUD_SERVICE_URL = "http://fraud-detection-service:8081/api/v1/evaluate";

    private final WebClient webClient;
    private final Bulkhead bulkhead;
    private final Duration requestTimeout;
    private final Cache<String, Boolean> fraudCache;

    public FraudDetectionAdapter(
            WebClient webClient,
            BulkheadRegistry bulkheadRegistry,
            ExternalTimeoutProperties timeoutProperties,
            FraudCacheProperties cacheProperties) {
        this.webClient = webClient;
        // Bulkhead dedicado del servicio antifraude: limita a 25 las llamadas concurrentes
        // y aísla su saturación de los demás servicios (Requirements 2.2, 5.4).
        this.bulkhead = bulkheadRegistry.bulkhead("fraudDetectionService");
        // Timeout de fraude coherente y externalizado (Requirement 1.6).
        this.requestTimeout = timeoutProperties.getFraud();
        // Caché Caffeine acotada y determinista (Requirements 5.1, 5.3, 5.5): maximumSize limita el
        // número de entradas (desalojo LRU aproximado del menos usado al superar el máximo) y
        // expireAfterWrite descarta entradas vencidas forzando el recálculo posterior.
        this.fraudCache = Caffeine.newBuilder()
                .maximumSize(cacheProperties.getMaximumSize())
                .expireAfterWrite(cacheProperties.getExpireAfterWrite())
                .build();
    }

    @Override
    @CircuitBreaker(name = "fraudDetectionService", fallbackMethod = "evaluateTransactionFallback")
    @Retry(name = "fraudDetectionService")
    public Mono<Boolean> evaluateTransaction(Transaction transaction) {
        String cacheKey = buildCacheKey(transaction);

        Boolean cached = fraudCache.getIfPresent(cacheKey);
        if (cached != null) {
            log.debug("Evaluación de fraude cacheada para transacción: {}", transaction.getId());
            return Mono.just(cached);
        }

        return Mono.fromCallable(() -> buildFraudRequest(transaction))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(requestBody -> webClient.post()
                        .uri(FRAUD_SERVICE_URL)
                        .bodyValue(requestBody)
                        .retrieve()
                        .onStatus(HttpStatusCode::is5xxServerError,
                                response -> Mono.error(new RuntimeException(
                                        "Error del servicio de detección de fraude: " + response.statusCode())))
                        .bodyToMono(FraudEvaluationResponse.class)
                        .timeout(requestTimeout))
                // Bulkhead reactivo: rechaza con BulkheadFullException si hay 25 llamadas en vuelo.
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .doOnSuccess(response -> {
                    if (response != null) {
                        log.info("Evaluación de fraude completada para transacción {}: isFraudulent={}",
                                transaction.getId(), response.isFraudulent());
                        fraudCache.put(cacheKey, response.isFraudulent());
                    }
                })
                .doOnError(error -> log.error("Error en evaluación de fraude para transacción {}: {}",
                        transaction.getId(), error.getMessage()))
                .map(FraudEvaluationResponse::isFraudulent);
    }

    /**
     * Fallback de resiliencia (D6): se invoca cuando el circuit breaker está abierto o cuando la
     * llamada falla tras agotar los reintentos. Preserva la lógica de falla segura previa: ante un
     * servicio no disponible se asume no fraudulento (false); ante cualquier otro error inesperado
     * se adopta la postura conservadora de marcar la transacción como sospechosa (true).
     */
    private Mono<Boolean> evaluateTransactionFallback(Transaction transaction, Throwable t) {
        if (t instanceof WebClientResponseException.ServiceUnavailable) {
            log.warn("Servicio de fraude no disponible para transacción {}, aplicando lógica de falla segura",
                    transaction.getId());
            return Mono.just(false);
        }
        log.error("Error inesperado en evaluación de fraude para transacción {}: {}",
                transaction.getId(), t.getMessage());
        return Mono.just(true);
    }

    private Map<String, Object> buildFraudRequest(Transaction transaction) {
        return Map.of(
                "transactionId", transaction.getId(),
                "accountId", transaction.getAccountId(),
                "amount", transaction.getAmount(),
                "currency", transaction.getCurrency(),
                "channel", transaction.getIdempotencyKey().getChannel(),
                "description", transaction.getDescription() != null ? transaction.getDescription() : ""
        );
    }

    /**
     * Construye la clave de caché únicamente a partir de atributos de entrada estables relevantes
     * para la detección de fraude (cuenta, monto, moneda y canal). No incluye marcas de tiempo
     * ({@code System.currentTimeMillis()}), identificadores por solicitud ni valores aleatorios, de
     * modo que dos solicitudes con los mismos atributos produzcan siempre la misma clave
     * (Requirement 5.2, corrige H5).
     */
    private String buildCacheKey(Transaction transaction) {
        String channel = transaction.getChannel() != null
                ? transaction.getChannel()
                : (transaction.getIdempotencyKey() != null
                        ? transaction.getIdempotencyKey().getChannel()
                        : null);
        return transaction.getAccountId() + "_"
                + transaction.getAmount() + "_"
                + transaction.getCurrency() + "_"
                + channel;
    }

    public void clearCache() {
        fraudCache.invalidateAll();
        log.info("Caché de detección de fraude limpiada");
    }

    public long getCacheSize() {
        fraudCache.cleanUp();
        return fraudCache.estimatedSize();
    }

    private record FraudEvaluationResponse(boolean isFraudulent, String riskLevel, String message) {}
}
