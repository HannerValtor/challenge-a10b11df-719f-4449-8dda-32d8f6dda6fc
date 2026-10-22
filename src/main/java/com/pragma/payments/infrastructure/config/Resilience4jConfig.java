package com.pragma.payments.infrastructure.config;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;

/**
 * Configuración de la estrategia de concurrencia y resiliencia del servicio de pagos.
 *
 * <p>Decisión de arquitectura (Fase 1): el procesamiento de pagos es I/O-bound dominante
 * (cada pago invoca 3 servicios externos por HTTP), por lo que la base es el modelo reactivo
 * no bloqueante de WebFlux/Reactor. Esta clase define los límites explícitos de concurrencia
 * que evitan la saturación de recursos del host:</p>
 *
 * <ul>
 *   <li><b>Bulkhead</b> (semáforo) por servicio externo: limita a 25 las llamadas concurrentes
 *       hacia cada actor (motor antifraude, buró de riesgos, core bancario), aislando fallos
 *       entre servicios. {@code maxWaitDuration = 0} ⇒ rechazo inmediato sin bloquear el hilo.</li>
 *   <li><b>TaskExecutor</b> acotado: dado que {@code @EnableAsync} está activo, se define un pool
 *       dedicado dimensionado por núcleos de CPU, en lugar del {@code SimpleAsyncTaskExecutor}
 *       ilimitado por defecto, que podría crear hilos sin control.</li>
 * </ul>
 */
@Configuration
public class Resilience4jConfig {

    private static final Logger log = LoggerFactory.getLogger(Resilience4jConfig.class);

    /** Límite de llamadas concurrentes por servicio externo (Requirement 2.2). */
    private static final int MAX_CONCURRENT_CALLS = 25;

    /** Núcleos de CPU disponibles; base para dimensionar el pool de trabajo desplazado. */
    private static final int CPU_CORES = Runtime.getRuntime().availableProcessors();

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .permittedNumberOfCallsInHalfOpenState(3)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        return CircuitBreakerRegistry.of(config);
    }

    @Bean
    public RetryRegistry retryRegistry() {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(1000))
                .build();
        return RetryRegistry.of(config);
    }

    @Bean
    public RateLimiterRegistry rateLimiterRegistry() {
        RateLimiterConfig config = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .limitForPeriod(10)
                .timeoutDuration(Duration.ofMillis(500))
                .build();
        return RateLimiterRegistry.of(config);
    }

    /**
     * Registro de bulkheads con un semáforo independiente por servicio externo.
     *
     * <p>{@code maxConcurrentCalls = 25} acota la concurrencia hacia cada actor; al alcanzar
     * el límite, las llamadas adicionales se rechazan de inmediato ({@code maxWaitDuration = 0})
     * sin bloquear el hilo invocante (event-loop de Netty), produciendo una señal de saturación
     * que la capa web mapea a 503 (Requirements 2.2, 2.3, 5.4).</p>
     */
    @Bean
    public BulkheadRegistry bulkheadRegistry() {
        BulkheadConfig config = BulkheadConfig.custom()
                .maxConcurrentCalls(MAX_CONCURRENT_CALLS)
                .maxWaitDuration(Duration.ZERO)
                .build();

        BulkheadRegistry registry = BulkheadRegistry.of(config);

        // Instancia por servicio: el fallo o saturación de uno no afecta a los demás.
        Bulkhead fraud = registry.bulkhead("fraudDetectionService");
        Bulkhead risk = registry.bulkhead("riskBureauService");
        Bulkhead core = registry.bulkhead("coreBankingService");

        log.info("BulkheadRegistry inicializado: {} llamadas concurrentes máx. por servicio [{}, {}, {}]",
                MAX_CONCURRENT_CALLS, fraud.getName(), risk.getName(), core.getName());

        return registry;
    }

    /**
     * Executor acotado para el trabajo {@code @Async} desplazado del event-loop.
     *
     * <p>Dimensionamiento justificado por núcleos de CPU: {@code corePoolSize = N núcleos}
     * (el trabajo CPU-bound no se beneficia de más hilos que núcleos), {@code maxPoolSize = 2 × N}
     * (margen para picos de trabajo mixto) y {@code queueCapacity = 100} (backlog acotado que
     * evita {@code OutOfMemoryError}). La política {@code CallerRunsPolicy} aplica contrapresión
     * cuando el pool y la cola se saturan (Requirement 2.4).</p>
     */
    @Bean("applicationTaskExecutor")
    public TaskExecutor applicationTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(CPU_CORES);
        executor.setMaxPoolSize(CPU_CORES * 2);
        executor.setQueueCapacity(100);
        executor.setThreadNamePrefix("payments-async-");
        executor.setRejectedExecutionHandler(
                new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();

        log.info("ThreadPoolTaskExecutor inicializado: corePoolSize={}, maxPoolSize={}, queueCapacity={}",
                CPU_CORES, CPU_CORES * 2, 100);

        return executor;
    }
}
