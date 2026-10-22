package com.pragma.payments.infrastructure.config;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests unitarios de la configuración de concurrencia acotada (Fase 1).
 * Valida los Requirements 2.2 (bulkhead 25 por servicio) y 2.4 (TaskExecutor acotado).
 */
@DisplayName("Resilience4jConfig - Configuración de concurrencia acotada")
class Resilience4jConfigTest {

    private final Resilience4jConfig config = new Resilience4jConfig();

    @Test
    @DisplayName("El BulkheadRegistry expone los tres servicios con maxConcurrentCalls=25 y maxWaitDuration=0")
    void bulkheadRegistryExposesThreeServicesWithExpectedLimits() {
        BulkheadRegistry registry = config.bulkheadRegistry();

        List<String> expectedServices = List.of(
                "fraudDetectionService", "riskBureauService", "coreBankingService");

        for (String serviceName : expectedServices) {
            Bulkhead bulkhead = registry.bulkhead(serviceName);
            assertThat(bulkhead).as("bulkhead %s debe existir", serviceName).isNotNull();
            assertThat(bulkhead.getBulkheadConfig().getMaxConcurrentCalls())
                    .as("maxConcurrentCalls de %s", serviceName)
                    .isEqualTo(25);
            assertThat(bulkhead.getBulkheadConfig().getMaxWaitDuration())
                    .as("maxWaitDuration de %s (rechazo inmediato, sin bloqueo)", serviceName)
                    .isEqualTo(Duration.ZERO);
        }
    }

    @Test
    @DisplayName("El ThreadPoolTaskExecutor tiene corePoolSize, maxPoolSize y queueCapacity finitos > 0")
    void taskExecutorHasFiniteBoundedPool() {
        TaskExecutor taskExecutor = config.applicationTaskExecutor();

        assertThat(taskExecutor).isInstanceOf(ThreadPoolTaskExecutor.class);
        ThreadPoolTaskExecutor executor = (ThreadPoolTaskExecutor) taskExecutor;

        int cores = Runtime.getRuntime().availableProcessors();

        assertThat(executor.getCorePoolSize())
                .as("corePoolSize finito > 0, dimensionado por núcleos de CPU")
                .isEqualTo(cores)
                .isGreaterThan(0);
        assertThat(executor.getMaxPoolSize())
                .as("maxPoolSize finito > 0 (2 x núcleos)")
                .isEqualTo(cores * 2)
                .isGreaterThan(0);
        assertThat(executor.getQueueCapacity())
                .as("queueCapacity finita > 0 (backlog acotado)")
                .isEqualTo(100)
                .isGreaterThan(0);
    }
}
