package com.pragma.payments.infrastructure.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Habilita el soporte de tareas programadas de Spring ({@code @Scheduled}).
 *
 * <p>Necesaria para que el Cleanup_Process ({@code IdempotencyCleanupJob}) se ejecute
 * de forma periódica barriendo los Idempotency_Record expirados (Requirements 6.3, 6.4).</p>
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
