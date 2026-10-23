package com.pragma.payments.infrastructure.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alinea el tamaño del scheduler {@code Schedulers.boundedElastic()} de Reactor con el tamaño
 * máximo del pool de conexiones HikariCP (Fase 3, decisión D5; Requirements 6.2, 6.5).
 *
 * <p><b>Problema (H8).</b> Todo acceso JPA bloqueante del {@code IdempotencyStoreAdapter}
 * (find/reserve/complete/markFailedOrRelease/deleteExpired) se aísla en
 * {@code Schedulers.boundedElastic()}. Por defecto, Reactor dimensiona ese scheduler en
 * {@code 10 × núcleos} hilos, un valor que fácilmente supera el {@code maximum-pool-size} de
 * HikariCP (20). Si más hilos de {@code boundedElastic} solicitan conexión de los que el pool
 * puede entregar, el excedente queda a la espera y, pasado el {@code connection-timeout}, falla
 * con agotamiento del pool: el pool se convierte en un cuello de botella silencioso bajo carga.</p>
 *
 * <p><b>Solución (D5).</b> El pool debe acotar a {@code boundedElastic}, no al revés. Se limita el
 * número de hilos de {@code boundedElastic} a {@code maximum-pool-size} fijando la propiedad de
 * sistema {@code reactor.schedulers.defaultBoundedElasticSize}. Así, el número de hilos que pueden
 * solicitar una conexión concurrentemente nunca excede el tamaño del pool (Requirement 6.2) y el
 * número de solicitudes de conexión concurrentes permanece acotado por {@code maximum-pool-size}
 * (Requirement 6.5).</p>
 *
 * <p><b>Momento de aplicación.</b> La propiedad DEBE establecerse <em>antes</em> de que Reactor
 * inicialice su scheduler global: {@code Schedulers} lee el valor de forma perezosa en el primer
 * acceso a {@code boundedElastic()} y lo cachea. Por ello {@link #align()} se invoca al inicio de
 * {@code Application.main(...)}, antes de arrancar el contexto Spring y antes de cualquier
 * suscripción reactiva. No puede hacerse mediante un {@code @Bean} porque para entonces el
 * scheduler ya podría estar inicializado.</p>
 *
 * <p>El valor se centraliza en {@link #MAXIMUM_POOL_SIZE}, que replica el
 * {@code spring.datasource.hikari.maximum-pool-size} de {@code application.yml} (20). Ambos valores
 * deben mantenerse coherentes; cualquier ajuste del pool debe reflejarse aquí.</p>
 */
public final class BoundedElasticSchedulerConfig {

    private static final Logger log = LoggerFactory.getLogger(BoundedElasticSchedulerConfig.class);

    /**
     * Nombre de la propiedad de sistema que Reactor consulta para dimensionar el número máximo de
     * hilos del scheduler {@code boundedElastic} global.
     */
    static final String BOUNDED_ELASTIC_SIZE_PROPERTY = "reactor.schedulers.defaultBoundedElasticSize";

    /**
     * Tamaño máximo del pool HikariCP ({@code spring.datasource.hikari.maximum-pool-size} = 20).
     * Es el tope de conexiones concurrentes y, por tanto, el tope útil de hilos de
     * {@code boundedElastic} que pueden pedir conexión a la vez (D5, Requirements 6.2, 6.5).
     */
    static final int MAXIMUM_POOL_SIZE = 20;

    private BoundedElasticSchedulerConfig() {
    }

    /**
     * Fija el tamaño del scheduler {@code boundedElastic} a {@link #MAXIMUM_POOL_SIZE} mediante la
     * propiedad de sistema {@link #BOUNDED_ELASTIC_SIZE_PROPERTY}, salvo que ya haya sido
     * establecida explícitamente (p. ej. por el operador vía {@code -D...}), en cuyo caso se
     * respeta el valor provisto.
     *
     * <p>Debe invocarse al inicio de {@code main(...)}, antes de que Reactor inicialice el
     * scheduler global.</p>
     */
    public static void align() {
        String existing = System.getProperty(BOUNDED_ELASTIC_SIZE_PROPERTY);
        if (existing != null && !existing.isBlank()) {
            log.info("boundedElastic ya dimensionado externamente ({}={}); se respeta el valor provisto.",
                    BOUNDED_ELASTIC_SIZE_PROPERTY, existing);
            return;
        }
        System.setProperty(BOUNDED_ELASTIC_SIZE_PROPERTY, Integer.toString(MAXIMUM_POOL_SIZE));
        log.info("boundedElastic alineado con el pool HikariCP: {}={} (maximum-pool-size).",
                BOUNDED_ELASTIC_SIZE_PROPERTY, MAXIMUM_POOL_SIZE);
    }
}
