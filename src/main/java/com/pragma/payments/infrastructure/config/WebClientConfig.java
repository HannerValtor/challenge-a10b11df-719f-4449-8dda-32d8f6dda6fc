package com.pragma.payments.infrastructure.config;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

@Configuration
public class WebClientConfig {

    private static final Logger log = LoggerFactory.getLogger(WebClientConfig.class);
    private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 5000;
    private static final int DEFAULT_RESPONSE_TIMEOUT_MS = 10000;
    private static final int DEFAULT_POOL_MAX_CONNECTIONS = 100;
    /** Tamaño máximo FINITO de la cola de adquisición de conexiones (Requirement 2.6). */
    static final int PENDING_ACQUIRE_MAX_COUNT = DEFAULT_POOL_MAX_CONNECTIONS * 2;

    @Value("${external.services.connection-timeout-ms:5000}")
    private int connectionTimeoutMs;

    @Value("${external.services.response-timeout-ms:10000}")
    private int responseTimeoutMs;

    @Value("${external.services.max-connections:100}")
    private int maxConnections;

    @Bean
    public WebClient webClient(ConnectionProvider connectionProvider) {
        log.info("Configurando WebClient con connectionTimeout: {}ms, responseTimeout: {}ms, maxConnections: {}",
                connectionTimeoutMs, responseTimeoutMs, maxConnections);

        HttpClient httpClient = HttpClient.create(connectionProvider)
                .responseTimeout(Duration.ofMillis(responseTimeoutMs))
                .option(io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS, connectionTimeoutMs);

        ExchangeStrategies strategies = ExchangeStrategies.builder()
                .codecs(configurer -> configurer
                        .defaultCodecs()
                        .maxInMemorySize(16 * 1024 * 1024))
                .build();

        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .exchangeStrategies(strategies)
                .defaultHeader("Content-Type", "application/json")
                .defaultHeader("Accept", "application/json")
                .build();
    }

    @Bean
    public ConnectionProvider connectionProvider() {
        return ConnectionProvider.builder("external-services-pool")
                .maxConnections(maxConnections > 0 ? maxConnections : DEFAULT_POOL_MAX_CONNECTIONS)
                // Cola de adquisición ACOTADA: evita encolar peticiones de forma ilimitada
                // cuando el pool de 100 conexiones está agotado. Al desbordar la cola o agotar
                // el timeout, se propaga un error de agotamiento sin bloquear indefinidamente
                // el hilo invocante (Requirements 2.5, 2.6).
                .pendingAcquireTimeout(Duration.ofMillis(5000))
                .pendingAcquireMaxCount(PENDING_ACQUIRE_MAX_COUNT)
                .maxIdleTime(Duration.ofSeconds(30))
                .maxLifeTime(Duration.ofMinutes(5))
                .evictInBackground(Duration.ofSeconds(30))
                .build();
    }

    @Bean
    public WebClient.Builder webClientBuilder(WebClient webClient) {
        return webClient.mutate();
    }
}