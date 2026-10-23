package com.pragma.payments;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableAsync;
import com.pragma.payments.infrastructure.config.BoundedElasticSchedulerConfig;
import reactor.core.publisher.Hooks;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableAsync
public class Application {

    public static void main(String[] args) {
        // Fase 3 (D5, Requirements 6.2, 6.5): alinear boundedElastic con maximum-pool-size de
        // HikariCP ANTES de que Reactor inicialice su scheduler global. La propiedad de sistema se
        // lee de forma perezosa en el primer acceso a Schedulers.boundedElastic(), por lo que debe
        // fijarse aquí, antes de cualquier suscripción reactiva o del arranque del contexto Spring.
        BoundedElasticSchedulerConfig.align();
        Hooks.onOperatorDebug();
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public WebClient webClient() {
        return WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(16 * 1024 * 1024))
                .build();
    }

    @Bean
    public String resilience4jCircuitBreakerHealthIndicator() {
        return "resilience4jCircuitBreaker";
    }
}