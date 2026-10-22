package com.pragma.payments.application.ports.out;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test de contrato de las firmas reactivas del puerto de salida {@link IdempotencyStorePort}.
 *
 * <p>Verifica por reflexión que todas las operaciones públicas declaradas en la interfaz
 * exponen un tipo de retorno reactivo ({@link Mono} o {@link Flux}), de modo que el acceso
 * al almacenamiento de idempotencia no bloquee el event-loop reactivo de WebFlux.</p>
 *
 * <p>Valida: Requirements 8.3 (el Idempotency_Store expone sus operaciones de consulta,
 * reserva, actualización y eliminación como un puerto de salida reactivo).</p>
 */
@DisplayName("IdempotencyStorePort - Contrato de firmas reactivas")
class IdempotencyStorePortContractTest {

    private static boolean isReactiveType(Class<?> type) {
        return Mono.class.isAssignableFrom(type) || Flux.class.isAssignableFrom(type);
    }

    private static List<Method> declaredPublicMethods() {
        return Arrays.stream(IdempotencyStorePort.class.getDeclaredMethods())
                .filter(m -> Modifier.isPublic(m.getModifiers()))
                .filter(m -> !m.isSynthetic())
                .toList();
    }

    @Test
    @DisplayName("El puerto es una interfaz y declara al menos una operación")
    void portIsInterfaceWithOperations() {
        assertThat(IdempotencyStorePort.class.isInterface())
                .as("IdempotencyStorePort debe ser una interfaz (puerto de salida)")
                .isTrue();

        assertThat(declaredPublicMethods())
                .as("el puerto debe declarar al menos una operación")
                .isNotEmpty();
    }

    @Test
    @DisplayName("Toda operación pública declarada devuelve un tipo reactivo (Mono/Flux)")
    void everyDeclaredOperationReturnsReactiveType() {
        List<Method> methods = declaredPublicMethods();

        assertThat(methods)
                .as("tipo de retorno de cada operación del puerto")
                .allSatisfy(method ->
                        assertThat(isReactiveType(method.getReturnType()))
                                .as("el método '%s' debe devolver Mono o Flux pero devuelve '%s'",
                                        method.getName(), method.getReturnType().getName())
                                .isTrue());
    }
}
