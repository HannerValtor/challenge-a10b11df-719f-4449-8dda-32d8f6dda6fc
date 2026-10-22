# Estrategia de Concurrencia — Sistema de Pagos (Fase 1)

Documento de la decisión de arquitectura de concurrencia del servicio de pagos
(`com.pragma.payments`). Es el entregable de la **Fase 1** del reto: diseño inicial
funcional que permite el procesamiento concurrente de solicitudes de pago.

> Spec completo: `.kiro/specs/diseno-concurrencia-pagos/` (requirements, design, tasks).

## 1. Contexto

Múltiples solicitudes de pago llegan simultáneamente desde tres canales (`web`, `mobile`,
`api`). Cada pago coordina cuatro actores: el **originador** (entrada HTTP), el **motor
antifraude**, el **buró de riesgos** y el **core bancario**. El sistema debe procesar de
forma concurrente, sin bloqueos, de forma idempotente y manteniendo la disponibilidad ante
fallos temporales de los servicios externos.

## 2. Decisión: modelo reactivo no bloqueante + executor acotado

**Base: programación reactiva (Spring WebFlux + Project Reactor sobre Netty).** El pago es
**I/O-bound dominante**: cada solicitud hace 3 llamadas HTTP externas. Con un modelo clásico
*thread-per-request*, el hilo queda bloqueado esperando la red, y atender N solicitudes exige
N hilos — no escala y agota memoria. El modelo reactivo usa pocos hilos de event-loop
(≈ núcleos de CPU) que **nunca se bloquean**: mientras una solicitud espera I/O, el mismo
hilo atiende otras.

| Criterio | Reactivo (WebFlux) | Thread-per-request |
|---|---|---|
| Hilos bajo carga | Pocos, constantes (≈ núcleos) | Uno por solicitud; crece sin límite |
| I/O-bound (3 servicios externos) | Óptimo: el hilo no espera bloqueado | Ineficiente: hilo bloqueado en la red |
| Memoria | Baja y acotada | Alta (~0.5–1 MB de pila por hilo) |
| Saturación | Degradación controlada (bulkhead/pool) | Agotamiento de hilos → colapso |

**Complemento: executor acotado.** El trabajo potencialmente bloqueante o intensivo en CPU se
desplaza fuera del event-loop:
- `Schedulers.boundedElastic()` para el acceso a la caché de idempotencia y pasos del use case.
- Un `ThreadPoolTaskExecutor` dedicado y acotado para cualquier `@Async` (ya que `@EnableAsync`
  está activo), en lugar del `SimpleAsyncTaskExecutor` ilimitado por defecto.

## 3. Respuestas a las preguntas guía del reto

### ¿Cuántos threads sin saturar recursos?
- **Event-loop:** ≈ núcleos de CPU (gestionado por Netty). **No** crece con el número de
  solicitudes; el no-bloqueo permite atender miles de solicitudes con pocos hilos.
- **ThreadPoolTaskExecutor:** `corePoolSize = N núcleos`, `maxPoolSize = 2N`, `queueCapacity = 100`.
  Más hilos que núcleos no acelera el cómputo CPU-bound y aumenta el context-switching.
- **Factores reales de saturación (I/O-bound):** se acotan con **bulkhead (25/servicio)** y
  **pool de conexiones (100)**, no con más hilos.

### ¿Cómo evitar que un thread bloquee a otros?
- Nunca ejecutar operaciones bloqueantes sobre el event-loop; desplazarlas a `boundedElastic`.
- Bulkhead con `maxWaitDuration = 0`: rechazo inmediato, sin espera bloqueante.
- Pool de conexiones con `pendingAcquireTimeout` finito y cola acotada (`pendingAcquireMaxCount`),
  en lugar de bloqueo indefinido.

### ¿Qué mecanismos de sincronización?
- **`ConcurrentHashMap` con operación atómica `putIfAbsent`** para la reserva de idempotencia
  (garantiza exactamente-una-ejecución ante carrera, sin locks).
- **Semáforo del `Bulkhead`** de Resilience4j para limitar la concurrencia por servicio.
- Se **evitan locks explícitos** (`synchronized`, `ReentrantLock`): introducen bloqueo y
  contención incompatibles con el event-loop.

## 4. Componentes clave

| Componente | Rol de concurrencia | Archivo |
|---|---|---|
| `Resilience4jConfig` | Define `BulkheadRegistry` (25/servicio, `maxWaitDuration=0`) y `ThreadPoolTaskExecutor` acotado | `infrastructure/config/Resilience4jConfig.java` |
| `WebClientConfig` | `ConnectionProvider` (100 conexiones) con cola de adquisición acotada | `infrastructure/config/WebClientConfig.java` |
| `PaymentUseCase` | Cadena reactiva + reserva atómica de idempotencia en `boundedElastic` | `application/usecases/PaymentUseCase.java` |
| `FraudDetectionAdapter` / `RiskBureauAdapter` / `CoreBankingAdapter` | `BulkheadOperator` reactivo + timeout 2s por servicio externo | `infrastructure/adapters/out/**` |

## 5. Idempotencia bajo concurrencia

La clave es `operationNumber + "_" + channel` (determinista; el canal distingue operaciones).
La reserva se hace con **una única operación atómica `putIfAbsent`** sobre un `ConcurrentHashMap`:
ante K solicitudes concurrentes con la misma clave, exactamente una gana la reserva
(`putIfAbsent == null`) y las K−1 restantes se rechazan como duplicadas. Esto elimina la ventana
de carrera TOCTOU del patrón `get`+`put` previo. Si el procesamiento falla por un fallo
transitorio (no duplicado), la reserva se libera para permitir reintentos.

Verificado con un test de 16 hilos simultáneos: exactamente 1 éxito, 15 duplicados.

## 6. Aislamiento de fallos temporales

- **Timeout 2000 ms** por servicio externo; al expirar se libera el permiso del bulkhead.
- **5xx del core** se tratan como fallo transitorio (circuit breaker + retry de Resilience4j).
- **Bulkhead lleno** → rechazo inmediato sin bloquear, mapeado a 503 en la capa web.
- Cada servicio tiene su propio bulkhead: la saturación de uno no afecta a los demás.

## 7. Límites configurados

| Parámetro | Valor | Justificación |
|---|---|---|
| Bulkhead `maxConcurrentCalls` / servicio | 25 | Acota concurrencia por actor externo |
| Bulkhead `maxWaitDuration` | 0 ms | Rechazo inmediato, sin bloqueo |
| `ConnectionProvider` `maxConnections` | 100 | Límite de conexiones del WebClient |
| `pendingAcquireMaxCount` | 200 | Cola de adquisición finita |
| `ThreadPoolTaskExecutor` core / max / queue | N / 2N / 100 | Dimensionado por núcleos de CPU |
| Timeout servicio externo | 2000 ms | Fallo temporal del buró > 2s |

## 8. Alcance

Este documento corresponde a la **Fase 1** (diseño inicial funcional). El tuning fino de pools,
el backpressure elaborado y el benchmarking corresponden a la **Fase 3** y quedan fuera de alcance.
