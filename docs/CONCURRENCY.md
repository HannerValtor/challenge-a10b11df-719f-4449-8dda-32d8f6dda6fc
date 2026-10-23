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

---

# Fase 3 — Optimización: tuning de pools y análisis de cuello de botella

Esta sección es el entregable de concurrencia de la **Fase 3**. Documenta el dimensionamiento
final del pool de conexiones (decisión D5), el valor anterior y nuevo de cada parámetro ajustado
con su justificación cuantitativa, y el análisis del cuello de botella del sistema con la métrica
que lo evidencia y la mitigación aplicada.

> Spec completo: `.kiro/specs/optimizacion-refactor-fase3/` (requirements, design, tasks).

## 9. Dimensionamiento del pool de conexiones (D5)

En la Fase 1 el pool de conexiones a BD no existía como cuello de botella porque la idempotencia
vivía en memoria (`ConcurrentHashMap`). En la Fase 2 la idempotencia pasó a ser **persistente**
(JPA/PostgreSQL sobre HikariCP), de modo que toda operación de idempotencia (`find`, `reserve`,
`complete`, `markFailedOrRelease`, `deleteExpired`) consume una conexión del pool a través de un
hilo de `boundedElastic`. La Fase 3 dimensiona explícitamente ese pool (hallazgo H8).

### 9.1 Valores finales (D5)

Configurados en `src/main/resources/application.yml` bajo `spring.datasource.hikari`:

| Parámetro | Valor final | Rol |
|---|---|---|
| `maximum-pool-size` | 20 | Conexiones activas máximas; acota el fan-out a la BD |
| `minimum-idle` | 10 | Conexiones ociosas listas para absorber ráfagas |
| `connection-timeout` | 3000 ms | Fallo rápido si no hay conexión libre (no bloqueo indefinido) |
| `max-lifetime` | 1 800 000 ms (30 min) | Recicla conexiones antes del corte del lado servidor |
| `idle-timeout` | 600 000 ms (10 min) | Retira conexiones ociosas por encima de `minimum-idle` |
| `connection-test-query` | `SELECT 1` | Validación ligera de conexión |

Invariante respetado: `minimum-idle (10) <= maximum-pool-size (20)`, todos los valores positivos
(Requirement 6.1).

### 9.2 Antes vs. después, con justificación cuantitativa

| Parámetro | Fase 1/2 (antes) | Fase 3 (nuevo) | Justificación cuantitativa |
|---|---|---|---|
| `maximum-pool-size` | 20 (implícito/por defecto del arranque previo) | **20 (explícito)** | Carga I/O-bound con JDBC: el límite útil del pool ronda `(2 × núcleos) + husos de disco efectivos`. Para 4–8 núcleos efectivos, 20 cubre el fan-out concurrente de idempotencia sin sobre-provisionar conexiones que el motor tendría que mantener abiertas. Se fija explícito para que **el pool acote a `boundedElastic`, no al revés**. |
| `minimum-idle` | 10 | **10** | Mantener la mitad del pool caliente evita el coste de *handshake* TCP/TLS + autenticación (del orden de decenas de ms por conexión fría) en la primera ráfaga tras un periodo ocioso. |
| `connection-timeout` | no fijado (default 30 000 ms) | **3000 ms** | Con default de 30 s, una solicitud podía quedar 30 s esperando una conexión bajo saturación, muy por encima del timeout de los servicios externos (≤ 2,5 s). Bajarlo a 3 s hace que el agotamiento del pool se manifieste como **fallo rápido** (→ 503 reintetable) en vez de latencia acumulada. |
| `max-lifetime` | no fijado | **1 800 000 ms** | Reciclar conexiones por debajo del tiempo de corte típico del lado servidor (p. ej. `idle_in_transaction_session_timeout` / balanceadores) evita usar conexiones medio-muertas que fallarían en mitad de una transacción. |
| `idle-timeout` | no fijado | **600 000 ms** | Libera conexiones ociosas por encima de `minimum-idle` tras 10 min, devolviendo recursos al motor sin penalizar el estado caliente mínimo. |
| Tamaño de `boundedElastic` | default de Reactor (`10 × núcleos`, hasta cientos de hilos) | **Acotado a ≤ `maximum-pool-size`** | Con el default, decenas/cientos de hilos de `boundedElastic` podrían pedir conexión a un pool de 20 simultáneamente: el exceso se encola o expira en `connection-timeout`. Alinear `boundedElastic` al pool (D5, Req 6.2/6.5) evita el agotamiento silencioso: el número de hilos que piden conexión no supera las conexiones disponibles. Se fija en el arranque vía `BoundedElasticSchedulerConfig.align()` antes de la primera suscripción. |

## 10. Análisis de cuello de botella (evidencia y mitigación)

### 10.1 Cuello de botella primario — cadena de servicios externos

**Componente limitante:** la cadena secuencial fraude → riesgo → core (3 llamadas HTTP por
transacción nueva).

**Métrica observada:** latencia de pared por transacción dominada por la suma de las 3 esperas de
red más reintentos. Con el timeout del buró mal dimensionado en la Fase 1 (2 s = límite del reto,
no `> 2 s`) y `maxAttempts=3` con backoff exponencial, el peor caso del buró podía acumular varios
segundos por transacción, y el timeout envolvente de 3 s del use case (H1) **nunca disparaba** para
riesgo/core (doble control de tiempo incoherente).

**Mitigación aplicada (D2, H1, H9):**
- Timeouts coherentes y externalizados por servicio (`ExternalTimeoutProperties`): fraude 1000 ms,
  **riesgo 2500 ms (> 2 s, requisito del reto)**, core 1000 ms. Se eliminó el timeout envolvente de
  3 s del use case: cada adaptador es el único dueño de su control de tiempo (Req 1.10).
- El fallo rápido coherente libera el permiso del bulkhead y la conexión antes, reduciendo el tiempo
  que los recursos quedan retenidos esperando un servicio lento.

### 10.2 Cuello de botella secundario (de recursos) — pool de conexiones + saltos de scheduler

**Componente limitante:** HikariCP (20) combinado con `boundedElastic`, más saltos de scheduler
redundantes (H7) y lecturas repetidas a BD (H6).

**Métrica observada:** bajo alta concurrencia, cada pago retiene una conexión en cada paso de
persistencia; al superar las 20 conexiones, las operaciones de idempotencia se encolan y la latencia
se dispara (y, con el `connection-timeout` por defecto de 30 s, el síntoma era latencia, no error).
Además, `boundedElastic` se aplicaba **dos veces** (use case + adaptador, H7), añadiendo overhead de
planificación sin beneficio.

**Mitigación aplicada (D3, D5, H6, H7):**
- Un **único** `subscribeOn(boundedElastic)` por operación de persistencia, propiedad exclusiva del
  `IdempotencyStoreAdapter` (Req 6.4, 7.10): se eliminó el segundo salto.
- `boundedElastic` alineado a `maximum-pool-size` (Req 6.2/6.5): los hilos que piden conexión no
  exceden el pool.
- **Caché de lectura `CachingIdempotencyStore`** (D3) para registros `COMPLETED` inmutables: las
  solicitudes duplicadas y las consultas repetidas (`/status`, reintentos) se sirven desde memoria,
  reduciendo el fan-out de conexiones a la BD (Req 4.x).
- `connection-timeout` de 3 s convierte el agotamiento en fallo rápido observable (503) en lugar de
  latencia acumulada.

### 10.3 Operación que más recursos consume

No hay trabajo CPU-bound relevante: el recurso crítico son **conexiones/hilos bloqueados esperando
I/O** (red externa y JDBC). Por eso la optimización se centró en acotar y alinear pools, evitar
accesos redundantes (caché de idempotencia y caché determinista de fraude) y hacer coherentes los
timeouts para que el fallo rápido libere recursos cuanto antes.
