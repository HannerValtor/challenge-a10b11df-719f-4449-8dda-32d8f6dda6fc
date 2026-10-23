# Optimización de Eventos Concurrentes en Sistema de Pagos

En un sistema de pagos distribuido, múltiples solicitudes de transacción llegan simultáneamente desde diferentes canales (web, móvil, API). El sistema debe procesar estas solicitudes de manera eficiente, evitando bloqueos y maximizando el rendimiento. Los actores involucrados son el 'originador de créditos', el'motor antifraude', el 'buró de riesgos' y el 'core bancario'. El sistema debe garantizar que las transacciones se procesen de manera idempotente (clave: número de operación + canal) y que los fallos temporales (timeout del buró >2s, respuesta 5xx del core) no afecten la disponibilidad del servicio.

## Informacion General

| Campo | Valor |
|-------|-------|
| **Tema** | gestión de concurrencia y paralelismo en sistemas distribuidos |
| **Nivel** | senior-l2 |
| **Tipo** | practical |
| **Tiempo estimado** | 8 horas |

## Fases del Reto

### Fase 0: Configuración del Proyecto

**Objetivo:** Obtener el proyecto base funcional enviando el Código Base a un asistente de IA, que lo analizará, corregirá errores y generará un ZIP listo para usar.

**Tiempo estimado:** 15-30 minutos

**Instrucciones:**

- Asegúrate de tener instalado para ejecutar el proyecto: JDK 17+, Maven 3.9+, IDE con soporte Java.
- Copia todo el contenido del campo **Código Base** de este reto — incluyendo el texto de instrucciones que aparece al inicio.
- Abre un asistente de IA (Claude en claude.ai, ChatGPT o Gemini — se recomienda Claude), pega el contenido copiado en el chat y envíalo.
- El asistente analizará los archivos, corregirá errores y generará un archivo ZIP descargable. Descárgalo y extráelo en la carpeta donde quieras trabajar.
- Ejecuta `mvn compile` en la raíz. Si no hay errores, estás listo.

**Entregable:** El proyecto compila/arranca sin errores.

<details>
<summary>Pistas de conocimiento</summary>

- Copia el Código Base completo incluyendo el texto de instrucciones al inicio — esas instrucciones le indican al asistente exactamente qué hacer con los archivos.
- Si el asistente no genera el ZIP automáticamente al terminar el análisis, escríbele: "genera el ZIP ahora".
- Si el proyecto tiene errores al arrancar, comparte el mensaje de error con el mismo asistente para que lo corrija.

</details>

### Fase 1: Diseño de Concurrencia Inicial

**Objetivo:** Establecer un diseño básico que permita el procesamiento concurrente de solicitudes de pago.

**Tiempo estimado:** 2 horas

**Instrucciones:**

- Identificar los puntos de entrada de solicitudes y los servicios involucrados en el proceso de pago.
- Definir cómo se manejarán las solicitudes concurrentes para evitar bloqueos.
- Establecer criterios de aceptación para la fase: procesamiento eficiente de solicitudes sin bloqueos.

**Entregable:** Diagrama de flujo que muestra el procesamiento concurrente de solicitudes.

<details>
<summary>Pistas de conocimiento</summary>

- Considera la diferencia entre procesos y hilos de ejecución.
- Piensa en cómo puedes aprovechar la concurrencia sin introducir bloqueos.

</details>

### Fase 2: Implementación de Idempotencia

**Objetivo:** Implementar la idempotencia en el procesamiento de solicitudes para garantizar la consistencia.

**Tiempo estimado:** 3 horas

**Instrucciones:**

- Define cómo se garantizará la idempotencia en el procesamiento de solicitudes (clave: número de operación + canal).
- Establece criterios de aceptación para la fase: procesamiento idempotente de solicitudes.
- Considera cómo manejarás los fallos temporales (timeout del buró >2s, respuesta 5xx del core).

**Entregable:** Descripción detallada del mecanismo de idempotencia implementado.

<details>
<summary>Pistas de conocimiento</summary>

- Piensa en cómo puedes usar la clave de idempotencia para evitar duplicados.
- Considera cómo manejarás los fallos temporales para mantener la disponibilidad del servicio.

</details>

### Fase 3: Optimización y Refactorización

**Objetivo:** Optimizar el diseño para mejorar el rendimiento y refactorizar el código para mayor mantenibilidad.

**Tiempo estimado:** 3 horas

**Instrucciones:**

- Identifica áreas de optimización en el diseño actual.
- Refactoriza el código para mejorar la mantenibilidad y escalabilidad.
- Establece criterios de aceptación para la fase: diseño optimizado y código refactorizado.

**Entregable:** Documentación del diseño optimizado y código refactorizado.

<details>
<summary>Pistas de conocimiento</summary>

- Considera cómo puedes mejorar el rendimiento sin sacrificar la consistencia.
- Piensa en cómo puedes refactorizar el código para que sea más mantenible y escalable.

</details>

## Dimensiones Evaluadas

- **queEs**: ¿Qué es la concurrencia y cómo difiere del paralelismo en el contexto de este reto?
- **paraQueSirve**: ¿Para qué sirve implementar la idempotencia en el procesamiento de solicitudes?
- **comoSeUsa**: ¿Cómo se usa la clave de idempotencia para garantizar la consistencia en el procesamiento de solicitudes?
- **erroresComunes**: ¿Cuáles son los errores comunes al manejar fallos temporales en un sistema distribuido?
- **queDecisionesImplica**: ¿Qué decisiones implica la optimización y refactorización del diseño?

## Criterios de Evaluacion

- Diseño inicial que permite el procesamiento concurrente de solicitudes.
- Implementación de idempotencia en el procesamiento de solicitudes.
- Manejo de fallos temporales para mantener la disponibilidad del servicio.
- Optimización y refactorización del diseño para mejorar el rendimiento y mantenibilidad.

## Como trabajar con un asistente de IA

Hay dos caminos, elegi uno:

- **AGENTS.md** (recomendado) — instrucciones nativas del repo. Abri esta carpeta con tu agente local (Claude Code, Cursor, Codex, Copilot, Gemini) y las carga solo. Sabe que archivos faltan y con que comando se verifica, y completa el scaffold escribiendo en disco.
- **PROMPT_MEJORA.md** — para copiar y pegar en un chat (claude.ai, ChatGPT). Devuelve un ZIP con el proyecto. Sirve si no tenes un agente en el IDE.

Ninguno de los dos resuelve las fases del reto: eso es tu trabajo.

## Verificacion

El proyecto esta listo para trabajar cuando este comando corre sin errores:

```bash
mvn clean compile
```

---

# Arquitectura de Fase 3 — Optimización y Refactorización

Esta sección documenta la solución de la **Fase 3**: las decisiones de optimización y
refactorización, el flujo de procesamiento de un pago de extremo a extremo y el comportamiento del
sistema ante fallos transitorios de los servicios externos.

> Specs completos: `.kiro/specs/optimizacion-refactor-fase3/` (requirements, design, tasks).
> Detalle de tuning de pools y análisis de cuello de botella: `docs/CONCURRENCY.md` (secciones 9–10).

## Stack

Java 21 · Spring Boot 3.5 · Spring WebFlux + Project Reactor (reactivo no bloqueante sobre Netty) ·
Spring Data JPA + HikariCP (PostgreSQL) · Resilience4j (Circuit Breaker, Retry, Bulkhead) ·
Caffeine (caché) · arquitectura hexagonal (puertos y adaptadores). El acceso JPA bloqueante se aísla
en `Schedulers.boundedElastic()`; el event-loop de Netty nunca se bloquea (verificado con BlockHound).

## Decisiones de optimización y refactorización

| ID | Decisión | Qué resuelve |
|---|---|---|
| **D1** | Separar `IdempotencyOrchestrator` (ciclo de vida de idempotencia) de `PaymentProcessingChain` (cadena externa fraude→riesgo→core). `PaymentUseCase` queda como coordinador delgado. | SRP: cada clase una razón de cambio; testeo aislado (H4). |
| **D2** | Centralizar timeouts por servicio en `ExternalTimeoutProperties` (externalizados en `application.yml`): fraude 1000 ms, **riesgo 2500 ms (> 2 s)**, core 1000 ms. Eliminar el timeout envolvente del use case. | Coherencia de timeouts; cumple el requisito del reto (buró > 2 s); un único control de tiempo por servicio (H1, H9). |
| **D3** | Caché de lectura `CachingIdempotencyStore` (Caffeine) **solo para registros `COMPLETED`** inmutables, como decorator del puerto. | Evita re-consultar la BD en duplicados y consultas repetidas (H6). |
| **D4** | Reemplazar la caché ad-hoc de fraude por Caffeine con `maximumSize` + `expireAfterWrite` y clave determinista (sin `currentTimeMillis`). | Acota memoria; clave estable; sin fugas (H5). |
| **D5** | Dimensionar HikariCP explícitamente y alinear `boundedElastic` al `maximum-pool-size`. | Evita el agotamiento silencioso del pool (H8). |
| **D6** | Mover Circuit Breaker/Retry a métodos públicos interceptables de los adapters; quitar las anotaciones inertes sobre métodos privados del use case. | Resiliencia efectiva; elimina código engañoso (H2). |
| **D7** | Reserva atómica solo por la restricción de unicidad de la BD; sin `synchronized`/`ReentrantLock`. | Atomicidad sin bloqueo incompatible con el event-loop. |

## Flujo de un pago de extremo a extremo

```
POST /api/v1/payments
  └─ PaymentController: valida el canal (web|mobile|api); canal inválido → 400
       └─ PaymentUseCase.processPayment  (coordinador delgado, delega)
            └─ IdempotencyOrchestrator.resolve(key, tx)
                 ├─ store.find(key)  [vía CachingIdempotencyStore]
                 │    ├─ COMPLETED   → devuelve el resultado cacheado (sin cadena externa) → 200
                 │    ├─ IN_PROGRESS → TransactionAlreadyProcessedException → 409
                 │    └─ FAILED/ausente → continúa a reservar
                 ├─ store.reserve(IN_PROGRESS)  [INSERT atómico; duplicado → relee y decide]
                 ├─ PaymentProcessingChain.execute(tx)   [exactamente una vez]
                 │    └─ fraude → riesgo → core   (short-circuit si REJECTED)
                 ├─ éxito  → store.complete(key, result) → puebla caché → 200
                 └─ fallo transitorio → store.markFailedOrRelease(key) → propaga → 503
```

- **Idempotencia exactamente-una-ejecución:** ante K solicitudes concurrentes con la misma clave
  (`operationNumber + "_" + channel`), exactamente una gana la reserva y ejecuta la cadena; las K−1
  restantes reciben el resultado cacheado (si `COMPLETED`) o 409 (si `IN_PROGRESS`), sin re-ejecutar.
- **Caché coherente:** solo se cachean registros `COMPLETED` (inmutables); `reserve`, `fail` y el
  barrido de expiración invalidan la caché. El TTL de caché ≤ TTL de idempotencia.

## Comportamiento ante fallos transitorios

| Escenario | Respuesta | Recuperación |
|---|---|---|
| Buró de riesgo no responde en > 2 s (timeout) | Circuit breaker cerrado: la cadena falla → el registro queda `FAILED`/liberado → **503**. Circuit breaker abierto: fallback con score 50 (modo degradado). | La clave queda elegible a reintento; el CB transita a half-open tras `waitDurationInOpenState`. |
| Core bancario 5xx / timeout | Fallo transitorio → `FAILED`/liberación → **503**. Fallback `DECLINED` si el CB está abierto. | Reintentable. |
| Pool de BD agotado / fallo JDBC | `connection-timeout` (3 s) → fallo rápido; si falla `find`, no se ejecuta la cadena (idempotencia preservada). | Reintento del cliente; D5 reduce la probabilidad de agotamiento. |
| Bulkhead lleno (25 llamadas en vuelo por servicio) | `BulkheadFullException` → **503** sin bloquear el hilo. | Rechazo inmediato; el cliente reintenta. |

El mapeo error → HTTP está centralizado en `GlobalExceptionHandler`: canal inválido → 400,
duplicado `IN_PROGRESS` → 409, timeout/bulkhead → 503, cualquier otro → 500 (sin exponer detalles
internos).

## Comandos de prueba (Maven)

La estrategia de pruebas tiene tres tipos. Todos corren sobre el build Maven existente
(`spring-boot-starter-parent`, Surefire con BlockHound habilitado). Si el proyecto tiene Maven
Wrapper, sustituye `mvn` por `./mvnw` (Linux/macOS) o `.\mvnw.cmd` (Windows).

```bash
# 1) Pruebas unitarias / de ejemplo (Mockito + Reactor StepVerifier)
#    Orquestador, cadena de procesamiento, decorator de caché, propiedades de timeout, etc.
mvn test

# 2) Pruebas de propiedad (jqwik, >= 100 iteraciones)
#    Exactamente-una-ejecución, determinismo de duplicados, coherencia de caché,
#    mapeo HTTP invariante, timeout del buró > 2s, no bloqueo del event-loop, liberación transitoria.
mvn test -Dtest=*PropertyTest

# 3) Pruebas de integración (Testcontainers PostgreSQL; requieren Docker)
#    Durabilidad, restricción de esquema, carga/concurrencia (>= 16), resiliencia ante fallos.
#    Sin Docker se SALTAN automáticamente (estado SKIPPED, el build sigue pasando).
mvn test -Dtest=*IT,*DurabilityTest,*ConstraintTest,*LoadTest,*ResilienceTest,*NonBlocking*Test

# Build completo con toda la suite
mvn clean test
```

---

*Reto generado automaticamente por Challenge Generator - Pragma*
