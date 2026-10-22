package com.pragma.payments.application.ports.out;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pragma.payments.domain.model.IdempotencyKey;
import com.pragma.payments.domain.model.IdempotencyRecord;
import com.pragma.payments.domain.model.RecordStatus;
import com.pragma.payments.domain.model.Transaction;
import com.pragma.payments.infrastructure.adapters.out.persistence.IdempotencyRecordEntity;
import com.pragma.payments.infrastructure.adapters.out.persistence.IdempotencyRecordMapper;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Builders;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test de propiedad para el round-trip de guardado atómico (clave + resultado).
 *
 * <p>Ejercita el ciclo real de serialización/deserialización del Processing_Result a
 * través de {@link IdempotencyRecordMapper} (dominio ⇄ entidad de persistencia). Este es
 * el punto donde se materializa el guardado atómico de la clave junto a su resultado
 * (Req 1.2): un registro reservado y luego completado se convierte a su entidad persistida
 * ({@code toEntity}) y se reconstruye ({@code toDomain}); el registro resultante debe quedar
 * en {@code COMPLETED}, con la misma clave y un Processing_Result equivalente al original.</p>
 *
 * <p>Se elige el mapper (y no un doble en memoria) porque valida mejor el "guardado atómico
 * (clave + resultado)" de Req 1.2: incluye la serialización/deserialización JSON del resultado,
 * que es la traducción real hacia/desde la columna {@code result_payload}. El {@link ObjectMapper}
 * se configura como en el proyecto (Spring Boot autoconfigurado): con {@link JavaTimeModule}
 * para los {@link Instant} de la transacción y sin timestamps numéricos.</p>
 *
 * <p>El test se ubica en {@code application.ports.out} (no en {@code adapters/out}) porque el
 * {@code maven-compiler-plugin} excluye de la compilación de test los ficheros bajo
 * {@code **}/adapters/out/**}/*Test.java}; situarlo aquí garantiza que la propiedad se compile
 * y ejecute, siguiendo el precedente de {@code AtomicReservationPropertyTest}.</p>
 */
class AtomicSaveRoundTripPropertyTest {

    /**
     * {@link ObjectMapper} equivalente al autoconfigurado por Spring Boot para esta app:
     * módulo JSR-310 registrado y fechas serializadas en formato ISO-8601 (no timestamps),
     * necesario para round-trip fiel de los {@link Instant} de la {@link Transaction}.
     */
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final IdempotencyRecordMapper mapper = new IdempotencyRecordMapper(objectMapper);

    // Feature: implementacion-idempotencia, Property 2: Round-trip de guardado atómico (clave + resultado)
    // Validates: Requirements 1.2, 4.2
    //
    // Para cualquier transacción válida completada, reservar su Idempotency_Record y luego
    // completarlo produce, al releer por la clave, un record con status = COMPLETED y un
    // Processing_Result equivalente al original; nunca existe un record COMPLETED sin resultado
    // ni un resultado sin su clave asociada.
    @Property(tries = 100)
    void reserveThenCompleteRoundTripsToCompletedWithEquivalentResult(
            @ForAll("transactions") Transaction transaction,
            @ForAll("positiveTtls") Duration ttl) {

        IdempotencyKey key = transaction.getIdempotencyKey();
        Instant createdAt = transaction.getCreatedAt();

        // 1) Reservar (IN_PROGRESS) y completar (COMPLETED + resultado) en el dominio.
        IdempotencyRecord completed = IdempotencyRecord.reserve(key, createdAt, ttl)
                .withResult(transaction);

        // Precondición del guardado atómico: un COMPLETED siempre lleva su resultado asociado.
        assertThat(completed.getStatus()).isEqualTo(RecordStatus.COMPLETED);
        assertThat(completed.getResult()).isNotNull();

        // 2) Round-trip real: dominio -> entidad persistida (serializa resultado) -> dominio.
        IdempotencyRecordEntity entity = mapper.toEntity(completed);
        IdempotencyRecord reread = mapper.toDomain(entity);

        // 3) El record releído está COMPLETED, nunca sin resultado.
        assertThat(reread.getStatus())
                .as("al releer, el estado persiste como COMPLETED")
                .isEqualTo(RecordStatus.COMPLETED);
        assertThat(reread.getResult())
                .as("un record COMPLETED nunca existe sin su Processing_Result")
                .isNotNull();

        // 4) La clave se conserva en su forma canónica (resultado nunca sin clave asociada).
        assertThat(reread.getKey().toUniqueString())
                .as("la clave sobrevive al round-trip en su forma canónica")
                .isEqualTo(key.toUniqueString());
        assertThat(reread.getResult().getIdempotencyKey().toUniqueString())
                .as("el resultado conserva su clave de idempotencia asociada")
                .isEqualTo(key.toUniqueString());

        // 5) El Processing_Result releído es equivalente al original.
        //    (Transaction.equals solo compara id, por eso se verifica campo a campo.)
        Transaction original = transaction;
        Transaction result = reread.getResult();
        assertThat(result.getId()).isEqualTo(original.getId());
        assertThat(result.getAccountId()).isEqualTo(original.getAccountId());
        assertThat(result.getChannel()).isEqualTo(original.getChannel());
        assertThat(result.getCurrency()).isEqualTo(original.getCurrency());
        assertThat(result.getDescription()).isEqualTo(original.getDescription());
        assertThat(result.getStatus()).isEqualTo(original.getStatus());
        assertThat(result.getExternalReference()).isEqualTo(original.getExternalReference());
        assertThat(result.getCreatedAt()).isEqualTo(original.getCreatedAt());
        assertThat(result.getProcessedAt()).isEqualTo(original.getProcessedAt());
        if (original.getAmount() == null) {
            assertThat(result.getAmount()).isNull();
        } else {
            assertThat(result.getAmount())
                    .as("el monto se conserva (comparación por valor)")
                    .isEqualByComparingTo(original.getAmount());
        }

        // Las marcas de tiempo del record (createdAt/expiresAt) también sobreviven al round-trip.
        assertThat(reread.getCreatedAt()).isEqualTo(completed.getCreatedAt());
        assertThat(reread.getExpiresAt()).isEqualTo(completed.getExpiresAt());
    }

    // ---- Generadores -------------------------------------------------------

    // operationNumber conforme a ^[A-Za-z0-9\-]+$ (1..50)
    @Provide
    Arbitrary<String> operationNumbers() {
        return Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1).ofMaxLength(50);
    }

    // channel conforme a ^[A-Za-z0-9_]+$ (1..20)
    @Provide
    Arbitrary<String> channels() {
        return Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_")
                .ofMinLength(1).ofMaxLength(20);
    }

    @Provide
    Arbitrary<Duration> positiveTtls() {
        // TTL positivo (segundos), conforme al invariante expiresAt > createdAt.
        return Arbitraries.longs().between(1L, Duration.ofDays(30).toSeconds()).map(Duration::ofSeconds);
    }

    /**
     * Transacciones válidas y variadas, con su {@link IdempotencyKey} asociada. Las marcas de
     * tiempo se truncan a milisegundos para que el round-trip JSON (ISO-8601) sea fiel con
     * independencia de la precisión del reloj. Algunos campos opcionales se generan como
     * {@code null} para ejercitar también la ausencia de valor.
     */
    @Provide
    Arbitrary<Transaction> transactions() {
        Arbitrary<IdempotencyKey> keys = Combinators.combine(operationNumbers(), channels())
                .as(IdempotencyKey::new);
        Arbitrary<String> ids = Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1).ofMaxLength(40);
        Arbitrary<String> accountIds = Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1).ofMaxLength(30);
        Arbitrary<BigDecimal> amounts = Arbitraries.longs().between(1L, 1_000_000_00L)
                .map(cents -> BigDecimal.valueOf(cents, 2));
        Arbitrary<String> currencies = Arbitraries.of("USD", "EUR", "COP", "MXN", "GBP");
        Arbitrary<String> channelsForTx = Arbitraries.of("web", "mobile", "api");
        Arbitrary<String> descriptions = Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ")
                .ofMinLength(0).ofMaxLength(80);
        Arbitrary<String> statuses = Arbitraries.of(
                Transaction.TransactionStatus.COMPLETED.name(),
                Transaction.TransactionStatus.REJECTED.name(),
                Transaction.TransactionStatus.APPROVED.name());
        Arbitrary<String> externalRefs = Arbitraries.strings()
                .withChars("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-")
                .ofMinLength(1).ofMaxLength(36)
                .injectNull(0.2);
        // Instante base acotado y truncado a milisegundos.
        Arbitrary<Instant> createdAts = Arbitraries.longs()
                .between(0L, Duration.ofDays(3650).toSeconds())
                .map(epochSecond -> Instant.ofEpochSecond(epochSecond).truncatedTo(ChronoUnit.MILLIS));

        // jqwik's Builders API evita el límite de aridad de Combinators.combine.
        return Builders.withBuilder(Transaction::builder)
                .use(keys).in(Transaction.Builder::idempotencyKey)
                .use(ids).in(Transaction.Builder::id)
                .use(accountIds).in(Transaction.Builder::accountId)
                .use(amounts).in(Transaction.Builder::amount)
                .use(currencies).in(Transaction.Builder::currency)
                .use(channelsForTx).in(Transaction.Builder::channel)
                .use(descriptions).in(Transaction.Builder::description)
                .use(statuses).in((Transaction.Builder b, String s) -> b.status(s))
                .use(createdAts).in(Transaction.Builder::createdAt)
                .build(Transaction.Builder::build)
                // Campos sin setter en el builder: externalReference y processedAt.
                .flatMap(tx -> externalRefs.map(ref -> {
                    tx.setExternalReference(ref);
                    tx.setProcessedAt(tx.getCreatedAt().plusSeconds(1));
                    return tx;
                }));
    }
}
