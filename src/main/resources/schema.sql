-- =============================================================================
-- Esquema de idempotencia persistente (Fase 2) - com.pragma.payments
--
-- Tabla idempotency_record: almacena los registros de idempotencia junto con su
-- resultado asociado, sobreviviendo a reinicios del Payment_Service (Req 1.1, 1.5).
--
-- La restriccion PRIMARY KEY / UNIQUE sobre idempotency_key es el fundamento de la
-- reserva atomica bajo condiciones de carrera: de K inserciones concurrentes con la
-- misma clave, el motor garantiza que solo una tiene exito (Req 4.1, 4.3, 5.3).
--
-- El indice sobre expires_at hace eficiente el barrido de limpieza por TTL del
-- Cleanup_Process (Req 6.3).
--
-- Idempotente: usa IF NOT EXISTS para poder ejecutarse en cada arranque sin error.
-- =============================================================================

CREATE TABLE IF NOT EXISTS idempotency_record (
    idempotency_key VARCHAR(80)  NOT NULL,
    status          VARCHAR(20)  NOT NULL,
    result_payload  TEXT,
    created_at      TIMESTAMP    NOT NULL,
    expires_at      TIMESTAMP    NOT NULL,
    CONSTRAINT pk_idempotency_record PRIMARY KEY (idempotency_key)
);

-- Indice para el barrido de expiracion del Cleanup_Process (deleteExpired).
CREATE INDEX IF NOT EXISTS idx_idempotency_record_expires_at
    ON idempotency_record (expires_at);
