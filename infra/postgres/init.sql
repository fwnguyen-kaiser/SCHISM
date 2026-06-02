-- SCHISM V2.0 — Postgres history store
-- Redis Streams = ephemeral transport. This schema = queryable long-term record.
--
-- Two concerns:
--   1. regime_history   — one row per 1h bar, written by the model service
--   2. gate_results     — one row per fold refit, written by stress_test
--
-- Both tables are append-only. No UPDATE path exists in the service layer.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ---------------------------------------------------------------------------
-- regime_history
-- One row per 1h bar after the model is live. fold_id identifies which refit
-- produced this decode so rows are traceable across refit events.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS regime_history (
    id              BIGSERIAL PRIMARY KEY,
    bar_timestamp   TIMESTAMPTZ NOT NULL,
    state           SMALLINT    NOT NULL CHECK (state >= 0),

    -- γ_t(k) posteriors, length = K (PROVISIONAL K=4).
    -- DB does not enforce length = K; application layer must validate before insert.
    -- Non-empty check prevents silent zero-length arrays if app passes wrong shape.
    posterior       FLOAT4[]    NOT NULL CHECK (array_length(posterior, 1) > 0),

    log_likelihood  FLOAT8      NOT NULL,

    -- Convention: "{ISO8601_UTC}_{fold_index}" e.g. "20240101T000000Z_7"
    -- Must match the filename convention in outputs/ (CLAUDE.md: {timestamp}_{fold_index}).
    fold_id         TEXT        NOT NULL CHECK (fold_id ~ '^[A-Za-z0-9_-]+$' AND length(fold_id) >= 3),

    -- Feature-set version. Increment when O_t/U_t schema changes so folds
    -- from different specs are not silently mixed in history queries.
    model_version   TEXT        NOT NULL DEFAULT 'v2.1',

    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_regime_history_bar_ts
    ON regime_history (bar_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_regime_history_fold_id
    ON regime_history (fold_id);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_regime_history_bar_fold
    ON regime_history (bar_timestamp, fold_id);


-- ---------------------------------------------------------------------------
-- gate_results
-- One row per refit fold. gates/advisories/details stored as JSONB so the
-- schema does not need to change as gates are added or renamed.
--
-- gates    JSONB: {"gate_name": true|false, ...} — strictly boolean values
-- advisories JSONB: {"metric_name": float, ...}  — advisory values, never gates
-- details  JSONB: per-gate raw diagnostics (distributions, thresholds, etc.)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gate_results (
    id           BIGSERIAL PRIMARY KEY,

    -- See regime_history.fold_id for convention.
    fold_id      TEXT        NOT NULL UNIQUE CHECK (fold_id ~ '^[A-Za-z0-9_-]+$' AND length(fold_id) >= 3),

    evaluated_at TIMESTAMPTZ NOT NULL,

    -- Derived automatically: true iff no entry in gates JSONB equals JSON false.
    -- This prevents drift between the column and gates content regardless of how
    -- many gates exist. Application must NOT insert this column; Postgres computes it.
    passed       BOOLEAN GENERATED ALWAYS AS (
                     NOT (gates @? '$.* ? (@ == false)')
                 ) STORED,

    gates        JSONB       NOT NULL,
    advisories   JSONB       NOT NULL,
    details      JSONB       NOT NULL,

    -- Feature-set version — see regime_history.model_version.
    model_version TEXT       NOT NULL DEFAULT 'v2.1',

    inserted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gate_results_evaluated_at
    ON gate_results (evaluated_at DESC);

CREATE INDEX IF NOT EXISTS idx_gate_results_passed
    ON gate_results (passed);
