from __future__ import annotations

from .db import get_conn


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS silos (
    silo_id TEXT PRIMARY KEY,
    capacity_kg DOUBLE PRECISION NOT NULL,
    body_diameter_m DOUBLE PRECISION NOT NULL,
    outlet_diameter_m DOUBLE PRECISION NOT NULL,
    initial_mass_kg DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS layers (
    id BIGSERIAL PRIMARY KEY,
    silo_id TEXT NOT NULL REFERENCES silos(silo_id) ON DELETE CASCADE,
    sim_event_id BIGINT,
    snapshot_id BIGINT NOT NULL DEFAULT 1,
    event_type TEXT NOT NULL DEFAULT 'snapshot',
    captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    layer_index INTEGER NOT NULL,
    lot_id TEXT NOT NULL,
    supplier TEXT NOT NULL,
    loaded_mass DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    moisture_pct DOUBLE PRECISION,
    fine_extract_db_pct DOUBLE PRECISION,
    wort_pH DOUBLE PRECISION,
    diastatic_power_WK DOUBLE PRECISION,
    total_protein_pct DOUBLE PRECISION,
    wort_colour_EBC DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS incoming_queue (
    id BIGSERIAL PRIMARY KEY,
    lot_id TEXT NOT NULL,
    supplier TEXT NOT NULL,
    mass_kg DOUBLE PRECISION NOT NULL,
    remaining_mass_kg DOUBLE PRECISION,
    is_fully_consumed BOOLEAN NOT NULL DEFAULT FALSE,
    moisture_pct DOUBLE PRECISION,
    fine_extract_db_pct DOUBLE PRECISION,
    wort_pH DOUBLE PRECISION,
    diastatic_power_WK DOUBLE PRECISION,
    total_protein_pct DOUBLE PRECISION,
    wort_colour_EBC DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lot_ledger (
    lot_id TEXT PRIMARY KEY,
    supplier TEXT NOT NULL,
    total_mass_kg DOUBLE PRECISION NOT NULL DEFAULT 0,
    discharged_kg DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS history (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action TEXT NOT NULL,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS stages (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action TEXT NOT NULL,
    before JSONB NOT NULL DEFAULT '{}'::jsonb,
    after JSONB NOT NULL DEFAULT '{}'::jsonb,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS results_run (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    action TEXT NOT NULL,
    inputs_hash TEXT,
    total_discharged_mass_kg DOUBLE PRECISION NOT NULL,
    total_remaining_mass_kg DOUBLE PRECISION NOT NULL,
    total_blended_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    per_silo JSONB NOT NULL DEFAULT '{}'::jsonb,
    silo_state_ledger JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS results_optimize (
    id BIGSERIAL PRIMARY KEY,
    sim_event_id BIGINT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    objective_score DOUBLE PRECISION NOT NULL,
    recommended_discharge JSONB NOT NULL DEFAULT '[]'::jsonb,
    target_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    top_candidates JSONB NOT NULL DEFAULT '[]'::jsonb,
    best_run JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS discharge_results (
    id BIGSERIAL PRIMARY KEY,
    sim_event_id BIGINT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    discharge_by_silo JSONB NOT NULL DEFAULT '{}'::jsonb,
    predicted_run JSONB NOT NULL DEFAULT '{}'::jsonb,
    summary_before JSONB NOT NULL DEFAULT '{}'::jsonb,
    summary_after JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS sim_events (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type TEXT NOT NULL,
    action TEXT,
    state_before JSONB NOT NULL DEFAULT '{}'::jsonb,
    state_after JSONB NOT NULL DEFAULT '{}'::jsonb,
    discharge_by_silo JSONB NOT NULL DEFAULT '{}'::jsonb,
    total_discharged_mass_kg DOUBLE PRECISION,
    total_remaining_mass_kg DOUBLE PRECISION,
    incoming_queue_count INTEGER,
    incoming_queue_mass_kg DOUBLE PRECISION,
    objective_score DOUBLE PRECISION,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS brew_schedules (
    schedule_id TEXT PRIMARY KEY,
    name TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS brew_schedule_items (
    id BIGSERIAL PRIMARY KEY,
    schedule_id TEXT NOT NULL REFERENCES brew_schedules(schedule_id) ON DELETE CASCADE,
    brew_id TEXT NOT NULL,
    brew_index INTEGER NOT NULL,
    target_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    target_discharge_kg DOUBLE PRECISION NOT NULL DEFAULT 12000.0,
    status TEXT NOT NULL DEFAULT 'pending',
    optimize_result JSONB NOT NULL DEFAULT '{}'::jsonb,
    selected_candidate_index INTEGER,
    applied_event_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(schedule_id, brew_id),
    UNIQUE(schedule_id, brew_index)
);
"""


def ensure_schema() -> None:
    with get_conn() as conn:
        with conn.transaction():
            conn.execute(SCHEMA_SQL)
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS moisture_pct DOUBLE PRECISION")
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS fine_extract_db_pct DOUBLE PRECISION")
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS wort_pH DOUBLE PRECISION")
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS diastatic_power_WK DOUBLE PRECISION")
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS total_protein_pct DOUBLE PRECISION")
            conn.execute("ALTER TABLE suppliers ADD COLUMN IF NOT EXISTS wort_colour_EBC DOUBLE PRECISION")
            conn.execute("ALTER TABLE incoming_queue ADD COLUMN IF NOT EXISTS remaining_mass_kg DOUBLE PRECISION")
            conn.execute("ALTER TABLE incoming_queue ADD COLUMN IF NOT EXISTS is_fully_consumed BOOLEAN NOT NULL DEFAULT FALSE")
            conn.execute("ALTER TABLE incoming_queue ADD COLUMN IF NOT EXISTS moisture_pct DOUBLE PRECISION")
            conn.execute("ALTER TABLE incoming_queue ADD COLUMN IF NOT EXISTS fine_extract_db_pct DOUBLE PRECISION")
            conn.execute("ALTER TABLE incoming_queue ADD COLUMN IF NOT EXISTS wort_pH DOUBLE PRECISION")
            conn.execute("ALTER TABLE incoming_queue ADD COLUMN IF NOT EXISTS diastatic_power_WK DOUBLE PRECISION")
            conn.execute("ALTER TABLE incoming_queue ADD COLUMN IF NOT EXISTS total_protein_pct DOUBLE PRECISION")
            conn.execute("ALTER TABLE incoming_queue ADD COLUMN IF NOT EXISTS wort_colour_EBC DOUBLE PRECISION")
            conn.execute("UPDATE incoming_queue SET remaining_mass_kg = mass_kg WHERE remaining_mass_kg IS NULL")
            conn.execute("ALTER TABLE layers ADD COLUMN IF NOT EXISTS snapshot_id BIGINT NOT NULL DEFAULT 1")
            conn.execute("ALTER TABLE layers ADD COLUMN IF NOT EXISTS sim_event_id BIGINT")
            conn.execute("ALTER TABLE layers ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'snapshot'")
            conn.execute("ALTER TABLE layers ADD COLUMN IF NOT EXISTS captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
            conn.execute("ALTER TABLE layers ADD COLUMN IF NOT EXISTS loaded_mass DOUBLE PRECISION")
            conn.execute("ALTER TABLE discharge_results ADD COLUMN IF NOT EXISTS sim_event_id BIGINT")
            conn.execute("ALTER TABLE results_optimize ADD COLUMN IF NOT EXISTS sim_event_id BIGINT")
            conn.execute(
                """
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name = 'layers'
                          AND column_name = 'remaining_mass_kg'
                    ) THEN
                        EXECUTE 'UPDATE layers SET loaded_mass = COALESCE(loaded_mass, remaining_mass_kg)';
                    END IF;
                END
                $$;
                """
            )
            conn.execute("ALTER TABLE layers DROP COLUMN IF EXISTS remaining_mass_kg")
            conn.execute("DROP TABLE IF EXISTS layers_history")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_layers_snapshot_id ON layers(snapshot_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_layers_captured_at ON layers(captured_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_layers_sim_event_id ON layers(sim_event_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_discharge_results_sim_event_id ON discharge_results(sim_event_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_results_optimize_sim_event_id ON results_optimize(sim_event_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_brew_schedule_items_schedule_id ON brew_schedule_items(schedule_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_brew_schedule_items_status ON brew_schedule_items(status)")
