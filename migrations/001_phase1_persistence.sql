CREATE TABLE IF NOT EXISTS sim_snapshots (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    action TEXT,
    state_json JSONB NOT NULL,
    summary_json JSONB,
    payload_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sim_stages (
    id BIGSERIAL PRIMARY KEY,
    stage_key TEXT UNIQUE NOT NULL,
    stage_timestamp TEXT,
    action TEXT NOT NULL,
    before_json JSONB,
    after_json JSONB,
    meta_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sim_history (
    id BIGSERIAL PRIMARY KEY,
    history_key TEXT UNIQUE NOT NULL,
    event_timestamp TEXT,
    action TEXT NOT NULL,
    meta_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sim_results (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    result_json JSONB NOT NULL,
    payload_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
