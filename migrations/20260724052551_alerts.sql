-- Scheduled query alerts, executed by the control leader.

CREATE TABLE alerts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    stream TEXT NOT NULL,
    -- ES query object; {} means match_all
    query JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- fire when the hit count over the window compares true:
    -- gt | lt against threshold
    condition_op TEXT NOT NULL DEFAULT 'gt' CHECK (condition_op IN ('gt', 'lt')),
    threshold BIGINT NOT NULL DEFAULT 0,
    window_secs BIGINT NOT NULL DEFAULT 300,
    interval_secs BIGINT NOT NULL DEFAULT 60,
    webhook_url TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    last_run_at TIMESTAMPTZ,
    last_status TEXT,
    last_count BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
