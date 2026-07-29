-- Metastore schema: streams, splits, nodes.
-- Retention lives on the stream (retention_hours) rather than a separate
-- policy table — one policy per stream is the v1 model.

CREATE TABLE streams (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    -- ES-shaped mapping JSON ({"properties": {...}})
    mapping JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- NULL = retain forever
    retention_hours INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE splits (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    split_id TEXT NOT NULL UNIQUE,
    stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    -- staged -> published -> marked_for_delete
    state TEXT NOT NULL DEFAULT 'staged'
        CHECK (state IN ('staged', 'published', 'marked_for_delete')),
    storage_key TEXT NOT NULL,
    doc_count BIGINT NOT NULL,
    size_bytes BIGINT NOT NULL,
    -- inclusive document-timestamp range, epoch milliseconds
    time_start_millis BIGINT NOT NULL,
    time_end_millis BIGINT NOT NULL,
    -- length of the footer metadata JSON (enables single-range-read opens)
    footer_len BIGINT NOT NULL DEFAULT 0,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The split-listing query: published splits of a stream overlapping a
-- time range.
CREATE INDEX idx_splits_stream_state_time
    ON splits (stream_id, state, time_start_millis, time_end_millis);
-- Background jobs scan by state (GC, retention).
CREATE INDEX idx_splits_state ON splits (state);

CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    roles TEXT[] NOT NULL,
    address TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_nodes_heartbeat ON nodes (last_heartbeat);
