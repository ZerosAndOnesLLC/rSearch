-- Stream routing rules: match a field condition on incoming documents
-- and route (move) or copy them to a target stream.

CREATE TABLE routing_rules (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    -- Document field the condition inspects (top-level key).
    field TEXT NOT NULL,
    -- eq | contains | exists
    op TEXT NOT NULL CHECK (op IN ('eq', 'contains', 'exists')),
    value TEXT NOT NULL DEFAULT '',
    target_stream TEXT NOT NULL,
    -- true: copy to target and keep original; false: move to target.
    copy BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
