-- Users, sessions, and API keys. Password hashing: PBKDF2-HMAC-SHA256
-- (FIPS-approved), format 'pbkdf2-sha256$<iters>$<salt_b64>$<hash_b64>'.

CREATE TABLE users (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    -- admin | user  (admin: everything; user: ingest/search per scopes)
    role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('admin', 'user')),
    -- stream scopes for non-admin users; '*' = all
    streams TEXT[] NOT NULL DEFAULT '{*}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE sessions (
    token_hash TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_sessions_expiry ON sessions (expires_at);

CREATE TABLE api_keys (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    key_hash TEXT NOT NULL UNIQUE,
    -- subset of {ingest, search, admin}
    actions TEXT[] NOT NULL,
    -- stream scopes; '*' = all
    streams TEXT[] NOT NULL DEFAULT '{*}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ
);
