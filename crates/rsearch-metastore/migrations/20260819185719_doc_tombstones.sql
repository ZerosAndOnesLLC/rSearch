-- Document-mode tombstones (phase 14, issue #34).
--
-- A tombstone hides every document in `stream_id` whose `_id` = `doc_id`
-- and whose `_seq` < `before_seq`. `delete` writes one with before_seq =
-- now; `index` on a document-mode stream writes one with before_seq = the
-- new version's `_seq`, so reads see exactly the newest version. One row
-- per (stream, doc) — a later write raises before_seq and re-issues the
-- row's `seq` so incremental readers (WHERE seq > last_seen) pick it up.
-- Rows are purged once no split can still hold a document they hide.

CREATE TABLE doc_tombstones (
    seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    doc_id TEXT NOT NULL,
    before_seq BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, doc_id)
);

-- Incremental fetch per stream.
CREATE INDEX idx_doc_tombstones_stream_seq ON doc_tombstones (stream_id, seq);

-- Per-split `_seq` range (NULL for legacy splits without ids: tombstones
-- never apply) and the highest tombstone seq already applied when the
-- split was (re)built — ingest-built splits start at 0.
ALTER TABLE splits
    ADD COLUMN seq_min BIGINT,
    ADD COLUMN seq_max BIGINT,
    ADD COLUMN tombstone_seq_applied BIGINT NOT NULL DEFAULT 0;
