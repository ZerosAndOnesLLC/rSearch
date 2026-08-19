-- Purge scans tombstones by age; the per-stream floor it joins against
-- groups id-carrying splits by stream and state.
CREATE INDEX idx_doc_tombstones_created_at ON doc_tombstones (created_at);
CREATE INDEX idx_splits_stream_applied
    ON splits (stream_id, tombstone_seq_applied)
    WHERE state IN ('staged', 'published') AND seq_min IS NOT NULL;
