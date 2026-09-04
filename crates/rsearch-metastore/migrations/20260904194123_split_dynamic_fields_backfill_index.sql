-- The dynamic-fields backfill scans for published splits without an
-- inventory every control tick; a partial index keeps that O(backlog)
-- instead of O(table), and empty once the backlog is done.
CREATE INDEX idx_splits_dynamic_fields_missing
    ON splits (time_end_millis DESC)
    WHERE state = 'published' AND dynamic_fields IS NULL;
