-- Recent-window queries ("last 15m") filter on time_end_millis >= start;
-- the existing (stream_id, state, time_start_millis, ...) index can't
-- prune them because time_start <= now matches all history. This index
-- prunes sharply on the recent-window predicate.
CREATE INDEX idx_splits_stream_state_end
    ON splits (stream_id, state, time_end_millis);
