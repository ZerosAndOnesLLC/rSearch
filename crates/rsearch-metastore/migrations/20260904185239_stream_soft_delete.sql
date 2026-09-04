-- Index deletion (issue #71). DELETE /{index} retires a stream without
-- touching storage inline: its splits are marked for delete (the GC job
-- removes the objects after the grace period, then the rows), and the
-- stream row is renamed out of the way and stamped so the name is free
-- for immediate re-creation. The control leader deletes the row once no
-- split references it (splits and tombstones cascade).
ALTER TABLE streams ADD COLUMN deleted_at TIMESTAMPTZ;

-- The purge job's scan.
CREATE INDEX idx_streams_deleted ON streams (deleted_at) WHERE deleted_at IS NOT NULL;
