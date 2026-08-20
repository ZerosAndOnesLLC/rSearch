-- no-transaction
-- The metastore->disk reconcile verify (#44) walks one node's placement
-- rows keyset-paginated by storage_key. Built CONCURRENTLY (migrations
-- run at node startup, and a plain CREATE INDEX would block every
-- placement write — each replicated ingest ack — for the build), which
-- also requires this migration to be a single statement outside a
-- transaction; the old node_id index it supersedes is dropped in the
-- next migration.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_object_locations_node_key
    ON object_locations (node_id, storage_key);
