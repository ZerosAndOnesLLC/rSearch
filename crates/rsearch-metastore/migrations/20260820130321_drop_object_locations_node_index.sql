-- no-transaction
-- The (node_id, storage_key) composite from the previous migration
-- covers every node_id-leading scan (drain, dead-node purge, reconcile
-- verify); retire the single-column index it supersedes. CONCURRENTLY
-- keeps the drop from waiting behind (and then blocking) placement
-- traffic, and requires running outside a transaction.
DROP INDEX CONCURRENTLY IF EXISTS idx_object_locations_node;
