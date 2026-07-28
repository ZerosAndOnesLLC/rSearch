-- Placement for the replicated storage backend: which nodes hold a copy
-- of each storage object. Keyed by storage key (not split id) so the
-- storage layer stays object-agnostic. No FK to nodes: rows for expired
-- nodes are purged explicitly by the control leader, and a returning node
-- must not silently resurrect placement that repair has already replaced.
CREATE TABLE object_locations (
    storage_key TEXT NOT NULL,
    node_id TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (storage_key, node_id)
);

-- Drain/decommission and dead-node purge scan by node.
CREATE INDEX idx_object_locations_node ON object_locations (node_id);
