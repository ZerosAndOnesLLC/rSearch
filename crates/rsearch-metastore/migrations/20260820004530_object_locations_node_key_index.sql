-- The metastore->disk reconcile verify (#44) walks one node's placement
-- rows keyset-paginated by storage_key; give it an index range scan and
-- retire the single-column index the composite now covers.
CREATE INDEX idx_object_locations_node_key
    ON object_locations (node_id, storage_key);
DROP INDEX idx_object_locations_node;
