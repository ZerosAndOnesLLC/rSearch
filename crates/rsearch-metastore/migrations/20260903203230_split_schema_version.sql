-- Split layout version (rsearch_index::CURRENT_SCHEMA_VERSION at build
-- time), so the control node can find splits written under an older
-- layout and rewrite them (issue #66). Existing rows default to 0: every
-- split registered before this column existed is a rebuild candidate,
-- which is correct — none of them carry the version-2 analyzer or the
-- `.keyword` view. The exact pre-upgrade version is only in the split
-- footer and does not matter for scheduling.
ALTER TABLE splits ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 0;

-- The upgrade scan: published splits below the current version, newest
-- data first.
CREATE INDEX idx_splits_schema_version_published
    ON splits (schema_version, time_end_millis DESC)
    WHERE state = 'published';
