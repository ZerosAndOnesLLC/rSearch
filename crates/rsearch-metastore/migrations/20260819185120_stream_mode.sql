-- Stream (index) mode: 'log' is the append-only default; 'document'
-- streams accept delete/update by _id and pay tombstone filtering at
-- query time (phase 14, issue #34).

ALTER TABLE streams
    ADD COLUMN mode TEXT NOT NULL DEFAULT 'log'
        CHECK (mode IN ('log', 'document'));
