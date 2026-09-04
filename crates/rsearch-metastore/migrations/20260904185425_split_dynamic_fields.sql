-- Per-split inventory of the unmapped (`_dynamic`) field paths and the
-- JSON value types seen for each, e.g. {"role": ["string"], "n": ["long"]}
-- (issue #76). GET /{index}/_mapping unions it across the stream's
-- published splits to report dynamic fields the way OpenSearch does.
-- NULL on splits registered before the column existed; the control
-- leader backfills those by scanning each split's term dictionary once.
ALTER TABLE splits ADD COLUMN dynamic_fields JSONB;
