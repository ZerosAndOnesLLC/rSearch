-- Graceful drain/decommission: a draining node keeps serving reads
-- but is excluded from write-target selection; the control leader copies
-- its objects off, after which it can be shut down and expired.
ALTER TABLE nodes ADD COLUMN draining BOOLEAN NOT NULL DEFAULT false;
