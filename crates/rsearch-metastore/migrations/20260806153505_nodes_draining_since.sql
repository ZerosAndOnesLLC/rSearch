-- Track when a drain began so a stale draining flag is visible: the API
-- and logs can report how long a node has been draining, and the control
-- leader can warn when a drain outlives the expected window (issue #4).
ALTER TABLE nodes ADD COLUMN draining_since TIMESTAMPTZ;
-- Nodes already draining get "now" — the true start time is unknown, but
-- the age still starts accumulating for the stale-drain warning.
UPDATE nodes SET draining_since = now() WHERE draining;
