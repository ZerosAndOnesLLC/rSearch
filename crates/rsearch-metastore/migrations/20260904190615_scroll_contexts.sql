-- Scroll contexts (issue #72): the server-side state behind
-- `_search?scroll=` / `_search/scroll`. Kept in the metastore rather than
-- in a node's memory so a scroll opened on one search node continues on
-- any other. A context is the original search (minus aggregations) plus
-- the cursor of the last page served; each page is a search_after query.
CREATE TABLE scroll_contexts (
    id TEXT PRIMARY KEY,
    stream TEXT NOT NULL,
    -- the _search body the scroll was opened with (query, size, sort, …)
    request JSONB NOT NULL,
    -- the last served hit's `sort` values; NULL until a page has hits
    cursor JSONB,
    -- hits.total of the first page, reported unchanged on every page
    total JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

-- The control leader's expiry sweep.
CREATE INDEX idx_scroll_contexts_expires ON scroll_contexts (expires_at);
