-- query_string's default_operator became OR (Elasticsearch/OpenSearch
-- compatible). Alerts saved by the console before that relied on the old
-- implicit AND; pin it on them so their meaning does not change.
UPDATE alerts
SET query = jsonb_set(query, '{query_string,default_operator}', '"and"'::jsonb)
WHERE query ? 'query_string'
  AND jsonb_typeof(query->'query_string') = 'object'
  AND NOT (query->'query_string' ? 'default_operator');
