#!/usr/bin/env bash
# Document-mode end-to-end test (phase 14, issue #34): one all-roles node
# on the fs storage backend + Postgres (DATABASE_URL from .env).
# Proves: mode at creation, index/replace/delete by _id, ES _doc routes,
# update/create semantics, ?refresh=wait_for, log streams unaffected,
# compaction making deletes physical, tombstone purge, and least-privilege
# keys (stream-scoped ingest key can create its index and write documents).
set -euo pipefail
cd "$(dirname "$0")/../.."

BIN=./target/release/rsearch
LOGDIR=/tmp/rsearch-docmode
PORT=9260
U="http://127.0.0.1:$PORT"
say() { echo "==> $*"; }
pause() { perl -e "select(undef,undef,undef,$1)"; }
fail() { echo "FAIL: $*"; exit 1; }

PID=""
cleanup() { [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null || true; }
trap cleanup EXIT

set -a; source .env; set +a
# Point the node at a dedicated test database when .env's DATABASE_URL
# is a shared dev instance (the table wipe below is destructive).
DATABASE_URL=${RSEARCH_TEST_DATABASE_URL:-$DATABASE_URL}
rm -rf "$LOGDIR"; mkdir -p "$LOGDIR"
psql "$DATABASE_URL" -qc "DELETE FROM streams; DELETE FROM nodes; DELETE FROM users; DELETE FROM api_keys; DELETE FROM sessions;" >/dev/null

say "starting node (document batches 1s, compaction after 5s, purge grace 5s)"
env DATABASE_URL="$DATABASE_URL" \
  RSEARCH_NODE__ID=docmode-1 \
  RSEARCH_NODE__DATA_DIR="$LOGDIR/node" \
  RSEARCH_HTTP__BIND_ADDR="127.0.0.1:$PORT" \
  RSEARCH_STORAGE__BACKEND=fs \
  RSEARCH_STORAGE__ROOT="$LOGDIR/store" \
  RSEARCH_INGEST__MAX_BATCH_SECS=30 \
  RSEARCH_INGEST__DOCUMENT_MAX_BATCH_SECS=1 \
  RSEARCH_INGEST__BALANCE_BULK=false \
  RSEARCH_CONTROL__INTERVAL_SECS=2 \
  RSEARCH_CONTROL__MERGE_MIN_MB=0 \
  RSEARCH_CONTROL__GC_GRACE_SECS=2 \
  RSEARCH_CONTROL__COMPACT_MAX_AGE_SECS=5 \
  RSEARCH_CONTROL__TOMBSTONE_PURGE_GRACE_SECS=5 \
  "$BIN" --roles ingest,search,control >"$LOGDIR/node.log" 2>&1 &
PID=$!
for i in $(seq 1 60); do curl -sf "$U/health" >/dev/null && break; pause 0.25; done
curl -sf "$U/health" >/dev/null || fail "node did not come up"

J='Content-Type: application/json'
ND='Content-Type: application/x-ndjson'
total() { curl -s -XPOST "$U/$1/_search" -H "$J" -d '{"size":0}' | jq -r '.hits.total.value'; }
ids() { curl -s -XPOST "$U/$1/_search" -H "$J" -d '{"size":100,"_source":false}' | jq -r '[.hits.hits[]._id] | sort | join(",")'; }
sql() { psql "$DATABASE_URL" -Atc "$1"; }

say "index modes"
curl -s -XPUT "$U/recs" -H "$J" -d '{"settings":{"index":{"mode":"document"}},"mappings":{"properties":{"title":{"type":"keyword"}}}}' | jq -e '.acknowledged' >/dev/null || fail "create document index"
[ "$(curl -s "$U/recs/_settings" | jq -r '.recs.settings.index.mode')" = document ] || fail "settings mode"
curl -s -XPUT "$U/logs" -H "$J" -d '{}' >/dev/null
[ "$(curl -s "$U/_cat/indices" | jq -r '.[] | select(.index=="logs") | .mode')" = log ] || fail "log default mode"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPUT "$U/recs" -H "$J" -d '{"settings":{"index":{"mode":"log"}}}')
[ "$code" = 200 ] || fail "empty stream may still switch mode (got $code)"
curl -s -XPUT "$U/recs" -H "$J" -d '{"settings":{"index":{"mode":"document"}}}' >/dev/null

say "bulk index with explicit ids, replace, delete"
curl -s -XPOST "$U/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{"_index":"recs","_id":"a"}}\n{"title":"a1","v":1}\n{"index":{"_index":"recs","_id":"b"}}\n{"title":"b1","v":1}\n{"index":{"_index":"recs","_id":"c"}}\n{"title":"c1","v":1}\n' | jq -e '.errors == false' >/dev/null || fail "bulk index"
[ "$(total recs)" = 3 ] || fail "3 docs visible after refresh"
curl -s -XPOST "$U/recs/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{"_id":"a"}}\n{"title":"a2","v":2}\n{"delete":{"_id":"b"}}\n' | jq -e '.errors == false' >/dev/null || fail "bulk replace/delete"
[ "$(total recs)" = 2 ] || fail "replace+delete -> 2 visible (got $(total recs))"
[ "$(ids recs)" = "a,c" ] || fail "ids after delete: $(ids recs)"
[ "$(curl -s "$U/recs/_doc/a" | jq -r '._source.v')" = 2 ] || fail "GET returns newest version"
[ "$(curl -s -XPOST "$U/recs/_search" -H "$J" -d '{"aggs":{"t":{"terms":{"field":"title"}}}}' | jq -r '[.aggregations.t.buckets[].key] | sort | join(",")')" = "a2,c1" ] || fail "aggregations skip hidden versions"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPUT "$U/recs" -H "$J" -d '{"settings":{"index":{"mode":"log"}}}')
[ "$code" = 409 ] || fail "mode is fixed once data exists (got $code)"

say "document routes"
curl -s -XPUT "$U/recs/_doc/d?refresh=wait_for" -H "$J" -d '{"title":"d1"}' | jq -e '._id == "d"' >/dev/null || fail "PUT _doc"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPUT "$U/recs/_create/d" -H "$J" -d '{"title":"dup"}')
[ "$code" = 409 ] || fail "_create on existing id -> 409 (got $code)"
curl -s -XPOST "$U/recs/_update/d?refresh=wait_for" -H "$J" -d '{"doc":{"n":1,"nested":{"x":1}}}' | jq -e '.result == "updated"' >/dev/null || fail "_update"
[ "$(curl -s "$U/recs/_source/d" | jq -c '.')" = '{"n":1,"nested":{"x":1},"title":"d1"}' ] || fail "_update merged: $(curl -s "$U/recs/_source/d")"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPOST "$U/recs/_update/ghost" -H "$J" -d '{"doc":{"n":1}}')
[ "$code" = 404 ] || fail "_update missing -> 404 (got $code)"
curl -s -XPOST "$U/recs/_update/ghost?refresh=wait_for" -H "$J" -d '{"doc":{"n":1},"doc_as_upsert":true}' | jq -e '.result == "created"' >/dev/null || fail "doc_as_upsert"
curl -s -XDELETE "$U/recs/_doc/ghost" | jq -e '.result == "deleted"' >/dev/null || fail "DELETE _doc"
code=$(curl -s -o /dev/null -w '%{http_code}' "$U/recs/_doc/ghost")
[ "$code" = 404 ] || fail "GET after delete -> 404 (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' -I "$U/recs/_doc/a")
[ "$code" = 200 ] || fail "HEAD existing -> 200 (got $code)"
gen=$(curl -s -XPOST "$U/recs/_doc?refresh=wait_for" -H "$J" -d '{"title":"gen"}' | jq -r '._id')
[ "${#gen}" = 32 ] || fail "POST _doc generates an id"
curl -s -XDELETE "$U/recs/_doc/$gen" >/dev/null

say "refresh semantics"
curl -s -XPUT "$U/recs/_doc/buffered" -H "$J" -d '{"title":"buf"}' >/dev/null
code=$(curl -s -o /dev/null -w '%{http_code}' "$U/recs/_doc/buffered")
[ "$code" = 404 ] || fail "write without refresh is not visible yet (got $code)"
curl -s -XPOST "$U/_bulk?refresh=true" -H "$ND" --data-binary $'{"index":{"_index":"recs","_id":"e"}}\n{"title":"e1"}\n' >/dev/null
code=$(curl -s -o /dev/null -w '%{http_code}' "$U/recs/_doc/buffered")
[ "$code" = 200 ] || fail "refresh=true flushed the earlier write too (got $code)"

say "log streams are unaffected"
curl -s -XPOST "$U/logs/_bulk" -H "$ND" --data-binary $'{"index":{"_id":"x"}}\n{"message":"one"}\n{"index":{"_id":"x"}}\n{"message":"two"}\n{"delete":{"_id":"x"}}\n' | jq -e '.items[2].delete.status == 400 and (.items[2].delete.error.reason | test("log-mode"))' >/dev/null || fail "log stream rejects delete with guidance"
code=$(curl -s -o /dev/null -w '%{http_code}' -XDELETE "$U/logs/_doc/x")
[ "$code" = 400 ] || fail "_doc delete on log stream -> 400 (got $code)"

say "delete/update on a missing index are 404 (no implicit creation)"
curl -s -XPOST "$U/_bulk" -H "$ND" --data-binary $'{"delete":{"_index":"nope","_id":"1"}}\n' | jq -e '.items[0].delete.status == 404' >/dev/null || fail "bulk delete on missing index -> 404"
code=$(curl -s -o /dev/null -w '%{http_code}' -XDELETE "$U/nope/_doc/1")
[ "$code" = 404 ] || fail "DELETE _doc on missing index -> 404 (got $code)"
[ "$(curl -s "$U/_cat/indices" | jq -r '[.[] | select(.index=="nope")] | length')" = 0 ] || fail "missing index was not created"
curl -s -XPOST "$U/recs/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{"_id":42}}\n{"title":"numeric"}\n' | jq -e '.items[0].index._id == "42"' >/dev/null || fail "numeric _id coerced"
curl -s -XDELETE "$U/recs/_doc/42" >/dev/null

say "delete_by_query"
curl -s -XPOST "$U/recs/_delete_by_query" -H "$J" -d '{"query":{"term":{"title":"e1"}}}' | jq -e '.deleted == 1' >/dev/null || fail "delete_by_query"
[ "$(ids recs)" = "a,buffered,c,d" ] || fail "ids after delete_by_query: $(ids recs)"

say "compaction makes deletes physical; tombstones purge"
# Log ingest of the 'x' docs above happens on the 30s window — irrelevant here.
before=$(sql "SELECT COALESCE(SUM(doc_count),0) FROM splits s JOIN streams st ON st.id=s.stream_id WHERE st.name='recs' AND s.state='published'")
[ "$before" -gt 4 ] || fail "hidden versions still physically present before compaction ($before)"
for i in $(seq 1 60); do
  physical=$(sql "SELECT COALESCE(SUM(doc_count),0) FROM splits s JOIN streams st ON st.id=s.stream_id WHERE st.name='recs' AND s.state='published'")
  left=$(sql "SELECT count(*) FROM doc_tombstones t JOIN streams st ON st.id=t.stream_id WHERE st.name='recs'")
  [ "$physical" = 4 ] && [ "$left" = 0 ] && break
  pause 1
done
[ "$physical" = 4 ] || fail "compaction did not remove hidden versions (physical=$physical)"
[ "$left" = 0 ] || fail "tombstones not purged ($left left)"
[ "$(ids recs)" = "a,buffered,c,d" ] || fail "ids after compaction: $(ids recs)"
[ "$(curl -s "$U/recs/_doc/a" | jq -r '._source.v')" = 2 ] || fail "newest version survives compaction"
grep -q "compacting split" "$LOGDIR/node.log" || fail "no compaction logged"

say "query_string fields / simple_query_string"
curl -s -XPOST "$U/recs/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{"_id":"q1"}}\n{"title":"lovelace","note":"ada wrote notes"}\n{"index":{"_id":"q2"}}\n{"title":"babbage","note":"lovelace annotated"}\n' >/dev/null
[ "$(curl -s -XPOST "$U/recs/_search" -H "$J" -d '{"query":{"query_string":{"query":"lovelace","fields":["title"]}}}' | jq -r '.hits.total.value')" = 1 ] || fail "query_string fields narrows"
[ "$(curl -s -XPOST "$U/recs/_search" -H "$J" -d '{"query":{"simple_query_string":{"query":"lovelace","fields":["title","note"]}}}' | jq -r '.hits.total.value')" = 2 ] || fail "simple_query_string across fields"
[ "$(curl -s -o /dev/null -w '%{http_code}' -XPOST "$U/recs/_search" -H "$J" -d '{"query":{"simple_query_string":{"query":"lovelace \"oops AND","fields":["note"],"default_operator":"or"}}}')" = 200 ] || fail "simple_query_string never 400s on typos"
curl -s -XPOST "$U/recs/_delete_by_query" -H "$J" -d '{"query":{"ids":{"values":["q1","q2"]}}}' >/dev/null

say "least-privilege key: stream-scoped ingest creates its index and writes"
curl -s -XPUT "$U/_rsearch/users/admin" -H "$J" -d '{"password":"adminpassword123","role":"admin"}' | jq -e '.acknowledged' >/dev/null || fail "create admin"
TOKEN=$(curl -s -XPOST "$U/_rsearch/login" -H "$J" -d '{"username":"admin","password":"adminpassword123"}' | jq -r '.token')
KEY=$(curl -s -XPOST "$U/_rsearch/api_keys" -H "Authorization: Bearer $TOKEN" -H "$J" -d '{"name":"app","actions":["ingest","search"],"streams":["app-*"]}' | jq -r '.key')
[ -n "$KEY" ] && [ "$KEY" != null ] || fail "create api key"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPUT "$U/app-items" -H "Authorization: Bearer $KEY" -H "$J" -d '{"settings":{"index":{"mode":"document"}}}')
[ "$code" = 200 ] || fail "scoped ingest key creates its index (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPUT "$U/app-items/_doc/1?refresh=wait_for" -H "Authorization: Bearer $KEY" -H "$J" -d '{"name":"one"}')
[ "$code" = 200 ] || fail "scoped key writes a document (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' "$U/app-items/_doc/1" -H "Authorization: Bearer $KEY")
[ "$code" = 200 ] || fail "scoped key reads a document (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPUT "$U/app-orders/_doc/1?refresh=wait_for" -H "Authorization: Bearer $KEY" -H "$J" -d '{"n":1}')
[ "$code" = 201 ] || fail "glob scope covers a new tenant index (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPUT "$U/other/_doc/1" -H "Authorization: Bearer $KEY" -H "$J" -d '{}')
[ "$code" = 403 ] || fail "scoped key is denied outside its streams (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' -XPUT "$U/app/_doc/1" -H "Authorization: Bearer $KEY" -H "$J" -d '{}')
[ "$code" = 403 ] || fail "glob needs the full prefix (got $code)"
code=$(curl -s -o /dev/null -w '%{http_code}' "$U/_rsearch/users" -H "Authorization: Bearer $KEY")
[ "$code" = 403 ] || fail "scoped key is not admin (got $code)"

say "PASS: document mode end to end"
