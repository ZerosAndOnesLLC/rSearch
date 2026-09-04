#!/usr/bin/env bash
# OpenSearch-compatibility end-to-end test for issues #71–#77 and #80:
# index deletion, scroll, PUT _mapping, _count, dynamic fields in
# _mapping, field sorting and _refresh. One all-roles node on the fs backend + Postgres. Every
# expected shape below was taken from a real OpenSearch 3.6.
set -euo pipefail
cd "$(dirname "$0")/../.."

BIN=./target/release/rsearch
LOGDIR=/tmp/rsearch-compat
PORT=9270
U="http://127.0.0.1:$PORT"
say() { echo "==> $*"; }
pause() { perl -e "select(undef,undef,undef,$1)"; }
fail() { echo "FAIL: $*"; exit 1; }

PID=""
cleanup() { [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null || true; }
trap cleanup EXIT

set -a; source .env; set +a
DATABASE_URL=${RSEARCH_TEST_DATABASE_URL:-$DATABASE_URL}
rm -rf "$LOGDIR"; mkdir -p "$LOGDIR"
# scroll_contexts arrives with this release's migrations (the node applies
# them at startup), so it may not exist yet on a fresh test database.
psql "$DATABASE_URL" -qc "DELETE FROM streams; DELETE FROM nodes; DELETE FROM users; DELETE FROM api_keys; DELETE FROM sessions;
  DO \$\$ BEGIN IF to_regclass('scroll_contexts') IS NOT NULL THEN DELETE FROM scroll_contexts; END IF; END \$\$;" >/dev/null

say "starting node"
env DATABASE_URL="$DATABASE_URL" \
  RSEARCH_NODE__ID=compat-1 \
  RSEARCH_NODE__DATA_DIR="$LOGDIR/node" \
  RSEARCH_HTTP__BIND_ADDR="127.0.0.1:$PORT" \
  RSEARCH_STORAGE__BACKEND=fs \
  RSEARCH_STORAGE__ROOT="$LOGDIR/store" \
  RSEARCH_INGEST__MAX_BATCH_SECS=3 \
  RSEARCH_INGEST__DOCUMENT_MAX_BATCH_SECS=1 \
  RSEARCH_INGEST__BALANCE_BULK=false \
  RSEARCH_CONTROL__INTERVAL_SECS=2 \
  RSEARCH_CONTROL__MERGE_TARGET_MB=0 \
  RSEARCH_CONTROL__MERGE_MIN_MB=0 \
  RSEARCH_CONTROL__GC_GRACE_SECS=2 \
  "$BIN" --roles ingest,search,control >"$LOGDIR/node.log" 2>&1 &
PID=$!
for i in $(seq 1 60); do curl -sf "$U/health" >/dev/null && break; pause 0.25; done
curl -sf "$U/health" >/dev/null || fail "node did not come up"

J='Content-Type: application/json'
ND='Content-Type: application/x-ndjson'
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
sql() { psql "$DATABASE_URL" -Atc "$1"; }
# ids of a search in order
order() { curl -s -XPOST "$U/people/_search" -H "$J" -d "$1" | jq -r '[.hits.hits[]._id] | join(",")'; }
sortvals() { curl -s -XPOST "$U/people/_search" -H "$J" -d "$1" | jq -c "[.hits.hits[].sort[0]]"; }
reason() { curl -s -XPOST "$U/people/_search" -H "$J" -d "$1" | jq -r '.error.root_cause[0].reason'; }

say "#73 PUT _mapping"
curl -s -XPUT "$U/people" -H "$J" -d '{"settings":{"index":{"mode":"document"}},"mappings":{"properties":{"name":{"type":"keyword"},"age":{"type":"long"},"born":{"type":"date"},"bio":{"type":"text"},"ip":{"type":"ip"}}}}' | jq -e '.acknowledged' >/dev/null || fail "create index"
[ "$(curl -s -XPUT "$U/people/_mapping" -H "$J" -d '{"properties":{"city":{"type":"keyword"}}}')" = '{"acknowledged":true}' ] || fail "put_mapping add field"
R=$(curl -s -XPUT "$U/people/_mapping" -H "$J" -d '{"properties":{"age":{"type":"keyword"}}}')
[ "$(echo "$R" | jq -r .status)" = 400 ] || fail "type change must be 400: $R"
[ "$(echo "$R" | jq -r '.error.root_cause[0].reason')" = "mapper [age] cannot be changed from type [long] to [keyword]" ] || fail "type change reason: $R"
[ "$(code -XPUT "$U/people/_mapping" -H "$J" -d '{"properties":{"age":{"type":"integer"}}}')" = 200 ] || fail "same (aliased) type is a no-op"
[ "$(code -XPUT "$U/nope/_mapping" -H "$J" -d '{"properties":{"age":{"type":"long"}}}')" = 404 ] || fail "put_mapping on missing index is 404"
[ "$(curl -s "$U/people/_mapping" | jq -r '.people.mappings.properties.city.type')" = keyword ] || fail "mapping shows the added field"

say "documents"
curl -s -XPOST "$U/people/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{"_id":"1"}}\n{"name":"bob","age":30,"born":"1990-01-01T00:00:00Z","bio":"a b","role":"tech_admin","score":1.5,"ok":true,"ip":"10.0.0.2","@timestamp":"2026-01-01T00:00:01Z"}\n{"index":{"_id":"2"}}\n{"name":"alice","age":25,"born":"1995-06-01T00:00:00Z","bio":"c d","role":"user","score":2,"ok":false,"ip":"10.0.0.1","@timestamp":"2026-01-01T00:00:02Z"}\n{"index":{"_id":"3"}}\n{"name":"carol","bio":"e f","nested":{"k":"v"},"@timestamp":"2026-01-01T00:00:03Z"}\n' | jq -e '.errors == false' >/dev/null || fail "bulk"

say "#74 _count"
[ "$(curl -s "$U/people/_count" | jq -cS .)" = '{"_shards":{"failed":0,"skipped":0,"successful":1,"total":1},"count":3}' ] || fail "GET _count: $(curl -s "$U/people/_count")"
[ "$(curl -s -XPOST "$U/people/_count" -H "$J" -d '{"query":{"term":{"name":"bob"}}}' | jq -r .count)" = 1 ] || fail "POST _count with query"
[ "$(curl -s "$U/people/_count?q=name:bob" | jq -r .count)" = 1 ] || fail "_count ?q"
[ "$(code "$U/nope/_count")" = 404 ] || fail "_count missing index"
R=$(curl -s -XPOST "$U/people/_count" -H "$J" -d '{"size":1}')
[ "$(echo "$R" | jq -r '.error.root_cause[0].reason')" = "request does not support [size]" ] || fail "_count rejects size: $R"

say "#76 dynamic fields in _mapping"
M=$(curl -s "$U/people/_mapping")
[ "$(echo "$M" | jq -cS '.people.mappings.properties.role')" = '{"fields":{"keyword":{"ignore_above":256,"type":"keyword"}},"type":"text"}' ] || fail "role mapping: $M"
[ "$(echo "$M" | jq -r '.people.mappings.properties.score.type')" = float ] || fail "score float"
[ "$(echo "$M" | jq -r '.people.mappings.properties.ok.type')" = boolean ] || fail "ok boolean"
[ "$(echo "$M" | jq -r '.people.mappings.properties.nested.properties.k.type')" = text ] || fail "nested object"
[ "$(echo "$M" | jq -r '.people.mappings.properties.name.type')" = keyword ] || fail "mapped field kept"
[ "$(echo "$M" | jq -r '.people.mappings.properties["@timestamp"].type')" = date ] || fail "@timestamp"
[ "$(curl -s "$U/people" | jq -r '.people.mappings.properties.role.type')" = text ] || fail "GET /{index} carries dynamic fields too"

say "#77 sort"
[ "$(order '{"sort":[{"name":"asc"}]}')" = "2,1,3" ] || fail "keyword asc"
[ "$(order '{"sort":[{"name":"desc"}]}')" = "3,1,2" ] || fail "keyword desc (missing last)"
[ "$(order '{"sort":"name"}')" = "2,1,3" ] || fail "string-form sort"
[ "$(order '{"sort":[{"age":"desc"}]}')" = "1,2,3" ] || fail "long desc"
[ "$(sortvals '{"sort":[{"age":"desc"}]}')" = "[30,25,-9223372036854775808]" ] || fail "long desc values: $(sortvals '{"sort":[{"age":"desc"}]}')"
[ "$(order '{"sort":[{"age":"asc"}]}')" = "2,1,3" ] || fail "long asc"
[ "$(sortvals '{"sort":[{"age":"asc"}]}')" = "[25,30,9223372036854775807]" ] || fail "long asc values"
[ "$(order '{"sort":[{"born":"asc"}]}')" = "1,2,3" ] || fail "date asc"
[ "$(sortvals '{"sort":[{"born":"asc"}]}' | jq -c '.[0]')" = 631152000000 ] || fail "date sort value is epoch millis"
[ "$(order '{"sort":[{"score":"desc"}]}')" = "2,1,3" ] || fail "float desc"
[ "$(sortvals '{"sort":[{"score":"desc"}]}')" = '[2.0,1.5,"-Infinity"]' ] || fail "float values: $(sortvals '{"sort":[{"score":"desc"}]}')"
[ "$(order '{"sort":[{"ok":"asc"}]}')" = "2,1,3" ] || fail "boolean asc"
[ "$(sortvals '{"sort":[{"ok":"asc"}]}')" = "[0,1,2147483647]" ] || fail "boolean values"
[ "$(order '{"sort":[{"ip":"desc"}]}')" = "1,2,3" ] || fail "ip desc"
[ "$(sortvals '{"sort":[{"ip":"desc"}]}' | jq -r '.[0]')" = "10.0.0.2" ] || fail "ip sort value"
[ "$(order '{"sort":[{"role.keyword":"desc"}]}')" = "2,1,3" ] || fail "dynamic .keyword desc"
[ "$(sortvals '{"sort":[{"role.keyword":"desc"}]}')" = '["user","tech_admin",null]' ] || fail "keyword values"
[ "$(order '{"sort":[{"score":"asc"},{"name":"desc"}]}')" = "1,2,3" ] || fail "dynamic numeric asc"
[ "$(reason '{"sort":[{"role":"desc"}]}' | cut -c1-40)" = "Text fields are not optimised for operat" ] || fail "dynamic text sort is 400"
[ "$(reason '{"sort":[{"bio":"desc"}]}' | cut -c1-40)" = "Text fields are not optimised for operat" ] || fail "mapped text sort is 400"
[ "$(reason '{"sort":[{"zzz":"desc"}]}')" = "No mapping found for [zzz] in order to sort on" ] || fail "unknown field is 400"
[ "$(sortvals '{"sort":[{"zzz":{"order":"desc","unmapped_type":"long"}}]}')" = "[-9223372036854775808,-9223372036854775808,-9223372036854775808]" ] || fail "unmapped_type sorts all-missing"
[ "$(order '{"sort":[{"age":{"order":"asc","missing":"_first"}}]}')" = "3,2,1" ] || fail "missing _first"
[ "$(order '{"sort":[{"age":{"order":"asc","missing":27}}]}')" = "2,3,1" ] || fail "missing value"
[ "$(sortvals '{"sort":[{"age":{"order":"asc","missing":27}}]}')" = "[25,27,30]" ] || fail "missing value reported"
[ "$(order '{"sort":[{"age":{"order":"asc","mode":"min"}}]}')" = "2,1,3" ] || fail "mode min accepted"
[ "$(order '{"sort":[{"age":"DESC"}]}')" = "1,2,3" ] || fail "order is case-insensitive"
[ "$(code -XPOST "$U/people/_search" -H "$J" -d '{"sort":[{"age":"desc","name":"asc"}]}')" = 400 ] || fail "multi-key sort object is refused (order cannot be honoured)"
curl -s -XPOST "$U/people/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{"_id":"m1"}}\n{"tags":[1,100]}\n{"index":{"_id":"m2"}}\n{"tags":[50,60]}\n' >/dev/null
# (a field first seen by the split just published becomes sortable once
# the node's 10s stream cache refreshes its dynamic-field inventory)
for i in $(seq 1 15); do
  got=$(curl -s -XPOST "$U/people/_search" -H "$J" -d '{"sort":[{"tags":{"order":"asc","mode":"max"}}],"query":{"exists":{"field":"tags"}}}' | jq -r '[.hits.hits[]?._id] | join(",")')
  [ "$got" = "m2,m1" ] && break; pause 1
done
[ "$got" = "m2,m1" ] || fail "mode max on a multi-valued field: $got"
curl -s -XPOST "$U/people/_delete_by_query" -H "$J" -d '{"query":{"ids":{"values":["m1","m2"]}}}' >/dev/null; curl -s -XPOST "$U/people/_search?size=0" >/dev/null; pause 1.5
R=$(curl -s -XPOST "$U/people/_search" -H "$J" -d '{"size":1,"sort":[{"age":"asc"},{"name":"desc"}],"_source":false}')
[ "$(echo "$R" | jq -r '.hits.hits[0]._id')" = 2 ] || fail "multi sort first"
SA=$(echo "$R" | jq -c '.hits.hits[0].sort')
[ "$(echo "$SA" | jq -c '.[0:2]')" = '[25,"alice"]' ] || fail "multi sort values: $SA"
[ "$(curl -s -XPOST "$U/people/_search" -H "$J" -d "{\"size\":1,\"sort\":[{\"age\":\"asc\"},{\"name\":\"desc\"}],\"search_after\":$SA}" | jq -r '.hits.hits[0]._id')" = 1 ] || fail "search_after with full cursor"
[ "$(curl -s -XPOST "$U/people/_search" -H "$J" -d '{"size":1,"sort":[{"age":"asc"},{"name":"desc"}],"search_after":[25,"alice"]}' | jq -r '.hits.hits[0]._id')" = 1 ] || fail "search_after with field-only cursor"
[ "$(reason '{"size":1,"sort":[{"age":"asc"},{"name":"desc"}],"search_after":[1]}')" = "search_after has 1 value(s) but sort has 2." ] || fail "search_after arity"
# paging by cursor to exhaustion never repeats or loops
seen=""; cur="null"
for i in 1 2 3 4 5; do
  R=$(curl -s -XPOST "$U/people/_search" -H "$J" -d "{\"size\":1,\"sort\":[{\"role.keyword\":\"asc\"}],\"_source\":false,\"search_after\":$cur}")
  id=$(echo "$R" | jq -r '.hits.hits[0]._id // empty'); [ -n "$id" ] || break
  seen="$seen$id,"; cur=$(echo "$R" | jq -c '.hits.hits[0].sort')
done
[ "$seen" = "1,2,3," ] || fail "cursor paging over keyword with missing: $seen"

say "#72 scroll"
R=$(curl -s -XPOST "$U/people/_search?scroll=1m" -H "$J" -d '{"size":2,"sort":["_doc"],"_source":false}')
SID=$(echo "$R" | jq -r '._scroll_id'); [ -n "$SID" ] && [ "$SID" != null ] || fail "scroll id: $R"
[ "$(echo "$R" | jq -r '.hits.hits | length')" = 2 ] || fail "first page size"
[ "$(echo "$R" | jq -r '.hits.total.value')" = 3 ] || fail "total on first page"
R=$(curl -s -XPOST "$U/_search/scroll" -H "$J" -d "{\"scroll\":\"1m\",\"scroll_id\":\"$SID\"}")
[ "$(echo "$R" | jq -r '.hits.hits | length')" = 1 ] || fail "second page: $R"
[ "$(echo "$R" | jq -r '._scroll_id')" = "$SID" ] || fail "scroll id stable"
[ "$(echo "$R" | jq -r '.hits.total.value')" = 3 ] || fail "total carried on later pages"
R=$(curl -s -XPOST "$U/_search/scroll" -H "$J" -d "{\"scroll\":\"1m\",\"scroll_id\":\"$SID\"}")
[ "$(echo "$R" | jq -r '.hits.hits | length')" = 0 ] || fail "exhausted page: $R"
[ "$(curl -s "$U/_search/scroll?scroll=1m&scroll_id=$SID" | jq -r '.hits.hits | length')" = 0 ] || fail "GET form"
[ "$(curl -s -XPOST "$U/_search/scroll/$SID?scroll=1m" | jq -r '._scroll_id')" = "$SID" ] || fail "path form"
[ "$(curl -s -XDELETE "$U/_search/scroll" -H "$J" -d "{\"scroll_id\":[\"$SID\"]}" | jq -cS .)" = '{"num_freed":1,"succeeded":true}' ] || fail "clear scroll"
R=$(curl -s -XPOST "$U/_search/scroll" -H "$J" -d "{\"scroll\":\"1m\",\"scroll_id\":\"$SID\"}")
[ "$(echo "$R" | jq -r '.status')" = 404 ] || fail "cleared scroll is 404: $R"
[ "$(echo "$R" | jq -r '.error.root_cause[0].type')" = search_context_missing_exception ] || fail "missing-context type"
R=$(curl -s -XPOST "$U/_search/scroll" -H "$J" -d '{"scroll":"1m","scroll_id":"bogus"}')
[ "$(echo "$R" | jq -r '.status')" = 400 ] && [ "$(echo "$R" | jq -r '.error.reason')" = "Cannot parse scroll id" ] || fail "bogus id: $R"
[ "$(curl -s -XDELETE "$U/_search/scroll" -H "$J" -d '{"scroll_id":["bogus"]}' | jq -r .status)" = 400 ] || fail "clear bogus"
[ "$(code -XPOST "$U/people/_search?scroll=1m" -H "$J" -d '{"size":2,"search_after":[1]}')" = 400 ] || fail "scroll + search_after"
[ "$(code -XPOST "$U/people/_search?scroll=1m" -H "$J" -d '{"size":2,"from":1}')" = 400 ] || fail "scroll + from"
[ "$(code -XPOST "$U/people/_search?scroll=1x" -H "$J" -d '{}')" = 400 ] || fail "bad keep-alive unit"
[ "$(code -XPOST "$U/people/_search?scroll=2d" -H "$J" -d '{}')" = 400 ] || fail "keep-alive over the ceiling"
R=$(curl -s -XPOST "$U/people/_search?scroll=1m" -H "$J" -d '{"size":1,"aggs":{"a":{"terms":{"field":"name"}}}}')
[ "$(echo "$R" | jq -r '.aggregations.a.buckets | length')" = 3 ] || fail "aggs on first scroll page"
SID=$(echo "$R" | jq -r '._scroll_id')
[ "$(curl -s -XPOST "$U/_search/scroll" -H "$J" -d "{\"scroll\":\"1m\",\"scroll_id\":\"$SID\"}" | jq -r '.aggregations')" = null ] || fail "no aggs on later pages"
# a field-sorted scroll pages in that order
R=$(curl -s -XPOST "$U/people/_search?scroll=1m" -H "$J" -d '{"size":1,"sort":[{"name":"asc"}],"_source":false}')
SID=$(echo "$R" | jq -r '._scroll_id'); s1=$(echo "$R" | jq -r '.hits.hits[0]._id')
s2=$(curl -s -XPOST "$U/_search/scroll" -H "$J" -d "{\"scroll\":\"1m\",\"scroll_id\":\"$SID\"}" | jq -r '.hits.hits[0]._id')
s3=$(curl -s -XPOST "$U/_search/scroll" -H "$J" -d "{\"scroll\":\"1m\",\"scroll_id\":\"$SID\"}" | jq -r '.hits.hits[0]._id')
[ "$s1,$s2,$s3" = "2,1,3" ] || fail "field-sorted scroll: $s1,$s2,$s3"
[ "$(curl -s -XDELETE "$U/_search/scroll/_all" | jq -r .succeeded)" = true ] || fail "clear _all"
[ "$(sql "SELECT count(*) FROM scroll_contexts")" = 0 ] || fail "contexts freed"

say "#71 DELETE /{index}"
[ "$(code -XDELETE "$U/nope")" = 404 ] || fail "delete missing is 404"
[ "$(code -XDELETE "$U/_all")" = 400 ] || fail "_all refused"
[ "$(code -XDELETE "$U/*")" = 400 ] || fail "bare * refused"
[ "$(curl -s -XDELETE "$U/tmp_*")" = '{"acknowledged":true}' ] || fail "glob with no match is acknowledged"
[ "$(curl -s -XDELETE "$U/people")" = '{"acknowledged":true}' ] || fail "delete people"
[ "$(code -I "$U/people")" = 404 ] || fail "HEAD after delete"
[ "$(code "$U/people/_count")" = 404 ] || fail "_count after delete"
[ "$(curl -s "$U/_cat/indices" | jq -r '[.[].index] | index("people")')" = null ] || fail "gone from _cat/indices"
say "  drop-and-rebuild under the same name"
curl -s -XPUT "$U/people" -H "$J" -d '{"settings":{"index":{"mode":"document"}},"mappings":{"properties":{"name":{"type":"keyword"}}}}' | jq -e '.acknowledged' >/dev/null || fail "re-create"
[ "$(curl -s "$U/people/_count" | jq -r .count)" = 0 ] || fail "re-created index is empty"
curl -s -XPOST "$U/people/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{"_id":"1"}}\n{"name":"dave"}\n' | jq -e '.errors == false' >/dev/null || fail "bulk into re-created index"
[ "$(curl -s "$U/people/_count" | jq -r .count)" = 1 ] || fail "rebuilt index holds only the new doc"
[ "$(curl -s "$U/people/_mapping" | jq -r '.people.mappings.properties | keys | join(",")')" = "@timestamp,name" ] || fail "old dynamic fields do not leak into the new index"
say "  storage reclaimed and the retired row purged"
for i in $(seq 1 30); do
  [ "$(sql "SELECT count(*) FROM streams WHERE deleted_at IS NOT NULL")" = 0 ] && break; pause 1
done
[ "$(sql "SELECT count(*) FROM streams WHERE deleted_at IS NOT NULL")" = 0 ] || fail "retired stream not purged"
[ "$(sql "SELECT count(*) FROM splits s JOIN streams st ON st.id = s.stream_id WHERE st.name = 'people'")" -ge 1 ] || fail "new stream keeps its split"
say "  glob and list deletion"
curl -s -XPUT "$U/tmp_a" >/dev/null; curl -s -XPUT "$U/tmp_b" >/dev/null; curl -s -XPUT "$U/keep" >/dev/null
[ "$(curl -s -XDELETE "$U/tmp_*,nope_*")" = '{"acknowledged":true}' ] || fail "glob delete"
# (deletes are audited into rsearch-audit, which appears alongside)
[ "$(curl -s "$U/_cat/indices" | jq -r '[.[].index | select(. != "rsearch-audit")] | sort | join(",")')" = "keep,people" ] || fail "globbed indices gone: $(curl -s "$U/_cat/indices" | jq -c '[.[].index]')"
say "  documents in flight at deletion die with the index (as in OpenSearch)"
curl -s -XPOST "$U/inflight/_bulk" -H "$ND" --data-binary $'{"index":{}}\n{"m":"buffered"}\n' | jq -e '.errors == false' >/dev/null || fail "bulk inflight"
[ "$(curl -s -XDELETE "$U/inflight")" = '{"acknowledged":true}' ] || fail "delete inflight"
pause 6
[ "$(code -I "$U/inflight")" = 404 ] || fail "a late flush must not resurrect a deleted index"
grep -q "index deleted; buffered documents dropped" "$LOGDIR/node.log" || fail "drop not logged"
say "  delete + re-create while a document-mode batch is buffered keeps the new mode"
curl -s -XPUT "$U/redo" -H "$J" -d '{"settings":{"index":{"mode":"document"}}}' >/dev/null
curl -s -XPOST "$U/redo/_bulk" -H "$ND" --data-binary $'{"index":{"_id":"old"}}\n{"v":"old"}\n' >/dev/null
[ "$(curl -s -XDELETE "$U/redo")" = '{"acknowledged":true}' ] || fail "delete redo"
curl -s -XPUT "$U/redo" -H "$J" -d '{"settings":{"index":{"mode":"document"}}}' | jq -e '.acknowledged' >/dev/null || fail "re-create redo as document mode"
curl -s -XPOST "$U/redo/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{"_id":"new"}}\n{"v":"new"}\n' | jq -e '.errors == false' >/dev/null || fail "bulk into re-created redo"
pause 3
[ "$(curl -s "$U/redo/_settings" | jq -r '.redo.settings.index.mode')" = document ] || fail "re-created index keeps document mode"
[ "$(curl -s "$U/redo/_search" -H "$J" -d '{"_source":false}' | jq -r '[.hits.hits[]._id] | sort | join(",")')" = "new" ] || fail "only the post-recreate document survives: $(curl -s "$U/redo/_search" -H "$J" -d '{"_source":false}' | jq -c '[.hits.hits[]._id]')"
say "  scoped keys delete only inside their streams"
curl -s -XPUT "$U/_rsearch/users/admin" -H "$J" -d '{"password":"adminpassword123","role":"admin"}' | jq -e '.acknowledged' >/dev/null || fail "create admin"
TOKEN=$(curl -s -XPOST "$U/_rsearch/login" -H "$J" -d '{"username":"admin","password":"adminpassword123"}' | jq -r '.token')
KEY=$(curl -s -XPOST "$U/_rsearch/api_keys" -H "Authorization: Bearer $TOKEN" -H "$J" -d '{"name":"app","actions":["ingest","search"],"streams":["app-*"]}' | jq -r '.key')
curl -s -XPUT "$U/app-x" -H "Authorization: Bearer $KEY" >/dev/null
[ "$(code -XDELETE "$U/app-x" -H "Authorization: Bearer $KEY")" = 200 ] || fail "scoped key deletes its own index"
[ "$(code -XDELETE "$U/keep" -H "Authorization: Bearer $KEY")" = 403 ] || fail "scoped key denied outside scope"
curl -s -XPUT "$U/app-y" -H "Authorization: Bearer $KEY" >/dev/null
[ "$(code -XDELETE "$U/app-*,keep" -H "Authorization: Bearer $KEY")" = 403 ] || fail "list escaping the scope is denied"
[ "$(code -I "$U/app-y" -H "Authorization: Bearer $KEY")" = 200 ] || fail "denied list deleted nothing"
[ "$(code "$U/_search/scroll?scroll_id=$(printf '%032d' 0)" -H "Authorization: Bearer $KEY")" = 404 ] || fail "scoped key can reach the scroll API"
say "  scoped keys clear only their own scroll contexts"
curl -s -XPUT "$U/app-s" -H "Authorization: Bearer $KEY" >/dev/null
FOREIGN=$(curl -s -XPOST "$U/keep/_search?scroll=1m" -H "Authorization: Bearer $TOKEN" -H "$J" -d '{}' | jq -r '._scroll_id')
MINE=$(curl -s -XPOST "$U/app-s/_search?scroll=1m" -H "Authorization: Bearer $KEY" -H "$J" -d '{}' | jq -r '._scroll_id')
[ "$(code -XDELETE "$U/_search/scroll" -H "Authorization: Bearer $KEY" -H "$J" -d "{\"scroll_id\":\"$FOREIGN\"}")" = 403 ] || fail "scoped key cannot free another tenant's scroll"
[ "$(curl -s -XDELETE "$U/_search/scroll/_all" -H "Authorization: Bearer $KEY" | jq -r .num_freed)" = 1 ] || fail "scoped _all frees only the key's own contexts"
[ "$(sql "SELECT count(*) FROM scroll_contexts WHERE id = '$FOREIGN'")" = 1 ] || fail "foreign context survived a scoped _all"
[ "$(code -XPOST "$U/_search/scroll" -H "Authorization: Bearer $TOKEN" -H "$J" -d "{\"scroll_id\":\"$MINE\"}")" = 404 ] || fail "own context was freed"

say "#80 _refresh"
A="Authorization: Bearer $TOKEN"
curl -s -XPUT "$U/rf" -H "$A" -H "$J" -d '{"settings":{"index":{"mode":"document"}}}' | jq -e '.acknowledged' >/dev/null || fail "create rf"
curl -s -XPOST "$U/rf/_bulk" -H "$A" -H "$ND" --data-binary $'{"index":{"_id":"1"}}\n{"v":1}\n{"index":{"_id":"2"}}\n{"v":2}\n' | jq -e '.errors == false' >/dev/null || fail "bulk rf"
[ "$(curl -s -XPOST "$U/rf/_refresh" -H "$A" | jq -cS .)" = '{"_shards":{"failed":0,"successful":1,"total":1}}' ] || fail "POST _refresh: $(curl -s -XPOST "$U/rf/_refresh" -H "$A")"
[ "$(curl -s "$U/rf/_count" -H "$A" | jq -r .count)" = 2 ] || fail "writes visible right after _refresh"
[ "$(curl -s "$U/rf/_refresh" -H "$A" | jq -cS .)" = '{"_shards":{"failed":0,"successful":1,"total":1}}' ] || fail "GET _refresh"
[ "$(curl -s -XPOST "$U/rf,people/_refresh" -H "$A" | jq -r '._shards.total')" = 2 ] || fail "list _refresh counts one shard per index"
[ "$(curl -s -XPOST "$U/re*/_refresh" -H "$A" | jq -r '._shards.total')" = 1 ] || fail "glob _refresh matches redo only"
[ "$(curl -s -XPOST "$U/nomatch-*/_refresh" -H "$A" | jq -cS .)" = '{"_shards":{"failed":0,"successful":0,"total":0}}' ] || fail "glob matching nothing is acknowledged"
R=$(curl -s -XPOST "$U/nope/_refresh" -H "$A")
[ "$(echo "$R" | jq -r .status)" = 404 ] || fail "missing index is 404: $R"
[ "$(echo "$R" | jq -r '.error.root_cause[0].reason')" = "no such index [nope]" ] || fail "missing index reason: $R"
[ "$(code -XPOST "$U/rf,nope/_refresh" -H "$A")" = 404 ] || fail "list with a missing index is 404"
ALL=$(curl -s "$U/_cat/indices?format=json" -H "$A" | jq 'length')
[ "$(curl -s -XPOST "$U/_refresh" -H "$A" | jq -r '._shards.total')" = "$ALL" ] || fail "/_refresh covers every index"
[ "$(curl -s -XPOST "$U/_all/_refresh" -H "$A" | jq -r '._shards.total')" = "$ALL" ] || fail "_all/_refresh covers every index"
[ "$(code -XPUT "$U/rf/_refresh" -H "$A")" = 405 ] || fail "PUT _refresh is 405"
say "  scoped keys refresh only inside their streams"
[ "$(code -XPOST "$U/app-y/_refresh" -H "Authorization: Bearer $KEY")" = 200 ] || fail "scoped key refreshes its own index"
[ "$(code -XPOST "$U/keep/_refresh" -H "Authorization: Bearer $KEY")" = 403 ] || fail "scoped key denied outside scope"
[ "$(code -XPOST "$U/app-*,keep/_refresh" -H "Authorization: Bearer $KEY")" = 403 ] || fail "list escaping the scope is denied"
[ "$(code -XPOST "$U/_refresh" -H "Authorization: Bearer $KEY")" = 403 ] || fail "scoped key cannot refresh everything"

say "PASS: OpenSearch compatibility (#71 #72 #73 #74 #76 #77 #80)"
