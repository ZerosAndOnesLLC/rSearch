#!/usr/bin/env bash
# OpenSearch-compatibility end-to-end test for issues #85, #86 and #87:
# declared multi-fields, keyword normalizers, `case_insensitive` on
# `term`, the full `strict_date_optional_time` grammar, malformed values
# failing a document-mode write, the log-mode `_id` guard and
# `action.auto_create_index`. One all-roles node on the fs backend +
# Postgres. Every expected shape below was taken from a real OpenSearch
# 3.8.
set -euo pipefail
cd "$(dirname "$0")/../.."

BIN=./target/release/rsearch
LOGDIR=/tmp/rsearch-mapping-compat
PORT=9271
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
psql "$DATABASE_URL" -qc "DELETE FROM streams; DELETE FROM nodes; DELETE FROM users; DELETE FROM api_keys; DELETE FROM sessions;" >/dev/null

say "starting node"
env DATABASE_URL="$DATABASE_URL" \
  RSEARCH_NODE__ID=mapping-1 \
  RSEARCH_NODE__DATA_DIR="$LOGDIR/node" \
  RSEARCH_HTTP__BIND_ADDR="127.0.0.1:$PORT" \
  RSEARCH_STORAGE__BACKEND=fs \
  RSEARCH_STORAGE__ROOT="$LOGDIR/store" \
  RSEARCH_INGEST__MAX_BATCH_SECS=3 \
  RSEARCH_INGEST__DOCUMENT_MAX_BATCH_SECS=1 \
  RSEARCH_INGEST__BALANCE_BULK=false \
  RSEARCH_INGEST__AUTO_CREATE_INDEX="-noauto-*,+*" \
  RSEARCH_CONTROL__INTERVAL_SECS=2 \
  RSEARCH_CONTROL__MERGE_TARGET_MB=0 \
  RSEARCH_CONTROL__MERGE_MIN_MB=0 \
  "$BIN" --roles ingest,search,control >"$LOGDIR/node.log" 2>&1 &
PID=$!
for i in $(seq 1 60); do curl -sf "$U/health" >/dev/null && break; pause 0.25; done
curl -sf "$U/health" >/dev/null || fail "node did not come up"

J='Content-Type: application/json'
ND='Content-Type: application/x-ndjson'
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
# hit count for a query body against $1
hits() { curl -s "$U/$1/_count" -H "$J" -d "$2" | jq -r '.count // ("ERR:" + .error.type)'; }
reason() { echo "$1" | jq -r '.error.root_cause[0].reason // .error.reason'; }
# `?refresh` only cuts a split on document-mode indices, so a log-mode
# write becomes searchable when its batch ages out (max_batch_secs).
wait_hits() { for _ in $(seq 1 40); do [ "$(hits "$1" "$2")" = "$3" ] && return 0; pause 0.5; done; return 1; }

say "#85 declared multi-fields are indexed"
curl -s -XPUT "$U/probe" -H "$J" -d '{
  "settings":{"index":{"mode":"document"},
              "analysis":{"normalizer":{"lower":{"type":"custom","filter":["lowercase"]}}}},
  "mappings":{"properties":{
    "name":{"type":"text","fields":{"keyword":{"type":"keyword"},
                                    "short":{"type":"keyword","ignore_above":5}}},
    "code":{"type":"keyword","normalizer":"lower"},
    "plain":{"type":"keyword"},
    "n":{"type":"long"},
    "d":{"type":"date"}}}}' | jq -e '.acknowledged' >/dev/null || fail "create probe"
curl -s -XPUT "$U/probe/_doc/1?refresh=wait_for" -H "$J" \
  -d '{"name":"Kirsten Andersen","code":"Kirsten Andersen","plain":"Kirsten Andersen","n":5,"d":"2026-09-16"}' \
  | jq -e '.result' >/dev/null || fail "index probe doc"

[ "$(hits probe '{"query":{"term":{"name.keyword":"Kirsten Andersen"}}}')" = 1 ] || fail "term on declared sub-field"
[ "$(hits probe '{"query":{"prefix":{"name.keyword":"Kirsten"}}}')" = 1 ] || fail "prefix on sub-field"
[ "$(hits probe '{"query":{"exists":{"field":"name.keyword"}}}')" = 1 ] || fail "exists on sub-field"
[ "$(hits probe '{"query":{"wildcard":{"name.keyword":"*Anders*"}}}')" = 1 ] || fail "wildcard on sub-field"
[ "$(hits probe '{"query":{"match":{"name":"andersen"}}}')" = 1 ] || fail "parent text field still analyzed"
# ignore_above 5 against a 16-character value: no keyword view at all.
[ "$(hits probe '{"query":{"exists":{"field":"name.short"}}}')" = 0 ] || fail "ignore_above sub-field must hold nothing"
B=$(curl -s -XPOST "$U/probe/_search" -H "$J" -d '{"size":0,"aggs":{"n":{"terms":{"field":"name.keyword"}}}}')
[ "$(echo "$B" | jq -r '.aggregations.n.buckets[0].key')" = "Kirsten Andersen" ] || fail "terms agg on sub-field: $B"
S=$(curl -s -XPOST "$U/probe/_search" -H "$J" -d '{"sort":[{"name.keyword":"asc"}]}')
[ "$(echo "$S" | jq -r '.hits.hits[0].sort[0]')" = "Kirsten Andersen" ] || fail "sort on sub-field: $S"
[ "$(curl -s "$U/probe/_mapping" | jq -r '.probe.mappings.properties.name.fields.keyword.type')" = keyword ] \
  || fail "_mapping echoes the declared sub-field"

say "#85 keyword normalizer applies on both sides"
[ "$(hits probe '{"query":{"term":{"code":"kirsten andersen"}}}')" = 1 ] || fail "normalized term"
# OpenSearch normalizes the query value too, so the original case matches as well.
[ "$(hits probe '{"query":{"term":{"code":"Kirsten Andersen"}}}')" = 1 ] || fail "query value is normalized"
[ "$(hits probe '{"query":{"prefix":{"code":"KIRSTEN"}}}')" = 1 ] || fail "normalized prefix"
[ "$(hits probe '{"query":{"wildcard":{"code":"*ANDERSEN"}}}')" = 1 ] || fail "normalized wildcard"
[ "$(hits probe '{"query":{"terms":{"code":["KIRSTEN ANDERSEN"]}}}')" = 1 ] || fail "normalized terms"
[ "$(hits probe '{"query":{"match":{"code":"KIRSTEN ANDERSEN"}}}')" = 1 ] || fail "normalized match"
[ "$(hits probe '{"query":{"range":{"code":{"gte":"KIRSTEN","lte":"KIRSTENZ"}}}}')" = 1 ] || fail "normalized range bound"
[ "$(curl -s "$U/probe/_settings" | jq -r '.probe.settings.index.analysis.normalizer.lower.filter[0]')" = lowercase ] \
  || fail "_settings echoes the normalizer"

say "#85 case_insensitive on term"
[ "$(hits probe '{"query":{"term":{"plain":{"value":"kirsten andersen","case_insensitive":true}}}}')" = 1 ] \
  || fail "case_insensitive term (lower)"
[ "$(hits probe '{"query":{"term":{"plain":{"value":"KIRSTEN ANDERSEN","case_insensitive":true}}}}')" = 1 ] \
  || fail "case_insensitive term (upper)"
[ "$(hits probe '{"query":{"term":{"plain":{"value":"kirsten andersen","case_insensitive":false}}}}')" = 0 ] \
  || fail "case_insensitive:false stays exact"
[ "$(hits probe '{"query":{"term":{"name":{"value":"ANDERSEN","case_insensitive":true}}}}')" = 1 ] \
  || fail "case_insensitive term on a text field"
R=$(curl -s "$U/probe/_search" -H "$J" -d '{"query":{"term":{"n":{"value":"5","case_insensitive":true}}}}')
[ "$(reason "$R")" = "[n] field which is of type [long], does not support case insensitive term queries" ] \
  || fail "case_insensitive on a numeric field: $R"
R=$(curl -s "$U/probe/_search" -H "$J" -d '{"query":{"terms":{"plain":["a"],"case_insensitive":true}}}')
[ "$(reason "$R")" = "[terms] query does not support [case_insensitive]" ] || fail "terms rejects it: $R"

say "#85 a mapping parameter that is not honored is refused"
R=$(curl -s -XPUT "$U/bad1" -H "$J" -d '{"mappings":{"properties":{"f":{"type":"keyword","bogus":true}}}}')
[ "$(echo "$R" | jq -r .status)" = 400 ] || fail "unknown parameter must be 400: $R"
[ "$(reason "$R")" = "unknown parameter [bogus] on mapper [f] of type [keyword]" ] || fail "unknown parameter reason: $R"
R=$(curl -s -XPUT "$U/bad2" -H "$J" -d '{"mappings":{"properties":{"f":{"type":"keyword","index":false}}}}')
[ "$(reason "$R")" = "parameter [index] on mapper [f] of type [keyword] is supported only at its default value [true]" ] \
  || fail "non-default parameter reason: $R"
R=$(curl -s -XPUT "$U/bad3" -H "$J" -d '{"mappings":{"properties":{"f":{"type":"keyword","normalizer":"nope"}}}}')
[ "$(reason "$R")" = "normalizer [nope] not found for field [f]" ] || fail "undefined normalizer: $R"
R=$(curl -s -XPUT "$U/bad4" -H "$J" -d '{"settings":{"analysis":{"normalizer":{"b":{"type":"custom","filter":["stop"]}}}},"mappings":{"properties":{"f":{"type":"keyword","normalizer":"b"}}}}')
[ "$(reason "$R")" = "Custom normalizer [b] may not use filter [stop]" ] || fail "tokenizing filter: $R"
# The parameters a stock client sends at their defaults still work.
[ "$(code -XPUT "$U/ok1" -H "$J" -d '{"mappings":{"properties":{
  "f":{"type":"keyword","index":true,"doc_values":true,"store":false,"eager_global_ordinals":false},
  "m":{"type":"text","analyzer":"standard","norms":true},
  "d":{"type":"date","format":"strict_date_optional_time||epoch_millis"}}}}')" = 200 ] \
  || fail "OpenSearch defaults must be accepted"

say "#86 strict_date_optional_time"
curl -s -XPUT "$U/dates" -H "$J" -d '{"settings":{"index":{"mode":"document"}},"mappings":{"properties":{"d":{"type":"date"}}}}' >/dev/null
i=0
# Every form of the default format, all naming the same instant except
# `2026-09` (September 1st) and the epoch stamp (17:04 that day).
for v in '"2026-09-16"' '"2026-09-16T00:00:00"' '"2026-09-16T00:00"' '"2026-09-16T00:00:00Z"' \
         '"2026-09-16T00:00:00.000Z"' '"2026-09-16T00:00:00+00:00"' '"2026-09-16T00:00:00+0000"' \
         '"2026-09"' '1789578275562'; do
  i=$((i+1))
  [ "$(code -XPUT "$U/dates/_doc/$i?refresh=wait_for" -H "$J" -d "{\"d\":$v}")" = 200 ] \
    || fail "date form $v must index"
done
[ "$(hits dates '{"query":{"range":{"d":{"gte":"2026-09-01","lt":"2026-10-01"}}}}')" = 9 ] \
  || fail "date-only range bound must cover every form"
[ "$(hits dates '{"query":{"term":{"d":"2026-09-16"}}}')" = 7 ] \
  || fail "the seven midnight forms are one instant"
[ "$(hits dates '{"query":{"range":{"d":{"gte":"2026-09-16T00:00:00","lte":"2026-09-16T00:00:00.000Z"}}}}')" = 7 ] \
  || fail "offset-less bounds are UTC"
R=$(curl -s "$U/dates/_search" -H "$J" -d '{"query":{"range":{"d":{"gte":"not a date"}}}}')
[ "$(echo "$R" | jq -r .status)" = 400 ] || fail "an unparseable bound is still a 400: $R"

say "#86 a value a mapped field cannot parse fails the document"
R=$(curl -s -XPUT "$U/probe/_doc/bad?refresh=wait_for" -H "$J" -d '{"d":"nonsense"}')
[ "$(echo "$R" | jq -r '.error.type')" = mapper_parsing_exception ] || fail "malformed date must be refused: $R"
[ "$(echo "$R" | jq -r '.error.reason')" = "failed to parse field [d] of type [date] in document with id 'bad'. Preview of field's value: 'nonsense'" ] \
  || fail "malformed reason: $R"
[ "$(echo "$R" | jq -r '.error.caused_by.reason')" = "failed to parse date field [nonsense] with format [strict_date_optional_time||epoch_millis]" ] \
  || fail "malformed caused_by: $R"
[ "$(code "$U/probe/_doc/bad")" = 404 ] || fail "a refused write must not be stored"
B=$(curl -s -XPOST "$U/probe/_bulk?refresh=wait_for" -H "$ND" --data-binary \
  $'{"index":{"_id":"b1"}}\n{"d":"nonsense"}\n{"index":{"_id":"b2"}}\n{"d":"2026-01-01"}\n')
[ "$(echo "$B" | jq -r '.errors')" = true ] || fail "bulk must report the error: $B"
[ "$(echo "$B" | jq -r '.items[0].index.error.type')" = mapper_parsing_exception ] || fail "per-item error: $B"
# An explicit id on a document-mode index always answers "updated": the
# write path does not look up whether the id was there already.
[ "$(echo "$B" | jq -r '.items[1].index.status')" = 200 ] || fail "the good item still indexes: $B"
# Every mapper, the way OpenSearch coerces and refuses.
for bad in '{"n":"abc"}' '{"n":" 5"}' '{"n":true}' '{"plain":{"a":1}}' '{"d":""}'; do
  [ "$(code -XPOST "$U/probe/_doc" -H "$J" -d "$bad")" = 400 ] || fail "$bad must be refused"
done
for good in '{"n":"5"}' '{"n":5.7}' '{"n":""}' '{"plain":5}' '{"d":1789578275562}'; do
  [ "$(code -XPOST "$U/probe/_doc" -H "$J" -d "$good")" = 201 ] || fail "$good must be accepted"
done
say "  ignore_malformed keeps the OpenSearch escape hatch"
curl -s -XPUT "$U/lenient" -H "$J" -d '{"settings":{"index":{"mode":"document"}},"mappings":{"properties":{"d":{"type":"date","ignore_malformed":true}}}}' >/dev/null
[ "$(code -XPUT "$U/lenient/_doc/1?refresh=wait_for" -H "$J" -d '{"d":"nonsense"}')" = 200 ] || fail "ignore_malformed accepts"
[ "$(hits lenient '{"query":{"exists":{"field":"d"}}}')" = 0 ] || fail "ignore_malformed drops the value"
say "  a log-mode index keeps ingesting, and counts what it dropped"
curl -s -XPUT "$U/logs" -H "$J" -d '{"mappings":{"properties":{"d":{"type":"date"}}}}' >/dev/null
[ "$(curl -s -XPOST "$U/logs/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{}}\n{"d":"nonsense","msg":"kept"}\n' | jq -r '.errors')" = false ] \
  || fail "log-mode ingest must not start failing"
wait_hits logs '{"query":{"match_all":{}}}' 1 || fail "the log document is still indexed"
[ "$(hits logs '{"query":{"exists":{"field":"d"}}}')" = 0 ] || fail "the unparseable value is dropped"
curl -s "$U/metrics" | grep -q '^rsearch_ingest_malformed_dropped_total 1$' \
  || fail "drop is counted: $(curl -s "$U/metrics" | grep malformed)"

say "#87 an explicit _id is refused on a log-mode index"
EXPECTED='index is not supported on log-mode index [logs]; create the index with {"settings":{"index":{"mode":"document"}}} to enable document-level writes'
B=$(curl -s -XPOST "$U/logs/_bulk" -H "$ND" --data-binary $'{"index":{"_id":"a1"}}\n{"v":1}\n')
[ "$(echo "$B" | jq -r '.items[0].index.status')" = 400 ] || fail "index with _id must be refused: $B"
[ "$(echo "$B" | jq -r '.items[0].index.error.reason')" = "$EXPECTED" ] || fail "log-mode reason: $B"
B=$(curl -s -XPOST "$U/logs/_bulk" -H "$ND" --data-binary $'{"create":{"_id":"a1"}}\n{"v":1}\n')
[ "$(echo "$B" | jq -r '.items[0].create.status')" = 400 ] || fail "create with _id must be refused: $B"
[ "$(code -XPUT "$U/logs/_doc/a1" -H "$J" -d '{"v":1}')" = 400 ] || fail "PUT _doc/{id} must be refused"
[ "$(curl -s -XPOST "$U/logs/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{}}\n{"v":1}\n' | jq -r '.errors')" = false ] \
  || fail "a log write without an _id still works"
wait_hits logs '{"query":{"match_all":{}}}' 2 || fail "only the id-less write landed"
say "  an _id into an index that does not exist yet creates a document index"
[ "$(code -XPUT "$U/fresh/_doc/1?refresh=wait_for" -H "$J" -d '{"v":1}')" = 200 ] \
  || fail "an explicit _id must create the index it can work in"
[ "$(curl -s "$U/fresh/_settings" | jq -r '.fresh.settings.index.mode')" = document ] \
  || fail "the implicitly created index must be a document index"
[ "$(hits fresh '{"query":{"ids":{"values":["1"]}}}')" = 1 ] || fail "the document is addressable by its id"
[ "$(code -XDELETE "$U/fresh/_doc/1")" = 200 ] || fail "and deletable, as in OpenSearch"
# Without an id it is still a log index, the way _bulk always created one.
curl -s -XPOST "$U/freshlog/_bulk" -H "$ND" --data-binary $'{"index":{}}\n{"v":1}\n' >/dev/null
[ "$(curl -s "$U/freshlog/_settings" | jq -r '.freshlog.settings.index.mode')" = log ] \
  || fail "an id-less write still creates a log index"

say "  a document-mode index still takes explicit ids"
[ "$(code -XPUT "$U/probe/_doc/keep" -H "$J" -d '{"n":1}')" = 200 ] || fail "document mode still honors _id"

say "#87 action.auto_create_index"
B=$(curl -s -XPOST "$U/noauto-1/_bulk" -H "$ND" --data-binary $'{"index":{}}\n{"v":1}\n')
[ "$(echo "$B" | jq -r '.items[0].index.status')" = 404 ] || fail "write to a missing index must fail: $B"
[ "$(echo "$B" | jq -r '.items[0].index.error.reason')" = "no such index [noauto-1] and [action.auto_create_index] is [-noauto-*,+*]" ] \
  || fail "auto_create reason: $B"
[ "$(code "$U/noauto-1")" = 404 ] || fail "the index must not have been created"
curl -s -XPUT "$U/noauto-1" -H "$J" -d '{"settings":{"index":{"mode":"document"}}}' >/dev/null
[ "$(curl -s -XPOST "$U/noauto-1/_bulk?refresh=wait_for" -H "$ND" --data-binary $'{"index":{"_id":"1"}}\n{"v":1}\n' | jq -r '.errors')" = false ] \
  || fail "writes work once the index exists with the client's own mapping"
[ "$(curl -s -XPOST "$U/other-1/_bulk" -H "$ND" --data-binary $'{"index":{}}\n{"v":1}\n' | jq -r '.errors')" = false ] \
  || fail "a name the pattern allows is still auto-created"

say "PASS: OpenSearch compatibility (#85 #86 #87)"
