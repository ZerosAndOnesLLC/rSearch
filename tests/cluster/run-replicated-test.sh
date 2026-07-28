#!/usr/bin/env bash
# Replicated-backend cluster test: 3 all-role nodes, no external object
# store — each node keeps a local object root and splits replicate to
# factor 2 over the internal peer API. Proves: quorum writes + placement,
# cross-node reads (peer fetch), holder death with search continuity,
# leader-driven repair back to factor, and fan-out delete via retention+GC.
set -euo pipefail
cd "$(dirname "$0")/../.."

BIN=./target/release/rsearch
LOGDIR=/tmp/rsearch-replicated
PG_CONTAINER=${PG_CONTAINER:-rsearch-pg}
TOKEN=replicated-cluster-test-token
say() { echo "==> $*"; }
pause() { perl -e "select(undef,undef,undef,$1)"; }
fail() { echo "FAIL: $*"; exit 1; }

declare -A PIDS
cleanup() {
  for pid in "${PIDS[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

psql_t() { docker exec "$PG_CONTAINER" psql -U rsearch -tAc "$1"; }

start_node() { # name port
  local name=$1 port=$2
  env \
    DATABASE_URL="$DATABASE_URL" \
    RSEARCH_NODE__ID="$name" \
    RSEARCH_NODE__DATA_DIR="$LOGDIR/$name" \
    RSEARCH_NODE__ADVERTISE_ADDR="http://127.0.0.1:$port" \
    RSEARCH_HTTP__BIND_ADDR="127.0.0.1:$port" \
    RSEARCH_STORAGE__BACKEND=replicated \
    RSEARCH_STORAGE__ROOT="$LOGDIR/$name/objects" \
    RSEARCH_STORAGE__REPLICATION_FACTOR=2 \
    RSEARCH_CLUSTER__INTERNAL_TOKEN="$TOKEN" \
    RSEARCH_INGEST__MAX_BATCH_SECS=1 \
    RSEARCH_CONTROL__INTERVAL_SECS=3 \
    RSEARCH_CONTROL__GC_GRACE_SECS=5 \
    RSEARCH_CONTROL__REPAIR_STALE_SECS=10 \
    "$BIN" --roles ingest,search,control >"$LOGDIR/$name.log" 2>&1 &
  PIDS[$name]=$!
}

wait_health() { # port
  for i in $(seq 1 60); do
    curl -sf "http://127.0.0.1:$1/health" >/dev/null && return 0
    pause 0.25
  done
  return 1
}

bulk() { # port index count offset
  local port=$1 index=$2 count=$3 offset=$4
  { for i in $(seq 1 "$count"); do
      printf '{"index":{"_index":"%s"}}\n{"@timestamp":%d,"service":"svc-%d","message":"replicated doc %d","n":%d}\n' \
        "$index" "$(( $(date +%s%3N) ))" $((i % 5)) $((offset + i)) $((offset + i))
    done
  } | curl -s -XPOST "http://127.0.0.1:$port/_bulk" \
        -H 'Content-Type: application/x-ndjson' --data-binary @- | jq -r '.errors'
}

count_docs() { # port index
  curl -s -XPOST "http://127.0.0.1:$1/$2/_search" \
    -H 'Content-Type: application/json' -d '{"size":0}' | jq -r '.hits.total.value // 0'
}

# Keys of published splits with fewer than $2 copies among nodes $3 (SQL
# id list, e.g. "'node-2','node-3'").
under_held() {
  psql_t "SELECT count(*) FROM (
            SELECT s.storage_key
            FROM splits s
            LEFT JOIN object_locations ol
              ON ol.storage_key = s.storage_key AND ol.node_id IN ($3)
            WHERE s.state = 'published'
            GROUP BY s.storage_key
            HAVING count(ol.node_id) < $2
          ) u"
}

# ---------- setup ----------
set -a; source .env; set +a
rm -rf "$LOGDIR"; mkdir -p "$LOGDIR"
pkill -x rsearch 2>/dev/null || true; pause 0.5
docker exec "$PG_CONTAINER" psql -U rsearch -qc \
  "DELETE FROM object_locations; DELETE FROM splits; DELETE FROM streams; DELETE FROM nodes;" >/dev/null

say "starting 3 replicated all-role nodes"
start_node node-1 9311
start_node node-2 9312
start_node node-3 9313
for port in 9311 9312 9313; do
  wait_health $port || fail "node on :$port did not become healthy"
done
say "all nodes healthy"

# ---------- quorum writes + cross-node reads ----------
say "TEST: docs ingested on two nodes visible from every node (peer reads)"
[ "$(bulk 9311 rlogs 100 0)" = "false" ] || fail "bulk to node-1 errored"
[ "$(bulk 9312 rlogs 100 100)" = "false" ] || fail "bulk to node-2 errored"
pause 6
for port in 9311 9312 9313; do
  C=$(count_docs $port rlogs)
  [ "$C" = "200" ] || fail "search on :$port sees $C/200"
done
say "PASS: all three nodes see all 200 docs"

# ---------- placement at factor ----------
say "TEST: every published split has 2 recorded copies"
UNDER=$(under_held rlogs 2 "'node-1','node-2','node-3'")
[ "$UNDER" = "0" ] || fail "$UNDER published splits below replication factor"
say "PASS: placement at factor 2"

# ---------- holder death ----------
say "TEST: killing node-1; survivors keep answering"
kill -9 "${PIDS[node-1]}"; unset 'PIDS[node-1]'
pause 1
for port in 9312 9313; do
  C=$(count_docs $port rlogs)
  [ "$C" = "200" ] || fail "search on :$port sees $C/200 after holder death"
done
say "PASS: search unaffected by holder death"

# ---------- repair ----------
say "TEST: repair restores factor 2 on the survivors"
# repair_stale_secs=10: node-1 goes stale, then the leader re-replicates.
for i in $(seq 1 45); do
  UNDER=$(under_held rlogs 2 "'node-2','node-3'")
  [ "$UNDER" = "0" ] && break; pause 1
done
[ "$UNDER" = "0" ] || fail "repair left $UNDER splits under-held on survivors"
grep -qh "repair: copy restored" "$LOGDIR"/node-*.log || fail "no repair logged"
[ "$(count_docs 9313 rlogs)" = "200" ] || fail "docs lost after repair"
say "PASS: all splits back at factor 2 on live nodes"

# ---------- retention + fan-out delete ----------
say "TEST: retention expiry fans the delete out and clears placement"
[ "$(bulk 9312 rep-ret 40 0)" = "false" ] || fail "bulk rep-ret errored"
pause 4
[ "$(count_docs 9313 rep-ret)" = "40" ] || fail "rep-ret not ingested"
docker exec "$PG_CONTAINER" psql -U rsearch -qc \
  "UPDATE streams SET retention_hours=0 WHERE name='rep-ret'"
for i in $(seq 1 30); do
  LEFT=$(psql_t "SELECT count(*) FROM object_locations WHERE storage_key LIKE 'streams/rep-ret/%'")
  [ "$LEFT" = "0" ] && [ "$(count_docs 9313 rep-ret)" = "0" ] && break
  pause 1
done
[ "$LEFT" = "0" ] || fail "placement rows not cleared by GC delete"
[ "$(count_docs 9313 rep-ret)" = "0" ] || fail "retention did not expire docs"
say "PASS: GC delete removed copies and placement cluster-wide"

echo
echo "REPLICATED CLUSTER TEST OK"
