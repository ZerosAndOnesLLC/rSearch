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
    RSEARCH_CONTROL__MERGE_MIN_MB=0 \
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
# Point the cluster at a dedicated test database (must be the one inside
# $PG_CONTAINER) when .env's DATABASE_URL is a shared dev instance.
DATABASE_URL=${RSEARCH_TEST_DATABASE_URL:-$DATABASE_URL}
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

# ---------- phantom placement rows (#44) ----------
say "TEST: placement row for a file lost from a node's volume is removed and repaired"
PHANTOM_KEY=$(psql_t "SELECT storage_key FROM object_locations WHERE node_id='node-3' LIMIT 1")
[ -n "$PHANTOM_KEY" ] || fail "no placement row on node-3 to test with"
# Simulate a replaced data volume: delete the file behind the cluster's
# back and restart the node — the startup reconcile verify must drop the
# now-phantom row. Rows younger than the verify age floor are skipped,
# so backdate them (this cluster is seconds old).
kill -9 "${PIDS[node-3]}"
rm "$LOGDIR/node-3/objects/$PHANTOM_KEY" || fail "could not delete $PHANTOM_KEY from node-3"
docker exec "$PG_CONTAINER" psql -U rsearch -qc \
  "UPDATE object_locations SET created_at = created_at - interval '2 hours' WHERE node_id='node-3'" >/dev/null
start_node node-3 9313
wait_health 9313 || fail "node-3 did not come back"
for i in $(seq 1 60); do
  ROWS=$(psql_t "SELECT count(*) FROM object_locations WHERE node_id='node-3' AND storage_key='$PHANTOM_KEY'")
  [ "$ROWS" = "0" ] && break; pause 1
done
[ "$ROWS" = "0" ] || fail "phantom placement row for $PHANTOM_KEY survived reconcile"
say "phantom row removed; bouncing node-3 so the leader rescans membership"
# The repair scan triggers on live-set changes (or a slow deadline); a
# bounce past the 10s staleness window forces one promptly. The leader
# only notices on a 3s control tick, so stay down long enough that a
# tick is guaranteed to sample node-3 as stale (10s window + one tick
# interval + margin), or the scan waits for the 300s deadline.
kill -9 "${PIDS[node-3]}"
pause 16
start_node node-3 9313
wait_health 9313 || fail "node-3 did not come back from bounce"
# Poll the placement row, not the file: the replicate handler records
# the row only after the file rename, so a file-based break could race
# the under_held check below. Row implies file (file lands first).
for i in $(seq 1 60); do
  ROWS=$(psql_t "SELECT count(*) FROM object_locations WHERE node_id='node-3' AND storage_key='$PHANTOM_KEY'")
  [ "$ROWS" = "1" ] && break; pause 1
done
[ "$ROWS" = "1" ] || fail "repair did not restore the lost copy to node-3"
[ -f "$LOGDIR/node-3/objects/$PHANTOM_KEY" ] || fail "placement row restored but file missing on node-3"
UNDER=$(under_held rlogs 2 "'node-2','node-3'")
[ "$UNDER" = "0" ] || fail "$UNDER splits under-held after phantom repair"
[ "$(count_docs 9313 rlogs)" = "200" ] || fail "docs lost after phantom repair"
say "PASS: phantom placement row dropped and real copy restored"

# ---------- graceful drain ----------
say "TEST: drain node-2 — copies move off, reads keep working, bulk refused"
# A fresh node joins as the drain target (3 live, rf=2). Deliberately NOT
# a restart of node-1: its WAL would replay the acked-but-unconfirmed
# batch (at-least-once) and duplicate rlogs docs, which is a separate
# accepted behavior, not what this test measures.
start_node node-4 9314
wait_health 9314 || fail "node-4 unhealthy"
[ "$(bulk 9313 drain-logs 40 0)" = "false" ] || fail "bulk drain-logs errored"
pause 4
[ "$(count_docs 9314 drain-logs)" = "40" ] || fail "drain-logs not ingested"
curl -s -XPOST http://127.0.0.1:9313/_rsearch/nodes/node-2/drain | jq -e '.acknowledged' >/dev/null \
  || fail "drain request not acknowledged"
for i in $(seq 1 45); do
  LEFT=$(psql_t "SELECT count(*) FROM object_locations WHERE node_id='node-2'")
  [ "$LEFT" = "0" ] && break; pause 1
done
[ "$LEFT" = "0" ] || fail "drain left $LEFT placement rows on node-2"
UNDER=$(under_held all 2 "'node-3','node-4'")
[ "$UNDER" = "0" ] || fail "$UNDER splits under-held on remaining nodes after drain"
[ "$(count_docs 9314 rlogs)" = "200" ] || fail "rlogs lost after drain"
[ "$(count_docs 9313 drain-logs)" = "40" ] || fail "drain-logs lost after drain"
# The flag reaches the node on its next 5s heartbeat; wait for it.
for i in $(seq 1 15); do
  CODE=$(printf '{"index":{"_index":"rlogs"}}\n{"@timestamp":1,"n":1}\n' | \
    curl -s -o /dev/null -w '%{http_code}' -XPOST http://127.0.0.1:9312/_bulk \
      -H 'Content-Type: application/x-ndjson' --data-binary @-)
  [ "$CODE" = "503" ] && break; pause 1
done
[ "$CODE" = "503" ] || fail "draining node accepted bulk (got $CODE)"
# "drain complete" logs on the first tick after the last row clears —
# wait for a tick rather than racing it.
for i in $(seq 1 20); do
  grep -qh "drain complete" "$LOGDIR"/node-*.log && break; pause 1
done
grep -qh "drain complete" "$LOGDIR"/node-*.log || fail "no drain completion logged"
say "PASS: node-2 drained cleanly and refuses new ingest"

# ---------- retention + fan-out delete ----------
say "TEST: retention expiry fans the delete out and clears placement"
[ "$(bulk 9313 rep-ret 40 0)" = "false" ] || fail "bulk rep-ret errored"
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
