#!/usr/bin/env bash
# Multi-node cluster test: 2 ingest + 2 search + 2 control nodes (one
# binary, role flags), shared Postgres metastore + MinIO object storage.
# Proves: cross-node visibility, searcher death, ingest death + WAL
# replay, control-leader failover, merge, retention, GC.
set -euo pipefail
cd "$(dirname "$0")/../.."

BIN=./target/release/rsearch
LOGDIR=/tmp/rsearch-cluster
say() { echo "==> $*"; }
pause() { perl -e "select(undef,undef,undef,$1)"; }
fail() { echo "FAIL: $*"; exit 1; }

declare -A PIDS
cleanup() {
  for pid in "${PIDS[@]}"; do kill -9 "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT

start_node() { # name port roles [extra env...]
  local name=$1 port=$2 roles=$3; shift 3
  env "$@" \
    DATABASE_URL="$DATABASE_URL" \
    RSEARCH_NODE__ID="$name" \
    RSEARCH_NODE__DATA_DIR="$LOGDIR/$name" \
    RSEARCH_HTTP__BIND_ADDR="127.0.0.1:$port" \
    RSEARCH_STORAGE__BACKEND=s3 \
    RSEARCH_STORAGE__BUCKET=rsearch-cluster \
    RSEARCH_STORAGE__ENDPOINT=http://127.0.0.1:9000 \
    RSEARCH_STORAGE__FORCE_PATH_STYLE=true \
    RSEARCH_STORAGE__ACCESS_KEY_ID=minioadmin \
    RSEARCH_STORAGE__SECRET_ACCESS_KEY=minioadmin \
    RSEARCH_INGEST__MAX_BATCH_SECS=1 \
    RSEARCH_CONTROL__INTERVAL_SECS=3 \
    RSEARCH_CONTROL__GC_GRACE_SECS=5 \
    "$BIN" --roles "$roles" >"$LOGDIR/$name.log" 2>&1 &
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
      printf '{"index":{"_index":"%s"}}\n{"@timestamp":%d,"service":"svc-%d","message":"cluster doc %d","n":%d}\n' \
        "$index" "$(( $(date +%s%3N) ))" $((i % 5)) $((offset + i)) $((offset + i))
    done
  } | curl -s -XPOST "http://127.0.0.1:$port/_bulk" \
        -H 'Content-Type: application/x-ndjson' --data-binary @- | jq -r '.errors'
}

count_docs() { # port index
  curl -s -XPOST "http://127.0.0.1:$1/$2/_search" \
    -H 'Content-Type: application/json' -d '{"size":0}' | jq -r '.hits.total.value // 0'
}

published_splits() { # index
  docker exec rsearch-pg psql -U rsearch -tAc \
    "SELECT count(*) FROM splits s JOIN streams st ON st.id=s.stream_id
     WHERE st.name='$1' AND s.state='published'"
}

# ---------- setup ----------
set -a; source .env; set +a
rm -rf "$LOGDIR"; mkdir -p "$LOGDIR"
pkill -x rsearch 2>/dev/null || true; pause 0.5
docker exec rsearch-pg psql -U rsearch -qc "DELETE FROM splits; DELETE FROM streams; DELETE FROM nodes;" >/dev/null
docker run --rm --network host --entrypoint sh minio/mc:latest -c \
  "mc alias set m http://127.0.0.1:9000 minioadmin minioadmin >/dev/null && mc rb --force m/rsearch-cluster >/dev/null 2>&1; mc mb -p m/rsearch-cluster >/dev/null" \
  || fail "minio bucket setup"

say "starting 6 nodes"
start_node ingest-1 9211 ingest
start_node ingest-2 9212 ingest
start_node search-1 9221 search
start_node search-2 9222 search
start_node control-1 9231 control
start_node control-2 9232 control
for port in 9211 9212 9221 9222 9231 9232; do
  wait_health $port || fail "node on :$port did not become healthy"
done
say "all nodes healthy"

# ---------- cross-node visibility ----------
say "TEST: docs ingested on both ingest nodes visible from both search nodes"
[ "$(bulk 9211 clogs 100 0)" = "false" ] || fail "bulk to ingest-1 errored"
[ "$(bulk 9212 clogs 100 100)" = "false" ] || fail "bulk to ingest-2 errored"
pause 6
C1=$(count_docs 9221 clogs); C2=$(count_docs 9222 clogs)
[ "$C1" = "200" ] || fail "search-1 sees $C1/200"
[ "$C2" = "200" ] || fail "search-2 sees $C2/200"
say "PASS: both searchers see all 200 docs"

# ---------- searcher death ----------
say "TEST: searcher death"
kill -9 "${PIDS[search-1]}"; unset 'PIDS[search-1]'
pause 1
[ "$(count_docs 9222 clogs)" = "200" ] || fail "search-2 broken after search-1 death"
say "PASS: surviving searcher unaffected"

# ---------- ingest death + WAL replay ----------
say "TEST: ingest node killed with unflushed docs; WAL replays on restart"
# Long batch window so docs sit in memory + WAL when we kill.
env RSEARCH_INGEST__MAX_BATCH_SECS=60 \
  DATABASE_URL="$DATABASE_URL" \
  RSEARCH_NODE__ID=ingest-3 RSEARCH_NODE__DATA_DIR="$LOGDIR/ingest-3" \
  RSEARCH_HTTP__BIND_ADDR=127.0.0.1:9213 \
  RSEARCH_STORAGE__BACKEND=s3 RSEARCH_STORAGE__BUCKET=rsearch-cluster \
  RSEARCH_STORAGE__ENDPOINT=http://127.0.0.1:9000 \
  RSEARCH_STORAGE__FORCE_PATH_STYLE=true \
  RSEARCH_STORAGE__ACCESS_KEY_ID=minioadmin \
  RSEARCH_STORAGE__SECRET_ACCESS_KEY=minioadmin \
  "$BIN" --roles ingest >"$LOGDIR/ingest-3.log" 2>&1 &
PIDS[ingest-3]=$!
wait_health 9213 || fail "ingest-3 unhealthy"
[ "$(bulk 9213 clogs 50 200)" = "false" ] || fail "bulk to ingest-3 errored"
kill -9 "${PIDS[ingest-3]}"; unset 'PIDS[ingest-3]'   # acked but unflushed
pause 1
[ "$(count_docs 9222 clogs)" = "200" ] || fail "docs flushed before kill — test invalid"
# Restart with a short window: WAL replay must recover the 50 docs.
start_node ingest-3 9213 ingest
wait_health 9213 || fail "ingest-3 restart unhealthy"
for i in $(seq 1 30); do
  [ "$(count_docs 9222 clogs)" = "250" ] && break; pause 1
done
[ "$(count_docs 9222 clogs)" = "250" ] || fail "WAL replay did not recover docs ($(count_docs 9222 clogs)/250)"
grep -q "replayed WAL records" "$LOGDIR/ingest-3.log" || fail "no WAL replay logged"
say "PASS: 50 acked-but-unflushed docs recovered via WAL replay"

# ---------- leader failover ----------
say "TEST: control leader failover"
pause 2
LEADER=""
grep -q "acquired control leadership" "$LOGDIR/control-1.log" && LEADER=control-1
grep -q "acquired control leadership" "$LOGDIR/control-2.log" && LEADER=${LEADER:-control-2}
[ -n "$LEADER" ] || fail "no control leader elected"
FOLLOWER=$([ "$LEADER" = control-1 ] && echo control-2 || echo control-1)
say "leader is $LEADER; killing it"
kill -9 "${PIDS[$LEADER]}"; unset "PIDS[$LEADER]"
for i in $(seq 1 30); do
  grep -q "acquired control leadership" "$LOGDIR/$FOLLOWER.log" && break; pause 1
done
grep -q "acquired control leadership" "$LOGDIR/$FOLLOWER.log" || fail "follower never took leadership"
say "PASS: $FOLLOWER took over leadership"

# ---------- merge + GC ----------
say "TEST: merge combines small splits; GC removes the old ones"
# Fresh stream, several separate 1s-window batches => several small splits.
for batch in 0 1 2 3; do
  [ "$(bulk 9211 mlogs 25 $((batch * 25)))" = "false" ] || fail "bulk mlogs errored"
  pause 1.6
done
# Merges run every 3s, so splits may collapse before we can observe the
# intermediate count — assert on convergence + the leader's merge log.
for i in $(seq 1 45); do
  AFTER=$(published_splits mlogs)
  [ "$AFTER" = "1" ] && [ "$(count_docs 9222 mlogs)" = "100" ] && break
  pause 1
done
AFTER=$(published_splits mlogs)
[ "$AFTER" = "1" ] || fail "splits did not converge to 1 (got $AFTER)"
[ "$(count_docs 9222 mlogs)" = "100" ] || fail "doc count wrong after merge ($(count_docs 9222 mlogs)/100)"
grep -h "merge complete" "$LOGDIR"/control-*.log | head -3
grep -qh "merge complete" "$LOGDIR"/control-*.log || fail "no merge logged by any controller"
say "PASS: 4 small splits merged to 1 with all 100 docs intact"
for i in $(seq 1 30); do
  MARKED=$(docker exec rsearch-pg psql -U rsearch -tAc "SELECT count(*) FROM splits WHERE state='marked_for_delete'")
  [ "$MARKED" = "0" ] && break; pause 1
done
[ "$MARKED" = "0" ] || fail "GC did not clean marked splits"
say "PASS: GC removed merged-away splits"

# ---------- retention ----------
say "TEST: retention expires an entire stream"
[ "$(bulk 9211 ret-logs 40 0)" = "false" ] || fail "bulk ret-logs errored"
pause 4
[ "$(count_docs 9222 ret-logs)" = "40" ] || fail "ret-logs not ingested"
docker exec rsearch-pg psql -U rsearch -qc "UPDATE streams SET retention_hours=0 WHERE name='ret-logs'"
for i in $(seq 1 30); do
  [ "$(count_docs 9222 ret-logs)" = "0" ] && break; pause 1
done
[ "$(count_docs 9222 ret-logs)" = "0" ] || fail "retention did not expire docs"
say "PASS: retention_hours=0 emptied the stream"

# ---------- cluster health ----------
NODES=$(curl -s http://127.0.0.1:9222/_cat/nodes | jq 'length')
say "registered nodes: $NODES"

echo
echo "CLUSTER TEST OK"
