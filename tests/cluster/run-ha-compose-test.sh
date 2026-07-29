#!/usr/bin/env bash
# HA test against the containerized 3-node replicated topology
# (docker-compose-replicated.yml): each node's volume stands in for its
# block device. Proves, at the container level: quorum placement, reads
# from every node, data-node death with search continuity, leader-driven
# repair, node rejoin with its original volume (block-device reattach),
# and that a rejoined node takes new replica writes.
#
# Slow first run: the image build compiles the FIPS module from source.
set -euo pipefail
cd "$(dirname "$0")/../.."

COMPOSE=(docker compose -f docker-compose-replicated.yml -f tests/cluster/ha-test-overrides.yml -p rsearch-ha)
say() { echo "==> $*"; }
pause() { perl -e "select(undef,undef,undef,$1)"; }
fail() { echo "FAIL: $*"; "${COMPOSE[@]}" logs --tail 20 node-1 node-2 node-3 2>/dev/null | tail -40; exit 1; }

psql_t() { "${COMPOSE[@]}" exec -T postgres psql -U rsearch -tAc "$1"; }

wait_health() { # port
  for i in $(seq 1 120); do
    curl -sf "http://127.0.0.1:$1/health" >/dev/null && return 0
    pause 0.5
  done
  return 1
}

bulk() { # port index count offset
  local port=$1 index=$2 count=$3 offset=$4
  { for i in $(seq 1 "$count"); do
      printf '{"index":{"_index":"%s"}}\n{"@timestamp":%d,"service":"svc-%d","message":"ha doc %d","n":%d}\n' \
        "$index" "$(( $(date +%s%3N) ))" $((i % 5)) $((offset + i)) $((offset + i))
    done
  } | curl -s -XPOST "http://127.0.0.1:$port/_bulk" \
        -H 'Content-Type: application/x-ndjson' --data-binary @- | jq -r '.errors'
}

count_docs() { # port index
  curl -s -XPOST "http://127.0.0.1:$1/$2/_search" \
    -H 'Content-Type: application/json' -d '{"size":0}' | jq -r '.hits.total.value // 0'
}

# Published splits with fewer than $1 copies among node ids $2 (SQL list).
under_held() {
  psql_t "SELECT count(*) FROM (
            SELECT s.storage_key
            FROM splits s
            LEFT JOIN object_locations ol
              ON ol.storage_key = s.storage_key AND ol.node_id IN ($2)
            WHERE s.state = 'published'
            GROUP BY s.storage_key
            HAVING count(ol.node_id) < $1
          ) u"
}

# ---------- setup: fresh volumes, build, start ----------
say "starting fresh 3-node HA stack (first run builds the image — slow)"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --build --quiet-pull 2>&1 | tail -3
for port in 9201 9202 9203; do
  wait_health $port || fail "node on :$port did not become healthy"
done
say "all nodes healthy"

# ---------- quorum writes + reads everywhere ----------
say "TEST: ingest on node-1 and node-3; every node serves all docs"
[ "$(bulk 9201 halogs 120 0)" = "false" ] || fail "bulk to node-1 errored"
[ "$(bulk 9203 halogs 80 120)" = "false" ] || fail "bulk to node-3 errored"
pause 6
for port in 9201 9202 9203; do
  C=$(count_docs $port halogs)
  [ "$C" = "200" ] || fail "search on :$port sees $C/200"
done
say "PASS: all three nodes serve all 200 docs"

say "TEST: every published split has 2 recorded copies"
UNDER=$(under_held 2 "'node-1','node-2','node-3'")
[ "$UNDER" = "0" ] || fail "$UNDER published splits below replication factor"
say "PASS: placement at factor 2"

# ---------- data-node death ----------
say "TEST: hard-stop node-2 (block device stays); survivors keep serving"
"${COMPOSE[@]}" stop -t 0 node-2 >/dev/null 2>&1
pause 1
for port in 9201 9203; do
  C=$(count_docs $port halogs)
  [ "$C" = "200" ] || fail "search on :$port sees $C/200 after node-2 death"
done
say "PASS: search continuity through data-node death"

# ---------- repair (proves leadership survived too) ----------
say "TEST: repair restores factor 2 on survivors"
for i in $(seq 1 60); do
  UNDER=$(under_held 2 "'node-1','node-3'")
  [ "$UNDER" = "0" ] && break; pause 1
done
[ "$UNDER" = "0" ] || fail "repair left $UNDER splits under-held"
"${COMPOSE[@]}" logs node-1 node-3 2>/dev/null | grep -q "repair: copy restored" \
  || fail "no repair logged by a survivor"
# Repair is a leader-only job: if node-2 held the advisory lock, its death
# released it and a survivor won it — repair completing proves failover.
say "PASS: repair done (control leadership live on survivors)"

# ---------- rejoin with original volume ----------
say "TEST: node-2 rejoins with its original volume and serves reads"
"${COMPOSE[@]}" start node-2 >/dev/null 2>&1
wait_health 9202 || fail "node-2 did not come back"
for i in $(seq 1 30); do
  C=$(count_docs 9202 halogs)
  [ "$C" = "200" ] && break; pause 1
done
[ "$C" = "200" ] || fail "rejoined node-2 sees $C/200"
LIVE=$(curl -s http://127.0.0.1:9201/_cat/nodes | jq '[.[] | select(.live)] | length')
[ "$LIVE" = "3" ] || fail "expected 3 live nodes, got $LIVE"
say "PASS: node-2 rejoined with its data intact"

# ---------- rejoined node takes new writes ----------
say "TEST: new docs ingested on rejoined node-2 replicate and serve everywhere"
[ "$(bulk 9202 halogs 60 200)" = "false" ] || fail "bulk to node-2 errored"
pause 6
for port in 9201 9202 9203; do
  C=$(count_docs $port halogs)
  [ "$C" = "260" ] || fail "search on :$port sees $C/260 after rejoin writes"
done
UNDER=$(under_held 2 "'node-1','node-2','node-3'")
[ "$UNDER" = "0" ] || fail "$UNDER splits below factor after rejoin writes"
say "PASS: rejoined node ingests, replicates, and serves"

say "stack left running (rsearch-ha project); tear down with:"
say "  docker compose -p rsearch-ha -f docker-compose-replicated.yml down -v"
echo
echo "HA COMPOSE TEST OK"
