#!/usr/bin/env bash
# Side-by-side benchmark: rsearch (release) vs OpenSearch container.
# Measures ingest CPU/RSS at target rates, query latencies, disk usage.
# Usage: scripts/bench.sh [rate1 rate2 ...]   (default: 5000 10000)
set -euo pipefail
cd "$(dirname "$0")/.."

RATES=("${@:-5000 10000}")
[ $# -eq 0 ] && RATES=(5000 10000)
DURATION=60
QUERY_ITERS=100
BENCH=./target/release/rsearch-bench
OS_HEAP=2g

say() { echo "==> $*" >&2; }
pause() { perl -e "select(undef,undef,undef,$1)"; }

# Sample RSS (KB) and CPU jiffies of a PID; prints "peak_rss_mb avg_cpu_pct".
sample_process() {
  local pid=$1 duration=$2 hz peak_rss=0 t0 t1 c0 c1
  hz=$(getconf CLK_TCK)
  c0=$(awk '{print $14+$15}' /proc/$pid/stat)
  t0=$(date +%s.%N)
  local end=$(echo "$t0 + $duration" | bc)
  while (( $(echo "$(date +%s.%N) < $end" | bc) )); do
    local rss
    rss=$(awk '/VmRSS/{print $2}' /proc/$pid/status 2>/dev/null || echo 0)
    [ "$rss" -gt "$peak_rss" ] && peak_rss=$rss
    pause 1
  done
  c1=$(awk '{print $14+$15}' /proc/$pid/stat 2>/dev/null || echo "$c0")
  t1=$(date +%s.%N)
  local cpu_pct
  cpu_pct=$(echo "scale=1; ($c1 - $c0) * 100 / $hz / ($t1 - $t0)" | bc)
  echo "$((peak_rss / 1024)) $cpu_pct"
}

# Sample a docker container; prints "peak_mem_mb avg_cpu_pct" (docker stats).
sample_container() {
  local name=$1 duration=$2 samples=0 cpu_sum=0 peak_mem=0
  local end=$(( $(date +%s) + duration ))
  while [ "$(date +%s)" -lt "$end" ]; do
    local line cpu mem
    line=$(docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' "$name" 2>/dev/null || echo "")
    [ -z "$line" ] && continue
    cpu=$(echo "$line" | awk '{gsub("%","",$1); print $1}')
    mem=$(echo "$line" | awk '{print $2}' | sed 's/MiB//; s/GiB/*1024/' | bc | cut -d. -f1)
    cpu_sum=$(echo "$cpu_sum + $cpu" | bc)
    samples=$((samples + 1))
    [ "${mem:-0}" -gt "$peak_mem" ] && peak_mem=$mem
  done
  [ "$samples" -eq 0 ] && { echo "0 0"; return; }
  echo "$peak_mem $(echo "scale=1; $cpu_sum / $samples" | bc)"
}

MAPPING='{"mappings":{"properties":{"service":{"type":"keyword"},"host":{"type":"keyword"},"level":{"type":"keyword"},"status":{"type":"long"},"latency_ms":{"type":"double"},"path":{"type":"keyword"},"trace_id":{"type":"keyword"},"seq":{"type":"long"},"message":{"type":"text"},"region":{"type":"keyword"}}}}'

results=()

#####################################
say "===== rsearch (release) ====="
#####################################
set -a; source .env; set +a
pkill -x rsearch 2>/dev/null || true; pause 0.5
rm -rf ./bench-data
docker exec rsearch-pg psql -U rsearch -qc "DELETE FROM splits; DELETE FROM streams;" >/dev/null
RSEARCH_NODE__DATA_DIR=./bench-data RSEARCH_STORAGE__ROOT=./bench-data/storage \
  ./target/release/rsearch >/tmp/bench-rsearch.log 2>&1 &
RS_PID=$!
for i in $(seq 1 60); do curl -sf http://127.0.0.1:9200/health >/dev/null && break; pause 0.25; done

say "rsearch idle baseline (10s)"
IDLE=$(sample_process $RS_PID 10)
results+=("rsearch idle: rss_mb=$(echo "$IDLE" | cut -d' ' -f1) cpu_pct=$(echo "$IDLE" | cut -d' ' -f2)")

curl -s -XPUT http://127.0.0.1:9200/bench-logs -H 'Content-Type: application/json' -d "$MAPPING" >/dev/null

for RATE in "${RATES[@]}"; do
  say "rsearch ingest @${RATE}/s for ${DURATION}s"
  $BENCH ingest --rate "$RATE" --duration-secs "$DURATION" > /tmp/bench-rs-ingest-$RATE.json &
  BENCH_PID=$!
  STATS=$(sample_process $RS_PID "$DURATION")
  wait $BENCH_PID
  INGEST=$(cat /tmp/bench-rs-ingest-$RATE.json)
  results+=("rsearch ingest@$RATE: $(echo "$INGEST" | jq -c '{achieved_rate, item_errors, request_errors}') peak_rss_mb=$(echo "$STATS" | cut -d' ' -f1) avg_cpu_pct=$(echo "$STATS" | cut -d' ' -f2)")
done

say "waiting for final flush"; pause 35
say "rsearch query bench"
QUERY=$($BENCH query --iterations $QUERY_ITERS)
results+=("rsearch queries: $(echo "$QUERY" | jq -c '.results')")
DISK=$(du -sm ./bench-data/storage 2>/dev/null | cut -f1)
results+=("rsearch disk_mb=$DISK")
kill $RS_PID 2>/dev/null || true

#####################################
say "===== OpenSearch ====="
#####################################
docker rm -f bench-os >/dev/null 2>&1 || true
docker run -d --name bench-os -p 9201:9200 \
  -e discovery.type=single-node \
  -e DISABLE_SECURITY_PLUGIN=true \
  -e DISABLE_INSTALL_DEMO_CONFIG=true \
  -e "OPENSEARCH_JAVA_OPTS=-Xms$OS_HEAP -Xmx$OS_HEAP" \
  opensearchproject/opensearch:2 >/dev/null
say "waiting for OpenSearch to come up"
for i in $(seq 1 240); do curl -sf http://127.0.0.1:9201/ >/dev/null && break; pause 1; done
curl -sf http://127.0.0.1:9201/ >/dev/null || { echo "OpenSearch failed to start"; exit 1; }

say "opensearch idle baseline (10s)"
IDLE=$(sample_container bench-os 10)
results+=("opensearch idle: mem_mb=$(echo "$IDLE" | cut -d' ' -f1) cpu_pct=$(echo "$IDLE" | cut -d' ' -f2)")

curl -s -XPUT http://127.0.0.1:9201/bench-logs -H 'Content-Type: application/json' -d "$MAPPING" >/dev/null

for RATE in "${RATES[@]}"; do
  say "opensearch ingest @${RATE}/s for ${DURATION}s"
  $BENCH ingest --endpoint http://127.0.0.1:9201 --rate "$RATE" --duration-secs "$DURATION" > /tmp/bench-os-ingest-$RATE.json &
  BENCH_PID=$!
  STATS=$(sample_container bench-os "$DURATION")
  wait $BENCH_PID
  INGEST=$(cat /tmp/bench-os-ingest-$RATE.json)
  results+=("opensearch ingest@$RATE: $(echo "$INGEST" | jq -c '{achieved_rate, item_errors, request_errors}') peak_mem_mb=$(echo "$STATS" | cut -d' ' -f1) avg_cpu_pct=$(echo "$STATS" | cut -d' ' -f2)")
done

say "refresh + query bench"
curl -s -XPOST http://127.0.0.1:9201/bench-logs/_refresh >/dev/null; pause 5
QUERY=$($BENCH query --endpoint http://127.0.0.1:9201 --iterations $QUERY_ITERS)
results+=("opensearch queries: $(echo "$QUERY" | jq -c '.results')")
DISK=$(curl -s http://127.0.0.1:9201/_stats/store | jq -r '._all.total.store.size_in_bytes / 1048576 | floor')
results+=("opensearch disk_mb=$DISK")
docker rm -f bench-os >/dev/null

echo
echo "================ RESULTS ================"
printf '%s\n' "${results[@]}"
