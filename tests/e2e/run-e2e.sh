#!/usr/bin/env bash
# End-to-end compatibility test: Vector and Fluent Bit ship logs into
# rsearch unmodified; Grafana's ES datasource queries them.
# Requires: rsearch built, Postgres up (see .env), docker.
set -euo pipefail
cd "$(dirname "$0")/../.."

say() { echo "==> $*"; }
pause() { perl -e "select(undef,undef,undef,$1)"; }

cleanup() {
  docker rm -f e2e-vector e2e-fluentbit e2e-grafana >/dev/null 2>&1 || true
  [ -n "${RSEARCH_PID:-}" ] && kill "$RSEARCH_PID" 2>/dev/null || true
}
trap cleanup EXIT

set -a; source .env; set +a
say "starting rsearch"
pkill -x rsearch 2>/dev/null || true; pause 0.5
rm -rf ./data
RSEARCH_INGEST__MAX_BATCH_SECS=2 ./target/debug/rsearch >/tmp/e2e-rsearch.log 2>&1 &
RSEARCH_PID=$!
for i in $(seq 1 60); do curl -sf http://127.0.0.1:9200/health >/dev/null && break; pause 0.25; done
docker exec rsearch-pg psql -U rsearch -qc "DELETE FROM splits; DELETE FROM streams;" >/dev/null

say "shipping with Vector (15s)"
docker run -d --rm --name e2e-vector --add-host=host.docker.internal:host-gateway \
  -v "$PWD/tests/e2e/vector.yaml:/etc/vector/vector.yaml:ro" \
  timberio/vector:latest-alpine >/dev/null

say "shipping with Fluent Bit (15s)"
docker run -d --rm --name e2e-fluentbit --add-host=host.docker.internal:host-gateway \
  -v "$PWD/tests/e2e/fluent-bit.conf:/fluent-bit/etc/fluent-bit.conf:ro" \
  fluent/fluent-bit:latest >/dev/null

pause 15
docker rm -f e2e-vector e2e-fluentbit >/dev/null

say "waiting for final flush"
pause 4
VECTOR_COUNT=$(curl -s -XPOST http://127.0.0.1:9200/vector-logs/_search \
  -H 'Content-Type: application/json' -d '{"size":0}' | jq -r '.hits.total.value')
FB_COUNT=$(curl -s -XPOST http://127.0.0.1:9200/fb-logs/_search \
  -H 'Content-Type: application/json' -d '{"size":0}' | jq -r '.hits.total.value')
say "vector-logs docs: $VECTOR_COUNT ; fb-logs docs: $FB_COUNT"
[ "$VECTOR_COUNT" -gt 0 ] || { echo "FAIL: Vector shipped nothing"; exit 1; }
[ "$FB_COUNT" -gt 0 ] || { echo "FAIL: Fluent Bit shipped nothing"; exit 1; }

say "starting Grafana"
docker run -d --rm --name e2e-grafana --add-host=host.docker.internal:host-gateway \
  -p 3000:3000 \
  -v "$PWD/tests/e2e/grafana-datasource.yaml:/etc/grafana/provisioning/datasources/rsearch.yaml:ro" \
  grafana/grafana:latest >/dev/null
for i in $(seq 1 120); do
  curl -sf http://admin:admin@127.0.0.1:3000/api/health >/dev/null && break; pause 0.5
done

say "querying rsearch through Grafana's ES datasource"
GRAFANA_RESULT=$(curl -s -XPOST http://admin:admin@127.0.0.1:3000/api/ds/query \
  -H 'Content-Type: application/json' -d '{
    "queries": [{
      "refId": "A",
      "datasource": {"type": "elasticsearch", "uid": "rsearch"},
      "query": "*",
      "metrics": [{"id": "1", "type": "count"}],
      "bucketAggs": [{"id": "2", "type": "date_histogram", "field": "@timestamp",
                      "settings": {"interval": "1m"}}],
      "timeField": "@timestamp"
    }],
    "from": "now-1h", "to": "now"
  }')
GRAFANA_TOTAL=$(echo "$GRAFANA_RESULT" \
  | jq '[.results.A.frames[].data.values[1][]?] | add // 0')
say "grafana date_histogram total count: $GRAFANA_TOTAL"
if [ "${GRAFANA_TOTAL:-0}" -le 0 ]; then
  echo "FAIL: Grafana query returned no data"
  echo "$GRAFANA_RESULT" | head -c 2000
  exit 1
fi

say "E2E OK: vector=$VECTOR_COUNT fluentbit=$FB_COUNT grafana_count=$GRAFANA_TOTAL"
