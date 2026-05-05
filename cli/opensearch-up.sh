#!/usr/bin/env bash
# Bring up OpenSearch + Dashboards and create the 3 indices.
# Idempotent — safe to re-run.
#
# Usage: bash cli/opensearch-up.sh

set -e
cd "$(dirname "$0")/.."

echo "▶ Starting OpenSearch + Dashboards containers..."
docker compose up -d opensearch opensearch-dashboards

echo ""
echo "▶ Waiting for OpenSearch to become healthy..."
for i in $(seq 1 30); do
  status=$(docker inspect --format='{{.State.Health.Status}}' opensearch 2>/dev/null || echo "starting")
  if [[ "${status}" == "healthy" ]]; then
    echo "  ✓ opensearch healthy"
    break
  fi
  sleep 5
  if [[ $i -eq 30 ]]; then
    echo "  ✗ opensearch did not become healthy in 150s — check 'docker logs opensearch'"
    exit 1
  fi
done

echo ""
bash opensearch/init_indices.sh

echo ""
echo "▶ Dashboards UI: http://localhost:5601"
echo "▶ REST API:      http://localhost:9200"
