#!/usr/bin/env bash
# Create the 3 OpenSearch indices used by the AI Agent retrieval layer.
# Idempotent: skips indices that already exist.
#
# Indices:
#   finch_catalog   — table/column metadata (kNN + BM25)
#   table_docs      — business documentation chunks
#   query_log       — NL question + generated SQL history
#
# Usage:  bash opensearch/init_indices.sh

set -e

OPENSEARCH="${OPENSEARCH_URL:-http://localhost:9200}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAPPINGS_DIR="${SCRIPT_DIR}/mappings"

INDICES=(finch_catalog table_docs query_log)

echo "▶ Waiting for OpenSearch at ${OPENSEARCH}..."
for i in $(seq 1 30); do
  if curl -sf "${OPENSEARCH}/_cluster/health" > /dev/null 2>&1; then
    echo "  ✓ OpenSearch ready"
    break
  fi
  sleep 5
  if [[ $i -eq 30 ]]; then
    echo "  ✗ OpenSearch did not become ready in 150s"
    exit 1
  fi
done

# ── Create each index from its mapping file ───────────────────
for idx in "${INDICES[@]}"; do
  mapping_file="${MAPPINGS_DIR}/${idx}.json"
  if [[ ! -f "${mapping_file}" ]]; then
    echo "  ✗ mapping file missing: ${mapping_file}"
    exit 1
  fi

  if curl -sf "${OPENSEARCH}/${idx}" > /dev/null 2>&1; then
    echo "▶ Index ${idx} already exists — skipping"
    continue
  fi

  echo "▶ Creating index ${idx}"
  status=$(curl -s -o /tmp/os_resp.json -w "%{http_code}" \
    -X PUT "${OPENSEARCH}/${idx}" \
    -H "Content-Type: application/json" \
    --data-binary "@${mapping_file}")

  if [[ "${status}" != "200" ]]; then
    echo "  ✗ create failed (HTTP ${status})"
    cat /tmp/os_resp.json
    exit 1
  fi
  echo "  ✓ ${idx} created"
done

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "▶ Indices summary:"
curl -s "${OPENSEARCH}/_cat/indices/finch_catalog,table_docs,query_log?v&h=index,docs.count,store.size" \
  || echo "  (could not fetch summary)"
echo ""
echo "  Dashboards UI: http://localhost:5601"
echo "  REST API:      ${OPENSEARCH}"
