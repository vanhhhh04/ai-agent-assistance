#!/usr/bin/env bash
# Quick health + indices view for OpenSearch.
#
# Usage: bash cli/opensearch-status.sh

set -e

OPENSEARCH="${OPENSEARCH_URL:-http://localhost:9200}"

# Pick a working Python (Windows Git Bash → `python`, Linux/macOS → `python3`)
if command -v python3 >/dev/null 2>&1 && python3 --version >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1 && python --version >/dev/null 2>&1; then
  PY=python
else
  echo "  ✗ no python interpreter found on PATH"
  exit 1
fi

echo "▶ Cluster health"
curl -s "${OPENSEARCH}/_cluster/health?pretty" \
  | "$PY" -c "import json,sys; d=json.load(sys.stdin); print(f\"  status     : {d['status']}\"); print(f\"  nodes      : {d['number_of_nodes']}\"); print(f\"  shards (a) : {d['active_shards']}\")" \
  2>/dev/null || { echo "  ✗ OpenSearch not reachable at ${OPENSEARCH}"; exit 1; }

echo ""
echo "▶ Finch indices"
curl -s "${OPENSEARCH}/_cat/indices/finch_catalog,table_docs,query_log?v&h=index,health,docs.count,store.size"

echo ""
echo "▶ Sample doc counts"
for idx in finch_catalog table_docs query_log; do
  count=$(curl -s "${OPENSEARCH}/${idx}/_count" 2>/dev/null \
    | "$PY" -c "import json,sys; print(json.load(sys.stdin).get('count', 'n/a'))" 2>/dev/null \
    || echo "missing")
  printf "  %-16s %s\n" "${idx}" "${count}"
done
