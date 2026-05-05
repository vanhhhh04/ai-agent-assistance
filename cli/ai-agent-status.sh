#!/usr/bin/env bash
# Quick health snapshot for the AI Agent stack.
# Run this any time you want to verify "can the agent query everything?".

cd "$(dirname "$0")/.."

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'

if command -v python3 >/dev/null 2>&1; then PY=python3
elif [[ -x venv/Scripts/python.exe ]]; then PY=venv/Scripts/python.exe
else PY=python; fi

echo "${B}=== AI AGENT STATUS ===${N}"
HEALTH=$(curl -sf http://localhost:8000/api/health 2>/dev/null)
if [[ -z "${HEALTH}" ]]; then
  echo "  ${R}✗ ai-agent not responding on :8000${N}"
  echo "    docker logs ai-agent --tail 50"
  exit 1
fi
echo "${HEALTH}" | "$PY" -m json.tool 2>/dev/null | sed 's/^/  /'

echo ""
echo "${B}=== HIVE GOLD TABLES (what the agent CAN query) ===${N}"
SCHEMA=$(curl -sf "http://localhost:8000/api/schema/full?backend=hive_gold" 2>/dev/null)
if [[ -z "${SCHEMA}" ]]; then
  echo "  ${R}schema endpoint not responding${N}"
else
  echo "${SCHEMA}" | "$PY" -c "
import json, sys
d = json.load(sys.stdin)
schema = d.get('schema', {})
print(f'  total tables: {len(schema)}')
for t, cols in sorted(schema.items()):
    fact_or_dim = '◆ FACT' if t.startswith('fact') else '◇ DIM ' if t.startswith('dim') else '·     '
    print(f'    {fact_or_dim} {t:25s} ({len(cols)} cols)')
" 2>/dev/null || echo "  ${R}(parse failed)${N}"
fi

echo ""
echo "${B}=== POSTGRES BRONZE TABLES ===${N}"
PG_SCHEMA=$(curl -sf "http://localhost:8000/api/schema/full?backend=postgres_bronze" 2>/dev/null)
if [[ -z "${PG_SCHEMA}" ]]; then
  echo "  (postgres backend not responding)"
else
  echo "${PG_SCHEMA}" | "$PY" -c "
import json, sys
d = json.load(sys.stdin)
schema = d.get('schema', {})
print(f'  total tables: {len(schema)}')
for t, cols in sorted(schema.items()):
    print(f'    · {t:25s} ({len(cols)} cols)')
" 2>/dev/null || echo "  ${R}(parse failed)${N}"
fi

echo ""
echo "${B}=== OPENSEARCH SEMANTIC LAYER ===${N}"
for idx in finch_catalog table_docs query_log; do
  count=$(curl -sf "http://localhost:9200/${idx}/_count" 2>/dev/null | "$PY" -c "import json,sys;print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "?")
  printf "  %-20s docs=%s\n" "${idx}" "${count}"
done

echo ""
echo "${B}▶ Try a query:${N}"
echo '  curl -N -X POST http://localhost:8000/api/query/ask \'
echo '    -H "Content-Type: application/json" \'
echo "    -d '{\"question\":\"Top 5 brands theo doanh thu\"}'"
