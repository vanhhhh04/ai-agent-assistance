#!/usr/bin/env bash
# Bring up the AI Agent stack (DataFinch).
# Idempotent — safe to re-run.
#
#   1. Sanity-check ANTHROPIC_API_KEY (warn if missing — agent still boots,
#      Supervisor + SQL Writer will fail at runtime)
#   2. Start opensearch + opensearch-dashboards, wait for healthy
#   3. Create the 3 OpenSearch indices (finch_catalog, table_docs, query_log)
#   4. Start ai-agent service (reuses existing image), wait for /api/health/ping
#   5. Best-effort: index the Hive Gold catalog into OpenSearch so the
#      semantic layer can retrieve context for ALL tables. This step is
#      skipped if Gold doesn't have tables yet (run the Airflow medallion
#      DAG first).
#
# Usage:
#   bash cli/ai-agent-up.sh            # reuse existing image (fast)
#   bash cli/ai-agent-up.sh --build    # force rebuild (when requirements.txt changed)

cd "$(dirname "$0")/.."

# Parse args — only --build is meaningful right now.
BUILD_FLAG=""
for arg in "$@"; do
  case "$arg" in
    --build) BUILD_FLAG="--build" ;;
    *)       echo "unknown arg: $arg" ; exit 1 ;;
  esac
done

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'
step() { echo ""; echo "${B}▶ $1${N}"; }
ok()   { echo "  ${G}✓${N} $1"; }
warn() { echo "  ${Y}!${N} $1"; }
err()  { echo "  ${R}✗${N} $1"; }

# ── 1. LLM provider key check ────────────────────────────────
step "1. LLM provider check"
PROVIDER=""
KEY_VAR=""
KEY_VAL=""
if [[ -f .env ]]; then
  PROVIDER=$(grep -E "^LLM_PROVIDER=" .env | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'")
  PROVIDER=${PROVIDER:-anthropic}
  case "${PROVIDER}" in
    gemini)    KEY_VAR=GEMINI_API_KEY ;;
    *)         KEY_VAR=ANTHROPIC_API_KEY; PROVIDER=anthropic ;;
  esac
  KEY_VAL=$(grep -E "^${KEY_VAR}=" .env | head -1 | cut -d= -f2- | tr -d '"' | tr -d "'")
fi
echo "  active provider: ${PROVIDER}"
if [[ -z "${KEY_VAL}" || "${KEY_VAL}" == "sk-ant-your-key-here" ]]; then
  warn "${KEY_VAR} is empty in .env — Supervisor + SQL Writer agents will fail."
  warn "Edit .env and add: ${KEY_VAR}=..."
  warn "Container WILL start, but /api/query/ask returns errors until the key is set."
else
  ok "${KEY_VAR} present (${KEY_VAL:0:10}…)"
fi

# ── 2. OpenSearch ─────────────────────────────────────────────
step "2. Start OpenSearch + Dashboards"
docker compose up -d opensearch opensearch-dashboards
for i in $(seq 1 30); do
  st=$(docker inspect --format='{{.State.Health.Status}}' opensearch 2>/dev/null || echo "starting")
  if [[ "${st}" == "healthy" ]]; then
    ok "opensearch healthy"
    break
  fi
  sleep 5
  if [[ $i -eq 30 ]]; then
    err "opensearch did not become healthy in 150s — check 'docker logs opensearch'"
    exit 1
  fi
done

step "3. Create OpenSearch indices (idempotent)"
bash opensearch/init_indices.sh

# ── 4. ai-agent service ──────────────────────────────────────
if [[ -n "$BUILD_FLAG" ]]; then
  step "4. Build + start ai-agent service (--build)"
else
  step "4. Start ai-agent service (reusing existing image — pass --build to rebuild)"
fi
# Source code is bind-mounted in compose, so code changes do NOT require
# rebuilding the image. Only pass --build when requirements.txt or Dockerfile
# itself changed. If the image doesn't exist yet, compose builds it once.
docker compose up -d $BUILD_FLAG ai-agent

step "5. Wait for ai-agent health"
# First boot can take 30-60s while sentence-transformers warms up + schema cache loads.
for i in $(seq 1 60); do
  if curl -sf http://localhost:8000/api/health/ping > /dev/null 2>&1; then
    ok "ai-agent is responding"
    break
  fi
  sleep 5
  if [[ $i -eq 60 ]]; then
    err "ai-agent did not become healthy in 300s — check 'docker logs ai-agent'"
    exit 1
  fi
done

# ── 6. Verify schema visibility ──────────────────────────────
step "6. Verify ai-agent can see Hive Gold tables"
# Pull schema count from the ai-agent's own /api/health endpoint — that's
# the source of truth for "how many tables can the agent currently query".
HEALTH_JSON=$(curl -sf http://localhost:8000/api/health 2>/dev/null || echo "{}")
if command -v python3 >/dev/null 2>&1; then PY=python3
elif [[ -x venv/Scripts/python.exe ]]; then PY=venv/Scripts/python.exe
else PY=python; fi

HIVE_TABLES=$(echo "$HEALTH_JSON" | $PY -c "import json,sys;d=json.load(sys.stdin);print(d.get('components',{}).get('schema_cache',{}).get('hive_tables',0))" 2>/dev/null || echo "0")
PG_TABLES=$(echo "$HEALTH_JSON" | $PY -c "import json,sys;d=json.load(sys.stdin);print(d.get('components',{}).get('schema_cache',{}).get('postgres_tables',0))" 2>/dev/null || echo "0")
HIVE_OK=$(echo "$HEALTH_JSON" | $PY -c "import json,sys;d=json.load(sys.stdin);print(d.get('components',{}).get('hive_gold',{}).get('reachable',False))" 2>/dev/null || echo "False")

echo "  hive_gold reachable:    ${HIVE_OK}"
echo "  hive_gold tables seen:  ${HIVE_TABLES}"
echo "  postgres_bronze tables: ${PG_TABLES}"

if [[ "${HIVE_TABLES}" == "0" ]]; then
  warn "Hive Gold has 0 tables — run the medallion ETL pipeline first:"
  warn "  Airflow UI → http://localhost:8080  → trigger 'medallion_pipeline'"
  warn "  AI agent will return empty schema until Gold tables exist."
fi

# ── 7. Best-effort catalog indexing ──────────────────────────
step "7. Index Hive Gold catalog into OpenSearch (best-effort)"
if [[ "${HIVE_TABLES}" -gt 0 ]]; then
  # Run the indexer from inside the ai-agent container so opensearch-py + pyhive
  # are guaranteed available, and HIVE_HOST/OPENSEARCH_URL resolve to the
  # compose network names (hiveserver2, opensearch).
  if docker exec ai-agent bash -c "cd /app && HIVE_HOST=hiveserver2 OPENSEARCH_URL=http://opensearch:9200 python -m opensearch.indexers.catalog_indexer" 2>&1 | tail -10; then
    ok "catalog indexed — semantic layer now sees all Gold tables"
  else
    warn "catalog indexer failed — semantic layer will fall back to schema_cache"
  fi

  # Markdown business docs (a few tables documented in opensearch/docs/*.md)
  docker exec ai-agent bash -c "cd /app && OPENSEARCH_URL=http://opensearch:9200 python -m opensearch.indexers.docs_indexer" 2>&1 | tail -5 \
    && ok "business docs indexed" || warn "docs indexer skipped"
else
  warn "skip (no Hive Gold tables)"
fi

# ── 8. Summary ───────────────────────────────────────────────
echo ""
echo "${B}▶ AI Agent ready${N}"
echo "  API:        http://localhost:8000        (POST /api/query/ask)"
echo "  Docs:       http://localhost:8000/docs   (FastAPI Swagger)"
echo "  Schema:     http://localhost:8000/api/schema/full?backend=hive_gold"
echo "  Health:     http://localhost:8000/api/health"
echo "  Dashboards: http://localhost:5601        (OpenSearch UI)"
