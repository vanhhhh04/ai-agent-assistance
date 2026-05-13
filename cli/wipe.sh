#!/usr/bin/env bash
# Nuclear reset — wipes EVERYTHING and brings the platform back to a clean slate.
#
# Difference from cli/startup.sh:
#   startup.sh = soft restart (Postgres + HDFS + NiFi flow are PRESERVED)
#   wipe.sh    = hard reset   (everything destroyed, including Postgres data,
#                              HDFS data, NiFi flow, Hive Metastore, Airflow logs)
#
# Use this when you want a true from-scratch build (e.g. after schema changes,
# or to demo the platform setup from zero).
#
# Usage:  bash cli/wipe.sh

set -e
cd "$(dirname "$0")/.."

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'

step() { echo ""; echo "${B}▶ $1${N}"; }
ok()   { echo "  ${G}✓${N} $1"; }
warn() { echo "  ${Y}!${N} $1"; }

# ------------------------------------------------------------------
step "0. Confirmation"
echo "  This will DESTROY application data:"
echo "    - All Docker containers"
echo "    - Named volumes: csv_shared, hive_metastore_db, airflow_logs, opensearch_data"
echo "    - data/postgres/ data/kafka/ data/zookeeper/"
echo "    - hdfs/namenode/ hdfs/datanode/"
echo "    - nifi/flowfile_repository nifi/content_repository nifi/provenance_repository"
echo "    - All sim logs in ./logs/"
echo ""
echo "  PRESERVED (asset caches, not application data):"
echo "    - aiagent_model_cache  (sentence-transformers + huggingface, ~420MB)"
echo "    - Docker image ai-agent (pip layers — rebuild only when requirements change)"
echo ""
read -p "  Type 'WIPE' to confirm: " ans
if [[ "$ans" != "WIPE" ]]; then
  echo "  Aborted."
  exit 0
fi

# ------------------------------------------------------------------
step "1. Stop containers, remove application volumes (keep asset caches)"
# down WITHOUT -v: keeps named volumes; we'll delete only the application-data
# ones below. This preserves aiagent_model_cache so reboot doesn't re-download
# the 420MB embedder model.
docker compose down --remove-orphans
ok "containers stopped"

# Compose prefixes named volumes with the project name (defaults to the
# compose-file's directory). Resolve it the same way compose does so this
# works regardless of where the user invoked from.
PROJECT_PREFIX="${COMPOSE_PROJECT_NAME:-$(basename "$PWD" | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9_-')}"

# Wipe application-data volumes one by one. `|| true` because the volume may
# not exist on first wipe, or compose may have pruned it already.
for short in csv_shared hive_metastore_db airflow_logs opensearch_data; do
  docker volume rm "${PROJECT_PREFIX}_${short}" 2>/dev/null || true
done
ok "application volumes removed (aiagent_model_cache preserved)"

# ------------------------------------------------------------------
step "2. Wipe persistent host directories"
# Direct host cleanup — works on Linux, macOS, and Git Bash on Windows.
# (The previous docker-based version failed on Windows due to path mounting.)
rm -rf data/postgres/* data/postgres/.[!.]* 2>/dev/null
rm -rf data/kafka/* data/kafka/.[!.]* 2>/dev/null
rm -rf data/zookeeper/data/* data/zookeeper/log/* 2>/dev/null
rm -rf hdfs/namenode/* hdfs/namenode/.[!.]* 2>/dev/null
rm -rf hdfs/datanode/* hdfs/datanode/.[!.]* 2>/dev/null
rm -rf nifi/flowfile_repository nifi/content_repository nifi/provenance_repository nifi/logs 2>/dev/null
rm -rf logs/* 2>/dev/null
ok "host dirs cleaned"

# ------------------------------------------------------------------
step "3. Re-create empty dir skeletons"
mkdir -p data/postgres data/kafka data/zookeeper/data data/zookeeper/log
mkdir -p hdfs/namenode hdfs/datanode
mkdir -p nifi/flowfile_repository nifi/content_repository nifi/provenance_repository nifi/logs
mkdir -p logs
ok "skeleton dirs ready"

echo ""
echo "${G}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"
echo "${G}  Wipe complete. Next:  bash cli/startup.sh${N}"
echo "${G}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${N}"
