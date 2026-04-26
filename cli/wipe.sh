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
echo "  This will DESTROY:"
echo "    - All Docker containers"
echo "    - All named volumes (csv_shared, hive_metastore_db, airflow_logs)"
echo "    - data/postgres/ data/kafka/ data/zookeeper/"
echo "    - hdfs/namenode/ hdfs/datanode/"
echo "    - nifi/flowfile_repository nifi/content_repository nifi/provenance_repository"
echo "    - All sim logs in ./logs/"
echo ""
read -p "  Type 'WIPE' to confirm: " ans
if [[ "$ans" != "WIPE" ]]; then
  echo "  Aborted."
  exit 0
fi

# ------------------------------------------------------------------
step "1. Stop & remove all containers + named volumes"
docker compose down -v --remove-orphans
ok "containers + volumes removed"

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
