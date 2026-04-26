#!/usr/bin/env bash
# Master startup script - idempotent, safe to run after laptop restart.
# Brings the platform from ANY state (fresh/stopped/partially crashed) to healthy.
#
# What it does:
#   1. docker compose up -d
#   2. Wait for each service's health endpoint
#   3. Fix CSV volume permissions (required for NiFi GetFile)
#   4. Register Debezium connector if missing
#   5. Create NiFi flow if missing (otherwise leave alone)
#   6. Start simulators (kill duplicates first)
#   7. Show pipeline status

cd "$(dirname "$0")/.."

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'

# Pick a working Python interpreter — Windows Git Bash has `python` but `python3`
# is a Microsoft Store stub; Linux/macOS usually have `python3` but not `python`.
if command -v python3 >/dev/null 2>&1 && python3 --version >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "no python interpreter found"; exit 1
fi

step() { echo ""; echo "${B}▶ $1${N}"; }
ok()   { echo "  ${G}✓${N} $1"; }
warn() { echo "  ${Y}!${N} $1"; }
err()  { echo "  ${R}✗${N} $1"; }

wait_http() {
  local name="$1" url="$2" timeout="${3:-120}"
  local start=$SECONDS
  while (( SECONDS - start < timeout )); do
    if curl -ksf -o /dev/null "${url}"; then ok "${name} ready"; return 0; fi
    sleep 5
  done
  err "${name} did not become ready in ${timeout}s"
  return 1
}

wait_tcp() {
  local name="$1" hostport="$2" timeout="${3:-120}"
  local start=$SECONDS
  while (( SECONDS - start < timeout )); do
    if bash -c "</dev/tcp/${hostport/:/\/}" 2>/dev/null; then ok "${name} reachable"; return 0; fi
    sleep 5
  done
  err "${name} not reachable on ${hostport} in ${timeout}s"
  return 1
}

# -------------------------------------------------------------
step "0. Wipe Kafka + ZooKeeper state (clean slate for demo)"
# Kafka/ZK can end up with inconsistent metadata after a laptop reboot.
# Postgres is the source of truth - Debezium will re-snapshot it automatically
# on startup, so nothing is permanently lost.
# WAREHOUSE/PAYMENT topics are rebuilt from NiFi processors + sim scripts.
docker compose stop kafka-connect kafka zookeeper 2>/dev/null
sudo rm -rf data/kafka/* data/zookeeper/data/* data/zookeeper/log/* 2>/dev/null || \
  docker run --rm -v "$(pwd)/data:/data" alpine sh -c \
    "rm -rf /data/kafka/* /data/zookeeper/data/* /data/zookeeper/log/*" 2>/dev/null
ok "Kafka + ZooKeeper state cleared"

# -------------------------------------------------------------
step "1. docker compose up -d"
docker compose up -d
ok "containers requested up"

# -------------------------------------------------------------
step "2. Wait for core services"
# Postgres is not an HTTP service; rely on container healthcheck state.
for i in $(seq 1 30); do
  st=$(docker inspect --format='{{.State.Health.Status}}' postgres 2>/dev/null)
  [[ "$st" == "healthy" ]] && ok "Postgres healthy" && break
  sleep 2
done

# Airflow metadata DB init script only runs on first-ever postgres init.
# If data/postgres already existed before that script was mounted, DB may be missing.
if docker exec postgres psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='airflow'" 2>/dev/null | grep -q 1; then
  ok "Airflow database exists"
else
  warn "Airflow database missing - creating..."
  if docker exec postgres psql -U postgres -c "CREATE DATABASE airflow;" >/dev/null 2>&1; then
    ok "Airflow database created"
  else
    err "could not create Airflow database automatically"
  fi
fi

wait_tcp "Kafka"            "localhost:9092"   120
wait_http "Kafka Connect"   "http://localhost:8083/"  180
wait_http "NiFi API"        "https://localhost:8443/nifi-api/access/config" 240
wait_http "HDFS Namenode"   "http://localhost:9870" 120
wait_tcp "HiveServer2"      "localhost:10000"  180
# Airflow takes longer because webserver waits for airflow-init to finish DB migrate
wait_http "Airflow"         "http://localhost:8080/health" 300

# -------------------------------------------------------------
step "3. Fix CSV volume permissions (NiFi GetFile needs write)"
# MSYS_NO_PATHCONV stops Git Bash from rewriting /opt/... into a Windows path.
for i in $(seq 1 20); do
  if MSYS_NO_PATHCONV=1 docker exec -u root nifi chmod 777 /opt/nifi/csv_input 2>/dev/null; then
    ok "chmod 777 done"; break
  fi
  sleep 3
  [[ $i -eq 20 ]] && warn "could not chmod after 60s (nifi may still be starting)"
done

# -------------------------------------------------------------
step "3b. HDFS Medallion directory structure"
# Wait for namenode dfs to actually accept commands (HTTP UI ready ≠ dfs ready).
for i in $(seq 1 20); do
  if MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -mkdir -p /datalake 2>/dev/null; then
    break
  fi
  sleep 3
done

MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -mkdir -p \
  /datalake/bronze/erp_raw /datalake/bronze/wh_raw /datalake/bronze/wh_dlq \
  /datalake/bronze/pay_raw /datalake/bronze/pay_dlq \
  /datalake/silver /datalake/gold \
  /datalake/_checkpoints/bronze \
  /user/hive/warehouse 2>/dev/null
MSYS_NO_PATHCONV=1 docker exec namenode hdfs dfs -chmod -R 777 /datalake /user/hive/warehouse 2>/dev/null && \
  ok "HDFS dirs ready" || warn "HDFS not reachable yet"

# -------------------------------------------------------------
step "4. Register Debezium connector if missing"
CONNECTORS=$(curl -sf http://localhost:8083/connectors 2>/dev/null || echo "[]")
if echo "$CONNECTORS" | grep -q "erp-postgres-cdc"; then
  ok "Debezium connector already registered"
else
  warn "Debezium connector missing - registering..."
  bash nifi/init_debezium.sh > /dev/null 2>&1 && ok "registered" || err "registration failed - run 'bash nifi/init_debezium.sh' manually"
fi

# -------------------------------------------------------------
step "5. Create NiFi flow if missing"
TOKEN=$(curl -sk -d "username=admin&password=adminadminadmin" https://localhost:8443/nifi-api/access/token 2>/dev/null)
ROOT=$(curl -sk -H "Authorization: Bearer $TOKEN" https://localhost:8443/nifi-api/process-groups/root 2>/dev/null | $PY -c "import json,sys;print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
NUM_PROCS=$(curl -sk -H "Authorization: Bearer $TOKEN" "https://localhost:8443/nifi-api/process-groups/${ROOT}/processors" 2>/dev/null | $PY -c "import json,sys;print(len(json.load(sys.stdin).get('processors',[])))" 2>/dev/null)

if [[ "${NUM_PROCS:-0}" -ge 6 ]]; then
  ok "NiFi flow already exists (${NUM_PROCS} processors)"
else
  warn "NiFi flow missing (found ${NUM_PROCS:-0} processors) - creating..."
  $PY nifi/setup_flows.py > /tmp/nifi_setup.log 2>&1 && ok "flow created" || err "setup failed - see /tmp/nifi_setup.log"
fi

# -------------------------------------------------------------
step "6. Kill any stale simulators, start fresh"
bash cli/sim-stop.sh > /dev/null 2>&1
sleep 2
bash cli/sim-start.sh all > /dev/null 2>&1
ok "simulators started (logs in ./logs/)"

# -------------------------------------------------------------
step "7. Pipeline status"
sleep 10
bash cli/pipeline-status.sh
