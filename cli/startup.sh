#!/usr/bin/env bash
# Master startup script — idempotent, safe to run after laptop restart.
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

# ─────────────────────────────────────────────────────────────
step "0. Wipe Kafka + ZooKeeper state (clean slate for demo)"
# Kafka/ZK can end up with inconsistent metadata after a laptop reboot.
# Postgres is the source of truth — Debezium will re-snapshot it automatically
# on startup, so nothing is permanently lost.
# WAREHOUSE/PAYMENT topics are rebuilt from NiFi processors + sim scripts.
docker compose stop kafka-connect kafka zookeeper 2>/dev/null
sudo rm -rf data/kafka/* data/zookeeper/data/* data/zookeeper/log/* 2>/dev/null || \
  docker run --rm -v "$(pwd)/data:/data" alpine sh -c \
    "rm -rf /data/kafka/* /data/zookeeper/data/* /data/zookeeper/log/*" 2>/dev/null
ok "Kafka + ZooKeeper state cleared"

# ─────────────────────────────────────────────────────────────
step "1. docker compose up -d"
docker compose up -d
ok "containers requested up"

# ─────────────────────────────────────────────────────────────
step "2. Wait for core services"
wait_http "Postgres (via container)" "http://localhost:5433/" 60 2>/dev/null
# Postgres isn't HTTP, use docker healthcheck state:
for i in $(seq 1 30); do
  st=$(docker inspect --format='{{.State.Health.Status}}' postgres 2>/dev/null)
  [[ "$st" == "healthy" ]] && ok "Postgres healthy" && break
  sleep 2
done

wait_tcp "Kafka"            "localhost:9092"   120
wait_http "Kafka Connect"   "http://localhost:8083/"  180
wait_http "NiFi API"        "https://localhost:8443/nifi-api/access/config" 240
wait_tcp "HiveServer2"      "localhost:10000"  180
wait_http "Airflow"         "http://localhost:8080/health" 120

# ─────────────────────────────────────────────────────────────
step "3. Fix CSV volume permissions (NiFi GetFile needs write)"
docker exec -u root nifi chmod 777 /opt/nifi/csv_input 2>/dev/null && ok "chmod 777 done" || warn "could not chmod (nifi container may not be ready)"

# ─────────────────────────────────────────────────────────────
step "4. Register Debezium connector if missing"
CONNECTORS=$(curl -sf http://localhost:8083/connectors 2>/dev/null || echo "[]")
if echo "$CONNECTORS" | grep -q "erp-postgres-cdc"; then
  ok "Debezium connector already registered"
else
  warn "Debezium connector missing — registering…"
  bash nifi/init_debezium.sh > /dev/null 2>&1 && ok "registered" || err "registration failed — run 'bash nifi/init_debezium.sh' manually"
fi

# ─────────────────────────────────────────────────────────────
step "5. Create NiFi flow if missing"
TOKEN=$(curl -sk -d "username=admin&password=adminadminadmin" https://localhost:8443/nifi-api/access/token 2>/dev/null)
ROOT=$(curl -sk -H "Authorization: Bearer $TOKEN" https://localhost:8443/nifi-api/process-groups/root 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
NUM_PROCS=$(curl -sk -H "Authorization: Bearer $TOKEN" "https://localhost:8443/nifi-api/process-groups/${ROOT}/processors" 2>/dev/null | python3 -c "import json,sys;print(len(json.load(sys.stdin).get('processors',[])))" 2>/dev/null)

if [[ "${NUM_PROCS:-0}" -ge 6 ]]; then
  ok "NiFi flow already exists (${NUM_PROCS} processors)"
else
  warn "NiFi flow missing (found ${NUM_PROCS:-0} processors) — creating…"
  python3 nifi/setup_flows.py > /tmp/nifi_setup.log 2>&1 && ok "flow created" || err "setup failed — see /tmp/nifi_setup.log"
fi

# ─────────────────────────────────────────────────────────────
step "6. Kill any stale simulators, start fresh"
bash cli/sim-stop.sh > /dev/null 2>&1
sleep 2
bash cli/sim-start.sh all > /dev/null 2>&1
ok "simulators started (logs in ./logs/)"

# ─────────────────────────────────────────────────────────────
step "7. Pipeline status"
sleep 10
bash cli/pipeline-status.sh
