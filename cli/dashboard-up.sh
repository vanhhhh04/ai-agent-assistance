#!/usr/bin/env bash
# Start the live pipeline dashboard (cli/dashboard.py) as a background process.
# Idempotent: if already running, it's killed first so the new one binds :5555.
#
# The dashboard polls Postgres / Kafka / NiFi / Airflow / Spark / Hive every 15s
# and serves a single-page HTML at http://localhost:5555.
#
# Usage:  bash cli/dashboard-up.sh

cd "$(dirname "$0")/.."

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; N=$'\033[0m'
ok()   { echo "  ${G}✓${N} $1"; }
warn() { echo "  ${Y}!${N} $1"; }

# Pick the same python interpreter startup.sh uses
if [[ -x venv/Scripts/python.exe ]]; then
  PY=venv/Scripts/python.exe
elif [[ -x venv/bin/python ]]; then
  PY=venv/bin/python
elif command -v python3 >/dev/null 2>&1; then
  PY=python3
else
  PY=python
fi

mkdir -p logs
PID_FILE=logs/dashboard.pid
LOG_FILE=logs/dashboard.log

# ── Stop any previous instance ──────────────────────────
if [[ -f "${PID_FILE}" ]]; then
  old_pid=$(cat "${PID_FILE}" 2>/dev/null)
  if [[ -n "${old_pid}" ]] && kill -0 "${old_pid}" 2>/dev/null; then
    warn "killing previous dashboard process (pid=${old_pid})"
    kill "${old_pid}" 2>/dev/null
    sleep 1
  fi
  rm -f "${PID_FILE}"
fi

# ── Defensive: kill anything still bound to :5555 ───────
# `lsof` isn't always present on Git Bash; netstat is. Fall back gracefully.
if command -v netstat >/dev/null 2>&1; then
  pid_on_port=$(netstat -ano 2>/dev/null | grep ":5555 " | grep LISTEN | awk '{print $NF}' | head -1)
  if [[ -n "${pid_on_port}" ]]; then
    warn "killing process bound to :5555 (pid=${pid_on_port})"
    taskkill //F //PID "${pid_on_port}" >/dev/null 2>&1 || kill -9 "${pid_on_port}" 2>/dev/null
    sleep 1
  fi
fi

# ── Verify deps available ───────────────────────────────
if ! "$PY" -c "import requests, urllib3" 2>/dev/null; then
  warn "Python ${PY} missing 'requests' — installing..."
  "$PY" -m pip install -r cli-requirements.txt >/dev/null 2>&1 || \
    warn "pip install failed — install manually: ${PY} -m pip install requests urllib3"
fi

# ── Start in background ─────────────────────────────────
"$PY" cli/dashboard.py >> "${LOG_FILE}" 2>&1 &
DASH_PID=$!
disown $DASH_PID 2>/dev/null || true
echo "${DASH_PID}" > "${PID_FILE}"

# ── Wait for it to bind :5555 (up to 30s) ───────────────
for i in $(seq 1 30); do
  if curl -sf http://localhost:5555/ > /dev/null 2>&1; then
    ok "dashboard live at http://localhost:5555 (pid=${DASH_PID})"
    exit 0
  fi
  sleep 1
done

warn "dashboard did not respond on :5555 within 30s — see ${LOG_FILE}"
tail -20 "${LOG_FILE}" 2>/dev/null
exit 1
