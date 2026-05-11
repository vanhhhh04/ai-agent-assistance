#!/usr/bin/env bash
# Stop the live pipeline dashboard started by dashboard-up.sh.
# Safe to run even if not started (idempotent).

cd "$(dirname "$0")/.."

B=$'\033[1m'; G=$'\033[32m'; Y=$'\033[33m'; N=$'\033[0m'
ok()   { echo "  ${G}✓${N} $1"; }
warn() { echo "  ${Y}!${N} $1"; }

PID_FILE=logs/dashboard.pid

if [[ -f "${PID_FILE}" ]]; then
  pid=$(cat "${PID_FILE}" 2>/dev/null)
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null
    sleep 1
    kill -9 "${pid}" 2>/dev/null   # force if still alive
    ok "stopped dashboard (pid=${pid})"
  else
    warn "PID ${pid} no longer running"
  fi
  rm -f "${PID_FILE}"
else
  warn "no pid file (dashboard not started via dashboard-up.sh)"
fi

# Defensive: kill anything still bound to :5555
if command -v netstat >/dev/null 2>&1; then
  leftover=$(netstat -ano 2>/dev/null | grep ":5555 " | grep LISTEN | awk '{print $NF}' | head -1)
  if [[ -n "${leftover}" ]]; then
    warn "killing leftover process on :5555 (pid=${leftover})"
    taskkill //F //PID "${leftover}" >/dev/null 2>&1 || kill -9 "${leftover}" 2>/dev/null
  fi
fi
