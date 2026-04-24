#!/usr/bin/env bash
# Enables all DISABLED controller services and starts all STOPPED processors
# in NiFi's root process group. Idempotent — safe to run any time.
#
# Usage: bash cli/nifi-recover.sh

set -e
cd "$(dirname "$0")/.."

NIFI="https://localhost:8443/nifi-api"
USER="admin"
PASS="adminadminadmin"

TOKEN=$(curl -sk -d "username=${USER}&password=${PASS}" "${NIFI}/access/token")
if [[ -z "${TOKEN}" ]]; then
  echo "Failed to authenticate to NiFi"
  exit 1
fi
AUTH="Authorization: Bearer ${TOKEN}"

ROOT_ID=$(curl -sk -H "${AUTH}" "${NIFI}/process-groups/root" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
echo "Root PG: ${ROOT_ID}"

echo ""
echo "── Enabling controller services ──"
curl -sk -H "${AUTH}" "${NIFI}/flow/process-groups/${ROOT_ID}/controller-services" | python3 -c "
import json, sys, subprocess
d = json.load(sys.stdin)
for svc in d['controllerServices']:
    c = svc['component']
    if c['state'] == 'DISABLED':
        print(f\"  enabling: {c['name']} ({c['id']})\")
        body = json.dumps({'revision': svc['revision'], 'state': 'ENABLED', 'disconnectedNodeAcknowledged': False})
        subprocess.run(['curl', '-sk', '-X', 'PUT',
                        '-H', '${AUTH}', '-H', 'Content-Type: application/json',
                        '-d', body, '${NIFI}/controller-services/' + c['id'] + '/run-status'],
                        capture_output=True)
    else:
        print(f\"  {c['name']}: {c['state']}\")
"

sleep 3  # wait for controller services to move DISABLED → ENABLING → ENABLED

echo ""
echo "── Starting stopped processors ──"
curl -sk -H "${AUTH}" "${NIFI}/flow/process-groups/${ROOT_ID}" | python3 -c "
import json, sys, subprocess
d = json.load(sys.stdin)
for p in d['processGroupFlow']['flow']['processors']:
    c = p['component']
    if c['state'] == 'STOPPED':
        print(f\"  starting: {c['name']} ({c['id']})\")
        body = json.dumps({'revision': p['revision'], 'state': 'RUNNING', 'disconnectedNodeAcknowledged': False})
        r = subprocess.run(['curl', '-sk', '-X', 'PUT',
                           '-H', '${AUTH}', '-H', 'Content-Type: application/json',
                           '-d', body, '${NIFI}/processors/' + c['id'] + '/run-status'],
                           capture_output=True, text=True)
        # Show validation errors if any
        try:
            resp = json.loads(r.stdout)
            if 'component' in resp and resp['component'].get('validationErrors'):
                for err in resp['component']['validationErrors']:
                    print(f\"      ERROR: {err}\")
        except Exception:
            pass
    else:
        print(f\"  {c['name']}: {c['state']}\")
"

echo ""
echo "Done. Run 'bash cli/pipeline-status.sh' to verify."
