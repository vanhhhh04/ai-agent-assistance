#!/usr/bin/env bash
# Nuclear reset of the NiFi flow: deletes every processor, connection, and
# controller service in the root PG, fixes the CSV volume permissions,
# then re-runs setup_flows.py with the corrected property set.
#
# Run from project root:  bash cli/nifi-reset.sh

cd "$(dirname "$0")/.."

# Pick a working Python interpreter — Windows Git Bash needs `python`,
# Linux/macOS need `python3`.
if command -v python3 >/dev/null 2>&1 && python3 --version >/dev/null 2>&1; then
  PY=python3
elif [[ -x venv/Scripts/python.exe ]]; then
  PY=venv/Scripts/python.exe
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "no python interpreter found"; exit 1
fi

NIFI="https://localhost:8443/nifi-api"
TOKEN=$(curl -sk -d "username=admin&password=adminadminadmin" "${NIFI}/access/token")
AUTH_HDR="Authorization: Bearer ${TOKEN}"

echo "Step 1: fix CSV volume permissions (NiFi's GetFile needs write access)"
docker exec -u root nifi chmod 777 /opt/nifi/csv_input 2>/dev/null
echo "  done."

ROOT=$(curl -sk -H "${AUTH_HDR}" "${NIFI}/process-groups/root" | "$PY" -c "import json,sys;print(json.load(sys.stdin)['id'])")
echo "  root PG: ${ROOT}"

echo ""
echo "Step 2: stop all processors in root PG (bulk)"
VER=$(curl -sk -H "${AUTH_HDR}" "${NIFI}/process-groups/${ROOT}" | "$PY" -c "import json,sys;print(json.load(sys.stdin)['revision']['version'])")
curl -sk -X PUT -H "${AUTH_HDR}" -H "Content-Type: application/json" \
  -d "{\"id\":\"${ROOT}\",\"state\":\"STOPPED\"}" \
  "${NIFI}/flow/process-groups/${ROOT}" > /dev/null
sleep 3
echo "  done."

echo ""
echo "Step 3: delete all connections"
# Correct endpoint: /process-groups/{id}/connections (no /flow/ prefix)
CONNECTIONS_JSON=$(curl -sk -H "${AUTH_HDR}" "${NIFI}/process-groups/${ROOT}/connections")
echo "${CONNECTIONS_JSON}" | "$PY" -c "
import json, sys, subprocess
try:
    d = json.loads(sys.stdin.read())
except Exception as e:
    print(f'  (skip — no connections endpoint response: {e})')
    sys.exit(0)
conns = d.get('connections', [])
if not conns:
    print('  (no connections to delete)')
for conn in conns:
    cid = conn['id']
    ver = conn['revision']['version']
    print(f'  deleting connection {cid}')
    subprocess.run(['curl','-sk','-X','DELETE','-H','${AUTH_HDR}',
                    f'${NIFI}/connections/{cid}?version={ver}'], capture_output=True)
"

echo ""
echo "Step 4: delete all processors"
PROCS_JSON=$(curl -sk -H "${AUTH_HDR}" "${NIFI}/process-groups/${ROOT}/processors")
echo "${PROCS_JSON}" | "$PY" -c "
import json, sys, subprocess
try:
    d = json.loads(sys.stdin.read())
except Exception as e:
    print(f'  (skip — no processors endpoint response: {e})')
    sys.exit(0)
procs = d.get('processors', [])
if not procs:
    print('  (no processors to delete)')
for p in procs:
    pid = p['id']
    ver = p['revision']['version']
    name = p['component']['name']
    print(f'  deleting {name} ({pid})')
    subprocess.run(['curl','-sk','-X','DELETE','-H','${AUTH_HDR}',
                    f'${NIFI}/processors/{pid}?version={ver}'], capture_output=True)
"

echo ""
echo "Step 5: disable all controller services"
SVC_JSON=$(curl -sk -H "${AUTH_HDR}" "${NIFI}/flow/process-groups/${ROOT}/controller-services")
echo "${SVC_JSON}" | "$PY" -c "
import json, sys, subprocess
try:
    d = json.loads(sys.stdin.read())
except Exception as e:
    print(f'  (skip: {e})')
    sys.exit(0)
svcs = d.get('controllerServices', [])
if not svcs:
    print('  (no controller services to disable)')
for svc in svcs:
    c = svc['component']
    if c['state'] in ('ENABLED','ENABLING'):
        print(f\"  disabling {c['name']}\")
        body = json.dumps({'revision': svc['revision'], 'state': 'DISABLED', 'disconnectedNodeAcknowledged': False})
        subprocess.run(['curl','-sk','-X','PUT','-H','${AUTH_HDR}','-H','Content-Type: application/json',
                        '-d', body, '${NIFI}/controller-services/' + c['id'] + '/run-status'], capture_output=True)
"
sleep 5   # give controller services time to disable

echo ""
echo "Step 6: delete all controller services"
SVC_JSON=$(curl -sk -H "${AUTH_HDR}" "${NIFI}/flow/process-groups/${ROOT}/controller-services")
echo "${SVC_JSON}" | "$PY" -c "
import json, sys, subprocess
try:
    d = json.loads(sys.stdin.read())
except Exception as e:
    print(f'  (skip: {e})')
    sys.exit(0)
for svc in d.get('controllerServices', []):
    c = svc['component']
    # Re-fetch to get latest revision after disable
    print(f\"  deleting {c['name']}\")
    subprocess.run(['curl','-sk','-X','DELETE','-H','${AUTH_HDR}',
                    '${NIFI}/controller-services/' + c['id'] + '?version=' + str(svc['revision']['version'])],
                   capture_output=True)
"

echo ""
echo "Step 7: re-run setup_flows.py with corrected properties"
"$PY" nifi/setup_flows.py

echo ""
echo "All done. Verify with: bash cli/pipeline-status.sh"
