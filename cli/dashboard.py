"""
Pipeline Live Dashboard — http://localhost:5555
================================================
A single-page HTML dashboard showing the data pipeline as a flow diagram with
live counts at each layer. Auto-refreshes every 5 seconds.

Run:
    venv/Scripts/python.exe cli/dashboard.py

Then open http://localhost:5555 in your browser.

What you see:
    [Postgres] -> [Debezium] -> [NiFi] -> [Kafka] -> [Bronze HDFS]
                                                  -> [Silver HDFS]
                                                  -> [Gold/Hive]
    Each box shows: row/message count + freshness + status color.
"""

import json
import subprocess
import threading
import time
import urllib3
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PORT  = 5555
CACHE = {"ts": 0, "data": None}
LOCK  = threading.Lock()


# ─────────────────────────────────────────────────────────────
#  Data collectors — one per layer
# ─────────────────────────────────────────────────────────────
def sh(*cmd, timeout=15) -> str:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception:
        return ""


def pg_counts() -> dict:
    out = sh("docker", "exec", "postgres", "psql", "-U", "postgres", "-d",
             "ecommerce", "-tA", "-F|", "-c",
             "SELECT 'orders',COUNT(*) FROM orders UNION ALL "
             "SELECT 'customers',COUNT(*) FROM customers UNION ALL "
             "SELECT 'products',COUNT(*) FROM products UNION ALL "
             "SELECT 'payments',COUNT(*) FROM payments UNION ALL "
             "SELECT 'shipping',COUNT(*) FROM shipping;")
    res = {}
    for line in out.splitlines():
        if "|" in line:
            k, v = line.split("|", 1)
            res[k.strip()] = int(v.strip() or 0)
    age = sh("docker", "exec", "postgres", "psql", "-U", "postgres",
             "-d", "ecommerce", "-tA", "-c",
             "SELECT EXTRACT(EPOCH FROM (NOW()-MAX(created_at)))::int FROM orders")
    res["_freshness_sec"] = int(age) if age.isdigit() else None
    return res


def cdc_status() -> dict:
    try:
        r = requests.get("http://localhost:8083/connectors/erp-postgres-cdc/status",
                         timeout=5)
        d = r.json()
        return {
            "connector": d["connector"]["state"],
            "task":      d["tasks"][0]["state"] if d.get("tasks") else "NO_TASK",
        }
    except Exception:
        return {"connector": "DOWN", "task": "—"}


def nifi_status() -> dict:
    try:
        token = requests.post(
            "https://localhost:8443/nifi-api/access/token",
            data={"username": "admin", "password": "adminadminadmin"},
            verify=False, timeout=5,
        ).text
        h = {"Authorization": f"Bearer {token}"}
        root = requests.get("https://localhost:8443/nifi-api/process-groups/root",
                            headers=h, verify=False, timeout=5).json()["id"]
        flow = requests.get(f"https://localhost:8443/nifi-api/flow/process-groups/{root}",
                            headers=h, verify=False, timeout=5).json()
        procs = flow["processGroupFlow"]["flow"]["processors"]
        running = sum(1 for p in procs if p["component"]["state"] == "RUNNING")
        ff_in   = sum(p["status"]["aggregateSnapshot"].get("flowFilesIn", 0)  for p in procs)
        ff_out  = sum(p["status"]["aggregateSnapshot"].get("flowFilesOut", 0) for p in procs)
        return {"running": running, "total": len(procs),
                "flow_files_in": ff_in, "flow_files_out": ff_out}
    except Exception as e:
        return {"running": 0, "total": 0, "error": str(e)[:50]}


def kafka_counts() -> dict:
    topics = ["erp.public.customers", "erp.public.orders", "erp.public.order_items",
              "erp.public.products", "warehouse.events", "warehouse.events.dlq",
              "payment.events", "payment.events.dlq"]
    res = {}
    for t in topics:
        out = sh("docker", "exec", "kafka", "kafka-run-class",
                 "kafka.tools.GetOffsetShell", "--broker-list", "kafka:29092",
                 "--topic", t)
        total = 0
        for line in out.splitlines():
            parts = line.split(":")
            if len(parts) >= 3 and parts[-1].isdigit():
                total += int(parts[-1])
        res[t] = total
    return res


def hdfs_sizes() -> dict:
    sinks = ["erp_raw", "wh_raw", "wh_dlq", "pay_raw", "pay_dlq"]
    res = {}
    for s in sinks:
        out = sh("docker", "exec", "namenode", "hdfs", "dfs", "-du", "-s", "-h",
                 f"/datalake/bronze/{s}")
        if out:
            parts = out.split()
            res[s] = f"{parts[0]} {parts[1]}" if len(parts) >= 2 else "0"
        else:
            res[s] = "—"
    return res


def airflow_run() -> dict:
    try:
        r = requests.get(
            "http://localhost:8080/api/v1/dags/medallion_pipeline/dagRuns",
            params={"order_by": "-execution_date", "limit": 1},
            auth=("admin", "admin123"), timeout=5,
        )
        runs = r.json().get("dag_runs", [])
        if not runs:
            return {"state": "no runs", "id": "-"}
        return {"state": runs[0]["state"], "id": runs[0]["dag_run_id"][-25:]}
    except Exception:
        return {"state": "DOWN", "id": "-"}


def gold_counts() -> dict:
    """Spark SQL count via spark-master container."""
    out = sh("docker", "exec", "spark-master", "/opt/bitnami/spark/bin/spark-sql",
             "--master", "spark://spark-master:7077",
             "--conf", "spark.sql.catalogImplementation=hive",
             "--conf", "spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083",
             "-e",
             "SELECT 'fact_sales', COUNT(*) FROM gold.fact_sales UNION ALL "
             "SELECT 'dim_customers', COUNT(*) FROM gold.dim_customers UNION ALL "
             "SELECT 'dim_products',  COUNT(*) FROM gold.dim_products UNION ALL "
             "SELECT 'dim_payments',  COUNT(*) FROM gold.dim_payments UNION ALL "
             "SELECT 'dim_shipping',  COUNT(*) FROM gold.dim_shipping;",
             timeout=60)
    res = {}
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) == 2 and parts[1].strip().isdigit():
            res[parts[0].strip()] = int(parts[1].strip())
    return res


def collect_all() -> dict:
    return {
        "ts":      int(time.time()),
        "pg":      pg_counts(),
        "cdc":     cdc_status(),
        "nifi":    nifi_status(),
        "kafka":   kafka_counts(),
        "hdfs":    hdfs_sizes(),
        "airflow": airflow_run(),
        "gold":    gold_counts(),
    }


# ─────────────────────────────────────────────────────────────
#  Background refresher — Gold count is slow so we cache 30s
# ─────────────────────────────────────────────────────────────
def refresh_loop():
    while True:
        try:
            data = collect_all()
            with LOCK:
                CACHE["data"] = data
                CACHE["ts"]   = data["ts"]
        except Exception as e:
            print(f"[refresh error] {e}")
        time.sleep(15)


# ─────────────────────────────────────────────────────────────
#  HTTP server
# ─────────────────────────────────────────────────────────────
HTML = """<!DOCTYPE html>
<html lang="vi"><head>
<meta charset="utf-8"><title>Pipeline Live Dashboard</title>
<style>
  body { font-family: -apple-system, Segoe UI, sans-serif; margin: 0; padding: 20px;
         background: #0f1419; color: #e6edf3; }
  h1 { font-size: 18px; margin: 0 0 8px 0; }
  .meta { color: #8b949e; font-size: 12px; margin-bottom: 20px; }
  .pipeline { display: flex; flex-direction: column; gap: 14px; }
  .row { display: flex; gap: 14px; flex-wrap: wrap; align-items: stretch; }
  .box { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
         padding: 12px 16px; min-width: 200px; flex: 1; }
  .box h3 { margin: 0 0 8px 0; font-size: 13px; color: #58a6ff; text-transform: uppercase;
            letter-spacing: 0.5px; }
  .box .row-stat { display: flex; justify-content: space-between; font-size: 13px;
                   padding: 2px 0; }
  .box .row-stat .key { color: #8b949e; }
  .box .row-stat .val { color: #e6edf3; font-family: ui-monospace, monospace; }
  .arrow { display: flex; align-items: center; color: #6e7681; font-size: 24px; }
  .ok    { border-left: 3px solid #3fb950; }
  .warn  { border-left: 3px solid #d29922; }
  .err   { border-left: 3px solid #f85149; }
  .badge { display: inline-block; padding: 2px 6px; border-radius: 3px;
           font-size: 11px; font-weight: 600; }
  .b-ok { background: #1b3b22; color: #3fb950; }
  .b-warn{ background: #3d2e0a; color: #d29922; }
  .b-err{ background: #3b1212; color: #f85149; }
  .footer { margin-top: 20px; color: #6e7681; font-size: 11px; }
  a { color: #58a6ff; text-decoration: none; margin-right: 14px; }
</style></head><body>

<h1>Medallion Pipeline — Live Dashboard</h1>
<div class="meta">Refresh every 5s · Last update: <span id="ts">…</span></div>

<div class="pipeline">
  <div class="row">
    <div id="pg"      class="box"><h3>1. PostgreSQL (Source)</h3><div class="content">…</div></div>
    <div class="arrow">→</div>
    <div id="cdc"     class="box"><h3>2. Debezium CDC</h3><div class="content">…</div></div>
    <div class="arrow">→</div>
    <div id="nifi"    class="box"><h3>3. NiFi Flow</h3><div class="content">…</div></div>
  </div>
  <div class="row">
    <div id="kafka"   class="box" style="flex:3"><h3>4. Kafka Topics</h3><div class="content">…</div></div>
    <div class="arrow">→</div>
    <div id="hdfs"    class="box" style="flex:2"><h3>5. Bronze HDFS</h3><div class="content">…</div></div>
  </div>
  <div class="row">
    <div id="airflow" class="box"><h3>6. Airflow DAG</h3><div class="content">…</div></div>
    <div class="arrow">→</div>
    <div id="gold"    class="box" style="flex:2"><h3>7. Gold (Hive)</h3><div class="content">…</div></div>
  </div>
</div>

<div class="footer">
  <a href="http://localhost:8081" target="_blank">Adminer</a>
  <a href="http://localhost:8888" target="_blank">Kafka UI</a>
  <a href="https://localhost:8443/nifi" target="_blank">NiFi</a>
  <a href="http://localhost:9870" target="_blank">HDFS</a>
  <a href="http://localhost:8090" target="_blank">Spark</a>
  <a href="http://localhost:8080" target="_blank">Airflow</a>
</div>

<script>
function row(k, v) { return `<div class="row-stat"><span class="key">${k}</span><span class="val">${v}</span></div>`; }
function fmt(n) { return n == null ? "—" : Number(n).toLocaleString(); }

async function refresh() {
  const r = await fetch('/api/status');
  const d = await r.json();
  document.getElementById('ts').textContent = new Date(d.ts * 1000).toLocaleTimeString();

  // Postgres
  let pg = '';
  for (const [k,v] of Object.entries(d.pg)) {
    if (k === '_freshness_sec') continue;
    pg += row(k, fmt(v));
  }
  let pgClass = 'box ok';
  if (d.pg._freshness_sec == null || d.pg._freshness_sec > 600) pgClass = 'box err';
  else if (d.pg._freshness_sec > 60) pgClass = 'box warn';
  pg += row('latest order', d.pg._freshness_sec == null ? '—' : d.pg._freshness_sec + 's ago');
  document.getElementById('pg').className = pgClass;
  document.getElementById('pg').querySelector('.content').innerHTML = pg;

  // CDC
  const cdcOK = d.cdc.connector === 'RUNNING' && d.cdc.task === 'RUNNING';
  document.getElementById('cdc').className = 'box ' + (cdcOK ? 'ok' : 'err');
  document.getElementById('cdc').querySelector('.content').innerHTML =
    row('connector', `<span class="badge ${cdcOK?'b-ok':'b-err'}">${d.cdc.connector}</span>`) +
    row('task[0]',   `<span class="badge ${d.cdc.task==='RUNNING'?'b-ok':'b-err'}">${d.cdc.task}</span>`);

  // NiFi
  const ok = d.nifi.running === d.nifi.total && d.nifi.total > 0;
  document.getElementById('nifi').className = 'box ' + (ok ? 'ok' : 'err');
  document.getElementById('nifi').querySelector('.content').innerHTML =
    row('processors', `<span class="badge ${ok?'b-ok':'b-err'}">${d.nifi.running}/${d.nifi.total}</span>`) +
    row('FlowFiles in',  fmt(d.nifi.flow_files_in)) +
    row('FlowFiles out', fmt(d.nifi.flow_files_out));

  // Kafka
  let kafka = '';
  for (const [t,c] of Object.entries(d.kafka)) {
    const cls = t.includes('.dlq') ? 'b-warn' : 'b-ok';
    kafka += row(t, `<span class="badge ${cls}">${fmt(c)}</span>`);
  }
  document.getElementById('kafka').className = 'box ok';
  document.getElementById('kafka').querySelector('.content').innerHTML = kafka;

  // HDFS
  let hdfs = '';
  for (const [k,v] of Object.entries(d.hdfs)) hdfs += row(k, v);
  document.getElementById('hdfs').className = 'box ok';
  document.getElementById('hdfs').querySelector('.content').innerHTML = hdfs;

  // Airflow
  const af = d.airflow;
  const afClass = af.state === 'success' ? 'b-ok' :
                  af.state === 'running' ? 'b-warn' :
                  af.state === 'failed'  ? 'b-err' : 'b-warn';
  document.getElementById('airflow').className = 'box ' + (af.state==='failed'?'err':'ok');
  document.getElementById('airflow').querySelector('.content').innerHTML =
    row('last run state', `<span class="badge ${afClass}">${af.state}</span>`) +
    row('run id', `<span style="font-size:10px">${af.id}</span>`);

  // Gold
  let gold = '';
  for (const [k,v] of Object.entries(d.gold)) gold += row(k, fmt(v));
  if (!Object.keys(d.gold).length) gold = row('status', '<span class="badge b-warn">computing or empty</span>');
  document.getElementById('gold').className = 'box ' + (Object.keys(d.gold).length?'ok':'warn');
  document.getElementById('gold').querySelector('.content').innerHTML = gold;
}

refresh();
setInterval(refresh, 5000);
</script></body></html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_): pass   # silence

    def do_GET(self):
        if self.path == "/api/status":
            with LOCK:
                data = CACHE["data"] or {"ts": 0, "pg": {}, "cdc": {}, "nifi": {},
                                          "kafka": {}, "hdfs": {}, "airflow": {}, "gold": {}}
            body = json.dumps(data).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(body)
        elif self.path in ("/", "/index.html"):
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404); self.end_headers()


def main():
    print(f"[dashboard] gathering initial data...")
    CACHE["data"] = collect_all()
    CACHE["ts"]   = CACHE["data"]["ts"]

    threading.Thread(target=refresh_loop, daemon=True).start()
    print(f"[dashboard] open http://localhost:{PORT} in your browser")
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
