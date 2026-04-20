"""
spark_harmonization.py
========================
Spark Silver Layer — Harmonization & ID Resolution

Đây là file quan trọng nhất trong pipeline: nơi DE thực sự làm việc.
Script này KHÔNG dùng PySpark thật (cần cluster) mà là bản
"runnable explanation" — mỗi kỹ thuật được viết dưới dạng Python thuần
với comment giải thích TƯƠNG ĐƯƠNG với Spark API.

Cách dùng để học:
    python spark_harmonization.py --demo all
    python spark_harmonization.py --demo dedup
    python spark_harmonization.py --demo late_join
    python spark_harmonization.py --demo quality_routing

Khi bạn có Spark cluster thật, uncomment phần PySpark
và comment phần Python simulation.

Cai dat:
    pip install psycopg2-binary

Chay:
    python spark_harmonization.py
"""

import hashlib
import json
import random
import argparse
from datetime import datetime, timedelta
from typing import Literal


# ═══════════════════════════════════════════════════════════════════
#  PHẦN 1: SURROGATE KEY GENERATION
#  Kỹ thuật đầu tiên và quan trọng nhất của DE
# ═══════════════════════════════════════════════════════════════════

def make_surrogate_key(source_system: str, source_id) -> str:
    """
    Tạo surrogate key từ source_system + source_id.

    VẤN ĐỀ surrogate key giải quyết:
    ─────────────────────────────────
    ERP:        order_id = 123    (BIGINT serial từ PostgreSQL)
    Payment GW: order_id = 123    (cùng số, nhưng đây là ERP ref, không phải PG internal ID)
    Warehouse:  product_id = 123  (BIGINT serial khác DB)

    Nếu Spark dùng thẳng 123 làm key trong fact table → COLLISION.
    Ví dụ: ERP order 123 và Warehouse product 123 cùng map về 1 row.

    SOLUTION: surrogate_key = MD5("erp:123") ≠ MD5("warehouse:123")
    ────────────────────────────────────────────────────────────────

    Trong PySpark thực tế, bạn viết:
        from pyspark.sql.functions import md5, concat, col, lit

        df = df.withColumn(
            "surrogate_key",
            md5(concat(lit(source_system + ":"), col("source_id").cast("string")))
        )

    Tại sao MD5 thay vì UUID?
    → MD5 là DETERMINISTIC: cùng input → cùng output
    → Khi Spark job chạy lại (retry), nó tạo ra CÙNG surrogate_key
    → UUID random → mỗi lần retry tạo key mới → duplicate trong fact table
    → Đây gọi là IDEMPOTENT write — quan trọng nhất trong distributed systems
    """
    raw = f"{source_system}:{source_id}"
    return hashlib.md5(raw.encode()).hexdigest()


def demo_surrogate_key():
    print("\n" + "═" * 60)
    print("  KỸ THUẬT 1: SURROGATE KEY")
    print("═" * 60)

    # Simulate: 3 nguồn cùng dùng ID = 100
    erp_order_100     = {"source": "erp",        "id": 100, "data": "order data"}
    payment_order_100 = {"source": "payment_gw", "id": 100, "data": "payment ref to erp order"}
    warehouse_prod_100 = {"source": "warehouse", "id": 100, "data": "product data"}

    print("\nRaw IDs từ 3 nguồn:")
    print(f"  ERP order_id=100        → source_id giống nhau!")
    print(f"  Payment order_ref=100   → source_id giống nhau!")
    print(f"  Warehouse product_id=100→ source_id giống nhau!")

    print("\nSau khi apply surrogate key:")
    for record in [erp_order_100, payment_order_100, warehouse_prod_100]:
        sk = make_surrogate_key(record["source"], record["id"])
        print(f"  MD5('{record['source']}:100') = {sk[:16]}... ← UNIQUE per system")

    print("\nKết quả: 3 records cùng source_id=100 nhưng 3 surrogate_key KHÁC NHAU")
    print("→ Fact table không bị collision khi JOIN")

    print("\n[PySpark code tương đương]")
    print("""
    # Trong Spark Silver transformation job:
    from pyspark.sql.functions import md5, concat, col, lit

    # Với ERP orders:
    erp_silver = erp_bronze.withColumn(
        "surrogate_order_key",
        md5(concat(lit("erp:"), col("order_id").cast("string")))
    )

    # Với Payment events — dùng order_id là ERP ref:
    payment_silver = payment_bronze.withColumn(
        "surrogate_order_key",                    # CÙNG column name
        md5(concat(lit("erp:"), col("order_id").cast("string")))  # CÙNG prefix "erp:"
        # ↑ Vì payment.order_id là FK về ERP.orders.id
        # → surrogate_key phải match với ERP silver
    )

    # JOIN sẽ work vì cả 2 bên đều dùng MD5("erp:<order_id>")
    fact = payment_silver.join(
        erp_silver,
        on="surrogate_order_key",
        how="left"
    )
    """)


# ═══════════════════════════════════════════════════════════════════
#  PHẦN 2: DEDUPLICATION
#  Xử lý duplicate txn_id, duplicate events từ retry
# ═══════════════════════════════════════════════════════════════════

def deduplicate_events(events: list[dict]) -> list[dict]:
    """
    Dedup events dựa trên business key (txn_id, _event_id).

    HAI LOẠI DUPLICATE DE phải xử lý:
    ────────────────────────────────────
    1. TECHNICAL DUPLICATE: cùng _event_id
       Nguyên nhân: NiFi retry, Kafka at-least-once delivery
       → Giữ 1, bỏ phần còn lại
       → Đây là idempotent dedup

    2. BUSINESS DUPLICATE: cùng txn_id nhưng khác _event_id
       Nguyên nhân: payment client retry gửi cùng transaction 2 lần
       → Giữ record ĐẦU TIÊN (earliest _ingested_at)
       → Bỏ record sau (là retry)

    Trong PySpark thực tế, bạn viết:
        from pyspark.sql.functions import row_number
        from pyspark.sql.window import Window

        # Technical dedup — by _event_id
        df = df.dropDuplicates(["_event_id"])

        # Business dedup — by txn_id, giữ record sớm nhất
        window = Window.partitionBy("transaction_id") \\
                       .orderBy(col("_ingested_at").asc())

        df = df.withColumn("rn", row_number().over(window)) \\
               .filter(col("rn") == 1) \\
               .drop("rn")
    """
    # Step 1: Technical dedup by _event_id
    seen_event_ids = set()
    after_tech_dedup = []
    for e in events:
        eid = e.get("_event_id")
        if eid not in seen_event_ids:
            seen_event_ids.add(eid)
            after_tech_dedup.append(e)

    # Step 2: Business dedup by txn_id — giữ earliest
    txn_map: dict[str, dict] = {}
    for e in after_tech_dedup:
        payload = e.get("payload", e)
        txn_id  = payload.get("transaction_id")
        if not txn_id:
            continue
        if txn_id not in txn_map:
            txn_map[txn_id] = e
        else:
            # So sánh timestamp — giữ record sớm hơn
            existing_ts = txn_map[txn_id].get("_ingested_at", "")
            current_ts  = e.get("_ingested_at", "")
            if current_ts < existing_ts:
                txn_map[txn_id] = e

    return list(txn_map.values())


def demo_deduplication():
    print("\n" + "═" * 60)
    print("  KỸ THUẬT 2: DEDUPLICATION")
    print("═" * 60)

    base_time = datetime.now()

    # Simulate: payment client retry → cùng txn_id, 2 _event_id khác nhau
    events = [
        {
            "_event_id":    "evt-001",
            "_event_type":  "payment.created",
            "_ingested_at": (base_time).isoformat(),
            "_quality_flag": "DIRTY",
            "payload": {"transaction_id": "TXN-ABC123", "amount": 500_000, "order_id": 101}
        },
        {
            "_event_id":    "evt-002",           # khác _event_id → technical dup
            "_event_type":  "payment.created",
            "_ingested_at": (base_time + timedelta(seconds=2)).isoformat(),
            "_quality_flag": "DIRTY",
            "payload": {"transaction_id": "TXN-ABC123", "amount": 500_000, "order_id": 101}
            # ↑ Cùng txn_id → business dup (retry)
        },
        {
            "_event_id":    "evt-003",
            "_event_type":  "payment.created",
            "_ingested_at": (base_time + timedelta(seconds=1)).isoformat(),
            "_quality_flag": "CLEAN",
            "payload": {"transaction_id": "TXN-XYZ789", "amount": 200_000, "order_id": 102}
        },
    ]

    print(f"\nInput: {len(events)} events")
    for e in events:
        txn = e["payload"]["transaction_id"]
        print(f"  _event_id={e['_event_id']} | txn_id={txn} | ts={e['_ingested_at'][11:19]}")

    result = deduplicate_events(events)

    print(f"\nOutput sau dedup: {len(result)} events")
    for e in result:
        txn = e["payload"]["transaction_id"]
        print(f"  _event_id={e['_event_id']} | txn_id={txn} ← KEPT")

    print(f"\n→ Loại bỏ {len(events) - len(result)} duplicate(s)")

    print("\n[PySpark code tương đương]")
    print("""
    from pyspark.sql.functions import row_number, col
    from pyspark.sql.window import Window

    # Step 1: Technical dedup (NiFi/Kafka at-least-once)
    df = df.dropDuplicates(["_event_id"])

    # Step 2: Business dedup (payment retry)
    window_spec = Window \\
        .partitionBy("transaction_id") \\
        .orderBy(col("_ingested_at").asc())   # giữ earliest

    df = (df
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )
    """)


# ═══════════════════════════════════════════════════════════════════
#  PHẦN 3: LATE ARRIVAL HANDLING với WATERMARK
#  Xử lý race condition: payment đến trước order
# ═══════════════════════════════════════════════════════════════════

def handle_late_arrival(payment_events: list[dict],
                         erp_orders: list[dict],
                         watermark_minutes: int = 10) -> dict:
    """
    Xử lý late arrival: payment event đến TRƯỚC order event từ CDC.

    VẤN ĐỀ:
    ────────
    t=0: payment.created POST lên NiFi (vì dirty: post trước commit)
    t=0: Spark batch nhận payment event → JOIN với ERP orders → MISS
    t=2: ERP CDC commit order → Spark nhận order event (quá muộn)
    → fact table có payment record nhưng NULL cho tất cả ERP columns

    SOLUTION: Watermark
    ────────────────────
    Spark giữ lại events trong "watermark window" (10 phút).
    Nếu payment đến trước order, Spark CHƯA output ngay.
    Spark đợi tối đa 10 phút để order CDC arrive.
    Sau 10 phút mà vẫn không có order → route về Dead Letter.

    Trong PySpark Structured Streaming thực tế:
        erp_stream = spark.readStream.format("kafka") \\
            .option("subscribe", "erp.events").load() \\
            .withWatermark("_ingested_at", "10 minutes")

        payment_stream = spark.readStream.format("kafka") \\
            .option("subscribe", "payment.events").load() \\
            .withWatermark("_ingested_at", "10 minutes")

        # Stream-stream JOIN với watermark
        joined = payment_stream.join(
            erp_stream,
            on=[
                payment_stream.order_id == erp_stream.order_id,
                # Điều kiện thời gian: payment không được đến quá sớm so với order
                payment_stream._ingested_at >= erp_stream._ingested_at - expr("INTERVAL 10 MINUTES"),
                payment_stream._ingested_at <= erp_stream._ingested_at + expr("INTERVAL 10 MINUTES"),
            ],
            how="left"
        )
    """
    watermark_delta = timedelta(minutes=watermark_minutes)
    results = {"joined": [], "late_to_dlq": [], "waiting": []}

    # Index ERP orders by order_id để JOIN nhanh hơn
    erp_index = {o["payload"]["id"]: o for o in erp_orders}

    for payment in payment_events:
        payload     = payment.get("payload", {})
        order_id    = payload.get("order_id")
        payment_ts  = datetime.fromisoformat(payment["_ingested_at"])
        erp_order   = erp_index.get(order_id)

        if erp_order:
            erp_ts = datetime.fromisoformat(erp_order["_ingested_at"])
            lag    = payment_ts - erp_ts  # positive = payment đến sau order (bình thường)

            if abs(lag) <= watermark_delta:
                # Trong watermark window → JOIN thành công
                results["joined"].append({
                    "payment_id":    payload.get("payment_id"),
                    "order_id":      order_id,
                    "amount":        payload.get("amount"),
                    "customer_id":   erp_order["payload"].get("customer_id"),
                    "order_status":  erp_order["payload"].get("status"),
                    "_join_lag_sec": lag.total_seconds(),
                    "_join_result":  "SUCCESS",
                })
            else:
                # Lag > watermark → Late arrival → Dead Letter Queue
                results["late_to_dlq"].append({
                    **payment,
                    "_dlq_reason": f"Payment arrived {lag.total_seconds():.0f}s before order CDC — exceeds {watermark_minutes}min watermark",
                })
        else:
            # Order chưa arrive trong window — đang đợi
            results["waiting"].append({
                **payment,
                "_status": f"Waiting for ERP order #{order_id} CDC event",
            })

    return results


def demo_late_arrival():
    print("\n" + "═" * 60)
    print("  KỸ THUẬT 3: LATE ARRIVAL HANDLING (WATERMARK)")
    print("═" * 60)

    now = datetime.now()

    # Simulate: payment đến 3 giây trước order (race condition từ sim_payment.py)
    payment_events = [
        {
            "_event_id":    "pay-001",
            "_event_type":  "payment.created",
            "_ingested_at": now.isoformat(),           # payment đến t=0
            "_quality_flag": "DIRTY",
            "payload": {"payment_id": 1, "order_id": 101, "amount": 500_000}
        },
        {
            "_event_id":    "pay-002",
            "_event_type":  "payment.created",
            "_ingested_at": now.isoformat(),
            "_quality_flag": "DIRTY",
            "payload": {"payment_id": 2, "order_id": 999,  "amount": 200_000}
            # ↑ order 999 không tồn tại trong ERP — mismatched order_ref
        },
        {
            "_event_id":    "pay-003",
            "_event_type":  "payment.created",
            "_ingested_at": (now - timedelta(minutes=15)).isoformat(),  # quá muộn
            "_quality_flag": "DIRTY",
            "payload": {"payment_id": 3, "order_id": 102, "amount": 300_000}
        },
    ]

    erp_orders = [
        {
            "_event_id":    "erp-101",
            "_event_type":  "order.created",
            "_ingested_at": (now - timedelta(seconds=2)).isoformat(),  # order đến t=-2s (trước payment)
            "payload": {"id": 101, "customer_id": 55, "status": "pending"}
        },
        {
            "_event_id":    "erp-102",
            "_event_type":  "order.created",
            "_ingested_at": (now - timedelta(minutes=30)).isoformat(),  # order cũ
            "payload": {"id": 102, "customer_id": 66, "status": "processing"}
        },
    ]

    print(f"\nInput: {len(payment_events)} payment events, {len(erp_orders)} ERP orders")
    print(f"Watermark: 10 phút")

    results = handle_late_arrival(payment_events, erp_orders, watermark_minutes=10)

    print(f"\n✓ JOIN thành công: {len(results['joined'])}")
    for r in results["joined"]:
        print(f"  payment #{r['payment_id']} → order #{r['order_id']} | lag={r['_join_lag_sec']:.1f}s")

    print(f"\n✗ Late arrival → Dead Letter Queue: {len(results['late_to_dlq'])}")
    for r in results["late_to_dlq"]:
        print(f"  {r['_dlq_reason']}")

    print(f"\n⏳ Đang đợi (order chưa arrive): {len(results['waiting'])}")
    for r in results["waiting"]:
        print(f"  {r['_status']}")

    print("\n[PySpark Structured Streaming code tương đương]")
    print("""
    from pyspark.sql.functions import expr

    erp_stream = (spark.readStream
        .format("kafka")
        .option("subscribe", "erp.events")
        .load()
        .withWatermark("_ingested_at", "10 minutes"))

    payment_stream = (spark.readStream
        .format("kafka")
        .option("subscribe", "payment.events")
        .load()
        .withWatermark("_ingested_at", "10 minutes"))

    # Stream-stream JOIN: Spark tự đợi trong watermark window
    joined = payment_stream.join(
        erp_stream,
        on=[
            payment_stream["payload.order_id"] == erp_stream["payload.id"],
            payment_stream._ingested_at.between(
                erp_stream._ingested_at - expr("INTERVAL 10 MINUTES"),
                erp_stream._ingested_at + expr("INTERVAL 10 MINUTES"),
            )
        ],
        how="left"
    )

    # Records không JOIN được sau watermark → Dead Letter
    dlq = joined.filter(col("erp_stream.payload.id").isNull())
    """)


# ═══════════════════════════════════════════════════════════════════
#  PHẦN 4: QUALITY ROUTING
#  Route CLEAN → Silver, DIRTY → Dead Letter Queue
# ═══════════════════════════════════════════════════════════════════

def route_by_quality(events: list[dict]) -> dict[str, list]:
    """
    Route events dựa theo _quality_flag.

    SILVER layer chỉ nhận CLEAN records.
    Dead Letter Queue nhận DIRTY records để review và fix.
    QUARANTINE records cần manual review trước khi quyết định.

    Trong PySpark:
        clean      = df.filter(col("_quality_flag") == "CLEAN")
        dirty      = df.filter(col("_quality_flag") == "DIRTY")
        quarantine = df.filter(col("_quality_flag") == "QUARANTINE")

        # Ghi vào các destination khác nhau
        clean.write.mode("append").parquet("hdfs:///silver/payments/")
        dirty.write.mode("append").parquet("hdfs:///dead_letter/payments/")
        quarantine.write.mode("append").parquet("hdfs:///quarantine/payments/")

    Nhưng quality routing KHÔNG chỉ là filter đơn giản.
    DE thực tế còn phải:
    1. Enrich dirty records với error context
    2. Tính dirty_score để prioritize manual review
    3. Attempt auto-fix cho các lỗi có thể sửa tự động
    4. Emit metrics về dirty rate cho monitoring
    """
    routed = {"silver": [], "dead_letter": [], "quarantine": [], "auto_fixed": []}

    for event in events:
        flag         = event.get("_quality_flag", "CLEAN")
        payload      = event.get("payload", event)
        dirty_reason = payload.get("_dirty_reason", "")

        if flag == "CLEAN":
            routed["silver"].append(event)

        elif flag == "DIRTY":
            # Attempt auto-fix cho các lỗi đơn giản
            fixed, fix_note = attempt_auto_fix(payload, dirty_reason)
            if fixed:
                event["payload"] = fixed
                event["_quality_flag"]  = "CLEAN"
                event["_auto_fix_note"] = fix_note
                routed["auto_fixed"].append(event)
                routed["silver"].append(event)  # sau fix → vào Silver
            else:
                # Không auto-fix được → Dead Letter
                event["_dlq_ingested_at"] = datetime.now().isoformat()
                event["_retry_count"]     = event.get("_retry_count", 0) + 1
                routed["dead_letter"].append(event)

        elif flag == "QUARANTINE":
            routed["quarantine"].append(event)

    return routed


def attempt_auto_fix(payload: dict, dirty_reason: str) -> tuple[dict | None, str]:
    """
    Thử tự động sửa các lỗi đơn giản.

    DE thực tế: auto-fix chỉ cho lỗi có confidence cao và business rule rõ ràng.
    Không auto-fix những gì ambiguous (ví dụ: price=0 có thể là flash sale).

    Returns:
        (fixed_payload, fix_note) nếu fix được
        (None, "") nếu không fix được → Dead Letter
    """
    payload = payload.copy()

    # Auto-fix: SKU format normalization
    # Business rule rõ ràng: canonical SKU format là "SKU-XXXXXXXX" uppercase
    if "SKU format variant" in dirty_reason:
        raw_sku = payload.get("sku", "")
        # Strip mọi prefix variant, uppercase, thêm "SKU-"
        clean_sku = raw_sku.upper()
        for prefix in ["SKU_", "SKU", "sku_", "sku"]:
            if clean_sku.startswith(prefix.upper()):
                clean_sku = clean_sku[len(prefix):]
                break
        clean_sku = clean_sku.lstrip("-").rstrip("-VN")  # bỏ suffix region
        payload["sku"] = f"SKU-{clean_sku}"
        del payload["_dirty_reason"]
        return payload, f"SKU normalized: '{raw_sku}' → '{payload['sku']}'"

    # Auto-fix: phone too long → truncate
    if "phone too long" in dirty_reason:
        payload["phone"] = payload.get("phone", "")[:20]
        del payload["_dirty_reason"]
        return payload, f"Phone truncated to 20 chars"

    # Không auto-fix các lỗi sau — cần manual review:
    # - null_price: không biết đúng giá bao nhiêu
    # - negative_amount: không biết đây là refund hay lỗi
    # - mismatched_order_ref: không biết order_id đúng là gì
    # - duplicate_txn_id: đã handle ở dedup step
    return None, ""


def demo_quality_routing():
    print("\n" + "═" * 60)
    print("  KỸ THUẬT 4: QUALITY ROUTING")
    print("═" * 60)

    events = [
        {
            "_event_id":    "e-001", "_quality_flag": "CLEAN",
            "payload": {"product_id": 1, "sku": "SKU-AB1234CD", "price": 500_000}
        },
        {
            "_event_id":    "e-002", "_quality_flag": "DIRTY",
            "payload": {"product_id": 2, "sku": "skuAB1234CD-VN", "price": 300_000,
                        "_dirty_reason": "SKU format variant: 'skuAB1234CD-VN'"}
        },
        {
            "_event_id":    "e-003", "_quality_flag": "DIRTY",
            "payload": {"product_id": 3, "sku": "SKU-XY5678ZW", "price": None,
                        "_dirty_reason": "price is NULL — missing from source CSV"}
        },
        {
            "_event_id":    "e-004", "_quality_flag": "DIRTY",
            "payload": {"customer_id": 5, "phone": "0" + "1234567890" * 3,
                        "_dirty_reason": "phone too long: 31 chars (max 20)"}
        },
        {
            "_event_id":    "e-005", "_quality_flag": "QUARANTINE",
            "payload": {"order_id": 101, "amount": -500_000,
                        "_dirty_reason": "negative total_amount — ambiguous: refund or error"}
        },
    ]

    print(f"\nInput: {len(events)} events")
    result = route_by_quality(events)

    print(f"\n→ Silver (CLEAN):     {len(result['silver'])} records")
    for e in result["silver"]:
        flag = e.get("_quality_flag", "CLEAN")
        note = e.get("_auto_fix_note", "")
        label = f"[AUTO-FIXED] {note}" if note else "[CLEAN]"
        print(f"    _event_id={e['_event_id']} {label}")

    print(f"\n→ Dead Letter Queue:  {len(result['dead_letter'])} records")
    for e in result["dead_letter"]:
        reason = e.get("payload", {}).get("_dirty_reason", "unknown")
        print(f"    _event_id={e['_event_id']} | reason: {reason[:60]}")

    print(f"\n→ Quarantine:         {len(result['quarantine'])} records")
    for e in result["quarantine"]:
        reason = e.get("payload", {}).get("_dirty_reason", "unknown")
        print(f"    _event_id={e['_event_id']} | reason: {reason[:60]}")

    print(f"\n→ Auto-fixed → Silver:{len(result['auto_fixed'])} records")

    print("\n[PySpark code tương đương]")
    print("""
    # Read từ Kafka (cả 3 topics)
    df = spark.readStream.format("kafka") \\
        .option("subscribe", "erp.events,warehouse.events,payment.events") \\
        .load()

    # Parse JSON envelope
    schema = StructType([
        StructField("_source_system",  StringType()),
        StructField("_event_id",       StringType()),
        StructField("_quality_flag",   StringType()),
        StructField("_dirty_reason",   StringType()),
        StructField("payload",         MapType(StringType(), StringType())),
    ])
    df = df.withColumn("data", from_json(col("value").cast("string"), schema)) \\
           .select("data.*")

    # Route by quality flag
    clean      = df.filter(col("_quality_flag") == "CLEAN")
    dirty      = df.filter(col("_quality_flag") == "DIRTY")
    quarantine = df.filter(col("_quality_flag") == "QUARANTINE")

    # Write destinations
    (clean.writeStream
        .format("parquet")
        .option("path", "hdfs:///datalake/silver/")
        .option("checkpointLocation", "hdfs:///checkpoints/silver/")
        .start())

    (dirty.writeStream
        .format("parquet")
        .option("path", "hdfs:///datalake/dead_letter/")
        .option("checkpointLocation", "hdfs:///checkpoints/dead_letter/")
        .start())
    """)


# ═══════════════════════════════════════════════════════════════════
#  PHẦN 5: SKU HARMONIZATION
#  Normalize SKU format từ nhiều warehouse region
# ═══════════════════════════════════════════════════════════════════

def normalize_sku(raw_sku: str) -> tuple[str, bool]:
    """
    Normalize SKU về canonical format.

    VẤN ĐỀ thực tế:
    ─────────────────
    Warehouse region HCM: "SKU-AB1234CD"
    Warehouse region HN:  "SKUAB1234CD"   (bỏ dấu gạch)
    Legacy import:        "sku_ab1234cd"  (lowercase + underscore)
    International:        "SKU-AB1234CD-VN" (thêm suffix)
    Fully stripped:       "AB1234CD"

    Canonical format: "SKU-XXXXXXXX" (uppercase, có gạch, không suffix)

    Trong PySpark:
        from pyspark.sql.functions import regexp_replace, upper, when

        df = df.withColumn(
            "sku_canonical",
            regexp_replace(
                upper(regexp_replace(col("sku"), r"(?i)sku[-_]?", "")),
                r"-[A-Z]{2}$", ""   # bỏ suffix region như -VN, -HN
            )
        ).withColumn(
            "sku_canonical",
            concat(lit("SKU-"), col("sku_canonical"))
        )
    """
    if not raw_sku:
        return "", False

    import re
    upper_sku = raw_sku.upper().strip()

    # Bỏ prefix variants
    upper_sku = re.sub(r"^SKU[-_]?", "", upper_sku)

    # Bỏ suffix region (-VN, -HN, -HCM)
    upper_sku = re.sub(r"-[A-Z]{2,3}$", "", upper_sku)

    # Bỏ ký tự không hợp lệ
    upper_sku = re.sub(r"[^A-Z0-9]", "", upper_sku)

    if not upper_sku:
        return raw_sku, False  # không normalize được

    canonical = f"SKU-{upper_sku}"
    is_changed = canonical != raw_sku.upper()
    return canonical, is_changed


def demo_sku_harmonization():
    print("\n" + "═" * 60)
    print("  KỸ THUẬT 5: SKU HARMONIZATION")
    print("═" * 60)

    test_skus = [
        "SKU-AB1234CD",      # canonical — không thay đổi
        "SKUAB1234CD",       # missing dash
        "sku_ab1234cd",      # lowercase + underscore (legacy import)
        "SKU-AB1234CD-VN",   # có suffix region
        "AB1234CD",          # stripped hoàn toàn
        "SKU-AB1234CD-HCM",  # suffix dài hơn
        "",                  # empty — không normalize được
    ]

    print(f"\n{'Raw SKU':<25} {'Canonical SKU':<20} {'Changed?'}")
    print("-" * 55)
    for raw in test_skus:
        canonical, changed = normalize_sku(raw)
        status = "✓ NORMALIZED" if changed else ("— UNCHANGED" if canonical else "✗ FAILED")
        print(f"  {raw!r:<23} {canonical!r:<20} {status}")


# ═══════════════════════════════════════════════════════════════════
#  PHẦN 6: FULL PIPELINE SUMMARY
# ═══════════════════════════════════════════════════════════════════

def demo_full_pipeline():
    print("\n" + "═" * 60)
    print("  FULL HARMONIZATION PIPELINE — TỔNG KẾT")
    print("═" * 60)
    print("""
  Bước 1 — INGEST (NiFi → Kafka)
  ────────────────────────────────
  NiFi đọc CSV delta từ Warehouse, CDC từ ERP, HTTP POST từ Payment GW.
  Mỗi event được wrap trong source envelope (_source_system, _event_id, _quality_flag).
  Route vào 3 Kafka topics: erp.events, warehouse.events, payment.events.

  Bước 2 — BRONZE (Spark → HDFS)
  ────────────────────────────────
  Spark đọc raw JSON từ Kafka, KHÔNG transform.
  Chỉ thêm: _kafka_offset, _kafka_partition, _bronze_ingested_at.
  Ghi vào HDFS Bronze layer theo partition: source_system / event_date.
  Purpose: raw archive — nếu Silver bị lỗi có thể replay từ Bronze.

  Bước 3 — SILVER (Spark Harmonization — file này)
  ──────────────────────────────────────────────────
  3a. Parse envelope → extract payload fields
  3b. Dedup by _event_id (technical) + business key (txn_id, order_id)
  3c. Validate schema: required fields, type checking, range validation
  3d. Normalize: SKU canonicalization, phone truncation, email lowercase
  3e. Quality routing: CLEAN → Silver, DIRTY → Dead Letter, QUARANTINE → review
  3f. Surrogate key: MD5(source_system:source_id) cho mọi entity
  3g. Late arrival: withWatermark("_ingested_at", "10 minutes") cho stream-stream JOIN
  3h. Write Silver: Parquet, partitioned by source_system / event_date

  Bước 4 — GOLD (Spark → Hive Star Schema)
  ──────────────────────────────────────────
  JOIN Silver tables:
    fact_orders = orders JOIN payments JOIN shipping
  Dimension tables:
    dim_customers, dim_products (từ Silver ERP + Warehouse)
    dim_payments, dim_shipping  (từ Silver Payment GW)
  Register schema với Hive Metastore.
  AI Agent query qua HiveQL / SparkSQL.

  Điểm mấu chốt:
  ─────────────────
  Bronze = RAW (không sửa, không xóa — audit trail)
  Silver = CLEAN (đã harmonize, có surrogate key)
  Gold   = ANALYTICAL (star schema, tối ưu cho OLAP query)
  DLQ    = DIRTY (chờ fix, có thể replay về Silver sau khi fix)
    """)


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Spark Harmonization Layer Demo")
    parser.add_argument("--demo", default="all",
                        choices=["all", "surrogate", "dedup", "late_join",
                                 "quality", "sku", "pipeline"],
                        help="Demo kỹ thuật cụ thể")
    args = parser.parse_args()

    print("=" * 60)
    print("  SPARK HARMONIZATION LAYER — DE-grade demo")
    print("=" * 60)

    demo_map = {
        "surrogate": demo_surrogate_key,
        "dedup":     demo_deduplication,
        "late_join": demo_late_arrival,
        "quality":   demo_quality_routing,
        "sku":       demo_sku_harmonization,
        "pipeline":  demo_full_pipeline,
    }

    if args.demo == "all":
        for fn in demo_map.values():
            fn()
    else:
        demo_map[args.demo]()

    print("\n" + "=" * 60)
    print("  Chạy từng kỹ thuật riêng:")
    print("  python spark_harmonization.py --demo surrogate")
    print("  python spark_harmonization.py --demo dedup")
    print("  python spark_harmonization.py --demo late_join")
    print("  python spark_harmonization.py --demo quality")
    print("  python spark_harmonization.py --demo sku")
    print("  python spark_harmonization.py --demo pipeline")
    print("=" * 60)


if __name__ == "__main__":
    main()
