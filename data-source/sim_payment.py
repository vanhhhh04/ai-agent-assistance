"""
sim_payment.py  (DE-grade version)
====================================
Nguon: JSON / Payment Gateway
Bang:  payments, shipping, reviews, feedback

THAY DOI SO VOI VERSION CU:
  + Source envelope trong moi NiFi POST (_source_system, _event_id, _op_type)
  + Dirty data injection: duplicate txn_id, mismatched order_ref, late arrival
  + Race condition simulation: POST len NiFi TRUOC khi commit → Spark late join
  + Anti-pattern fix: shipping khong con UPDATE orders.status truc tiep
    (thay vao do: emit event de ERP tu xu ly — domain boundary respect)
  + Idempotency key: moi event co _event_id de Spark dedup

Khong co bootstrap — chi co realtime loop:
    - Moi 3s : tao payment cho order moi   → INSERT payments
    - Moi 5s : cap nhat payment status     → UPDATE payments
    - Moi 15s: cap nhat shipping status    → INSERT/UPDATE shipping
    - Moi 30s: them review sau giao hang   → INSERT reviews
    - Moi 45s: them feedback / khieu nai   → INSERT feedback

Moi event duoc:
    1. INSERT/UPDATE vao PostgreSQL
    2. Wrap trong source envelope
    3. POST toi NiFi ListenHTTP → payment.events Kafka topic

Cai dat:
    pip install faker psycopg2-binary requests

Chay:
    python sim_payment.py
"""

import json
import os
import uuid
import random
import time
import logging
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_values
import requests
from faker import Faker

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "ecommerce"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

NIFI_ENDPOINT = os.getenv("NIFI_ENDPOINT", "http://localhost:8080/payment-events")

INTERVAL_NEW_PAYMENT    = 3
INTERVAL_UPDATE_PAYMENT = 5
INTERVAL_SHIPPING       = 15
INTERVAL_REVIEW         = 30
INTERVAL_FEEDBACK       = 45

# ── Dirty data control ──────────────────────
DIRTY_RATE = 0.05
DIRTY_CONFIG = {
    "duplicate_txn_id":      True,  # retry gửi cùng txn_id → Spark phải dedup
    "mismatched_order_ref":  True,  # order_ref trỏ đến order không tồn tại
    "late_arrival":          True,  # POST lên NiFi trước khi commit vào DB (race condition)
    "wrong_amount":          True,  # amount khác với orders.total_amount
    "invalid_payment_method":True,  # method không trong enum cho phép
    "duplicate_review":      True,  # review cùng (order_id, product_id) gửi 2 lần
}

SOURCE_SYSTEM   = "payment_gw"
SCHEMA_VERSION  = "1.0"

CDC_OP_CREATE = "c"
CDC_OP_UPDATE = "u"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PAYMENT-GW] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

fake = Faker("vi_VN")
Faker.seed(3001)
random.seed(3001)

# Pool txn_ids đã dùng — để inject duplicate có chủ ý
_used_txn_ids: list[str] = []
# Pool (order_id, product_id) đã review — để inject duplicate review
_reviewed_pairs: list[tuple] = []


# ─────────────────────────────────────────────
#  SOURCE ENVELOPE
# ─────────────────────────────────────────────
def make_envelope(event_type: str, payload: dict,
                  op_type: str = CDC_OP_CREATE,
                  quality_flag: str = "CLEAN") -> dict:
    """
    Payment GW envelope — giống format webhook thực tế.

    Điểm khác với ERP envelope:
    - Payment GW KHÔNG có surrogate_hint vì nó không biết ERP's internal ID scheme
    - Spark phải resolve: payload.order_id (ERP) → dim_orders.surrogate_key
    - Đây là JOIN quan trọng nhất trong pipeline: payment JOIN orders by order_id
    """
    return {
        "_source_system":  SOURCE_SYSTEM,
        "_schema_version": SCHEMA_VERSION,
        "_event_id":       str(uuid.uuid4()),   # Spark dùng field này để dedup
        "_event_type":     event_type,
        "_op_type":        op_type,
        "_ingested_at":    datetime.now().isoformat(),
        "_quality_flag":   quality_flag,
        # Payment GW không có surrogate_hint — Spark tự resolve qua JOIN
        "payload": payload,
    }


# ─────────────────────────────────────────────
#  DIRTY DATA INJECTORS
# ─────────────────────────────────────────────
def maybe_dirty_payment(payload: dict) -> tuple[dict, str]:
    """
    Inject lỗi vào payment event.

    Payment dirty data là nguy hiểm nhất vì:
    - Liên quan đến tiền
    - Khó phát hiện (amount sai 1% không ai thấy)
    - Race condition với ERP là phổ biến nhất trong microservices thực tế
    """
    if random.random() > DIRTY_RATE:
        return payload, "CLEAN"

    active = [k for k, v in DIRTY_CONFIG.items() if v]
    dirty_type = random.choice(active)
    payload = payload.copy()

    if dirty_type == "duplicate_txn_id":
        # Xảy ra khi: network timeout → client retry → 2 events cùng txn_id
        # Spark phải detect và giữ lại chỉ 1
        if _used_txn_ids:
            payload["transaction_id"] = random.choice(_used_txn_ids)
            payload["_dirty_reason"]  = f"duplicate txn_id: '{payload['transaction_id']}' — network retry"
        else:
            return payload, "CLEAN"

    elif dirty_type == "mismatched_order_ref":
        # Xảy ra khi: race condition — payment created trước order committed
        # order_id = 999999999 sẽ không tồn tại trong ERP
        fake_order_id = random.randint(9_000_000, 9_999_999)
        payload["order_id"]      = fake_order_id
        payload["_dirty_reason"] = f"order_ref {fake_order_id} not found in ERP — race condition"

    elif dirty_type == "wrong_amount":
        # Xảy ra khi: currency conversion rounding, hoặc promotion applied twice
        original = payload.get("amount", 100_000)
        delta    = random.uniform(-0.05, 0.05)  # ±5% sai lệch
        payload["amount"]        = round(original * (1 + delta), 2)
        payload["_dirty_reason"] = (
            f"amount mismatch: {payload['amount']} vs expected {original} "
            f"({delta:+.1%}) — currency rounding or double promotion"
        )

    elif dirty_type == "invalid_payment_method":
        invalid_methods = ["bitcoin", "barter", "TRANSFER", "e-wallet", ""]
        payload["payment_method"] = random.choice(invalid_methods)
        payload["_dirty_reason"]  = f"invalid method: '{payload['payment_method']}' — not in allowed enum"

    elif dirty_type == "duplicate_review":
        if _reviewed_pairs:
            oid, pid = random.choice(_reviewed_pairs)
            payload["order_id"]      = oid
            payload["product_id"]    = pid
            payload["_dirty_reason"] = f"duplicate review for order={oid}, product={pid}"
        else:
            return payload, "CLEAN"

    return payload, "DIRTY"


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def post_to_nifi(envelope: dict):
    """
    POST envelope lên NiFi ListenHTTP.
    Nếu NiFi chưa chạy, chỉ log — không crash.

    Lý do POST envelope thay vì raw payload:
    → NiFi cần _event_type để route vào đúng Kafka topic
    → NiFi cần _quality_flag để route DIRTY events vào Dead Letter Queue
    → NiFi cần _event_id để downstream (Spark) có thể dedup
    """
    try:
        requests.post(
            NIFI_ENDPOINT,
            data=json.dumps(envelope, default=str),
            headers={
                "Content-Type":  "application/json",
                "X-Event-Type":  envelope.get("_event_type", "unknown"),
                "X-Quality-Flag": envelope.get("_quality_flag", "CLEAN"),
                "X-Event-ID":    envelope.get("_event_id", ""),
            },
            timeout=3,
        )
    except requests.exceptions.ConnectionError:
        log.debug(f"NiFi chưa sẵn sàng — event {envelope.get('_event_type')} chỉ lưu vào DB")
    except Exception as e:
        log.warning(f"NiFi POST thất bại: {e}")


def to_serializable(d: dict) -> dict:
    return json.loads(json.dumps(d, default=str))


PAYMENT_METHODS = ["credit_card", "momo", "vnpay", "bank_transfer", "cod", "paypal"]
CARRIERS        = ["GHN", "GHTK", "ViettelPost", "J&T Express", "Ninja Van"]
SHIPPING_STATUS_FLOW = ["pending", "picked_up", "in_transit", "out_for_delivery", "delivered"]
PAYMENT_STATUS_FLOW  = ["pending", "processing", "success"]


def gateway_meta(method: str) -> dict:
    if method == "credit_card":
        return {"gateway": "Stripe",
                "card_brand": random.choice(["Visa", "Mastercard", "JCB"]),
                "card_last4": str(random.randint(1000, 9999)),
                "stripe_charge_id": f"ch_{fake.bothify('?' * 24)}"}
    if method == "momo":
        return {"gateway": "MoMo",
                "momo_order_id": f"MOMO{random.randint(10**9, 10**10-1)}",
                "momo_trans_id": random.randint(10**12, 10**13-1)}
    if method == "vnpay":
        return {"gateway": "VNPay",
                "vnp_TxnRef": str(random.randint(10**9, 10**10-1)),
                "vnp_BankCode": random.choice(["VCB", "TCB", "ACB", "BIDV", "MB"]),
                "vnp_ResponseCode": "00"}
    if method == "bank_transfer":
        return {"gateway": "Bank",
                "bank_name": random.choice(["Vietcombank", "Techcombank", "ACB"]),
                "ref_code": f"REF{random.randint(100000, 999999)}"}
    if method == "paypal":
        return {"gateway": "PayPal",
                "paypal_order": f"PAYID-{fake.bothify('?' * 20).upper()}",
                "payer_email": fake.email()}
    return {"gateway": "COD", "collected_by": fake.name()}


# ─────────────────────────────────────────────
#  EVENT HANDLERS
# ─────────────────────────────────────────────
def handle_new_payment(cur, conn):
    cur.execute("""
        SELECT o.id, o.total_amount FROM orders o
        LEFT JOIN payments p ON p.order_id = o.id
        WHERE p.id IS NULL AND o.status != 'cancelled'
        ORDER BY RANDOM() LIMIT 5
    """)
    rows = cur.fetchall()
    if not rows:
        return

    for order_id, amount in rows:
        method = random.choice(PAYMENT_METHODS)
        txn_id = f"TXN-{fake.bothify('??######??').upper()}"
        _used_txn_ids.append(txn_id)

        payload = {
            "order_id":       order_id,
            "payment_method": method,
            "amount":         float(amount),
            "status":         "pending",
            "transaction_id": txn_id,
            "gateway_meta":   gateway_meta(method),
        }

        # Inject dirty TRƯỚC khi decide whether to POST trước hay sau commit
        dirty_payload, quality_flag = maybe_dirty_payment(payload)

        # ── Race condition simulation ──────────────────────────────
        # DIRTY CONFIG: late_arrival = POST lên NiFi TRƯỚC khi commit vào DB
        # Đây là race condition thực tế phổ biến nhất trong payment microservice:
        #
        # Timeline thực tế (DIRTY):
        #   t=0: Payment service gọi POST /payment → NiFi nhận event
        #   t=0: Spark job nhận payment.created từ Kafka
        #   t=1: ERP CDC chưa commit order → Spark JOIN miss → NULL ERP fields
        #   t=2: Payment service INSERT vào DB → commit
        #
        # Timeline đúng (CLEAN):
        #   t=0: DB commit thành công
        #   t=1: Payment service POST lên NiFi
        #   t=2: Spark nhận event → ERP order đã có trong Silver → JOIN OK
        #
        # Spark xử lý bằng: withWatermark("_ingested_at", "10 minutes")
        if DIRTY_CONFIG.get("late_arrival") and quality_flag == "DIRTY":
            # POST TRƯỚC commit → race condition
            envelope = make_envelope("payment.created", to_serializable(dirty_payload),
                                     quality_flag=quality_flag)
            post_to_nifi(envelope)
            log.info(f"  [DIRTY] Late arrival: POST trước commit cho order #{order_id}")

        # DB INSERT (sau POST nếu dirty, trước POST nếu clean)
        safe_method = method if method in PAYMENT_METHODS else "cod"
        cur.execute(
            """INSERT INTO payments
               (order_id, payment_method, amount, status, transaction_id, paid_at, created_at)
               VALUES (%s, %s, %s, %s, %s, NULL, NOW())
               ON CONFLICT DO NOTHING
               RETURNING id""",
            (order_id, safe_method, float(amount), "pending", txn_id),
        )
        result = cur.fetchone()
        if not result:
            continue
        payment_id = result[0]
        conn.commit()

        # POST SAU commit nếu CLEAN
        if not (DIRTY_CONFIG.get("late_arrival") and quality_flag == "DIRTY"):
            dirty_payload["payment_id"] = payment_id
            envelope = make_envelope("payment.created", to_serializable(dirty_payload),
                                     quality_flag=quality_flag)
            post_to_nifi(envelope)

        log.info(
            f"INSERT payment #{payment_id} order #{order_id} via {method} | "
            f"flag={quality_flag}"
        )


def handle_update_payment(cur, conn):
    for from_status, to_status in [("pending", "processing"), ("processing", "success")]:
        cur.execute(
            """SELECT id, order_id, payment_method, amount, transaction_id
               FROM payments WHERE status=%s ORDER BY RANDOM() LIMIT 10""",
            (from_status,),
        )
        rows = cur.fetchall()
        for pid, order_id, method, amount, txn_id in rows:
            cur.execute(
                """UPDATE payments
                   SET status=%s,
                       paid_at = CASE WHEN %s='success' THEN NOW() ELSE NULL END,
                       created_at = created_at
                   WHERE id=%s""",
                (to_status, to_status, pid),
            )
            conn.commit()

            payload = {
                "payment_id":     pid,
                "order_id":       order_id,
                "transaction_id": txn_id,
                "status_from":    from_status,
                "status_to":      to_status,
                "amount":         float(amount),
            }
            envelope = make_envelope("payment.status_updated", to_serializable(payload),
                                     op_type=CDC_OP_UPDATE)
            post_to_nifi(envelope)
            log.info(f"UPDATE payment #{pid}: {from_status} → {to_status}")


def handle_shipping(cur, conn):
    # INSERT shipping mới cho orders đã có payment success nhưng chưa có shipping
    cur.execute("""
        SELECT o.id FROM orders o
        JOIN payments p ON p.order_id = o.id
        LEFT JOIN shipping s ON s.order_id = o.id
        WHERE p.status = 'success' AND s.id IS NULL
        AND o.status NOT IN ('cancelled', 'refunded')
        ORDER BY RANDOM() LIMIT 3
    """)
    for (order_id,) in cur.fetchall():
        carrier  = random.choice(CARRIERS)
        tracking = f"TRK{random.randint(10**8, 10**9-1)}"
        est_del  = (datetime.now() + timedelta(days=random.randint(1, 7))).date()
        cur.execute(
            """INSERT INTO shipping
               (order_id, carrier, tracking_number, status, shipped_at,
                estimated_delivery, delivered_at, created_at, updated_at)
               VALUES (%s, %s, %s, 'pending', NULL, %s, NULL, NOW(), NOW())
               RETURNING id""",
            (order_id, carrier, tracking, est_del),
        )
        ship_id = cur.fetchone()[0]
        conn.commit()

        payload = {"shipping_id": ship_id, "order_id": order_id,
                   "carrier": carrier, "tracking_number": tracking,
                   "status": "pending", "estimated_delivery": str(est_del)}
        envelope = make_envelope("shipping.created", to_serializable(payload))
        post_to_nifi(envelope)
        log.info(f"INSERT shipping #{ship_id} order #{order_id} via {carrier}")

    # UPDATE shipping status theo flow
    status_pairs = [
        ("pending", "picked_up"),
        ("picked_up", "in_transit"),
        ("in_transit", "out_for_delivery"),
        ("out_for_delivery", "delivered"),
    ]
    for from_s, to_s in status_pairs:
        cur.execute(
            """SELECT id, order_id, carrier, tracking_number
               FROM shipping WHERE status=%s ORDER BY RANDOM() LIMIT 5""",
            (from_s,),
        )
        for ship_id, order_id, carrier, tracking in cur.fetchall():
            cur.execute(
                """UPDATE shipping
                   SET status=%s, updated_at=NOW(),
                       shipped_at    = CASE WHEN %s='picked_up'  THEN NOW() ELSE shipped_at END,
                       delivered_at  = CASE WHEN %s='delivered'  THEN NOW() ELSE delivered_at END
                   WHERE id=%s""",
                (to_s, to_s, to_s, ship_id),
            )
            conn.commit()

            payload = {"shipping_id": ship_id, "order_id": order_id,
                       "carrier": carrier, "tracking_number": tracking,
                       "status_from": from_s, "status_to": to_s}
            envelope = make_envelope("shipping.status_updated", to_serializable(payload),
                                     op_type=CDC_OP_UPDATE)
            post_to_nifi(envelope)
            log.info(f"UPDATE shipping #{ship_id}: {from_s} → {to_s}")

            # FIX ANTI-PATTERN: Payment GW không được UPDATE orders.status trực tiếp
            # Version cũ: cur.execute("UPDATE orders SET status='delivered' WHERE id=%s", ...)
            # → Vi phạm domain boundary: Payment domain viết vào ERP domain
            #
            # DE-grade solution: emit event, để ERP tự xử lý
            # Trong thực tế: publish "shipping.delivered" event lên Kafka
            # ERP consumer nghe và tự UPDATE orders.status
            # Ở đây chúng ta emit qua NiFi để simulate pattern này
            if to_s == "delivered":
                delivered_event = {
                    "order_id":    order_id,
                    "shipping_id": ship_id,
                    "event":       "order.delivered_by_shipping",
                    "note":        "ERP should consume this and update orders.status",
                }
                delivered_envelope = make_envelope(
                    "order.delivered_notification",
                    to_serializable(delivered_event),
                )
                post_to_nifi(delivered_envelope)
                log.info(
                    f"  [DE-pattern] Emitted order.delivered_notification for order #{order_id} "
                    f"(ERP sẽ tự UPDATE status — không vi phạm domain boundary)"
                )


def handle_review(cur, conn):
    comments = [
        "Sản phẩm rất tốt, đúng mô tả!",
        "Giao hàng nhanh, đóng gói cẩn thận.",
        "Chất lượng ổn với giá tiền, sẽ mua lại.",
        "Tuyệt vời! Recommend mọi người.",
        "Hơi thất vọng, không như kỳ vọng.",
        "Sản phẩm chính hãng, rất hài lòng.",
        "Chat lượng vượt mong đợi! 5 sao.",
        "Bình thường, không có gì đặc biệt.",
    ]
    cur.execute("""
        SELECT o.id, oi.product_id, o.customer_id
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.id
        LEFT JOIN reviews r ON r.order_id = o.id AND r.product_id = oi.product_id
        WHERE o.status = 'delivered' AND r.id IS NULL
        ORDER BY RANDOM() LIMIT 3
    """)
    rows = cur.fetchall()
    for order_id, product_id, customer_id in rows:
        rating  = random.choices([1, 2, 3, 4, 5], weights=[3, 5, 12, 35, 45])[0]
        comment = random.choice(comments)
        title   = f"Sản phẩm {'tuyệt vời' if rating >= 4 else 'bình thường'}"

        payload = {
            "product_id":  product_id,
            "customer_id": customer_id,
            "order_id":    order_id,
            "rating":      rating,
            "title":       title,
            "comment":     comment,
            "is_verified": True,
        }
        dirty_payload, quality_flag = maybe_dirty_payment(payload)

        # Track reviewed pairs — để có thể inject duplicate sau này
        _reviewed_pairs.append((order_id, product_id))

        cur.execute(
            """INSERT INTO reviews
               (product_id, customer_id, order_id, rating, title, comment, is_verified, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,TRUE,NOW())
               ON CONFLICT DO NOTHING
               RETURNING id""",
            (product_id, customer_id, order_id, rating, title, comment),
        )
        result = cur.fetchone()
        if not result:
            continue
        review_id = result[0]
        conn.commit()

        dirty_payload["review_id"] = review_id
        envelope = make_envelope("review.created", to_serializable(dirty_payload),
                                 quality_flag=quality_flag)
        post_to_nifi(envelope)
        log.info(f"INSERT review #{review_id} product #{product_id} rating={rating} | flag={quality_flag}")


def handle_feedback(cur, conn):
    types    = ["complaint", "suggestion", "compliment", "other"]
    subjects = {
        "complaint":  ["Sản phẩm lỗi khi nhận", "Giao hàng sai địa chỉ", "Không nhận được hàng"],
        "suggestion": ["Thêm tính năng trả góp", "Cải thiện giao diện"],
        "compliment": ["Nhân viên hỗ trợ tận tình", "Giao hàng nhanh hơn dự kiến"],
        "other":      ["Hỏi về chính sách đổi trả", "Cần hóa đơn VAT"],
    }
    priorities = ["low", "medium", "high", "urgent"]

    cur.execute("SELECT id FROM customers ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return
    customer_id = row[0]

    cur.execute("SELECT id FROM orders WHERE customer_id=%s ORDER BY RANDOM() LIMIT 1", (customer_id,))
    row      = cur.fetchone()
    order_id = row[0] if row else None

    fb_type  = random.choice(types)
    subject  = random.choice(subjects[fb_type])
    priority = "urgent" if fb_type == "complaint" and random.random() > 0.7 else random.choice(priorities)

    cur.execute(
        """INSERT INTO feedback
           (customer_id, order_id, type, subject, message, status, priority, created_at, resolved_at)
           VALUES (%s,%s,%s,%s,%s,'open',%s,NOW(),NULL) RETURNING id""",
        (customer_id, order_id, fb_type, subject, fake.paragraph(nb_sentences=2), priority),
    )
    fb_id = cur.fetchone()[0]
    conn.commit()

    payload = {"feedback_id": fb_id, "customer_id": customer_id, "order_id": order_id,
               "type": fb_type, "subject": subject, "priority": priority, "status": "open"}
    envelope = make_envelope("feedback.created", to_serializable(payload))
    post_to_nifi(envelope)
    log.info(f"INSERT feedback #{fb_id} type={fb_type} priority={priority}")


# ─────────────────────────────────────────────
#  REALTIME LOOP
# ─────────────────────────────────────────────
def realtime_loop():
    log.info("Bắt đầu realtime Payment GW loop (Ctrl+C để dừng)...")
    last_new_payment    = time.time()
    last_update_payment = time.time()
    last_shipping       = time.time()
    last_review         = time.time()
    last_feedback       = time.time()

    while True:
        now  = time.time()
        conn = get_conn()
        cur  = conn.cursor()
        try:
            if now - last_new_payment >= INTERVAL_NEW_PAYMENT:
                handle_new_payment(cur, conn)
                last_new_payment = now

            if now - last_update_payment >= INTERVAL_UPDATE_PAYMENT:
                handle_update_payment(cur, conn)
                last_update_payment = now

            if now - last_shipping >= INTERVAL_SHIPPING:
                handle_shipping(cur, conn)
                last_shipping = now

            if now - last_review >= INTERVAL_REVIEW:
                handle_review(cur, conn)
                last_review = now

            if now - last_feedback >= INTERVAL_FEEDBACK:
                handle_feedback(cur, conn)
                last_feedback = now

        except Exception as e:
            log.error(f"Lỗi trong loop: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            cur.close()
            conn.close()

        time.sleep(1)


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  sim_payment.py — DE-grade version")
    log.info(f"  Dirty rate: {DIRTY_RATE:.0%} | Source: {SOURCE_SYSTEM} v{SCHEMA_VERSION}")
    log.info(f"  NiFi endpoint: {NIFI_ENDPOINT}")
    log.info("=" * 60)

    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM orders")
    order_count = cur.fetchone()[0]
    cur.close(); conn.close()

    if order_count == 0:
        log.error("Chưa có orders! Hãy chạy sim_erp.py trước.")
        return

    log.info(f"Tìm thấy {order_count:,} orders — bắt đầu simulate payment events...")
    realtime_loop()


if __name__ == "__main__":
    main()
