# DATABASE_ANALYSIS.md

> **Source files**: `data/initial_table.sql`, `spark/jobs/silver_transform.py`, `spark/jobs/gold_transform.py`.
> Three logical databases coexist:
> 1. **PostgreSQL `ecommerce`** — operational source-of-truth (OLTP)
> 2. **PostgreSQL `airflow`** — Airflow metadata
> 3. **PostgreSQL `metastore`** — Hive Metastore catalog (separate container)
> 4. **HDFS Parquet + Hive `gold` DB** — analytical star schema (OLAP)

---

## 1. ER Diagram (Operational — PostgreSQL `ecommerce`)

```mermaid
erDiagram
    customers   ||--o{ addresses    : has
    customers   ||--o{ orders       : places
    customers   ||--o{ reviews      : writes
    customers   ||--o{ feedback     : sends
    addresses   ||--o{ orders       : ships_to
    addresses   ||--o{ orders       : bills_to
    coupons     ||--o{ orders       : discounts
    orders      ||--|{ order_items  : contains
    orders      ||--|| payments     : paid_by
    orders      ||--|| shipping     : shipped_by
    orders      ||--o{ reviews      : reviewed_by
    orders      ||--o{ feedback     : about
    products    ||--o{ order_items  : referenced
    products    ||--o{ reviews      : about
    categories  ||--o{ products     : classifies
    categories  ||--o{ categories   : parent_of

    customers {
        int id PK
        text first_name
        text last_name
        text email UK
        text phone
        text gender
        date date_of_birth
        timestamp created_at
        timestamp updated_at
    }
    addresses {
        int id PK
        int customer_id FK
        text type "billing | shipping"
        text street
        text city
        text state
        text zip_code
        text country
        bool is_default
        timestamp created_at
    }
    categories {
        int id PK
        text name UK
        text description
        int parent_category_id FK
        timestamp created_at
    }
    products {
        int id PK
        int category_id FK
        text name
        text description
        text sku UK
        numeric price
        numeric cost
        int stock_quantity
        text brand
        real weight
        bool is_active
        timestamp created_at
        timestamp updated_at
    }
    coupons {
        int id PK
        text code UK
        text discount_type "percentage | fixed"
        numeric discount_value
        numeric min_order_amount
        int max_uses
        int times_used
        date valid_from
        date valid_until
        bool is_active
        timestamp created_at
    }
    orders {
        int id PK
        int customer_id FK
        int shipping_address_id FK
        int billing_address_id FK
        int coupon_id FK
        text status "pending|processing|shipped|delivered|cancelled|returned"
        numeric subtotal
        numeric discount_amount
        numeric tax_amount
        numeric shipping_cost
        numeric total_amount
        timestamp order_date
        timestamp created_at
        timestamp updated_at
    }
    order_items {
        int id PK
        int order_id FK
        int product_id FK
        int quantity
        numeric unit_price
        numeric total_price
        timestamp created_at
    }
    payments {
        int id PK
        int order_id FK
        text payment_method "credit_card|debit_card|paypal|apple_pay|google_pay"
        numeric amount
        text status "pending|completed|failed|refunded"
        text transaction_id UK
        timestamp paid_at
        timestamp created_at
    }
    shipping {
        int id PK
        int order_id FK
        text carrier "UPS|FedEx|USPS|DHL"
        text tracking_number UK
        text status
        timestamp shipped_at
        date estimated_delivery
        timestamp delivered_at
        timestamp created_at
        timestamp updated_at
    }
    reviews {
        int id PK
        int product_id FK
        int customer_id FK
        int order_id FK
        int rating "1..5"
        text title
        text comment
        bool is_verified
        timestamp created_at
    }
    feedback {
        int id PK
        int customer_id FK
        int order_id FK
        text type "complaint|suggestion|praise|question"
        text subject
        text message
        text status "open|in_progress|resolved|closed"
        text priority "low|medium|high"
        timestamp created_at
        timestamp resolved_at
    }
```

---

## 2. Table Inventory (Operational — 11 tables)

| Table | Role | Cardinality (after dirty inject) | Key constraints |
|---|---|---|---|
| `customers` | core dim | ~5,350 | UNIQUE email |
| `addresses` | dim attached to customers | ~8,190 | FK customer_id, CHECK type ∈ {billing,shipping} |
| `categories` | dim, self-referential hierarchy | ~21 | UNIQUE name |
| `products` | core dim | ~535 | UNIQUE sku, FK category_id |
| `coupons` | dim | ~53 | UNIQUE code, CHECK discount_type |
| `orders` | core fact (header) | ~10,900 | FK ×4 (customer, ship_addr, bill_addr, coupon) |
| `order_items` | core fact (line) | ~25,067 | FK ×2 (order, product) |
| `payments` | fact extension | ~10,700 | FK order_id, UNIQUE transaction_id |
| `shipping` | fact extension | ~10,352 | FK order_id, UNIQUE tracking_number |
| `reviews` | satellite | ~4,357 | FK ×3 + CHECK rating BETWEEN 1 AND 5 |
| `feedback` | satellite | ~1,635 | FK ×2 + CHECK type, status, priority |

> The **post-dirty-inject** counts come from `documentations/CHI_TIET_LOI_DU_LIEU.md`. Original clean cardinalities are smaller (e.g., 5,000 customers).

### Core business tables vs satellites
- **Core business**: customers, products, orders, order_items, payments, shipping
- **Reference**: categories, coupons
- **Customer-generated**: addresses, reviews, feedback
- **No audit/log tables exist** — operational schema doesn't capture change history. Debezium WAL+Kafka is the de-facto audit log.

---

## 3. Constraint Summary

| Type | Tables affected |
|---|---|
| `NOT NULL` | almost everywhere — appropriately strict |
| `UNIQUE` | customers.email, products.sku, coupons.code, payments.transaction_id, shipping.tracking_number, categories.name |
| `CHECK` (enum) | gender, address.type, coupons.discount_type, orders.status, payments.payment_method, payments.status, shipping.carrier, shipping.status, reviews.rating, feedback.type/status/priority |
| `FOREIGN KEY` | 14 FKs total, all with default `NO ACTION` |

> The schema **does not** define `ON DELETE CASCADE`. Deleting a customer with orders is blocked by FK — correct for transactional integrity but means cleanup is manual.

---

## 4. Indexing — **Critical Gap**

`data/initial_table.sql` declares only `PRIMARY KEY` and `UNIQUE` constraints, both of which create indexes implicitly. **Foreign-key columns are not indexed**.

This is a major performance hole because nearly every analytical query joins on FKs. **Required additions** (see `PERFORMANCE_ANALYSIS.md` §4):

```sql
CREATE INDEX idx_addresses_customer_id    ON addresses(customer_id);
CREATE INDEX idx_products_category_id     ON products(category_id);
CREATE INDEX idx_orders_customer_id       ON orders(customer_id);
CREATE INDEX idx_orders_shipping_addr     ON orders(shipping_address_id);
CREATE INDEX idx_orders_billing_addr      ON orders(billing_address_id);
CREATE INDEX idx_orders_coupon_id         ON orders(coupon_id);
CREATE INDEX idx_orders_status            ON orders(status);
CREATE INDEX idx_orders_order_date        ON orders(order_date);
CREATE INDEX idx_order_items_order_id     ON order_items(order_id);
CREATE INDEX idx_order_items_product_id   ON order_items(product_id);
CREATE INDEX idx_payments_order_id        ON payments(order_id);
CREATE INDEX idx_shipping_order_id        ON shipping(order_id);
CREATE INDEX idx_reviews_product_id       ON reviews(product_id);
CREATE INDEX idx_reviews_customer_id      ON reviews(customer_id);
CREATE INDEX idx_reviews_order_id         ON reviews(order_id);
CREATE INDEX idx_feedback_customer_id     ON feedback(customer_id);
CREATE INDEX idx_feedback_order_id        ON feedback(order_id);
```

Also recommended:
- Partial index for active flags: `CREATE INDEX idx_products_active ON products(id) WHERE is_active = true;`
- Date BRIN for very large `orders`: `CREATE INDEX idx_orders_date_brin ON orders USING BRIN (order_date);`

---

## 5. Star Schema (Analytical — Hive `gold` DB)

```mermaid
erDiagram
    fact_sales       }o--|| dim_customers : customer_key
    fact_sales       }o--|| dim_products  : product_key
    fact_sales       }o--|| dim_payments  : payment_key
    fact_sales       }o--|| dim_shipping  : shipping_key

    fact_sales {
        string order_item_key PK
        string order_key
        string customer_key FK
        string product_key FK
        string payment_key FK
        string shipping_key FK
        string order_status
        timestamp order_date
        int order_year "partition"
        int order_month "partition"
        int order_day
        decimal subtotal
        decimal discount_amount
        decimal tax_amount
        decimal shipping_cost
        decimal order_total
        int quantity
        decimal unit_price
        decimal item_total
        string customer_name "denorm"
        string gender "denorm"
        string sku "denorm"
        string product_name "denorm"
        string brand "denorm"
        decimal list_price "denorm"
        string payment_method "denorm"
        string payment_status "denorm"
        string carrier "denorm"
        string shipping_status "denorm"
        double delivery_days "computed"
    }
    dim_customers {
        string customer_key PK
        string customer_name
        string email
        string gender
        date date_of_birth
        timestamp customer_since
    }
    dim_products {
        string product_key PK
        string sku
        string product_name
        string brand
        int category_id
        decimal list_price
        decimal cost
        bool is_active
    }
    dim_payments {
        string payment_key PK
        int order_id
        string payment_method
        decimal payment_amount
        string payment_status
        string transaction_id
        timestamp paid_at
    }
    dim_shipping {
        string shipping_key PK
        int order_id
        string carrier
        string tracking_number
        string shipping_status
        timestamp shipped_at
        timestamp delivered_at
        double delivery_days
    }
```

### Design decisions

- **`fact_sales` grain = one order_item** (line item, not order header). Allows analyses like "which products drive most orders" without re-disaggregation.
- **Partition by `(order_year, order_month)`** — typical date filter pruning.
- **Denormalized columns** (`customer_name`, `product_name`, etc.) — sacrifices storage for query speed. The AI agent can answer "top 10 customers by spend" without joining; reduces LLM-generated JOIN complexity.
- **EXTERNAL tables** — Hive only stores metadata; `DROP TABLE` does not delete HDFS files. Allows safe re-registration.
- **No SCD (slowly-changing dimensions)** — `dim_customers` is a snapshot; if a customer changes email, history is lost. For graduation project this is fine.

---

## 6. Transaction Patterns

### OLTP side
The simulators use simple `INSERT`/`UPDATE` with `psycopg2.commit()`. There are **no multi-statement transactions** for related operations like "create order + items + payment". Failure mid-flow leaves partial state.

For graduation, this is acceptable. For production: wrap related writes in `BEGIN ... COMMIT`, or use the saga pattern via outbox events.

### Snapshot read for initial load
`data/migrate.py` (a.k.a. `initial_load.py`) **does it correctly**:

```python
cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
cur.execute("SELECT pg_current_wal_lsn()")
# ... dump every table ...
conn.rollback()  # nothing to commit, just close
```

This gives a **consistent snapshot** across all tables — the same pattern Debezium uses for its initial snapshot. Rollback (rather than commit) is correct since it's read-only.

### CDC offset durability
The replication slot `erp_debezium_slot` persists offset durably. Even if Kafka Connect restarts, the slot remembers where to resume. **However** — if Connect is gone for too long, WAL accumulates on the Postgres side and can fill the disk. Monitor:

```sql
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots;
```

---

## 7. Data Types — Specific Choices

- `NUMERIC(12,2)` everywhere for money. Correct (avoids float drift). Spark Silver casts to `DecimalType(12,2)` to match.
- `TIMESTAMP` (no timezone) — risky in distributed systems (different containers may have different TZ). Should use `TIMESTAMPTZ` to make timezone explicit.
- `TEXT` for short fields like `gender`, `country`. Postgres treats `TEXT` and `VARCHAR(n)` equivalently for storage but `VARCHAR(n)` constraints are checked. The `CHECK (gender IN ('male','female','non_binary'))` covers gender; could be ENUM types for better catalog discoverability.
- `BOOLEAN NOT NULL DEFAULT TRUE` — sane.
- Surrogate keys are **integers** (`SERIAL`). For Spark's surrogate key (`MD5(source:id)`), the Hive Gold layer keeps a `string` key — this is an explicit translation, not a schema mismatch.

---

## 8. Heavy Queries (predicted)

The AI agent will likely emit these patterns. Ranked by anticipated cost:

### A. Top-N revenue (high cost without indexes)
```sql
SELECT p.name, SUM(oi.total_price) AS revenue
FROM order_items oi
JOIN products p ON p.id = oi.product_id
JOIN orders o   ON o.id = oi.order_id
WHERE o.order_date >= NOW() - INTERVAL '30 days'
  AND o.status NOT IN ('cancelled','returned')
GROUP BY p.id, p.name
ORDER BY revenue DESC
LIMIT 10;
```
- Without indexes: full sequential scan on `order_items` (~25k rows) — fine for graduation, slow for production
- With indexes: index scan on `orders.order_date`, hash join on order_id

### B. Customer LTV
```sql
SELECT c.id, c.email, COUNT(DISTINCT o.id) AS orders_n, SUM(o.total_amount) AS lifetime_value
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
GROUP BY c.id, c.email
HAVING SUM(o.total_amount) > 1000
ORDER BY lifetime_value DESC
LIMIT 100;
```

### C. Daily revenue trend (good candidate for a materialized view)
```sql
SELECT DATE_TRUNC('day', order_date) AS day, SUM(total_amount) AS revenue
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY 1
ORDER BY 1;
```

For (C), introduce a daily `mv_daily_revenue` materialized view, refreshed by the same Airflow DAG.

---

## 9. Schema Evolution Plan

The current schema is fixed. As the AI agent matures, expect to add:
- `customers.preferred_language` (`text`) — drives AI response language
- `orders.predicted_ltv` (`numeric`) — populated by an offline ML model
- `products.embedding` (`vector(1536)` if `pgvector` enabled) — for semantic search

Schema changes require:
1. Add column in source PG (NOT NULL with default, or NULL-allowed)
2. Update Debezium config: `table.include.list` is column-agnostic, but Spark `*_SCHEMA` definitions in `silver_transform.py` are **hand-coded** — must be updated
3. Re-register Gold tables (drop + recreate via `gold_transform.py`)

> **This is brittle** — schema changes require Spark code changes. Industry pattern: use **Schema Registry** (Confluent) with Avro/Protobuf, and consume schemas dynamically in Spark.

---

## 10. Hive Metastore (`metastore` DB)

Separate Postgres 13 container (the **legacy version** is intentional — newer Postgres versions default to SCRAM-SHA-256 auth which the bundled JDBC driver in `bde2020/hive` cannot speak).

Stores:
- Database list
- Table definitions (columns, types, partitions)
- File locations (HDFS paths)
- Statistics (row counts after `ANALYZE`)

Direct queries (for debugging):
```bash
docker compose exec hive-metastore-db psql -U hive -d metastore -c "
  SELECT t.tbl_name, p.part_name, s.location
  FROM tbls t
  JOIN partitions p ON p.tbl_id = t.tbl_id
  JOIN sds s ON s.sd_id = p.sd_id
  WHERE t.tbl_name = 'fact_sales'
  LIMIT 20;
"
```

---

## 11. Data Quality Layer — DLQ

`/datalake/silver/dlq` collects rows that failed Silver validation. The schema is minimal:

```
id                     bigint
_quality_flag          string
_bronze_ingested_at    timestamp
```

This is **insufficient** for triage — the original payload is lost. Two fixes:
1. Include `raw_data` (full JSON string) in the DLQ.
2. Include `_dlq_reason` (e.g., `"failed_email_regex"`, `"null_required_field:total_amount"`).

Currently the dirty data classification (CLEAN | DIRTY | QUARANTINE) is set by the **simulator** (`_quality_flag` in the envelope), not by Silver itself. Silver just routes based on the flag and a few hardcoded null-checks. A more rigorous setup would have Silver **emit** quality issues based on its own rules (e.g., `Great Expectations` integration).

---

## 12. Quick-Reference Connection Strings

| DB | URL (from inside Docker network) |
|---|---|
| ERP | `postgresql://postgres:postgres@postgres:5432/ecommerce` |
| Airflow meta | `postgresql://postgres:postgres@postgres:5432/airflow` |
| Hive metastore | `postgresql://hive:hive@hive-metastore-db:5432/metastore` |
| HiveServer2 (analytical) | `jdbc:hive2://hiveserver2:10000/gold` |

From host (mapped ports):
| DB | URL |
|---|---|
| ERP | `postgresql://postgres:postgres@localhost:5433/ecommerce` |
| HiveServer2 | `jdbc:hive2://localhost:10000/gold` |
