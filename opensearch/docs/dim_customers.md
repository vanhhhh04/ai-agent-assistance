---
table_name: dim_customers
section: overview
tags: [customer, dimension, demographics]
---

# dim_customers

**Grain**: 1 dòng = 1 khách hàng.

Dim chứa thông tin nhân khẩu học của khách. Dùng để segment, demographic analysis, hoặc làm filter trong query với `fact_sales`/`fact_reviews`/`fact_feedback`.

## Câu hỏi business hay dùng

- Phân bố khách theo giới tính / độ tuổi
- Khách hàng mới đăng ký theo tháng (`customer_since`)
- Top customer theo doanh thu (JOIN `fact_sales`)

## Cột chính

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `customer_key` | int | PK, JOIN với `fact_*.customer_key` |
| `customer_name` | string | Concat `first_name + last_name` từ Silver |
| `email` | string | **PII** — lowercase, validated regex ở Silver |
| `gender` | string | `MALE`, `FEMALE`, `NON_BINARY` (uppercase) |
| `date_of_birth` | date | **PII** — dùng để tính tuổi |
| `customer_since` | timestamp | = `created_at` của customer trong ERP |

## Lưu ý

- `email` và `date_of_birth` là PII — guardrail check ở Phase 4 sẽ chặn SELECT trực tiếp 2 cột này nếu user không có quyền.
- Customer có nhiều địa chỉ — JOIN qua `dim_addresses.customer_id` (1-N).

## SQL examples

**Phân bố giới tính:**
```sql
SELECT gender, COUNT(*) AS total
FROM gold.dim_customers
GROUP BY gender;
```

**Top 10 khách hàng theo doanh thu:**
```sql
SELECT c.customer_name, SUM(f.item_total) AS lifetime_value
FROM gold.dim_customers c
JOIN gold.fact_sales f ON f.customer_key = c.customer_key
WHERE f.order_status = 'DELIVERED'
GROUP BY c.customer_key, c.customer_name
ORDER BY lifetime_value DESC
LIMIT 10;
```

**Khách đăng ký mới năm 2026:**
```sql
SELECT YEAR(customer_since) AS y, MONTH(customer_since) AS m, COUNT(*) AS new_customers
FROM gold.dim_customers
WHERE YEAR(customer_since) = 2026
GROUP BY YEAR(customer_since), MONTH(customer_since)
ORDER BY y, m;
```
