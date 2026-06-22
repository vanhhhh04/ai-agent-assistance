---
table_name: dim_shipping
section: overview
tags: [shipping, delivery, carrier, logistics, tracking]
---

# dim_shipping

**Grain**: 1 dòng = 1 lô vận chuyển cho 1 đơn hàng.

Bảng dimension chứa **thông tin vận chuyển**: carrier (UPS, FedEx, USPS, DHL), tracking number, trạng thái giao hàng, thời gian shipped + delivered. Quan hệ 1-1 với `fact_sales` qua `order_id`.

## Câu hỏi business hay dùng

- **Thời gian giao hàng trung bình theo carrier**: AVG `delivery_days` GROUP BY `carrier`
- **Carrier nhanh nhất / chậm nhất**: ORDER BY AVG `delivery_days`
- **Đơn chưa giao quá lâu**: filter `shipped_at < NOW - 7 days AND shipping_status != 'delivered'`
- **Phân bố sử dụng carrier**: COUNT theo `carrier`
- **Tỷ lệ giao hàng đúng SLA**: % đơn có `delivery_days <= 3`

## Cột chính

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `shipping_key` | int | PK — surrogate key |
| `order_id` | int | FK → `fact_sales.order_key` (1-1) |
| `carrier` | string | Đơn vị vận chuyển: `UPS`, `FedEx`, `USPS`, `DHL` |
| `tracking_number` | string | Mã tracking (vd: "TRK123456789") |
| `shipping_status` | string | `pending`, `picked_up`, `in_transit`, `out_for_delivery`, `delivered`, `returned` |
| `shipped_at` | timestamp | Thời điểm carrier nhận hàng (status chuyển sang `picked_up`) |
| `delivered_at` | timestamp | Thời điểm giao thành công (NULL nếu chưa giao) |
| `delivery_days` | double | Số ngày từ `shipped_at` đến `delivered_at` (NULL nếu chưa giao) |

## Lưu ý quan trọng

- **Phân biệt `shipping_status` và `order_status`**: shipping_status từ carrier (chi tiết hơn); order_status từ ERP (cấp business). Một đơn có thể `order_status='shipped'` mà `shipping_status='in_transit'`.
- **`delivery_days` chỉ có khi `shipping_status='delivered'`** — NULL với đơn đang in_transit.
- **Carrier + delivery_days cũng có trong `fact_sales`** đã denormalized — không cần JOIN dim_shipping cho 80% câu hỏi.
- `dim_shipping` chủ yếu hữu ích khi cần tracking_number cụ thể hoặc xem chi tiết lifecycle `shipped_at` → `delivered_at`.
- Đơn `returned` có `delivered_at` rồi quay lại → cần dùng riêng nếu phân tích reverse logistics.

## SQL examples

**Thời gian giao trung bình theo carrier:**
```sql
SELECT carrier,
       AVG(delivery_days) AS avg_days,
       COUNT(*) AS num_orders
FROM gold.dim_shipping
WHERE shipping_status = 'delivered'
GROUP BY carrier
ORDER BY avg_days ASC;
```

**Carrier có tỷ lệ giao đúng SLA (≤3 ngày) cao nhất:**
```sql
SELECT carrier,
       SUM(CASE WHEN delivery_days <= 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS on_time_pct,
       COUNT(*) AS total
FROM gold.dim_shipping
WHERE shipping_status = 'delivered'
GROUP BY carrier
ORDER BY on_time_pct DESC;
```

**Đơn đang ship quá 7 ngày chưa giao:**
```sql
SELECT order_id, carrier, tracking_number, shipped_at, shipping_status
FROM gold.dim_shipping
WHERE shipping_status NOT IN ('delivered', 'returned')
  AND shipped_at < DATE_SUB(CURRENT_DATE, 7)
ORDER BY shipped_at ASC
LIMIT 100;
```
