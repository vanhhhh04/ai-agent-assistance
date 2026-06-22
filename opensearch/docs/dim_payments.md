---
table_name: dim_payments
section: overview
tags: [payment, transaction, money, gateway]
---

# dim_payments

**Grain**: 1 dòng = 1 giao dịch thanh toán cho 1 đơn hàng.

Bảng dimension chứa **thông tin giao dịch thanh toán**: phương thức (credit_card, paypal, ...), số tiền, trạng thái (pending/completed/failed/refunded), thời gian thanh toán. Mỗi `order_id` chỉ có 1 payment row (1-1 relationship).

## Câu hỏi business hay dùng

- **Phương thức thanh toán phổ biến nhất**: GROUP BY `payment_method`, COUNT
- **Tổng tiền thu được**: SUM `payment_amount` WHERE `payment_status='completed'`
- **Tỷ lệ thanh toán thất bại**: COUNT(failed)/COUNT(*) theo `payment_method`
- **Đơn hàng đã refund**: filter `payment_status = 'refunded'`
- **Doanh thu theo gateway**: nhóm theo `payment_method` và SUM completed payments

## Cột chính

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `payment_key` | int | PK — surrogate key |
| `order_id` | int | FK → `fact_sales.order_key` (1-1) |
| `payment_method` | string | `credit_card`, `debit_card`, `paypal`, `apple_pay`, `google_pay` |
| `payment_amount` | decimal(12,2) | Số tiền giao dịch (VND) |
| `payment_status` | string | `pending`, `completed`, `failed`, `refunded` |
| `transaction_id` | string | ID giao dịch từ gateway (vd: "TXN-ABC123") |
| `paid_at` | timestamp | Thời điểm thanh toán thành công (NULL nếu chưa completed) |

## Lưu ý quan trọng

- **Phân biệt `payment_amount` và `order_total`**: payment_amount là số tiền thực thu (sau coupon + tax + shipping). Trong `fact_sales`, dùng `order_total` cho cấp đơn hoặc `item_total` cho cấp item.
- `payment_status = 'completed'` ≠ `order_status = 'delivered'`. Đơn có thể đã thanh toán nhưng chưa giao.
- `paid_at` chỉ có giá trị khi status = `completed`. Nếu cần lifecycle thời gian, dùng `paid_at` thay cho `order_date`.
- **Phương thức thanh toán cũng có trong `fact_sales.payment_method`** đã denormalized — không cần JOIN dim_payments cho 80% câu hỏi.
- `transaction_id` là UNIQUE — nếu trùng đó là dirty data (sim inject 5% duplicate_txn_id).

## SQL examples

**Phân bố phương thức thanh toán:**
```sql
SELECT payment_method,
       COUNT(*) AS num_transactions,
       SUM(payment_amount) AS total_amount
FROM gold.dim_payments
WHERE payment_status = 'completed'
GROUP BY payment_method
ORDER BY total_amount DESC;
```

**Tỷ lệ thanh toán thất bại theo gateway:**
```sql
SELECT payment_method,
       SUM(CASE WHEN payment_status = 'failed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS fail_rate_pct,
       COUNT(*) AS total_txn
FROM gold.dim_payments
GROUP BY payment_method
ORDER BY fail_rate_pct DESC;
```

**Đơn hàng đã refund tháng này:**
```sql
SELECT order_id, payment_method, payment_amount, transaction_id, paid_at
FROM gold.dim_payments
WHERE payment_status = 'refunded'
  AND MONTH(paid_at) = MONTH(CURRENT_DATE)
  AND YEAR(paid_at) = YEAR(CURRENT_DATE)
ORDER BY paid_at DESC
LIMIT 100;
```
