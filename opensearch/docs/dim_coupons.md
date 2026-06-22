---
table_name: dim_coupons
section: overview
tags: [coupon, discount, promotion, marketing]
---

# dim_coupons

**Grain**: 1 dòng = 1 mã giảm giá.

Bảng dimension chứa **toàn bộ coupon/voucher** của hệ thống. Mỗi coupon có loại giảm giá (%, fixed), số tiền đơn tối thiểu để áp dụng, số lần dùng tối đa, thời gian hiệu lực, và bộ đếm `times_used` cập nhật khi customer thanh toán thành công.

## Câu hỏi business hay dùng

- **Coupon được dùng nhiều nhất**: ORDER BY `times_used` DESC
- **Coupon còn hiệu lực hôm nay**: filter `valid_from <= current_date <= valid_until AND coupon_active`
- **Coupon sắp hết hạn**: filter `valid_until` trong 7 ngày tới
- **Hiệu quả coupon**: JOIN `fact_sales` qua `coupon_key` để tính tổng discount đã áp
- **Coupon chưa được sử dụng**: filter `times_used = 0`

## Cột chính

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `coupon_key` | int | PK — surrogate key |
| `coupon_code` | string | Mã coupon hiển thị cho user (vd: "SUMMER2026", "VIP10") |
| `discount_type` | string | `percentage` (giảm %) hoặc `fixed` (giảm số tiền cố định VND) |
| `discount_value` | decimal(12,2) | Giá trị giảm: % nếu percentage, VND nếu fixed |
| `min_order_amount` | decimal(12,2) | Đơn hàng tối thiểu để áp coupon (VND) |
| `max_uses` | int | Số lần dùng tối đa (NULL = không giới hạn) |
| `times_used` | int | Số lần coupon đã được áp dụng thực tế |
| `valid_from` | date | Ngày bắt đầu hiệu lực |
| `valid_until` | date | Ngày hết hiệu lực |
| `coupon_active` | boolean | True nếu coupon đang active, false nếu admin đã disable |

## Lưu ý quan trọng

- **Phân biệt `discount_value`**: với `discount_type='percentage'`, value=10 nghĩa là 10%. Với `discount_type='fixed'`, value=100000 nghĩa là 100.000đ.
- **`times_used` chỉ tăng khi đơn thanh toán DELIVERED** — không phải khi user click apply.
- Để biết "doanh thu mất đi vì coupon", JOIN `fact_sales` qua `coupon_key` và SUM `discount_amount` (không phải `discount_value` của coupon).
- Coupon hết hạn vẫn lưu trong dim — không bị xóa, chỉ `coupon_active` chuyển false.

## SQL examples

**Top 10 coupon được dùng nhiều nhất:**
```sql
SELECT coupon_code, discount_type, discount_value, times_used
FROM gold.dim_coupons
ORDER BY times_used DESC
LIMIT 10;
```

**Coupon còn hiệu lực:**
```sql
SELECT coupon_code, discount_type, discount_value, min_order_amount,
       valid_until, max_uses - times_used AS remaining_uses
FROM gold.dim_coupons
WHERE coupon_active = TRUE
  AND CURRENT_DATE BETWEEN valid_from AND valid_until
ORDER BY valid_until ASC;
```

**Tổng tiền giảm giá đã chi cho từng coupon (top 10):**
```sql
SELECT c.coupon_code,
       COUNT(DISTINCT s.order_key) AS orders_used,
       SUM(s.discount_amount) AS total_discount_given
FROM gold.fact_sales s
JOIN gold.dim_coupons c ON s.coupon_key = c.coupon_key
WHERE s.order_status = 'DELIVERED'
GROUP BY c.coupon_code
ORDER BY total_discount_given DESC
LIMIT 10;
```
