---
table_name: dim_addresses
section: overview
tags: [address, customer, shipping, location, geography]
---

# dim_addresses

**Grain**: 1 dòng = 1 địa chỉ của 1 khách hàng (có thể có nhiều địa chỉ/khách).

Bảng dimension chứa **thông tin địa chỉ giao hàng/thanh toán** của khách hàng. Mỗi customer có thể có nhiều địa chỉ (shipping + billing). Cột `is_default` đánh dấu địa chỉ mặc định.

## Câu hỏi business hay dùng

- **Phân bố khách hàng theo thành phố/tỉnh**: GROUP BY `city`, `state`
- **Khu vực có nhiều khách hàng nhất**: COUNT theo `city` hoặc `state`
- **Đếm số địa chỉ trung bình mỗi khách**: AVG over `customer_id`
- **Khách hàng có địa chỉ ở nhiều tỉnh**: `customer_id` xuất hiện với nhiều `state`

## Cột chính

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `address_key` | int | PK — surrogate key của địa chỉ |
| `customer_id` | int | FK → `dim_customers.customer_key` (link địa chỉ với khách) |
| `address_type` | string | Loại địa chỉ: `billing`, `shipping` |
| `street` | string | Đường + số nhà |
| `city` | string | Thành phố (vd: "Hà Nội", "TP. Hồ Chí Minh", "Đà Nẵng") |
| `state` | string | Tỉnh/Thành |
| `zip_code` | string | Mã bưu chính |
| `country` | string | Quốc gia (mặc định "Vietnam") |
| `is_default` | boolean | Địa chỉ mặc định của khách (true/false) |

## Lưu ý quan trọng

- 1 customer có thể có nhiều address rows → khi đếm khách hàng theo địa lý, cần `COUNT(DISTINCT customer_id)`, không phải `COUNT(*)`.
- Nếu phân tích "khu vực mua hàng", **dùng `shipping_city/shipping_state` ngay trong `fact_sales`** (đã denormalized) — không cần JOIN dim_addresses.
- `dim_addresses` hữu ích khi cần địa chỉ billing cũng — fact_sales chỉ có shipping_*.
- `address_type` enum: `billing` | `shipping`. Lọc trước khi đếm để tránh nhân đôi.

## SQL examples

**Top 10 thành phố có nhiều khách hàng nhất:**
```sql
SELECT city, COUNT(DISTINCT customer_id) AS unique_customers
FROM gold.dim_addresses
WHERE address_type = 'shipping'
GROUP BY city
ORDER BY unique_customers DESC
LIMIT 10;
```

**Phân bố khách hàng theo tỉnh:**
```sql
SELECT state, COUNT(DISTINCT customer_id) AS customers
FROM gold.dim_addresses
WHERE is_default = TRUE
GROUP BY state
ORDER BY customers DESC;
```

**Khách có địa chỉ ở > 1 tỉnh:**
```sql
SELECT customer_id, COUNT(DISTINCT state) AS num_states
FROM gold.dim_addresses
GROUP BY customer_id
HAVING COUNT(DISTINCT state) > 1;
```
