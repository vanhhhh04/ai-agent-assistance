---
table_name: fact_sales
section: overview
tags: [revenue, sales, transactions]
---

# fact_sales

**Grain**: 1 dòng = 1 sản phẩm trong 1 đơn hàng (order_item level).

`fact_sales` là bảng fact chính của hệ thống, partition theo `order_year`, `order_month` để query theo thời gian luôn nhanh. Để tăng tốc cho AI agent, các thuộc tính quan trọng của customer/product/payment/shipping đã được denormalized sẵn — không cần JOIN cho 80% câu hỏi business.

## Câu hỏi business hay dùng

- **Doanh thu theo tháng/quý/năm**: dùng `item_total` hoặc `order_total`
- **Top sản phẩm theo doanh thu**: GROUP BY `product_name`, `brand`, `category_name`
- **Doanh thu theo phương thức thanh toán**: GROUP BY `payment_method`
- **Phân tích discount**: dùng `discount_amount`, `coupon_code`
- **Thời gian giao hàng trung bình**: AVG `delivery_days`
- **Doanh thu theo khu vực**: GROUP BY `shipping_city`, `shipping_state`

## Cột chính

| Nhóm | Cột | Ý nghĩa |
|---|---|---|
| Keys | `order_item_key`, `order_key`, `customer_key`, `product_key`, `payment_key`, `shipping_key`, `coupon_key`, `shipping_address_key` | Khóa nối với các dim |
| Time | `order_date`, `order_year`, `order_month`, `order_day` | `order_year/month` là partition keys |
| Order measures | `subtotal`, `discount_amount`, `tax_amount`, `shipping_cost`, `order_total` | Số liệu cấp đơn hàng (nhân bản trên mọi item của đơn) |
| Item measures | `quantity`, `unit_price`, `item_total` | **Số liệu cấp item — dùng cái này khi tính doanh thu theo sản phẩm** |
| Denormalized | `customer_name`, `gender`, `product_name`, `brand`, `payment_method`, `carrier`, `shipping_city` | Tránh JOIN |

## Lưu ý quan trọng

- **Tránh nhầm `order_total` và `item_total`**: `order_total` lặp trên các item của cùng order (nếu SUM mọi item sẽ nhân đôi). Dùng `item_total` cho aggregation theo sản phẩm; dùng `DISTINCT order_key` rồi SUM `order_total` cho aggregation theo đơn.
- Cột `order_status` có thể là `PENDING`, `PROCESSING`, `SHIPPED`, `DELIVERED`, `CANCELLED`, `RETURNED`. Lọc `DELIVERED` nếu chỉ muốn doanh thu thực thu.
- `delivery_days` có thể NULL nếu shipping chưa giao xong.

## SQL examples

**Doanh thu tháng 4/2026:**
```sql
SELECT SUM(item_total) AS revenue
FROM gold.fact_sales
WHERE order_year = 2026 AND order_month = 4 AND order_status = 'DELIVERED';
```

**Top 10 brands theo doanh thu năm 2026:**
```sql
SELECT brand, SUM(item_total) AS revenue, COUNT(*) AS items_sold
FROM gold.fact_sales
WHERE order_year = 2026 AND order_status = 'DELIVERED'
GROUP BY brand
ORDER BY revenue DESC
LIMIT 10;
```

**Doanh thu theo carrier shipping:**
```sql
SELECT carrier, SUM(order_total) AS shipped_revenue, AVG(delivery_days) AS avg_days
FROM gold.fact_sales
WHERE shipping_status = 'DELIVERED'
GROUP BY carrier;
```
