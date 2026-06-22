---
table_name: fact_reviews
section: overview
tags: [review, rating, product feedback, satisfaction, csat]
---

# fact_reviews

**Grain**: 1 dòng = 1 đánh giá của 1 khách hàng cho 1 sản phẩm trong 1 đơn hàng.

Bảng fact chứa **đánh giá sản phẩm** (rating 1-5 + title + comment). Partition theo `review_year`, `review_month`. Đã denormalized customer_name + product_name + brand + category để query không cần JOIN.

## Câu hỏi business hay dùng

- **Rating trung bình theo brand/product**: AVG `rating` GROUP BY `brand` hoặc `product_name`
- **Sản phẩm có rating cao nhất**: ORDER BY AVG `rating` DESC
- **Customer hay review nhất**: COUNT theo `customer_key`
- **Xu hướng đánh giá theo thời gian**: GROUP BY `review_year`, `review_month`
- **Tỷ lệ verified reviews**: COUNT(`is_verified`=true) / COUNT(*)
- **Sản phẩm có rating thấp cần attention**: rating ≤ 2 trong tháng gần nhất

## Cột chính

| Nhóm | Cột | Ý nghĩa |
|---|---|---|
| Keys | `review_key`, `customer_key`, `product_key`, `order_key` | PK + FK |
| Rating | `rating` (1-5) | Điểm đánh giá |
| Content | `review_title`, `review_comment` | Nội dung review (text) |
| Trust | `is_verified` (boolean) | True nếu khách đã mua thật |
| Time | `review_date`, `review_year`, `review_month` | `review_year/month` là partition keys |
| Denormalized | `customer_name`, `product_name`, `brand`, `category_name` | Tránh JOIN |

## Lưu ý quan trọng

- **`rating` là 1-5 integer** — không phải float. Để hiện điểm trung bình có lẻ, ROUND(AVG(rating), 2).
- Mỗi customer chỉ review 1 lần cho mỗi (order, product) — duplicate được dedup ở Silver layer.
- `is_verified = TRUE` nghĩa là sim chắc chắn khách đã mua đơn đó (post-purchase review).
- **Lọc theo thời gian nên dùng partition keys** `review_year` + `review_month` cho query nhanh — Hive partition pruning sẽ skip files không match.
- `review_comment` có thể dài → khi SELECT * dễ chậm. Chỉ SELECT khi cần.

## SQL examples

**Top 10 sản phẩm có rating trung bình cao nhất (≥ 10 reviews):**
```sql
SELECT product_name, brand,
       ROUND(AVG(rating), 2) AS avg_rating,
       COUNT(*) AS num_reviews
FROM gold.fact_reviews
GROUP BY product_name, brand
HAVING COUNT(*) >= 10
ORDER BY avg_rating DESC, num_reviews DESC
LIMIT 10;
```

**Phân bố rating tháng này:**
```sql
SELECT rating, COUNT(*) AS num_reviews
FROM gold.fact_reviews
WHERE review_year = YEAR(CURRENT_DATE) AND review_month = MONTH(CURRENT_DATE)
GROUP BY rating
ORDER BY rating;
```

**Brand có rating trung bình thấp (cần cải thiện):**
```sql
SELECT brand,
       ROUND(AVG(rating), 2) AS avg_rating,
       COUNT(*) AS num_reviews
FROM gold.fact_reviews
GROUP BY brand
HAVING COUNT(*) >= 50
ORDER BY avg_rating ASC
LIMIT 10;
```

**Khách hàng có nhiều reviews tốt nhất (rating ≥ 4):**
```sql
SELECT customer_name,
       COUNT(*) AS num_reviews,
       ROUND(AVG(rating), 2) AS avg_rating
FROM gold.fact_reviews
WHERE rating >= 4
GROUP BY customer_name
ORDER BY num_reviews DESC
LIMIT 20;
```
