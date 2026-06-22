---
table_name: fact_feedback
section: overview
tags: [feedback, complaint, support, customer service, csat]
---

# fact_feedback

**Grain**: 1 dòng = 1 phản hồi/khiếu nại của 1 khách hàng (có thể link order hoặc không).

Bảng fact chứa **phản hồi/khiếu nại khách hàng** đến bộ phận hỗ trợ. Khác `fact_reviews` (đánh giá sản phẩm cụ thể) — `fact_feedback` là các loại phản ánh chung: complaint/suggestion/praise/question. Partition theo `feedback_year`, `feedback_month`.

## Câu hỏi business hay dùng

- **Số khiếu nại tháng này**: filter `feedback_type='complaint'` và partition keys
- **Tỷ lệ feedback đã giải quyết**: COUNT WHERE `feedback_status='resolved'` / COUNT(*)
- **Thời gian giải quyết trung bình**: AVG `resolution_days` GROUP BY `priority`
- **Top khách hàng phản hồi nhiều nhất**: COUNT GROUP BY `customer_name`
- **Phân bố loại feedback**: COUNT GROUP BY `feedback_type`
- **Feedback ưu tiên cao chưa giải quyết**: filter `priority='high' AND feedback_status != 'resolved'`

## Cột chính

| Nhóm | Cột | Ý nghĩa |
|---|---|---|
| Keys | `feedback_key`, `customer_key`, `order_key` | PK + FK (order_key có thể NULL cho feedback không gắn đơn) |
| Type | `feedback_type` | `complaint` / `suggestion` / `praise` / `question` |
| Content | `subject`, `message` | Tiêu đề + nội dung phản hồi |
| Workflow | `feedback_status` | `open` / `in_progress` / `resolved` / `closed` |
| Priority | `priority` | `low` / `medium` / `high` |
| Time | `feedback_date` (raise), `resolved_at` (close) | Mở/đóng feedback |
| Metric | `resolution_days` | Số ngày từ raise → resolved (NULL nếu chưa resolved) |
| Partition | `feedback_year`, `feedback_month` | Partition keys |
| Denorm | `customer_name` | Tránh JOIN dim_customers |

## Lưu ý quan trọng

- **`order_key` có thể NULL**: ~20% feedback là standalone (không liên quan đơn cụ thể, vd: hỏi chính sách). Khi JOIN với fact_sales, dùng LEFT JOIN.
- **`feedback_type` ≠ `feedback_status`**:
  - type = loại (complaint/suggestion/...) — không đổi qua lifecycle
  - status = trạng thái workflow (open → in_progress → resolved → closed)
- **`resolution_days` chỉ có khi status = 'resolved' hoặc 'closed'**. Đơn vị: ngày (float, có thể 0.5 = 12 giờ).
- `priority='high'` thường cho complaints (50% xác suất), suggestions/praise hiếm khi high.
- **Lọc thời gian dùng partition keys** `feedback_year` + `feedback_month` cho query nhanh.

## SQL examples

**Tỷ lệ feedback đã giải quyết theo loại:**
```sql
SELECT feedback_type,
       COUNT(*) AS total,
       SUM(CASE WHEN feedback_status = 'resolved' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS resolved_pct
FROM gold.fact_feedback
GROUP BY feedback_type
ORDER BY total DESC;
```

**Khiếu nại priority cao chưa giải quyết:**
```sql
SELECT feedback_key, customer_name, subject, feedback_date,
       DATEDIFF(CURRENT_DATE, feedback_date) AS days_open
FROM gold.fact_feedback
WHERE feedback_type = 'complaint'
  AND priority = 'high'
  AND feedback_status NOT IN ('resolved', 'closed')
ORDER BY feedback_date ASC
LIMIT 50;
```

**Thời gian giải quyết trung bình theo priority:**
```sql
SELECT priority,
       ROUND(AVG(resolution_days), 1) AS avg_days,
       COUNT(*) AS resolved_count
FROM gold.fact_feedback
WHERE feedback_status = 'resolved'
GROUP BY priority
ORDER BY priority;
```

**Top 10 khách hàng feedback nhiều nhất tháng này:**
```sql
SELECT customer_name, COUNT(*) AS num_feedback,
       SUM(CASE WHEN feedback_type = 'complaint' THEN 1 ELSE 0 END) AS num_complaints
FROM gold.fact_feedback
WHERE feedback_year = YEAR(CURRENT_DATE) AND feedback_month = MONTH(CURRENT_DATE)
GROUP BY customer_name
ORDER BY num_feedback DESC
LIMIT 10;
```
