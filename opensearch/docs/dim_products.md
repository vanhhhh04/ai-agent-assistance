---
table_name: dim_products
section: overview
tags: [product, dimension, catalog, inventory]
---

# dim_products

**Grain**: 1 dòng = 1 SKU sản phẩm.

Dim sản phẩm — đã enrich `category_name` và `parent_category_name` từ `dim_categories` để query không cần JOIN thêm.

## Câu hỏi business hay dùng

- Sản phẩm theo brand / category
- Margin analysis: `list_price - cost`
- Sản phẩm đã ngừng bán (`is_active = false`)
- Top sản phẩm bán chạy (JOIN `fact_sales`)

## Cột chính

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `product_key` | int | PK, JOIN với `fact_*.product_key` |
| `sku` | string | Mã sản phẩm canonical (UPPERCASE, dấu `-` thay `_`/space) |
| `product_name` | string | Tên hiển thị |
| `brand` | string | Initcap (ví dụ: "Apple", "Samsung") |
| `category_id` | int | FK → `dim_categories.category_key` |
| `category_name` | string | **Đã denorm** — không cần JOIN `dim_categories` |
| `parent_category_name` | string | Danh mục cha (nullable) |
| `list_price` | decimal | Giá niêm yết |
| `cost` | decimal | Giá vốn — dùng tính margin |
| `is_active` | boolean | `false` = đã ngừng bán |

## Lưu ý

- **Không có cột `stock_quantity` ở Gold** — stock realtime nằm ở Silver `warehouse_events`. Gold chỉ là snapshot product master.
- Margin = `list_price - cost`, nhưng AI agent thường tính `(list_price - cost) / list_price` cho margin %.

## SQL examples

**Top 5 brands theo số SKU:**
```sql
SELECT brand, COUNT(*) AS sku_count
FROM gold.dim_products
WHERE is_active = TRUE
GROUP BY brand
ORDER BY sku_count DESC
LIMIT 5;
```

**Sản phẩm có margin > 30%:**
```sql
SELECT sku, product_name, brand,
       list_price, cost,
       (list_price - cost) / list_price AS margin_pct
FROM gold.dim_products
WHERE is_active = TRUE
  AND (list_price - cost) / list_price > 0.30
ORDER BY margin_pct DESC;
```

**Phân bố sản phẩm theo category:**
```sql
SELECT category_name, COUNT(*) AS sku_count, AVG(list_price) AS avg_price
FROM gold.dim_products
WHERE is_active = TRUE
GROUP BY category_name
ORDER BY sku_count DESC;
```
