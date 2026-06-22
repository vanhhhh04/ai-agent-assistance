---
table_name: dim_categories
section: overview
tags: [category, taxonomy, hierarchy, product]
---

# dim_categories

**Grain**: 1 dòng = 1 danh mục sản phẩm (parent hoặc child).

Bảng dimension chứa **cây phân loại sản phẩm 2 cấp** (parent → child). Ví dụ: parent "Điện tử" → children "Điện thoại", "Laptop", "Tai nghe". Cột `parent_category_name` đã được denormalized để query không cần self-JOIN.

## Câu hỏi business hay dùng

- **Liệt kê tất cả ngành hàng (parent categories)**: filter `parent_category_id IS NULL`
- **Danh mục con của 1 ngành**: filter `parent_category_name = 'Điện tử'`
- **Đếm số danh mục theo ngành**: GROUP BY `parent_category_name`
- **Tìm thông tin danh mục từ tên sản phẩm**: JOIN với `dim_products`

## Cột chính

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `category_key` | int | PK — surrogate key |
| `category_name` | string | Tên danh mục (vd: "Điện thoại", "Áo nam", "Đồ bếp") |
| `category_description` | string | Mô tả chi tiết danh mục |
| `parent_category_id` | int | FK self → `category_key` của parent (NULL nếu là parent root) |
| `parent_category_name` | string | Denormalized: tên ngành hàng cha |
| `category_created_at` | timestamp | Ngày tạo danh mục |

## Lưu ý quan trọng

- **Hierarchy 2 cấp**: parent có `parent_category_id = NULL`, child có `parent_category_id != NULL` trỏ về parent.
- 10 ngành hàng cha cố định: Điện tử, Thời trang, Gia dụng, Sách & Văn phòng, Thể thao, Mẹ & Bé, Làm đẹp, Thực phẩm, Đồ chơi, Ô tô & Xe máy.
- Khi muốn group sản phẩm theo ngành, **dùng `category_name` trong `dim_products`** (đã được join sẵn) — không cần join `dim_categories`.
- `dim_categories` chủ yếu hữu ích khi cần xem hierarchy đầy đủ hoặc query về danh mục độc lập với sản phẩm.

## SQL examples

**Danh sách 10 ngành hàng chính:**
```sql
SELECT category_name, category_description
FROM gold.dim_categories
WHERE parent_category_id IS NULL
ORDER BY category_name;
```

**Số danh mục con theo ngành:**
```sql
SELECT parent_category_name, COUNT(*) AS num_subcategories
FROM gold.dim_categories
WHERE parent_category_id IS NOT NULL
GROUP BY parent_category_name
ORDER BY num_subcategories DESC;
```

**Tất cả danh mục thuộc ngành "Điện tử":**
```sql
SELECT category_name
FROM gold.dim_categories
WHERE parent_category_name = 'Điện tử'
ORDER BY category_name;
```
