# 📋 TÀI LIỆU CHI TIẾT LỖI DỮ LIỆU - BỘ DIRTY DATA E-COMMERCE

> Tài liệu mô tả chi tiết các lỗi dữ liệu đã được inject vào từng bảng trong bộ dữ liệu.
> Dùng làm tài liệu tham chiếu khi thực hành làm sạch dữ liệu với Apache Spark.

---

## 📌 Quy ước ký hiệu

| Ký hiệu | Ý nghĩa |
|----------|---------|
| 🔴 Cat 1 | Dữ liệu khuyết thiếu (Missing & Null) |
| 🟠 Cat 2 | Sai định dạng & cấu trúc (Malformed & Incorrectly Formatted) |
| 🟡 Cat 3 | Giá trị không hợp lệ (Invalid Values) |
| 🟢 Cat 4 | Dữ liệu trùng lặp (Duplicate Data) |
| 🔵 Cat 5 | Schema Inconsistency (Mixed Types) |
| 🟣 Cat 6 | Lỗi ngữ nghĩa & ngữ cảnh (Semantic & Contextual Errors) |

### Tỷ lệ lỗi chung áp dụng cho mọi bảng:
- 🔴 Cat 1: ~10% (4% null + 3% empty string + 3% placeholder)
- 🟠 Cat 2: ~8% (trên cột date và cột số)
- 🟡 Cat 3: ~6% (trên cột categorical và cột số)
- 🟢 Cat 4: ~7% (5% exact duplicate + 2% near-duplicate)
- 🔵 Cat 5: ~5% (trên cột số và cột text)
- 🟣 Cat 6: ~6% (4% casing + 2% orphaned records)
- 📄 CSV only: ~1% dòng bị malformed (thừa/thiếu cột)

### Các placeholder values sử dụng:
`"UNKNOWN"`, `"N/A"`, `"#N/A"`, `"None"`, `"null"`, `"EMPTY"`, `"--"`, `"???"`, `"TBD"`, `"not available"`

---

## 1. 🏠 BẢNG `addresses` (7,514 dòng gốc → ~8,190 dòng bẩn)

### Cột được bảo vệ (không bị lỗi):
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |
| `customer_id` | Foreign Key → customers.id |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🟡 Invalid | 🔵 Mixed Type | 🟣 Semantic |
|-----|-----------|------------|-----------|------------|---------------|-------------|
| `type` | TEXT | null, `""`, `"N/A"`... | — | Typos: `"billing_typo"`, `"shipping_typo"` | — | UPPER/lower: `"BILLING"` vs `"billing"` |
| `street` | TEXT | null, `""`, `"UNKNOWN"`... | — | — | ALL CAPS, thừa space/tab, truncated `"123 Mai..."`, double vowels | — |
| `city` | TEXT | null, `""`, `"TBD"`... | — | — | ALL CAPS, whitespace, empty string ẩn, truncated | — |
| `state` | TEXT | null, `""`, `"#N/A"`... | — | Typos có suffix `_typo` | — | UPPER/lower không nhất quán |
| `zip_code` | TEXT | null, `""`, `"EMPTY"`... | — | — | ALL CAPS, whitespace, truncated | — |
| `country` | TEXT | null, `""`, `"None"`... | — | Typos có suffix `_typo` | — | UPPER/lower: `"VIETNAM"` vs `"vietnam"` |
| `is_default` | BOOLEAN | null, `""`, placeholder | — | — | — | — |
| `created_at` | DATETIME | null, `""`, `"??"`... | `"25/06/2024"`, `"06-25-2024"`, `"Jun 25, 2024"`, `"00/00/0000"`, `"2024-13-45"`, unix timestamp, `"not a date"`, năm 3024, thừa space | — | — | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~375 dòng trùng hoàn toàn
- 🟢 **Near-duplicates**: ~150 dòng gần trùng (khác 1 space hoặc ±0.01)
- 🟣 **Orphaned records**: ~150 dòng có `customer_id` = 999990+ (không tồn tại trong bảng customers)
- 📄 **Malformed CSV rows**: ~1% dòng thừa/thiếu cột

---

## 2. 📂 BẢNG `categories` (20 dòng gốc → ~21 dòng bẩn)

### Cột được bảo vệ:
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |
| `parent_category_id` | Foreign Key → categories.id (self-reference) |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🔵 Mixed Type |
|-----|-----------|------------|-----------|---------------|
| `name` | TEXT | null, `""`, placeholder | — | ALL CAPS, thừa space/tab, double vowels, truncated, empty ẩn |
| `description` | TEXT | null, `""`, placeholder | — | ALL CAPS, whitespace, truncated, trailing newline |
| `created_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn, ngày không hợp lệ | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~1 dòng
- 🟣 **Orphaned records**: ~0-1 dòng có `parent_category_id` → ID không tồn tại

> ⚠️ Bảng nhỏ nên số lỗi tuyệt đối ít, nhưng tỷ lệ % vẫn đáng kể.

---

## 3. 🎟️ BẢNG `coupons` (50 dòng gốc → ~53 dòng bẩn)

### Cột được bảo vệ:
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🟡 Invalid | 🔵 Mixed Type | 🟣 Semantic |
|-----|-----------|------------|-----------|------------|---------------|-------------|
| `code` | TEXT | null, `""`, placeholder | — | — | ALL CAPS, whitespace, truncated | — |
| `discount_type` | TEXT | null, `""`, placeholder | — | Typos | — | UPPER/lower |
| `discount_value` | REAL | null, `""`, placeholder | `"$25.00"`, `"25USD"`, `"1,234.56"` | Giá trị âm, nhân 1000x, = 0 | `"abc"`, `"NaN"`, `"#REF!"`, `true` | — |
| `min_order_amount` | REAL | null, `""`, placeholder | Currency string, commas | Giá trị âm, outlier | `"null"`, `"Inf"` | — |
| `max_uses` | INTEGER | null, `""`, placeholder | Format số lẫn text | Giá trị âm/cực lớn | Mixed type | — |
| `current_uses` | INTEGER | null, `""`, placeholder | Format số lẫn text | Giá trị âm | Mixed type | — |
| `start_date` | DATETIME | null, `""`, placeholder | DD/MM/YYYY, MM-DD-YYYY, DD.MM.YYYY, unix... | — | — | — |
| `end_date` | DATETIME | null, `""`, placeholder | Tương tự start_date | — | — | — |
| `is_active` | BOOLEAN | null, `""`, placeholder | — | Typos | — | UPPER/lower |
| `created_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn | — | — | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~2-3 dòng
- 🟢 **Near-duplicates**: ~1 dòng

---

## 4. 👤 BẢNG `customers` (5,000 dòng gốc → ~5,350 dòng bẩn)

### Cột được bảo vệ:
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🟡 Invalid | 🔵 Mixed Type | 🟣 Semantic |
|-----|-----------|------------|-----------|------------|---------------|-------------|
| `first_name` | TEXT | null, `""`, `"UNKNOWN"`... | — | — | ALL CAPS: `"NGUYỄN"`, whitespace: `"  John  \t"`, double vowels: `"Jooohn"`, truncated: `"Ngu..."`, empty ẩn | — |
| `last_name` | TEXT | null, `""`, placeholder | — | — | Tương tự first_name | — |
| `email` | TEXT | null, `""`, placeholder | — | — | ALL CAPS: `"JOHN@GMAIL.COM"`, whitespace, truncated: `"john@gm..."` | — |
| `phone` | TEXT | null, `""`, placeholder | — | — | ALL CAPS, whitespace, truncated | — |
| `gender` | TEXT | null, `""`, placeholder | — | Typos: `"M"`, `"m"`, `"F"`, `"f"`, `"  male"`, `"female "` | — | `"MALE"` vs `"male"` vs `"Male"` |
| `date_of_birth` | DATETIME | null, `""`, placeholder | `"15/03/1990"`, `"03-15-1990"`, `"Mar 15, 1990"`, `"00/00/0000"`, năm 2990, unix timestamp | — | — | — |
| `created_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn | — | — | — |
| `updated_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn | — | — | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~250 dòng
- 🟢 **Near-duplicates**: ~100 dòng (ví dụ: cùng email nhưng thêm 1 space ở tên)

---

## 5. 💬 BẢNG `feedback` (1,500 dòng gốc → ~1,635 dòng bẩn)

### Cột được bảo vệ:
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |
| `customer_id` | Foreign Key → customers.id |
| `order_id` | Foreign Key → orders.id |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🟡 Invalid | 🔵 Mixed Type | 🟣 Semantic |
|-----|-----------|------------|-----------|------------|---------------|-------------|
| `type` | TEXT | null, `""`, placeholder | — | Typos: suffix `_typo` | — | UPPER/lower |
| `subject` | TEXT | null, `""`, placeholder | — | — | ALL CAPS, whitespace, truncated, double vowels | — |
| `message` | TEXT | null, `""`, placeholder | — | — | ALL CAPS, whitespace, trailing newline, truncated | — |
| `status` | TEXT | null, `""`, placeholder | — | Typos: `"pendng"`, `"completd"`, `"cancled"` | — | `"PENDING"` vs `"pending"` |
| `priority` | TEXT | null, `""`, placeholder | — | Typos: `"hgh"`, `"medum"`, `"lo"`, `"  high"` | — | `"HIGH"` vs `"high"` vs `"High"` |
| `created_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn | — | — | — |
| `resolved_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn | — | — | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~75 dòng
- 🟢 **Near-duplicates**: ~30 dòng
- 🟣 **Orphaned records**: ~30 dòng có `customer_id` hoặc `order_id` = 999990+ (không tồn tại)

---

## 6. 📦 BẢNG `order_items` (23,426 dòng gốc → ~25,067 dòng bẩn)

### Cột được bảo vệ:
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |
| `order_id` | Foreign Key → orders.id |
| `product_id` | Foreign Key → products.id |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🟡 Invalid | 🔵 Mixed Type |
|-----|-----------|------------|-----------|------------|---------------|
| `quantity` | INTEGER | null, `""`, placeholder | `"$5.00"`, `"5USD"` | **Giá trị**: `-5`, `-1`, `0`, `99999` | `"abc"`, `"NaN"`, `"#REF!"`, `true`, `""` |
| `unit_price` | REAL | null, `""`, placeholder | `"$29.99"`, `"29.99USD"`, `"1,234.56"`, precision: 29.990000000000001 | Giá trị âm, nhân 1000x, = 0 | `"null"`, `"Inf"`, `"abc"` |
| `total_price` | REAL | null, `""`, placeholder | Currency string, commas, precision issues | Giá trị âm, nhân 1000x | `"NaN"`, `"#REF!"`, mixed |
| `created_at` | DATETIME | null, `""`, placeholder | DD/MM/YYYY, unix, `"not a date"`, năm 3024... | — | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~1,171 dòng
- 🟢 **Near-duplicates**: ~468 dòng (ví dụ: total_price khác 0.01)
- 🟣 **Orphaned records**: ~468 dòng có `order_id` hoặc `product_id` = 999990+

> ⚠️ **Lỗi logic quan trọng**: Sau khi format bị hỏng, công thức `quantity × unit_price = total_price` sẽ không còn đúng.

---

## 7. 🛒 BẢNG `orders` (10,000 dòng gốc → ~10,900 dòng bẩn)

### Cột được bảo vệ:
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |
| `customer_id` | Foreign Key → customers.id |
| `shipping_address_id` | Foreign Key → addresses.id |
| `billing_address_id` | Foreign Key → addresses.id |
| `coupon_id` | Foreign Key → coupons.id |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🟡 Invalid | 🔵 Mixed Type | 🟣 Semantic |
|-----|-----------|------------|-----------|------------|---------------|-------------|
| `status` | TEXT | null, `""`, placeholder | — | Typos: `"pendng"`, `"shiped"`, `"deliverd"`, `"procesing"`, `"cancled"`, `"completd"`, `"confirmd"`, `"refundd"` | — | `"SHIPPED"` vs `"shipped"` vs `"Shipped "` |
| `subtotal` | REAL | null, `""`, placeholder | `"$150.00"`, `"150USD"`, `"1,500.00"`, precision | Giá trị âm, nhân 1000x, = 0 | `"abc"`, `"NaN"`, `"Inf"`, `"#REF!"`, `true` | — |
| `discount_amount` | REAL | null, `""`, placeholder | Currency string, commas | Giá trị âm, nhân 100x | Mixed type | — |
| `tax_amount` | REAL | null, `""`, placeholder | Currency string, commas | Giá trị âm, nhân 1000x | Mixed type | — |
| `shipping_cost` | REAL | null, `""`, placeholder | Currency string, commas | Giá trị âm, = 0 | Mixed type | — |
| `total_amount` | REAL | null, `""`, placeholder | `"$500.00"`, `"500USD"`, commas, precision | **Outlier**: nhân 1000x, giá trị âm, = 0 | `"null"`, `"NaN"` | — |
| `order_date` | DATETIME | null, `""`, placeholder | `"25/12/2024"`, `"12-25-2024"`, `"Dec 25, 2024"`, `"25.12.2024"`, `"00/00/0000"`, `"2024-13-45"`, unix, `"not a date"`, năm 3024 | — | — | — |
| `created_at` | DATETIME | null, `""`, placeholder | Tương tự order_date | — | — | — |
| `updated_at` | DATETIME | null, `""`, placeholder | Tương tự order_date | — | — | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~500 dòng
- 🟢 **Near-duplicates**: ~200 dòng
- 🟣 **Orphaned records**: ~200 dòng có `customer_id`, `shipping_address_id`, `billing_address_id`, hoặc `coupon_id` = 999990+

> ⚠️ **Lỗi logic**: `subtotal - discount_amount + tax_amount + shipping_cost = total_amount` sẽ không đúng khi các cột bị format sai.

---

## 8. 💳 BẢNG `payments` (10,000 dòng gốc → ~10,700 dòng bẩn)

### Cột được bảo vệ:
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |
| `order_id` | Foreign Key → orders.id |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🟡 Invalid | 🔵 Mixed Type | 🟣 Semantic |
|-----|-----------|------------|-----------|------------|---------------|-------------|
| `payment_method` | TEXT | null, `""`, placeholder | — | Typos: `"Credit Card"`, `"CreditCard"`, `"credit_Card"`, `"Bank Transfer"`, `"BankTransfer"` | — | `"CREDIT_CARD"` vs `"credit_card"` |
| `amount` | REAL | null, `""`, placeholder | `"$250.00"`, `"250USD"`, `"1,250.00"`, precision issues | Giá trị âm, nhân 1000x, = 0 | `"abc"`, `"NaN"`, `"Inf"`, `"#REF!"` | — |
| `status` | TEXT | null, `""`, placeholder | — | Typos: `"completd"`, `"pendng"`, `"faild"`, `"refundd"` | — | `"COMPLETED"` vs `"completed"` vs `"Completed"` |
| `transaction_id` | TEXT (UNIQUE) | null, `""`, placeholder | — | — | ALL CAPS, whitespace: `"  TXN123  \t"`, truncated, double vowels | — |
| `paid_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn, ngày không hợp lệ | — | — | — |
| `created_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn | — | — | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~500 dòng (⚠️ vi phạm UNIQUE constraint trên `transaction_id`)
- 🟢 **Near-duplicates**: ~200 dòng
- 🟣 **Orphaned records**: ~200 dòng có `order_id` = 999990+

---

## 9. 🏷️ BẢNG `products` (500 dòng gốc → ~535 dòng bẩn)

### Cột được bảo vệ:
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |
| `category_id` | Foreign Key → categories.id |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🟡 Invalid | 🔵 Mixed Type | 🟣 Semantic |
|-----|-----------|------------|-----------|------------|---------------|-------------|
| `name` | TEXT | null, `""`, placeholder | — | — | ALL CAPS, whitespace, truncated, double vowels | — |
| `description` | TEXT | null, `""`, placeholder | — | — | ALL CAPS, whitespace, trailing newline, truncated | — |
| `sku` | TEXT (UNIQUE) | null, `""`, placeholder | — | — | ALL CAPS, whitespace, truncated | — |
| `price` | REAL | null, `""`, placeholder | `"$49.99"`, `"49.99USD"`, `"1,234.00"`, precision | **Giá âm**, nhân 1000x, = 0 | `"abc"`, `"NaN"`, `"#REF!"` | — |
| `cost` | REAL | null, `""`, placeholder | Currency string, commas | Giá âm, nhân 1000x | Mixed type | — |
| `stock_quantity` | INTEGER | null, `""`, placeholder | Format số lẫn text | **Giá trị âm**: `-5`, `-1`; cực lớn: `99999` | `"abc"`, `"null"` | — |
| `weight` | REAL | null, `""`, placeholder | Currency string, commas | Giá trị âm | Mixed type | — |
| `brand` | TEXT | null, `""`, placeholder | — | — | ALL CAPS, whitespace, truncated | — |
| `is_active` | BOOLEAN | null, `""`, placeholder | — | Typos | — | UPPER/lower |
| `created_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn | — | — | — |
| `updated_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn | — | — | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~25 dòng (⚠️ vi phạm UNIQUE constraint trên `sku`)
- 🟢 **Near-duplicates**: ~10 dòng
- 🟣 **Orphaned records**: ~10 dòng có `category_id` = 999990+

> ⚠️ **Lỗi logic**: `cost` > `price` (lỗ vốn) có thể xảy ra do giá trị bị biến đổi.

---

## 10. ⭐ BẢNG `reviews` (3,998 dòng gốc → ~4,357 dòng bẩn)

### Cột được bảo vệ:
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |
| `product_id` | Foreign Key → products.id |
| `customer_id` | Foreign Key → customers.id |
| `order_id` | Foreign Key → orders.id |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🟡 Invalid | 🔵 Mixed Type | 🟣 Semantic |
|-----|-----------|------------|-----------|------------|---------------|-------------|
| `rating` | INTEGER | null, `""`, placeholder | `"$4.00"`, `"4USD"` | **Ngoài phạm vi**: `-1`, `0`, `6`, `10`, `99` (hợp lệ: 1-5) | `"abc"`, `"NaN"`, `"#REF!"`, `true` | — |
| `title` | TEXT | null, `""`, placeholder | — | — | ALL CAPS, whitespace, truncated, double vowels, empty ẩn | — |
| `comment` | TEXT | null, `""`, placeholder | — | — | ALL CAPS, whitespace, trailing newline, truncated | — |
| `is_verified` | BOOLEAN | null, `""`, placeholder | — | Typos | — | UPPER/lower |
| `created_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn, ngày không hợp lệ | — | — | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~200 dòng
- 🟢 **Near-duplicates**: ~80 dòng
- 🟣 **Orphaned records**: ~80 dòng có `product_id`, `customer_id`, hoặc `order_id` = 999990+

> ⚠️ **Lỗi đặc biệt của bảng này**: Rating ngoài phạm vi (1-5) là lỗi rất phổ biến trong thực tế, cần validate bằng business rule.

---

## 11. 🚚 BẢNG `shipping` (9,516 dòng gốc → ~10,352 dòng bẩn)

### Cột được bảo vệ:
| Cột | Vai trò |
|-----|---------|
| `id` | Primary Key |
| `order_id` | Foreign Key → orders.id |

### Chi tiết lỗi theo cột:

| Cột | Kiểu gốc | 🔴 Missing | 🟠 Format | 🟡 Invalid | 🟣 Semantic |
|-----|-----------|------------|-----------|------------|-------------|
| `carrier` | TEXT | null, `""`, placeholder | — | Typos | `"FEDEX"` vs `"fedex"` vs `"FedEx"` |
| `tracking_number` | TEXT (UNIQUE) | null, `""`, placeholder | — | — | — |
| `status` | TEXT | null, `""`, placeholder | — | Typos: `"shiped"`, `"deliverd"`, `"pendng"`, `"ship"`, `"delvered"` | `"SHIPPED"` vs `"shipped"` vs `"Shipped "` |
| `shipped_at` | DATETIME | null, `""`, placeholder | `"25/01/2024"`, `"01-25-2024"`, `"Jan 25, 2024"`, `"25.01.2024"`, unix, `"not a date"`, `"00/00/0000"`, năm 3024 | — | — |
| `estimated_delivery` | DATE | null, `""`, placeholder | Tương tự shipped_at | — | — |
| `delivered_at` | DATETIME | null, `""`, placeholder | Tương tự shipped_at | — | — |
| `created_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn | — | — |
| `updated_at` | DATETIME | null, `""`, placeholder | Nhiều format lẫn lộn | — | — |

### Lỗi cấp bảng:
- 🟢 **Exact duplicates**: ~475 dòng (⚠️ vi phạm UNIQUE constraint trên `tracking_number`)
- 🟢 **Near-duplicates**: ~190 dòng
- 🟣 **Orphaned records**: ~190 dòng có `order_id` = 999990+

> ⚠️ **Lỗi logic thời gian**: `shipped_at` > `delivered_at` hoặc `delivered_at` > `estimated_delivery` có thể xảy ra do date format bị hỏng.

---

## 📊 TỔNG HỢP SỐ LƯỢNG LỖI DỰ KIẾN

| Bảng | Dòng gốc | Dòng bẩn (~) | Exact Dup | Near Dup | Orphan | Cột bị ảnh hưởng |
|------|----------|-------------|-----------|----------|--------|-------------------|
| addresses | 7,514 | ~8,190 | ~375 | ~150 | ~150 | 8/10 cột |
| categories | 20 | ~21 | ~1 | ~0 | ~0 | 3/4 cột |
| coupons | 50 | ~53 | ~2 | ~1 | 0 | 9/10 cột |
| customers | 5,000 | ~5,350 | ~250 | ~100 | 0 | 7/8 cột |
| feedback | 1,500 | ~1,635 | ~75 | ~30 | ~30 | 7/9 cột |
| order_items | 23,426 | ~25,067 | ~1,171 | ~468 | ~468 | 4/6 cột |
| orders | 10,000 | ~10,900 | ~500 | ~200 | ~200 | 9/14 cột |
| payments | 10,000 | ~10,700 | ~500 | ~200 | ~200 | 6/8 cột |
| products | 500 | ~535 | ~25 | ~10 | ~10 | 11/13 cột |
| reviews | 3,998 | ~4,357 | ~200 | ~80 | ~80 | 5/9 cột |
| shipping | 9,516 | ~10,352 | ~475 | ~190 | ~190 | 8/10 cột |
| **TỔNG** | **71,524** | **~77,160** | **~3,574** | **~1,429** | **~1,328** | — |

---

## 🔍 CHECKLIST KIỂM TRA SAU KHI LÀM SẠCH

Sau khi clean xong từng bảng, hãy verify:

- [ ] Không còn null/empty/placeholder ở các cột bắt buộc
- [ ] Tất cả date columns đều ở format `yyyy-MM-dd` hoặc `yyyy-MM-dd HH:mm:ss`
- [ ] Tất cả numeric columns đều là số (không chứa ký tự)
- [ ] Không còn giá trị âm ở cột price, quantity, amount
- [ ] Rating nằm trong khoảng [1, 5]
- [ ] Categorical columns đồng nhất về casing (lowercase)
- [ ] Không còn exact duplicates
- [ ] Kiểm tra near-duplicates bằng business key
- [ ] FK integrity: mọi FK đều tham chiếu đến PK tồn tại
- [ ] Không còn whitespace thừa (leading, trailing, tab, newline)
- [ ] Tổng hợp logic đúng: `subtotal - discount + tax + shipping = total`
- [ ] Thứ tự thời gian đúng: `shipped_at ≤ delivered_at`
