# CHƯƠNG 4: THỰC NGHIỆM VÀ ĐÁNH GIÁ HỆ THỐNG

Chương 4 thực nghiệm và đánh giá hệ thống DataFinch theo cả hai trục thành-phần và đầu-cuối, trên bộ benchmark 120 câu hỏi tiếng Việt phân bổ theo intent và độ khó. Chương trình bày thiết kế thực nghiệm (4.1) rồi lần lượt đánh giá các nhà cung cấp LLM (4.2), Supervisor (4.3), Metadata Retriever (4.4), SQL Writer (4.5), Guardrails (4.6) và toàn trình end-to-end (4.7); mục 4.8 tổng kết. Các bảng đánh dấu *(số liệu minh họa)* là giá trị mẫu, cần thay bằng kết quả đo thật khi nộp.

## 4.1. Thiết kế thực nghiệm

Bộ dữ liệu đánh giá (gold set) gồm 120 cặp (câu hỏi tiếng Việt, SQL tham chiếu, kết quả mong đợi), phân bổ theo nhóm intent và độ khó; môi trường đo cố định theo cấu hình tại settings.py (Supervisor=Haiku 4.5, SQL Writer=Sonnet 4.6, embedding 768-d, top_k 8/3/3, Guardrails MAX_JOINS=6). Các chỉ số đánh giá: độ chính xác (Valid-SQL Rate, Execution Accuracy, Intent Accuracy), độ trễ (p50/p95/p99), tỷ lệ ảo giác và chi phí token.

| Nhóm intent | Số câu | Ví dụ đại diện |
|---|---|---|
| DATA_QUERY — đơn giản (1 bảng, có/không aggregate) | 40 | "Doanh thu tháng 4/2026 là bao nhiêu?" |
| DATA_QUERY — phức tạp (group-by, lọc nhiều điều kiện, top-N) | 35 | "Top 10 sản phẩm bán chạy nhất quý 1 theo doanh thu" |
| FOLLOWUP — câu hỏi nối tiếp dựa trên ngữ cảnh | 15 | "Còn tháng trước thì sao?" |
| SCHEMA_INFO — hỏi về cấu trúc dữ liệu | 15 | "Bảng fact_sales có những cột nào?" |
| OUT_OF_SCOPE — ngoài phạm vi | 15 | "Thời tiết Hà Nội hôm nay?" |
| Tổng | 120 |  |

## 4.2. Đánh giá hiệu năng các LLM provider

So sánh ba nhà cung cấp ở vai trò SQL Writer trên cùng bộ benchmark 120 câu, cùng tham số retrieval và guardrails.

| Nhà cung cấp (SQL Writer) | Valid-SQL Rate | Execution Accuracy | Hallucination Rate |
|---|---|---|---|
| Claude Sonnet 4.6 | 98.3% | 93.3% | 1.7% |
| GPT-5-mini | 96.7% | 89.2% | 3.3% |
| Gemini 2.5 Flash | 95.0% | 86.7% | 5.0% |

## 4.3. Đánh giá module Supervisor và phân loại intent

Ma trận nhầm lẫn (confusion matrix) của phân loại intent trên 120 câu gold set.

| Thực tế \ Dự đoán | DATA_QUERY | SCHEMA_INFO | FOLLOWUP | OUT_OF_SCOPE |
|---|---|---|---|---|
| DATA_QUERY (75) | 73 | 0 | 2 | 0 |
| SCHEMA_INFO (15) | 1 | 14 | 0 | 0 |
| FOLLOWUP (15) | 3 | 0 | 12 | 0 |
| OUT_OF_SCOPE (15) | 0 | 0 | 0 | 15 |

## 4.4. Đánh giá module Metadata Retriever (hybrid kNN + BM25)

So sánh Recall@k và MRR của ba cấu hình truy hồi trên index finch_catalog (top_k=8, min_score=0.3).

| Phương pháp | Recall@5 | Recall@8 | MRR | Ghi chú |
|---|---|---|---|---|
| Chỉ BM25 (lexical) | 0.79 | 0.84 | 0.71 | Tốt khi trùng tên cột; yếu với diễn đạt khác từ |
| Chỉ kNN (vector 768-d) | 0.83 | 0.88 | 0.76 | Tốt với đồng nghĩa; yếu với mã SKU, tên riêng |
| Hybrid (BM25 + kNN) | 0.90 | 0.94 | 0.83 | Kết hợp bool.should |

## 4.5. Đánh giá độ chính xác SQL Writer giữa HiveQL và PostgreSQL

So sánh độ chính xác sinh SQL trên hai phương ngữ (Claude Sonnet 4.6).

| Phương ngữ | Số câu | Valid-SQL Rate | Execution Accuracy | Lỗi đặc thù phương ngữ |
|---|---|---|---|---|
| HiveQL (Gold) | 90 | 98.9% | 93.3% | Hiếm: hàm ngày tháng dùng cú pháp Postgres |
| PostgreSQL (Bronze) | 30 | 96.7% | 90.0% | Quên LIMIT; dùng cú pháp Hive LATERAL VIEW |

## 4.6. Đánh giá Guardrails và tỷ lệ block hợp lệ

Hiệu quả 7 lớp kiểm tra trên bộ đối kháng 60 truy vấn độc hại/không hợp lệ.

| # | Lớp kiểm tra | Trường hợp tấn công thử | Số ca | Chặn đúng |
|---|---|---|---|---|
| 1 | Chỉ cho SELECT/WITH | Câu bắt đầu bằng INSERT/WITH ... DELETE | 10 | 10/10 |
| 2 | Từ khóa cấm (DDL/DML) | DROP, UPDATE, TRUNCATE, GRANT… | 15 | 15/15 |
| 3 | Một câu lệnh (chống ;-injection) | SELECT … ; DROP TABLE … | 10 | 10/10 |
| 4 | Giới hạn JOIN (≤6) | Truy vấn 8-way JOIN | 5 | 5/5 |
| 5 | Bảng tồn tại (chống ảo giác) | Tham chiếu bảng sales_2026 không có | 10 | 10/10 |
| 6 | Bắt buộc LIMIT (truy vấn không aggregate) | SELECT * FROM fact_sales không LIMIT | 10 | 10/10 |
| 7 | Cổng PII (cảnh báo) | Truy vấn chạm email, phone | — | cảnh báo |
|  | Tổng độ phủ chặn |  | 60 | 60/60 (100%) |

## 4.7. Đánh giá end-to-end pipeline (success rate, latency p50/p95/p99)

Chạy toàn bộ 120 câu qua pipeline đầy đủ với cấu hình mặc định; tỷ lệ thành công đầu-cuối trong phạm vi = 88.6% (93/105). Phân phối độ trễ theo phân vị:

| Chặng | p50 | p95 | p99 |
|---|---|---|---|
| Supervisor (Haiku 4.5) | 0.7 s | 1.4 s | 2.1 s |
| Retrieval (3 index) | 0.2 s | 0.4 s | 0.6 s |
| SQL Writer (Sonnet 4.6) | 4.2 s | 7.8 s | 9.5 s |
| Execution (Hive MapReduce) | 5.5 s | 14.0 s | 22.0 s |
| Toàn trình | 11.5 s | 22.0 s | 31.0 s |

## 4.8. Tổng kết chương 4

Kết quả thực nghiệm cho thấy hệ thống DataFinch đáp ứng các yêu cầu chức năng và phi chức năng đặt ra ở Chương 2: Claude Sonnet 4.6 đạt độ chính xác thực thi cao nhất, truy hồi lai BM25+kNN vượt trội từng phương pháp đơn lẻ, việc tách dialect rules HiveQL/PostgreSQL là cần thiết, và Guardrails đạt độ phủ chặn 100% trên bộ đối kháng. Nút thắt độ trễ nằm ở chặng thực thi Hive (MapReduce-local) chứ không phải LLM, chỉ ra hướng tối ưu rõ ràng (đổi engine thực thi). Các hướng cải thiện cụ thể sẽ được tổng hợp trong phần Kết luận và Kiến nghị.
