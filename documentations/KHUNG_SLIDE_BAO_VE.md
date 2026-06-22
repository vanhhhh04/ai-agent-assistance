# KHUNG SLIDE BẢO VỆ ĐỒ ÁN TỐT NGHIỆP

**Đề tài:** Xây dựng hệ thống AI Agent hỗ trợ truy vấn dữ liệu doanh nghiệp bằng ngôn ngữ tự nhiên (DataFinch)
**SV:** Cao Việt Anh — Ngành KHMT — ĐH Công nghiệp Hà Nội — 2026

> Bộ khung 15 slide, bám đúng bố cục mẫu (Powerpoint_VuThiThaiHa.pptx): Title → Nội dung → 5 phần → Hướng phát triển → Cảm ơn. Mỗi slide ghi rõ **nội dung trên slide** (ngắn) và **lời nói/ghi chú** (để thuyết trình). Thời lượng gợi ý: 12–15 phút.

---

## Slide 1 — Trang bìa
*(slide tiêu đề mẫu)*

- **Báo cáo đồ án tốt nghiệp**
- **Xây dựng hệ thống AI Agent hỗ trợ truy vấn dữ liệu doanh nghiệp bằng ngôn ngữ tự nhiên**
- Giảng viên hướng dẫn: *(điền)*
- Sinh viên: Cao Việt Anh
- Lớp / Khóa: *(điền)*

---

## Slide 2 — Nội dung trình bày (Agenda)
*(slide mục lục mẫu — 5 mục)*

1. Tổng quan đề tài
2. Phân tích & thiết kế hệ thống
3. Luồng xử lý câu hỏi (NL→SQL)
4. Thực nghiệm & kết quả đạt được
5. Demo sản phẩm

> *Nói:* "Phần trình bày của em gồm 5 nội dung chính sau đây."

---

# PHẦN 1 — TỔNG QUAN ĐỀ TÀI

## Slide 3 — Bài toán & Lý do chọn đề tài
*(slide section-1 + đoạn mô tả)*

- **Vấn đề:** Dữ liệu doanh nghiệp tăng nhanh nhưng "ngủ yên" — muốn khai thác phải **biết viết SQL & hiểu lược đồ**. BI truyền thống thì cứng nhắc, kém linh hoạt với câu hỏi phát sinh.
- **Cơ hội:** LLM cho phép hỏi dữ liệu bằng **ngôn ngữ tự nhiên (NL→SQL)**.
- **Thách thức:** cách single-prompt bị **ảo giác (hallucination)** và **khó kiểm soát an toàn** khi schema lớn & câu hỏi tiếng Việt.
- **→ Giải pháp:** Hệ thống **DataFinch** — trợ lý dữ liệu AI theo kiến trúc **đa tác tử (multi-agent)** + nền tảng dữ liệu **Medallion**.

> *Nói:* nhấn vào 3 ý: rào cản SQL → cơ hội từ LLM → nhưng phải giải quyết ảo giác + an toàn.

## Slide 4 — Mục tiêu & Phạm vi
*(thay cho slide "Tổng quan" thứ 2)*

- **Mục tiêu:** (i) nhận câu hỏi tiếng Việt → sinh SQL đúng ngữ nghĩa; (ii) thực thi & trả kết quả kèm giải thích, trực quan hóa; (iii) **an toàn truy vấn, giảm ảo giác**; (iv) chứng minh khả thi bằng thực nghiệm định lượng.
- **Phạm vi:** Pipeline Medallion (Bronze→Silver→Gold) trên HDFS/Spark/Hive; lớp ngữ nghĩa truy hồi lai BM25+kNN (OpenSearch); cụm 5 agent trên FastAPI; giao diện Next.js. Tập trung **lõi NL→SQL**; một số màn hình quản trị ở mức demo.

## Slide 5 — Công nghệ sử dụng
*(slide "Công nghệ sử dụng" mẫu — dạng logo/khối)*

Nhóm theo tầng (chọn ~10 công nghệ tiêu biểu để hiển thị):

| Tầng | Công nghệ |
|---|---|
| Nguồn & CDC | PostgreSQL 15, Debezium, Kafka |
| Tiếp nhận & ETL | Apache NiFi, Apache Spark (PySpark) |
| Lưu trữ & kho phân tích | HDFS (Hadoop), Apache Hive |
| Điều phối | Apache Airflow (*/15 phút) |
| Lớp ngữ nghĩa / AI | OpenSearch, sentence-transformers 768-d, FastAPI |
| LLM | Claude (Anthropic) / OpenAI / Google |
| Giao diện & đóng gói | Next.js + React + Tailwind, Docker Compose (18+ service) |

> *Nói:* "Toàn hệ thống chạy thật trên một máy bằng Docker Compose với hơn 18 service."

---

# PHẦN 2 — PHÂN TÍCH & THIẾT KẾ HỆ THỐNG

## Slide 6 — Mục lục phần 2 + Tác nhân hệ thống
*(slide section-2 mẫu liệt kê các sơ đồ + slide "Sơ đồ Actor")*

Các nội dung thiết kế: **Tác nhân & Use case → Kiến trúc tổng thể → Pipeline dữ liệu → Kiến trúc Multi-Agent → Cơ sở dữ liệu**.

**5 tác nhân chính:**
- Người dùng nghiệp vụ (không biết SQL) — đặt câu hỏi, xem kết quả
- Quản trị viên dữ liệu — cấu hình nguồn, biên tập mô tả schema
- Hệ thống nguồn (ERP, kho vận, thanh toán) — phát sinh dữ liệu
- Bộ điều phối Airflow — chạy pipeline định kỳ
- Nhà cung cấp LLM — sinh quyết định & SQL

> *Chèn:* sơ đồ use case tổng quát (lấy từ Chương 2 trong docx).

## Slide 7 — Kiến trúc tổng thể
*(thay cho slide "Sơ đồ usecase" — đặt diagram kiến trúc)*

- 2 nửa tách biệt: **Data Engineering Pipeline** ↔ **AI Agent Service**.
- Dữ liệu chảy: Nguồn (CDC/CSV/HTTP) → **Bronze → Silver → Gold (star schema)** → AI Agent truy vấn.
- Câu hỏi NL đi qua **5 agent** rồi thực thi trên Gold (Hive) hoặc Bronze (Postgres realtime).

> *Chèn:* sơ đồ kiến trúc tổng thể (project workflow / diagram trong documentations).

## Slide 8 — Pipeline dữ liệu Medallion
*(slide diagram thứ 2)*

| Tầng | Vai trò |
|---|---|
| **Bronze** | Lưu raw events, chỉ append, giữ dòng dõi (Parquet/HDFS) |
| **Silver** | Làm sạch, ép kiểu, **CDC dedup**, tách bản ghi bẩn sang DLQ |
| **Gold** | **Star schema** (fact_sales, fact_reviews, fact_feedback + dim), đăng ký Hive Metastore |

> *Nói:* Airflow điều phối toàn pipeline tự động mỗi 15 phút; Spark xử lý exactly-once nhờ checkpoint.

## Slide 9 — Kiến trúc Multi-Agent & Cơ sở dữ liệu (ERD)
*(slide "Biểu đồ thực thể liên kết" mẫu)*

**Cụm 5 tác tử:** Supervisor (phân loại intent + chọn backend) → Metadata Retriever (hybrid kNN+BM25) → SQL Writer (sinh SQL) → Guardrails (7 lớp an toàn) → Executor (thực thi + stream SSE).

**3 lớp lưu trữ:** PostgreSQL (ERP nguồn) · Hive Gold (star schema) · OpenSearch (finch_catalog, table_docs, query_log).

> *Chèn:* ERD star schema (fact + dim) lấy từ Chương 2.

---

# PHẦN 3 — LUỒNG XỬ LÝ CÂU HỎI (NL→SQL)
*(thay cho "Quy trình mua hàng" — dùng layout 6 bước mẫu)*

## Slide 10 — Section divider phần 3

## Slide 11 — Luồng xử lý 1 câu hỏi (6 bước)
*(slide 6-step mẫu)*

1. **Nhận câu hỏi tiếng Việt** qua giao diện chat
2. **Supervisor** phân loại intent (DATA_QUERY / SCHEMA_INFO / FOLLOWUP / OUT_OF_SCOPE) & chọn backend
3. **Metadata Retriever** truy hồi schema/tài liệu liên quan (hybrid BM25 + kNN)
4. **SQL Writer** sinh SQL đúng phương ngữ (HiveQL/PostgreSQL) + schema augmentation chống ảo giác
5. **Guardrails** kiểm tra 7 lớp an toàn (chỉ SELECT, chặn DDL/DML, giới hạn JOIN, ép LIMIT, che PII)
6. **Executor** thực thi & **stream kết quả realtime (SSE)** kèm bảng + giải thích

> *Nói:* nhấn mạnh đây là điểm khác biệt với single-prompt: mỗi bước có một agent chuyên trách → giảm ảo giác, kiểm soát an toàn.

---

# PHẦN 4 — THỰC NGHIỆM & KẾT QUẢ
*(slide "Kết quả đạt được" mẫu)*

## Slide 12 — Thiết kế thực nghiệm & kết quả chính

- **Bộ đánh giá:** 120 cặp (câu hỏi tiếng Việt, SQL tham chiếu, kết quả mong đợi), phân theo intent & độ khó.
- **So sánh LLM (vai trò SQL Writer):** Claude Sonnet 4.6 tốt nhất — **EX 93.3%**, ảo giác **1.7%** (GPT-5-mini 89.2%, Gemini 2.5 Flash 86.7%).
- **Truy hồi lai:** Hybrid **Recall@8 = 0.94, MRR = 0.83** — vượt BM25 (0.84) và kNN (0.88) đơn lẻ.
- **Guardrails:** chặn đúng **60/60 (100%)** trên bộ đối kháng.
- **End-to-end:** tỷ lệ thành công **88.6%**; nút thắt độ trễ ở **engine Hive** (không phải LLM).

> *Nói:* chốt 4 con số đắt: 93.3% / 0.94 / 100% / 88.6%. Lưu ý một số bảng là số liệu minh họa cần đo lại quy mô lớn hơn (trung thực với hội đồng).

## Slide 13 — Kết quả đạt được (tổng kết)

- Hoàn thiện **pipeline Medallion** đầy đủ (CDC realtime → làm sạch → star schema).
- Hiện thực **cụm 5 agent** trên FastAPI, streaming SSE, **độc lập nhà cung cấp LLM**.
- Áp dụng kỹ thuật **chống ảo giác** (schema augmentation, dialect rules, hybrid retrieval) + **7 lớp Guardrails**.
- Hệ thống **chạy thật, tái lập hoàn toàn trên 1 máy** bằng Docker Compose.

---

# PHẦN 5 — DEMO

## Slide 14 — Demo sản phẩm
*(slide "DEMO" mẫu)*

- Đặt câu hỏi tiếng Việt → xem **agent pipeline visualizer** chạy realtime → kết quả dạng bảng + biểu đồ.
- Kịch bản demo gợi ý: "Top 10 sản phẩm bán chạy quý 1 theo doanh thu" → followup "Còn tháng trước thì sao?" → 1 câu OUT_OF_SCOPE để minh họa Guardrails.

> *Chuẩn bị video clip dự phòng phòng khi mạng/LLM lỗi.*

## Slide 15 — Hướng phát triển & Cảm ơn
*(gộp slide "Hướng phát triển" + "Thank you" mẫu)*

**Hướng phát triển:**
- Tích hợp **xác thực/phân quyền thật** + lưu trữ đa người dùng phía server
- **Thay engine thực thi** (Trino/Spark SQL) để giảm độ trễ
- Mở rộng benchmark + **vòng lặp tự cải tiến** từ query_log
- Hoàn thiện các màn hình quản trị từ demo lên sản phẩm

**THANK YOU** — Em chân thành cảm ơn hội đồng thầy cô đã lắng nghe.
