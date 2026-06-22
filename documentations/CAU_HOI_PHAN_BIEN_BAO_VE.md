# BỘ CÂU HỎI PHẢN BIỆN & GỢI Ý TRẢ LỜI — BẢO VỆ DataFinch

> Soạn bám sát mã nguồn và tài liệu thực tế của dự án (DATA_FLOW.md, AI_AGENT_HIEU_SCHEMA_VA_CAU_HOI.md, guardrails.py, CHI_TIET_LOI_DU_LIEU.md, HUONG_DAN_LAM_SACH_SPARK.md).
> Mỗi câu trả lời viết ngắn gọn theo kiểu "nói được ngay trước hội đồng". Phần *(Nếu bị hỏi sâu)* là phòng khi giáo viên truy thêm.

---

# PHẦN A — TỪ NGUỒN DỮ LIỆU ĐẾN KHO DỮ LIỆU (Layer 1)

## A1. Ba nguồn dữ liệu mô phỏng — nguồn nào cho bảng nào? Xuất ra loại dữ liệu gì?

Em cố tình mô phỏng **3 pattern thu thập dữ liệu khác nhau** để giống một doanh nghiệp thật (hệ thống dị nguyên):

| Nguồn | Bảng dữ liệu | Cơ chế đưa vào | Có CDC? | Mô phỏng cái gì |
|---|---|---|---|---|
| **ERP** (`sim_erp.py`) | customers, orders, order_items, coupons | PostgreSQL → Debezium đọc WAL → Kafka | **Có** (logical decoding) | Nguồn OLTP giao dịch chuẩn |
| **Warehouse** (`sim_warehouse.py`) | categories, products | Ghi file CSV → NiFi GetFile → Kafka | Không | Hệ thống cũ xuất file theo lô (batch dump) |
| **Payment Gateway** (`sim_payment.py`) | payments, shipping, reviews, feedback | HTTP POST → NiFi ListenHTTP → Kafka | Không | Dịch vụ SaaS gửi webhook |

- **Loại dữ liệu xuất ra**: tất cả đều là **JSON event** đẩy vào Kafka topic, nhưng cấu trúc khác nhau cố ý:
  - ERP: JSON phẳng + trường metadata CDC (`__op`, `__table`, `__source_ts_ms`).
  - Warehouse: JSON **envelope phẳng** (metadata nằm cùng cấp với dữ liệu).
  - Payment: JSON **envelope lồng nhau** (`payload: {...}` bọc bên trong).
- Cả 3 hội tụ vào Kafka rồi gom về một tầng Bronze duy nhất.

## A2. Các loại dữ liệu "bẩn" được tạo ra là gì?

Em chủ động inject **6 nhóm lỗi** trên 11 bảng (~71.500 dòng sạch → ~77.000 dòng bẩn):

1. **Khuyết thiếu (Missing/Null)** ~10%: null, empty string `""`, placeholder (`UNKNOWN`, `N/A`, `#N/A`, `TBD`, `--`…).
2. **Sai định dạng (Malformed)** ~8%: ngày trộn nhiều format (`25/06/2024`, `Jun 25 2024`, unix timestamp, `2024-13-45`, `00/00/0000`); số có `$`, dấu phẩy, `100USD`.
3. **Giá trị không hợp lệ (Invalid)** ~6%: giá/số lượng âm, rating ngoài khoảng 1–5, typo trạng thái (`shiped`, `deliverd`, `pendng`), outlier nhân 1000×.
4. **Trùng lặp (Duplicate)** ~7%: trùng hoàn toàn (~3.574 dòng) + gần trùng (~1.429 dòng, lệch 1 space hoặc ±0.01).
5. **Sai kiểu (Mixed type)** ~5%: cột số chứa `"abc"`, `"NaN"`, `"Inf"`, `"#REF!"`, `true`.
6. **Lỗi ngữ nghĩa/ngữ cảnh (Semantic)** ~6%: FK mồ côi (~1.328 dòng tham chiếu ID 999990+ không tồn tại), casing lẫn lộn (`DELIVERED`/`delivered`/`Delivered`).

*(Nếu bị hỏi sâu)* Ví dụ lỗi logic: sau khi format hỏng thì `quantity × unit_price ≠ total_price`, hoặc `cost > price` (bán lỗ vốn). Đây là loại lỗi phải bắt bằng **business rule**, không bắt được bằng kiểm tra kiểu dữ liệu.

## A3. "Envelope" là gì? Mục đích của nó?

**Envelope** là lớp metadata chuẩn bọc quanh mỗi bản ghi trước khi đẩy vào Kafka:
`_source_system`, `_schema_version`, `_event_id`, `_event_type`, `_ingested_at`, `_quality_flag` (`CLEAN`/`DIRTY`), `_dirty_reason`.

Mục đích:
- **Truy nguồn**: biết event đến từ hệ thống nào, version schema nào.
- **Idempotency**: `_event_id` cho phép Spark khử trùng lặp khi event bị gửi lại.
- **Phân luồng chất lượng**: `_quality_flag` để tách thẳng dòng bẩn sang DLQ.
- Em cố tình làm **2 dạng envelope** (warehouse phẳng, payment lồng nhau) để Spark Silver phải xử lý cả hai cấu trúc — đúng như thực tế dữ liệu dị nguyên.

## A4. NiFi xử lý 2 nguồn warehouse và payment như thế nào?

- **Warehouse (file CSV)**: `GetFile` (poll thư mục mỗi 1s) → `ConvertRecord` (CSVReader → JsonRecordSetWriter) → `PublishKafkaRecord` vào topic `warehouse.events`.
- **Payment (webhook)**: `ListenHTTP` (cổng 8181, path `payment-events`) nhận POST → `PublishKafkaRecord` vào topic `payment.events`.

## A5. Tại sao ERP chảy thẳng vào Kafka mà không qua NiFi?

Vì ERP dùng **CDC**: Debezium đọc trực tiếp WAL (Write-Ahead Log) của PostgreSQL qua logical decoding, rồi Kafka Connect đẩy vào Kafka. **Debezium đã là cầu nối chuyên dụng DB → Kafka** nên không cần NiFi.

NiFi chỉ cần cho **2 nguồn không có CDC** — file CSV và HTTP webhook — nơi phải có adapter để đọc/định dạng/đẩy vào Kafka. Đây chính là dụng ý: trình bày **3 pattern ingestion khác nhau** (CDC, file batch, webhook).

## A6. Tại sao dùng NiFi mà không phải công cụ khác?

- NiFi mạnh nhất ở bài toán **thu thập & định tuyến dữ liệu từ nguồn dị nguyên**: có sẵn hàng trăm processor (`GetFile`, `ListenHTTP`, `ConvertRecord`, `PublishKafka`), kéo-thả trực quan, không phải viết code producer riêng.
- Có **back-pressure** và **data provenance** (truy vết từng luồng) — hợp cho việc demo/mô phỏng nhiều nguồn cùng lúc.
- Một mình NiFi xử lý được **cả file lẫn HTTP** trong cùng một nơi.

*(So sánh)* Custom Kafka producer thì phải tự code và bảo trì; Logstash thiên về log; Airbyte thiên về connector dạng batch ELT. NiFi cho luồng (flow-based) trực quan và linh hoạt nhất cho mục tiêu mô phỏng của em.

## A7. Kafka lưu các topic như thế nào? Xử lý DLQ ra sao?

- **6 topic**: `erp.public.{customers, orders, order_items, coupons}`, `warehouse.events`, `payment.events`. Mỗi topic 1 partition (quy mô đồ án; production nên ≥3 để có song song).
- Spark đọc theo **batch** từ `earliest → latest`, không commit offset → mỗi lần chạy đọc lại toàn bộ, **idempotent và tái lập được**.
- **DLQ (Dead Letter Queue)**: ở tầng Silver, mỗi bảng có hàm `drop_dirty()` trả về `(clean, dirty)`. Dòng bẩn được ghi sang `hdfs://.../silver/dlq/` (mode append) kèm `id`, `_quality_flag`, `_bronze_ingested_at` để rà soát sau.

*(Nếu bị hỏi sâu)* Hạn chế đã biết: DLQ gộp nhiều bảng chỉ giữ cột chung nên mất chi tiết — muốn xem đầy đủ phải quay lại Bronze. Em ghi nhận đây là điểm cải tiến.

## A8. Spark xử lý dữ liệu như thế nào? Vai trò của envelope trong Spark?

Spark chạy 3 job theo kiến trúc **Medallion**:
1. **Bronze** (`bronze_ingestion.py`): đọc raw từ Kafka → ghi Parquet HDFS, **giữ nguyên** `raw_data` + metadata Kafka, partition theo `_source_topic`/`_kafka_partition`, mode `overwrite`.
2. **Silver** (`silver_transform.py`): 3 nhánh theo nguồn, parse schema có kiểu, drop event xoá của Debezium, làm sạch, khử trùng, tách DLQ.
3. **Gold** (`gold_transform.py`): dựng star schema, đăng ký Hive EXTERNAL table.

**Envelope trong Spark**: Silver dùng envelope để parse đúng từng nguồn — ERP đọc phẳng + `__op`/`__table`; Warehouse đọc `WH_SCHEMA` phẳng; Payment đọc 2 lớp (`ENVELOPE_SCHEMA` rồi `from_json(payload)`). Đồng thời dùng `_event_id` để khử trùng idempotent và `_quality_flag` để tách DLQ.

## A9. 3 layer (Bronze/Silver/Gold) có mục đích gì? Vai trò từng layer?

| Layer | Vai trò | Đặc điểm |
|---|---|---|
| **Bronze** | Lưu dữ liệu **thô, bất biến**, "nguồn sự thật" để có thể replay | Giữ nguyên raw + metadata, không sửa |
| **Silver** | Dữ liệu **sạch, chuẩn hoá, có kiểu, đã khử trùng** — một snapshot mới nhất | Mỗi nguồn 1 nhánh xử lý riêng |
| **Gold** | Dữ liệu **mức nghiệp vụ**, star schema tối ưu cho phân tích | Đăng ký vào Hive cho AI agent truy vấn |

**Mục đích tách 3 tầng**: tách bạch trách nhiệm; debug được từng tầng; **replay không mất dữ liệu gốc** (lỗi ở Silver/Gold thì dựng lại từ Bronze); mỗi tầng tối ưu cho mục tiêu riêng (lưu trữ thô vs phân tích).

## A10. Các bước làm sạch như thế nào? (theo 6 loại lỗi)

1. **Missing/Null** → đổi placeholder & empty string thành null; điền (impute median/mode) hoặc loại bỏ dòng thiếu quá nửa.
2. **Sai định dạng** → parse ngày đa định dạng bằng `coalesce` nhiều pattern; bỏ ký hiệu tiền tệ/dấu phẩy rồi cast số; làm tròn precision.
3. **Giá trị không hợp lệ** → ép phạm vi (rating 1–5), loại giá trị âm, sửa typo trạng thái bằng **mapping/fuzzy match**, phát hiện outlier bằng **IQR**.
4. **Trùng lặp** → `dropDuplicates` cho trùng tuyệt đối; trùng gần thì dedup theo **business key** hoặc Window `row_number`.
5. **Sai kiểu** → đọc bằng **schema tường minh** (không inferSchema), trim whitespace/tab/newline, dùng regex `rlike` trước khi cast.
6. **Ngữ nghĩa/ngữ cảnh** → xử lý FK mồ côi bằng `left_anti`/`left_semi`, chuẩn hoá casing về lowercase, kiểm tra referential integrity toàn hệ.

**Thứ tự làm sạch**: bảng tham chiếu → bảng cha → bảng con (đảm bảo FK).

## A11. Star schema là gì? Mục đích?

Star schema (sơ đồ hình sao) gồm **1 bảng fact trung tâm** (`fact_sales`, mỗi dòng = 1 order_item) nối tới các **bảng dimension** (`dim_customers`, `dim_products`, `dim_payments`, `dim_shipping`) qua surrogate key — nhìn như ngôi sao.

Mục đích:
- **Tối ưu truy vấn phân tích**: ít JOIN, dimension đã denormalized → query nhanh, dễ viết.
- **Dễ hiểu cho người dùng nghiệp vụ/BI**: fact = số đo, dim = ngữ cảnh (ai, cái gì, khi nào).
- `fact_sales` **partition theo `order_year/order_month`** → prune phân vùng khi lọc theo ngày.
- Dùng **LEFT JOIN** nên fact mồ côi (FK không khớp dim) vẫn giữ lại, dim key = NULL — đây là hành vi **đúng**, không phải bug.

---

# PHẦN B — TỪ CÂU HỎI ĐẾN AI AGENT VÀ KHO DỮ LIỆU (Layer 2)

## B1. Luồng cơ bản từ giao diện đến khi sinh câu trả lời?

`POST /api/query/ask` → chạy qua 5 bước (stream SSE từng bước về UI):

1. **Supervisor** (`supervisor.py`) — 1 lần gọi LLM (model rẻ Haiku): phân loại intent + chọn backend, trả JSON.
2. **Semantic layer** (`semantic_layer.py`): embed câu hỏi 1 lần thành vector 768 chiều, **hybrid BM25 + kNN** trên 3 index OpenSearch.
3. **SQL Writer** (`sql_writer.py`) — model mạnh (Sonnet): dựng prompt theo dialect, gọi LLM sinh SQL → đưa qua **Guardrails** `validate_sql()`.
4. **Execution** (`retrieval/{hive,postgres}_agent.py`): chạy SQL trên kho tương ứng, trả về các dòng kết quả.
5. **Log** (`query_logger.py`): ghi `query_log` để vòng sau dùng làm few-shot.

→ Trả về `{sql, rows, columns, explanation}`.

## B2. Mục đích của việc tách nhiều lớp AI agent?

- **Tối ưu chi phí**: việc đơn giản (phân loại intent) dùng model rẻ (Haiku); việc khó (sinh SQL) dùng model mạnh (Sonnet).
- **Kiểm soát & audit từng bước**: đặc biệt Guardrails tách riêng để dùng chung cho nhiều agent và dễ kiểm tra bảo mật.
- **Dễ debug & thay thế**: mỗi agent một trách nhiệm rõ ràng, sửa/đổi một phần không ảnh hưởng phần khác.
- **An toàn**: LLM dù "hiểu schema" vẫn có thể hallucinate, nên bắt buộc có lớp Guardrails độc lập chốt chặn cuối.

## B3. 4 intent xử lý là gì? Mỗi intent đi về đâu?

| Intent | Ý nghĩa | Đi về đâu |
|---|---|---|
| **DATA_QUERY** | Hỏi số liệu: doanh thu, top-N, xu hướng, so sánh | Vào pipeline retrieval + SQL Writer |
| **SCHEMA_INFO** | Hỏi "có bảng gì / cột nào?" | Trả thẳng từ **Schema Cache**, KHÔNG gọi SQL Writer |
| **FOLLOWUP** | Nối tiếp câu trước ("so với tháng trước thì sao") | Nhồi lịch sử hội thoại vào prompt, tắt cache |
| **OUT_OF_SCOPE** | Chào hỏi, đùa, ngoài phạm vi | Trả lời lịch sự rồi kết thúc |

**Backend**: `hive_gold` (mặc định, ~95% câu phân tích) / `postgres_bronze` (chỉ khi cần dữ liệu live/realtime).

## B4. AI agent hiểu ngữ cảnh dữ liệu như thế nào? Cách xử lý context?

Hệ thống có **2 lớp hiểu schema song song**:
- **Lớp 1 — Schema Cache (RAM)**: nắm **toàn bộ** bảng/cột, load 1 lần lúc khởi động. Chỉ phục vụ **Guardrails** (whitelist bảng) và trả lời **SCHEMA_INFO**. **Không bao giờ** nhét vào prompt LLM.
- **Lớp 2 — Semantic Layer (OpenSearch)**: chỉ kéo **top-K bảng/cột/tài liệu liên quan** vào prompt.

**Cách xử lý context**: embed câu hỏi 1 lần → hybrid search 3 index → hàm `as_prompt_block()` render kết quả thành **markdown gọn** → nhét vào system prompt của SQL Writer.

LLM **không bao giờ thấy full schema/DDL**. Nó chỉ thấy ~5 bảng × vài cột top match, kèm **sample values**, **tài liệu nghiệp vụ**, và 1–3 câu hỏi cũ thành công làm few-shot.

*(Vì sao không nhồi full schema?)* Lãng phí token (50 bảng × 30 cột ≈ 120k ký tự nhiễu mỗi request), làm LLM phân tâm chọn sai bảng, và tên cột kỹ thuật không gợi ngữ nghĩa.

## B5. 3 OpenSearch index là gì?

| Index | Mỗi document chứa | Vai trò |
|---|---|---|
| **`finch_catalog`** | 1 doc/bảng + **1 doc/cột** (type, sample_values, cờ PII) | Mô tả schema kỹ thuật |
| **`table_docs`** | 1 doc/section markdown do người viết | **Tri thức nghiệp vụ** (vd: "không SUM `order_total` vì lặp trên các item") |
| **`query_log`** | 1 doc/lần hỏi (NL + SQL + status) | **Few-shot từ lịch sử** — chỉ lấy `status=success` |

`top_k` mặc định: catalog 8, docs 3, history 3.

*(Điểm hay để nhấn mạnh)* `sample_values` rất quan trọng: khi user hỏi "đơn đã giao", BM25 match được sample `['DELIVERED','PENDING',...]` → LLM biết phải lọc `order_status = 'DELIVERED'`.

## B6. BM25 cân nhắc yếu tố nào? Điểm mạnh/yếu?

BM25 là thuật toán xếp hạng **từ vựng (lexical)**, dựa trên 3 yếu tố:
1. **Term frequency (TF)** — từ khoá xuất hiện càng nhiều trong document càng phù hợp.
2. **Inverse document frequency (IDF)** — từ càng hiếm trên toàn corpus càng có trọng số cao.
3. **Field-length normalization** — document ngắn được ưu tiên (tránh thiên vị doc dài).

Trong dự án còn thêm **boost**: `table_name^3`, `column_name^2`, `description^1`, và `fuzziness: AUTO` để chịu lỗi gõ.

- **Mạnh**: khớp từ khoá chính xác (`brand`, `fact_sales`, `DELIVERED`), nhanh, không cần huấn luyện.
- **Yếu**: không hiểu paraphrase / tiếng Việt tự nhiên — "hãng nào bán chạy" **không** match được cột `brand`. (Đây chính là lý do cần kNN.)

## B7. kNN là gì? Phương pháp đánh giá độ phù hợp? Điểm mạnh/yếu?

kNN (k-nearest neighbors) tìm **k vector gần nhất** với vector câu hỏi trong không gian embedding.
- **Đánh giá độ phù hợp** = **cosine similarity** giữa vector câu hỏi và vector document. Model embedding: `paraphrase-multilingual-mpnet-base-v2`, **768 chiều**, đa ngôn ngữ, vector được normalize (cosine ≡ dot product). Index dùng **HNSW**.
- **Mạnh**: hiểu **ngữ nghĩa** và **đa ngôn ngữ** — "hãng bán chạy" ↔ `brand`, "doanh thu" ↔ `item_total`.
- **Yếu**: kém với tên kỹ thuật hiếm gặp trong corpus huấn luyện; cần model embedding + index, nặng RAM và chậm hơn BM25.

**→ Vì vậy dùng Hybrid**: kết hợp `should + minimum_should_match=1` — document khớp **bất kỳ** nhánh nào (BM25 hoặc kNN) đều được trả về, OpenSearch xếp hạng theo `_score` tổng. BM25 lo từ khoá chính xác, kNN lo ngữ nghĩa.

## B8. SQL Writer agent — vai trò? Cách prompt?

- **Vai trò**: nhận context từ semantic layer → sinh ra **một** câu SQL hợp lệ. Đây là việc khó nhất nên dùng model mạnh (Sonnet).
- **Cách prompt**: build system prompt theo **dialect** (HiveQL cho `gold.*` / PostgreSQL), gồm 8–9 rule, trong đó có **mapping ngữ nghĩa tiếng Việt**:
  - "bán chạy nhất" → `SUM(quantity)` hoặc `COUNT(DISTINCT order_key)`
  - "doanh thu" → `SUM(item_total)`
  - "đắt nhất" → unit price cao nhất
- Phần "Available context" chỉ gồm bảng/cột đã retrieve + business doc + few-shot.
- **Ép output JSON thuần**: `{sql, explanation, tables_used, complexity, has_date_filter, aggregated}` → parse bằng `parse_json()`.
- Bật prompt cache (trừ FOLLOWUP vì context thay đổi mỗi lượt).

## B9. Guardrails kiểm tra an toàn như thế nào? 7 bước validate là gì?

Mọi SQL **bắt buộc** qua `validate_sql()` (`core/guardrails.py`) trước khi chạm kho. 7 bước:

1. **Chỉ cho SELECT/WITH** — bác mọi câu không phải truy vấn đọc.
2. **Chặn từ khoá nguy hiểm** — `DELETE/UPDATE/INSERT/DROP/CREATE/ALTER/TRUNCATE/GRANT/REVOKE/EXEC…` (so khớp theo **biên từ**).
3. **Chỉ một câu lệnh** — phát hiện `;` để chặn SQL injection (nhét lén câu thứ hai).
4. **Giới hạn JOIN/UNION** — `MAX_JOINS = 6`, `MAX_UNION = 3` → chống join N chiều làm sập cluster.
5. **Whitelist bảng** — bảng tham chiếu phải nằm trong `known_tables` (gộp từ catalog hits + schema cache) → **chống hallucinate bảng không tồn tại**.
6. **Bắt buộc LIMIT** với câu không tổng hợp → chống full table scan.
7. **Cổng PII** — nếu chạm cột nhạy cảm (email, phone, address) thì cảnh báo/chặn theo cờ `allow_pii`.

Nếu fail → trả `ValidationResult(valid=False, error)` về user và ghi `query_log` với `status=guardrail_fail`. LLM có thể được prompt lại kèm lý do lỗi.

*(Vì sao cần Guardrails dù LLM đã hiểu schema?)* Vì LLM vẫn có thể sinh câu nguy hiểm hoặc bịa tên bảng. Guardrails là lớp chốt chặn **độc lập, có thể audit**, tách riêng để mọi agent dùng chung.

## B10. Vòng feedback — agent tự học như thế nào?

Mỗi câu hỏi chạy thành công được ghi vào `query_log`. Lần sau gặp câu tương tự, kNN match cao → đẩy SQL cũ vào prompt làm few-shot → SQL mới chính xác hơn. History **chỉ lấy `status=success`** và **loại các câu bị thumbs-down** để không học lại lỗi.

---

# PHẦN C — CÂU HỎI BẪY THƯỜNG GẶP (chuẩn bị sẵn)

**C1. "Số liệu thực nghiệm báo cáo Claude/GPT/Gemini, nhưng code mặc định Ollama?"**
→ Lớp LLM được trừu tượng hoá qua biến môi trường (`LLM_PROVIDER`, `LLM_MODEL`); cấu hình thực nghiệm dùng nhà cung cấp tương ứng. (Lưu ý: cần kiểm tra lại code đã trỏ đúng provider trước buổi bảo vệ để số khớp.)

**C2. "Dự án có dùng LangGraph không?"**
→ **Không.** Dự án dùng **LangChain** cho phần kết nối LLM và truy vấn SQL. Luồng multi-agent (Supervisor → Semantic → SQL Writer → Guardrails → Executor) do em **tự xây dựng** để kiểm soát chặt từng bước, đặc biệt là khâu Guardrails.

**C3. "Tại sao không nhồi cả schema vào prompt cho chắc?"**
→ Lãng phí token, gây nhiễu khiến LLM chọn sai bảng, tăng latency và giảm cache hit. Semantic retrieval chỉ đưa phần liên quan nên vừa rẻ vừa chính xác hơn.

**C4. "BM25 đã đủ chưa, cần kNN làm gì?"**
→ BM25 không hiểu tiếng Việt/paraphrase. "Hãng nào bán chạy" chỉ match được cột `brand` nhờ kNN. Hybrid lấy điểm mạnh của cả hai.

**C5. "Hệ thống chống SQL độc hại / rò rỉ dữ liệu thế nào?"**
→ 7 bước Guardrails (xem B9): chỉ SELECT, chặn từ khoá DML/DDL, chặn `;`-injection, giới hạn JOIN/UNION, whitelist bảng, bắt buộc LIMIT, cổng PII.

**C6. "Dữ liệu thật hay mô phỏng?"**
→ Mô phỏng ở quy mô đồ án (~71.500 dòng sạch), nhưng **cố tình tạo lỗi giống thực tế** (6 nhóm lỗi, race condition, FK mồ côi) và **3 pattern ingestion thật** (CDC, file batch, webhook) để chứng minh pipeline xử lý được dữ liệu bẩn dị nguyên.

**C7. "Độ trễ từ nguồn đến câu trả lời?"**
→ Pipeline Bronze→Gold chạy theo lịch Airflow (hiện @hourly) nên Gold có thể trễ tới ~1 giờ; để có số liệu tươi hơn, agent có thể truy vấn thẳng PostgreSQL (cấu hình qua backend `postgres_bronze`).

**C8. "Điểm yếu/hướng cải tiến của hệ thống?"**
→ (1) Kafka 1 partition — production cần ≥3; (2) Spark Bronze overwrite toàn bộ — nên chuyển sang streaming + append khi >10M event/ngày; (3) DLQ gộp mất chi tiết; (4) Guardrails kiểm cột mới ở mức bảng, validation cột để DB lo; (5) FOLLOWUP còn miss khi câu hỏi quá ngắn — có thể re-formulate query trước khi retrieve.
