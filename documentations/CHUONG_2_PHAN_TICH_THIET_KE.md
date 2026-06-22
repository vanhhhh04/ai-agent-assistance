# CHƯƠNG 2: PHÂN TÍCH VÀ THIẾT KẾ HỆ THỐNG

Trên cơ sở lý thuyết ở Chương 1, Chương 2 phân tích bài toán và thiết kế kiến trúc hệ thống DataFinch: phát biểu bài toán, mục tiêu, tác nhân, ca sử dụng và yêu cầu (2.1); kiến trúc tổng thể hai trụ cột — đường ống dữ liệu Medallion và dịch vụ AI Agent đa tác tử (2.2); rồi thiết kế từng thành phần — luồng dữ liệu (2.3), kiến trúc multi-agent (2.4), cơ sở dữ liệu (2.5), giao diện và API (2.6); mục 2.7 tổng kết.

## 2.1. Mô tả bài toán

### 2.1.1. Phát biểu bài toán

Doanh nghiệp Việt Nam đối mặt nghịch lý "giàu dữ liệu nhưng nghèo thông tin": dữ liệu giao dịch sinh ra liên tục nhưng người dùng nghiệp vụ không thể tự khai thác do rào cản SQL. Bài toán của đồ án là **xây dựng một trợ lý dữ liệu AI cho phép người dùng đặt câu hỏi bằng tiếng Việt, hệ thống tự chuyển thành SQL, thực thi trên hạ tầng phù hợp và trả kết quả (bảng, giải thích, biểu đồ) một cách chính xác, an toàn và tức thời.**

Bài toán được chia thành hai bài toán con gắn kết: (1) **Kỹ nghệ dữ liệu** — thu thập dữ liệu từ các nguồn (ERP, kho vận, thanh toán) gần thời gian thực, làm sạch, chuẩn hóa và tổ chức thành kho dữ liệu phân tích; (2) **Hỏi đáp NL→SQL** — hiểu ý định, định tuyến tới đúng nguồn, truy hồi ngữ cảnh schema, sinh SQL chính xác, kiểm tra an toàn và thực thi. Các ràng buộc nổi bật: đầu vào là tiếng Việt tự do; đầu ra phải đúng ngữ nghĩa, an toàn (không phá hủy dữ liệu, không lộ PII) và đủ nhanh; schema lớn không thể nhồi toàn bộ vào mô hình; có hai loại nhu cầu — phân tích tổng hợp (OLAP) và tra cứu thời gian thực (OLTP).

### 2.1.2. Mục tiêu của hệ thống

**Mục tiêu chức năng:** (M1) hỏi dữ liệu bằng tiếng Việt không cần biết SQL; (M2) tự phân loại ý định và định tuyến tới backend phù hợp (Hive/PostgreSQL); (M3) sinh SQL chính xác trên schema lớn nhờ truy hồi ngữ cảnh thay vì nhồi toàn bộ schema; (M4) đảm bảo an toàn — chặn thao tác phá hủy và kiểm soát PII; (M5) hiển thị tiến trình theo thời gian thực (streaming); (M6) xây dựng đường ống dữ liệu tự động hóa, cập nhật định kỳ. **Mục tiêu phi chức năng:** (M7) độ trễ ở mức tương tác; (M8) độc lập nhà cung cấp LLM để tối ưu chi phí và tránh khóa nhà cung cấp; (M9) mở rộng và bảo trì nhờ tách thành phần, container hóa; (M10) tự cải tiến nhờ ghi nhật ký truy vấn và phản hồi người dùng.

### 2.1.3. Phân tích tác nhân và ca sử dụng

Hệ thống có năm nhóm tác nhân, gồm tác nhân người dùng và tác nhân hệ thống:

| Tác nhân | Mô tả | Tương tác chính |
|---|---|---|
| Người dùng nghiệp vụ | Chủ doanh nghiệp, quản lý, nhân viên — không biết SQL | Đặt câu hỏi, xem kết quả, lưu truy vấn, đánh giá 👍/👎 |
| Quản trị viên dữ liệu | Người quản lý nguồn dữ liệu và danh mục | Cấu hình nguồn, duyệt/biên tập mô tả schema |
| Hệ thống nguồn | ERP, kho vận, cổng thanh toán | Phát sinh dữ liệu giao dịch đẩy vào pipeline |
| Bộ điều phối (Airflow) | Tác nhân hệ thống chạy định kỳ | Kích hoạt pipeline Bronze→Silver→Gold theo lịch |
| Nhà cung cấp LLM | Dịch vụ ngoài (Anthropic/OpenAI/Google) | Nhận prompt, trả lời cho các agent |

[Hình 2.1: Sơ đồ ca sử dụng (use case) của hệ thống DataFinch]

Ca sử dụng tiêu biểu là **UC1 — "Hỏi dữ liệu bằng ngôn ngữ tự nhiên"**: người dùng nhập câu hỏi; hệ thống phân loại ý định và chọn backend; truy hồi bảng/cột liên quan; sinh SQL; kiểm tra an toàn; thực thi; stream kết quả về giao diện. Các luồng phụ: câu hỏi ngoài phạm vi → từ chối lịch sự; câu hỏi về schema → trả thông tin schema; SQL vi phạm an toàn → trả lỗi, không thực thi. Ngoài UC1 còn có các ca: hỏi schema (UC6), quản lý saved queries/báo cáo (UC3–UC5), cấu hình nguồn dữ liệu (UC7), pipeline tự động theo lịch (UC8–UC9).

### 2.1.4. Yêu cầu chức năng và phi chức năng

| Mã | Yêu cầu chức năng | Liên quan |
|---|---|---|
| FR1 | Tiếp nhận câu hỏi tiếng Việt qua giao diện chat | UC1 |
| FR2 | Phân loại ý định: DATA_QUERY / SCHEMA_INFO / FOLLOWUP / OUT_OF_SCOPE | UC1 |
| FR3 | Chọn backend: hive_gold (phân tích) hoặc postgres_bronze (thời gian thực) | UC1 |
| FR4 | Truy hồi ngữ cảnh schema/tài liệu liên quan từ lớp ngữ nghĩa | UC1 |
| FR5 | Sinh SQL đúng phương ngữ (HiveQL/PostgreSQL) kèm giải thích | UC1, UC2 |
| FR6 | Kiểm tra an toàn SQL (chặn DDL/DML, giới hạn JOIN, LIMIT, PII) | UC1 |
| FR7 | Thực thi SQL, trả kết quả dạng bảng + metadata | UC1 |
| FR8 | Truyền tiến trình + kết quả theo thời gian thực (SSE) | UC1 |
| FR9 | Hỗ trợ câu hỏi nối tiếp (follow-up) theo lịch sử hội thoại | UC1 |
| FR10 | Trả lời câu hỏi về schema (liệt kê bảng/cột) | UC6 |
| FR11 | Lưu truy vấn, ghim báo cáo, đánh giá câu trả lời | UC3–UC5 |
| FR12 | Ghi nhật ký truy vấn (câu hỏi, SQL, trạng thái, độ trễ) | UC5, M10 |
| FR13 | Tự động hóa pipeline Bronze→Silver→Gold theo lịch | UC8 |
| FR14 | Đồng bộ dữ liệu nguồn qua CDC, CSV và HTTP | UC9 |
| FR15 | Lập danh mục (index) schema Gold vào lớp ngữ nghĩa | M3 |

| Mã | Yêu cầu phi chức năng | Tiêu chí/Ghi chú |
|---|---|---|
| NFR1 | Hiệu năng | Độ trễ mục tiêu vài giây; Postgres ~1–3s, Hive ~3–10s |
| NFR2 | Độ chính xác | Giảm ảo giác qua nhiều lớp (retrieval, schema augmentation, dialect rules, guardrails) |
| NFR3 | An toàn & bảo mật | Chỉ cho SELECT/WITH; che/chặn cột PII; chống tiêm nhiều câu lệnh |
| NFR4 | Khả năng mở rộng | Truy hồi cho schema lớn; container hóa, scale theo dịch vụ |
| NFR5 | Provider-agnostic | Đổi nhà cung cấp LLM qua cấu hình, không sửa mã nghiệp vụ |
| NFR6 | Độ tin cậy | Retry cho lỗi tạm thời; checkpoint đảm bảo exactly-once ở pipeline |
| NFR7 | Khả năng bảo trì | Kiến trúc tách lớp; mỗi agent kiểm thử độc lập |
| NFR8 | Trải nghiệm người dùng | Hiển thị tiến trình streaming; giao diện tiếng Việt |
| NFR9 | Khả năng quan sát | Nhật ký truy vấn tập trung trên OpenSearch; health check dịch vụ |

15 yêu cầu chức năng và 9 yêu cầu phi chức năng là cơ sở đánh giá hệ thống ở Chương 4.

## 2.2. Kiến trúc tổng thể hệ thống

Hệ thống được thiết kế theo hai trụ cột tách biệt nhưng liên kết qua kho dữ liệu chung: (A) **Đường ống Kỹ nghệ Dữ liệu** biến dữ liệu thô thành dữ liệu phân tích sạch; (B) **Dịch vụ AI Agent** hỏi đáp NL→SQL trên dữ liệu đó.

[Hình 2.2: Kiến trúc tổng thể hai trụ cột (Data Pipeline + AI Agent Service)]

### 2.2.1. Mô hình phân tầng Medallion (Bronze → Silver → Gold)

Đường ống dữ liệu tổ chức theo kiến trúc Medallion — ba tầng chất lượng tăng dần, mỗi tầng có trách nhiệm riêng:

| Tầng | Vai trò | Nguyên tắc thiết kế | Định dạng/Vị trí |
|---|---|---|---|
| Bronze (đồng) | Lưu dữ liệu thô đúng như khi tiếp nhận | Chỉ append, không biến đổi nghiệp vụ; giữ raw_data + metadata dòng dõi | Parquet HDFS, phân vùng theo _source_topic + ingest_date |
| Silver (bạc) | Làm sạch, ép kiểu, khử trùng lặp, chuẩn hóa | Parse JSON; ép kiểu; chuẩn hóa enum; CDC dedup giữ bản ghi mới nhất; tách bản ghi bẩn sang DLQ | Parquet, ghi đè mỗi lần chạy |
| Gold (vàng) | Mô hình hóa star schema cho phân tích | JOIN thực thể, sinh khóa thay thế, phi chuẩn hóa thuộc tính hay dùng vào fact; đăng ký Hive Metastore | Parquet, bảng EXTERNAL Hive, phân vùng theo năm/tháng |

Triết lý Medallion mang lại ba lợi ích: **truy vết và xử lý lại** (Bronze giữ dữ liệu thô nên tính lại Silver/Gold bất cứ lúc nào), **tách bạch trách nhiệm** (mỗi tầng một nhiệm vụ, dễ kiểm thử) và **kiểm soát chất lượng** (bản ghi bẩn tách sang Dead Letter Queue thay vì làm hỏng dữ liệu phân tích).

### 2.2.2. Quy trình xử lý truy vấn NL→SQL

Phía dịch vụ AI Agent, mỗi câu hỏi đi qua một pipeline tám bước, điều phối bởi async generator trong FastAPI và phát sự kiện SSE về giao diện sau mỗi bước.

[Hình 2.3: Quy trình xử lý truy vấn NL→SQL tám bước]

Điểm thiết kế quan trọng: các ý định kết thúc sớm (OUT_OF_SCOPE, SCHEMA_INFO) được xử lý ngay sau Supervisor mà không gọi SQL Writer hay backend, tiết kiệm chi phí LLM và độ trễ. Bước truy hồi và sinh SQL gộp logic trong generate_sql() nhưng vẫn phát hai sự kiện SSE riêng.

### 2.2.3. Tách biệt Data Engineering Pipeline và AI Agent Service

Một quyết định kiến trúc cốt lõi là tách rời hoàn toàn hai trụ cột, chỉ liên kết qua kho dữ liệu Gold (Hive) và CSDL nguồn (Postgres). Lợi ích: **tách rời nhịp độ** (pipeline chạy theo lô ~15 phút, AI Agent chạy thường trực), **tách rời tài nguyên và lỗi** (pipeline nặng tính toán Spark, AI Agent nặng I/O mạng — sự cố một bên không kéo sập bên kia) và **tách rời công nghệ**. Điểm nối duy nhất là **hợp đồng dữ liệu (data contract)**: schema các bảng Gold và CSDL nguồn; khi pipeline cập nhật Gold, bộ lập danh mục đồng bộ metadata sang OpenSearch để AI Agent truy hồi. Sự tách biệt này phản ánh nguyên lý giảm ghép nối (loose coupling), tăng gắn kết (high cohesion).

## 2.3. Thiết kế luồng xử lý dữ liệu (Data Pipeline)

### 2.3.1. Tiếp nhận dữ liệu từ nguồn (CDC + CSV + HTTP)

Hệ thống mô phỏng ba nguồn dữ liệu doanh nghiệp, mỗi nguồn dùng một cơ chế tiếp nhận khác nhau: **ERP** qua **CDC Debezium** đọc transaction log của PostgreSQL (topic erp.public.<table>); **kho vận** xuất **CSV → NiFi** (topic warehouse.events); **thanh toán & vận chuyển** gửi **HTTP JSON lồng → NiFi** (topic payment.events). Mọi nguồn hội tụ về Kafka — "điểm tập kết" thống nhất tách rời nguồn khỏi các tầng xử lý sau. Thiết kế ba cơ chế khác nhau thể hiện khả năng hấp thụ dữ liệu đa định dạng, đa giao thức. Đáng chú ý, CDC dựa trên log (đọc Write-Ahead Log) cho phép đồng bộ gần thời gian thực mà không gây tải cho CSDL nguồn như quét bảng định kỳ.

### 2.3.2. Lớp Bronze – lưu trữ raw events không biến đổi

Lớp Bronze áp dụng nguyên tắc bất biến: giữ nguyên dữ liệu thô, chỉ bổ sung metadata dòng dõi. Điểm thiết kế: dùng **Spark Structured Streaming với Trigger.AvailableNow** — hưởng cơ chế theo dõi offset và *exactly-once* như streaming nhưng tự kết thúc khi đọc hết dữ liệu mới (vòng đời giống batch), để Airflow lập lịch định kỳ mà không nuôi tiến trình chạy mãi; **idempotency qua checkpoint** (lưu offset đã tiêu thụ, chạy lại không đọc trùng); và tách CLEAN/DLQ ngay từ Bronze (mỗi nguồn có topic chính và topic .dlq riêng). Không áp logic nghiệp vụ ở Bronze đảm bảo dữ liệu gốc luôn được bảo toàn để tính lại tầng sau khi quy tắc thay đổi.

### 2.3.3. Lớp Silver – làm sạch, dedup và harmonization

Lớp Silver biến dữ liệu thô đa định dạng thành bảng sạch có kiểu, xử lý theo từng nguồn rồi hội tụ, qua các bước: **parse JSON theo schema tường minh**, **ép kiểu** (tiền tệ → Decimal, timestamp Debezium int64 mili-giây → timestamp), **harmonization** (chuẩn hóa enum trạng thái, email, tên, SKU; placeholder rác → NULL), **CDC dedup** (window phân theo id sắp theo __source_ts_ms giảm dần, giữ bản ghi mới nhất) và **kiểm tra hợp lệ + tách DLQ** (bản ghi thiếu trường bắt buộc hoặc gắn cờ _quality_flag tách sang Dead Letter Queue). Một quyết định quan trọng là chọn **nguồn canonical cho products** là ERP/CDC (có id nguyên ổn định). Đầu ra Silver gồm 11 bảng sạch + 1 bảng dlq.

### 2.3.4. Lớp Gold – star schema cho analytical queries

Lớp Gold mô hình hóa dữ liệu Silver thành **star schema** — các bảng fact (sự kiện đo lường) ở trung tâm, bao quanh là các bảng dimension (chiều mô tả).

[Hình 2.4: Cấu trúc star schema của tầng Gold]

| Bảng fact | Hạt (grain) | Khóa ngoại chính | Measure tiêu biểu |
|---|---|---|---|
| fact_sales | 1 dòng / 1 dòng-hàng trong đơn | customer_key, product_key, payment_key, shipping_key, coupon_key | quantity, unit_price, item_total, order_total, discount_amount, tax_amount |
| fact_reviews | 1 dòng / 1 đánh giá sản phẩm | customer_key, product_key, order_key | rating |
| fact_feedback | 1 dòng / 1 phản hồi khách hàng | customer_key, order_key | resolution_days |

Bảy bảng dimension gồm: dim_customers, dim_products (đã làm giàu category_name), dim_categories, dim_addresses, dim_coupons, dim_payments, dim_shipping (tính sẵn delivery_days). Hai kỹ thuật thiết kế Gold quan trọng: (1) **phi chuẩn hóa vào fact** — fact_sales nhúng sẵn các thuộc tính hay dùng (customer_name, product_name, brand, payment_method…) để giảm số JOIN mà AI cần sinh, vừa tăng tốc vừa giảm xác suất LLM sinh JOIN sai; (2) **phân vùng theo thời gian** — fact_sales phân vùng theo order_year/order_month giúp truy vấn chỉ quét phân vùng cần thiết. Các bảng Gold là EXTERNAL Hive table trỏ tới Parquet trên HDFS, đăng ký vào Metastore bằng saveAsTable, để AI Agent truy vấn qua HiveServer2 bằng SQL chuẩn.

## 2.4. Thiết kế kiến trúc AI Agent Multi-Agent

Dịch vụ AI Agent thiết kế theo kiến trúc đa tác tử, gồm năm tác tử chuyên biệt nối tiếp thành pipeline, đặt sau một FastAPI Gateway điều phối bởi async generator và phát sự kiện SSE.

### 2.4.1. Supervisor Agent – phân loại intent và chọn backend

Supervisor là "bộ não định tuyến" — thực hiện *một* lời gọi LLM nhỏ, nhanh (mô hình hạng nhẹ như Haiku/GPT-5-mini) để đồng thời phân loại ý định và chọn backend. **Đầu vào**: câu hỏi tiếng Việt + (tùy chọn) lịch sử hội thoại. **Đầu ra**: SupervisorDecision { intent, backend, confidence, reasoning }, với intent ∈ {DATA_QUERY, SCHEMA_INFO, FOLLOWUP, OUT_OF_SCOPE} và backend ∈ {hive_gold, postgres_bronze}. Quy tắc định tuyến đặc biệt (mã hóa trong prompt): câu hỏi chứa "tồn kho"/"stock"/"còn hàng" → bắt buộc postgres_bronze (Gold không có stock_quantity thời gian thực); còn lại → hive_gold (mặc định, ~80% câu phân tích). Chống lỗi: nếu LLM timeout hoặc trả JSON sai, Supervisor fallback về (DATA_QUERY, hive_gold); các ý định kết thúc sớm được xử lý ngay, không gọi tiếp pipeline.

### 2.4.2. Metadata Retriever – hybrid kNN + BM25

Metadata Retriever giải bài toán "schema lớn" — chỉ truy hồi phần ngữ cảnh liên quan nhất thay vì nhồi toàn bộ schema. Với mỗi câu hỏi, hệ thống chạy song song hai cơ chế trên OpenSearch rồi hợp nhất: **BM25 (từ khóa)** bắt trùng khớp từ vựng (fuzziness AUTO dung lỗi chính tả), và **kNN (ngữ nghĩa)** — câu hỏi được embedding thành vector 768 chiều, tìm các vector schema gần nhất, bắt được liên hệ ngữ nghĩa kể cả khác từ ("thu nhập" ↔ "revenue"). Ba nguồn ngữ cảnh được truy hồi (kèm Top-K): **finch_catalog** (metadata bảng/cột + sample values + cờ PII, K=8), **table_docs** (tài liệu nghiệp vụ Markdown, K=3) và **query_log** (truy vấn quá khứ kèm phản hồi, K=3). Đầu ra là RetrievalContext kết xuất ngữ cảnh thành khối Markdown chèn vào prompt SQL Writer; Retriever luôn lọc theo backend (gold/public) để không lẫn bảng giữa hai hệ.

### 2.4.3. SQL Writer Agent – sinh câu lệnh SQL

SQL Writer là khâu sáng tạo lõi — dùng LLM mạnh hơn (Sonnet/GPT-5/Gemini) nhận ngữ cảnh đã truy hồi và sinh SQL kèm giải thích. Luồng trong generate_sql(): xác định phương ngữ và luật theo backend; truy hồi ngữ cảnh; **schema augmentation** (then chốt chống ảo giác — bơm danh sách đầy đủ cột của các bảng trong phạm vi; nếu truy hồi rỗng thì bơm toàn bộ schema từ cache); dựng prompt từ template (với FOLLOWUP ghép thêm 6 lượt hội thoại gần nhất); gọi LLM qua gateway; rồi chuyển sang Guardrails. Năm lớp chống ảo giác được thiết kế chồng lên nhau: (1) lọc truy hồi theo backend — không lẫn bảng giữa hai hệ; (2) schema augmentation liệt kê đầy đủ cột các bảng trong phạm vi; (3) định dạng mỗi cột một dòng để LLM không "trộn" cột giữa các bảng; (4) dialect rules liệt kê tường minh cột KHÔNG tồn tại; (5) Guardrails hậu kiểm từ chối SQL tham chiếu bảng/cột không hợp lệ.

### 2.4.4. Guardrails Layer – kiểm tra an toàn

Guardrails là "van an toàn" — phân tích tĩnh câu SQL trước khi thực thi, đảm bảo không câu lệnh nguy hiểm nào được chạy.

| # | Kiểm tra | Hành động khi vi phạm |
|---|---|---|
| 1 | Từ khóa cấm: DELETE, UPDATE, INSERT, DROP, TRUNCATE, ALTER, CREATE | Từ chối |
| 2 | Chỉ cho phép câu bắt đầu bằng SELECT/WITH | Từ chối |
| 3 | Không cho dấu ; lồng (chống tiêm nhiều câu lệnh) | Từ chối |
| 4 | Giới hạn số JOIN (mặc định ≤ 6) | Từ chối nếu vượt |
| 5 | Bắt buộc LIMIT cho truy vấn không tổng hợp | Tự thêm LIMIT + cảnh báo |
| 6 | Kiểm soát cột PII (nếu allow_pii = false) | Từ chối/che + cảnh báo |

Đây là lớp phòng vệ độc lập, không phụ thuộc việc LLM "ngoan ngoãn" — kể cả khi LLM sinh câu lệnh nguy hiểm, Guardrails vẫn chặn. Việc tách thành module riêng cho phép mọi agent dùng chung và dễ kiểm toán an ninh.

### 2.4.5. Executor Agent – thực thi và stream kết quả

Executor định tuyến câu SQL đã duyệt tới đúng client, thực thi và trả kết quả. Hàm dispatch(backend, sql) route tới hive_agent.execute() hoặc postgres_agent.execute():

| Backend | Client | Cơ chế | Đặc điểm |
|---|---|---|---|
| hive_gold | hive_client.py (pyhive + thrift) | Sync execute bọc asyncio.to_thread; retry 3 lần cho lỗi tạm thời với backoff; timeout cấu hình được | Latency ~3–10s (MapReduce) |
| postgres_bronze | postgres_client.py (asyncpg) | Async native với connection pool (1–10 kết nối) | Latency ~10–100ms |

[Hình 2.5: Ví dụ luồng sự kiện Server-Sent Events (SSE)]

Toàn bộ pipeline phát sự kiện Server-Sent Events sau mỗi bước để giao diện "thắp sáng" thanh tiến trình agent theo thời gian thực. Sau khi thực thi, một bộ ghi nhật ký lưu câu hỏi, SQL, trạng thái và độ trễ vào index query_log phục vụ quan sát và vòng lặp tự cải tiến.

## 2.5. Thiết kế cơ sở dữ liệu

Hệ thống có ba kho dữ liệu: **PostgreSQL** (nguồn tác nghiệp & backend thời gian thực), **Hive Gold** (kho phân tích star schema) và **OpenSearch** (lớp ngữ nghĩa phục vụ truy hồi); ngoài ra còn schema cache trong RAM.

### 2.5.1. Cơ sở dữ liệu PostgreSQL nguồn (ERP)

PostgreSQL đóng hai vai trò: nguồn dữ liệu tác nghiệp (được Debezium CDC theo dõi) và backend postgres_bronze cho câu hỏi thời gian thực. Schema ERP gồm **11 bảng quan hệ chuẩn hóa**: customers (email, phone là PII), addresses, categories (tự tham chiếu cây), products (có stock_quantity — chỉ tồn tại ở đây, không có trên Gold), orders, order_items, coupons, payments (transaction_id là PII), shipping (tracking_number là PII), reviews, feedback. Đây là schema chuẩn hóa (3NF) điển hình của OLTP — tối ưu cho ghi/đọc bản ghi đơn nhưng cần nhiều JOIN khi phân tích, lý do phải có lớp Gold phi chuẩn hóa.

### 2.5.2. Hive Gold star schema (fact + dim tables)

Lớp Gold tổ chức dữ liệu thành star schema gồm 3 fact + 7 dimension. Bảng trung tâm **gold.fact_sales** (hạt = 1 order_item) minh họa thiết kế phi chuẩn hóa: ngoài các khóa thay thế và measure (quantity, item_total, order_total…), nó nhúng sẵn cột thời gian phân vùng (order_year, order_month) và hàng loạt thuộc tính chiều phi chuẩn hóa (customer_name, sku, product_name, brand, payment_method, carrier, delivery_days, shipping_city…) — nhờ vậy ~80% câu hỏi giải được mà không cần JOIN. So sánh hai thiết kế:

| Tiêu chí | PostgreSQL (nguồn) | Hive Gold (phân tích) |
|---|---|---|
| Mô hình | Chuẩn hóa (3NF) | Star schema, phi chuẩn hóa |
| Tối ưu cho | Ghi/đọc bản ghi đơn (OLTP) | Tổng hợp trên khối lớn (OLAP) |
| Số JOIN khi truy vấn | Nhiều | Ít (đã nhúng sẵn thuộc tính) |
| Phân vùng | Không | Theo năm/tháng |
| Vai trò trong NL→SQL | backend thời gian thực | backend phân tích mặc định |

### 2.5.3. OpenSearch indices (finch_catalog, table_docs, query_log)

OpenSearch là lớp ngữ nghĩa với ba index: **finch_catalog** — danh mục bảng/cột (mỗi bảng có document mức bảng + mức cột, kèm mô tả, từ đồng nghĩa, sample values, cờ PII và vector embedding 768 chiều); **table_docs** — tài liệu nghiệp vụ Markdown bổ sung "ý nghĩa kinh doanh"; **query_log** — nhật ký truy vấn, vừa phục vụ quan sát vừa là nguồn few-shot tự cải tiến. Mọi document finch_catalog được nhúng bằng cùng mô hình đa ngôn ngữ (văn bản ghép từ table_name, column_name, description, synonyms, sample_values) — chìa khóa cho phép câu hỏi tiếng Việt khớp ngữ nghĩa với mô tả schema tiếng Anh.

### 2.5.4. Hệ thống schema cache trong RAM và schema fallback

Để giảm độ trễ và đảm bảo độ chính xác ngay cả khi truy hồi thất bại, hệ thống thiết kế **schema cache trong RAM**: nạp một lần lúc khởi động (trong lifespan của FastAPI, nạp song song toàn bộ schema Hive và Postgres vào bộ nhớ); chia sẻ giữa mọi request làm schema_fallback cho SQL Writer; và đóng vai trò "lưới an toàn" — khi Metadata Retriever trả rỗng (cold start, hoặc Postgres chưa index), SQL Writer dùng cache bơm toàn bộ bảng/cột vào prompt. Thiết kế hai lớp (truy hồi chọn lọc + cache toàn bộ) cân bằng giữa độ chính xác cho schema lớn và độ bền vững.

## 2.6. Thiết kế giao diện và API Gateway

### 2.6.1. Giao diện chat hội thoại người dùng

Giao diện chính là trang chat NL→SQL (/app/ask) trên Next.js + React + Tailwind, thiết kế tập trung vào tính minh bạch của pipeline để tạo lòng tin: ô nhập câu hỏi tiếng Việt + gợi ý câu hỏi mẫu; **thanh tiến trình agent** hiển thị trạng thái từng bước theo thời gian thực nhờ SSE ("Supervisor ✓ → Truy hồi ✓ → Sinh SQL ✓ → Thực thi ✓"); khu vực kết quả gồm bảng số liệu + câu SQL sinh ra + giải thích tiếng Việt + biểu đồ (Recharts); hành động trên mỗi câu trả lời (Lưu, Đánh giá 👍/👎, Chia sẻ); và lịch sử hội thoại hỗ trợ câu hỏi nối tiếp.

### 2.6.2. Giao diện quản lý saved queries, reports và data sources

Ngoài chat còn có các trang quản lý: **/app/saved** (danh sách truy vấn đã lưu — mở lại, sửa, xóa; lưu localStorage giai đoạn đầu); **/app/reports** (dashboard KPI + biểu đồ Recharts theo tab Sales/Orders/Customers); **/app/data** (quản lý nguồn dữ liệu, duyệt danh mục schema, biên tập mô tả bảng — phục vụ Data Admin); **/app/settings** (cấu hình hồ sơ, AI Model — chọn nhà cung cấp LLM + model + API key). Trang Settings phản ánh trực tiếp thiết kế provider-agnostic ở backend.

### 2.6.3. REST API cho external integration

FastAPI Gateway cung cấp các nhóm endpoint, đăng ký theo router:

| Endpoint | Phương thức | Vai trò |
|---|---|---|
| /api/query/ask | POST | Endpoint lõi — nhận câu hỏi, trả StreamingResponse dạng text/event-stream (SSE) |
| /api/query/feedback | POST | Ghi đánh giá 👍/👎 vào query_log |
| /api/schema/full | GET | Trả toàn bộ schema từ cache (Schema browser) |
| /api/health | GET | Tổng hợp tình trạng Hive + Postgres + OpenSearch + LLM gateway |
| /api/health/ping | GET | Health check nhẹ (cho Docker healthcheck) |

Request /ask gồm question (bắt buộc), conversation_history, session_id, user_id, allow_pii; response là luồng SSE với hai loại sự kiện — type=step (tiến trình từng agent) và type=result (kết quả cuối: sql, rows, columns, exec_ms, total_ms, tables_used, explanation). Thiết kế SSE thay vì WebSocket phù hợp với luồng một chiều server→client, nhẹ, chạy trên HTTP thuần; header X-Accel-Buffering: no vô hiệu hóa buffering của reverse proxy để sự kiện tới giao diện tức thời.

## 2.7. Tổng kết chương 2

Chương 2 đã hoàn thành phân tích và thiết kế hệ thống DataFinch ở mức chi tiết. Về **phân tích bài toán** (2.1): phát biểu bài toán thành hai bài toán con, xác định 10 mục tiêu, phân tích 5 nhóm tác nhân với các ca sử dụng, đặc tả 15 yêu cầu chức năng và 9 yêu cầu phi chức năng. Về **kiến trúc tổng thể** (2.2): thiết kế hai trụ cột tách biệt liên kết qua hợp đồng dữ liệu, cùng quy trình NL→SQL tám bước. Về **thiết kế chi tiết**: luồng dữ liệu Bronze→Silver→Gold với ba cơ chế tiếp nhận và star schema 3 fact + 7 dimension (2.3); kiến trúc năm agent với cơ chế chống lỗi rõ ràng (2.4); ba kho dữ liệu cùng schema cache (2.5); giao diện chat minh bạch pipeline và API Gateway dựa trên SSE (2.6). Các quyết định thiết kế xuyên suốt — tách biệt pipeline/agent, phi chuẩn hóa Gold, truy hồi lai, năm lớp chống ảo giác, Guardrails độc lập, provider-agnostic — đều hướng tới giải quyết các thách thức nêu ở Chương 1. Trên nền thiết kế này, Chương 3 trình bày quá trình xây dựng và triển khai cụ thể.
