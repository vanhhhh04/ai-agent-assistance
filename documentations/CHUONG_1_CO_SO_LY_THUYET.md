# CHƯƠNG 1: CƠ SỞ LÝ THUYẾT

Chương này trình bày các cơ sở lý thuyết nền tảng cho việc phân tích, thiết kế và xây dựng hệ thống ở các chương sau, theo bốn trục: (1) phân tích dữ liệu doanh nghiệp — bối cảnh và nhu cầu; (2) trí tuệ nhân tạo và mô hình ngôn ngữ lớn — nền tảng hiểu và sinh ngôn ngữ; (3) kho dữ liệu và truy xuất dữ liệu — nơi tổ chức dữ liệu để hỏi – đáp; và (4) tác tử thông minh (AI Agent) cùng kỹ thuật truy hồi tăng cường sinh (RAG) — cốt lõi phương pháp luận của đề tài.

---

## 1.1. Tổng quan về phân tích dữ liệu doanh nghiệp

### 1.1.1. Khái niệm dữ liệu doanh nghiệp

Dữ liệu doanh nghiệp (enterprise data) là toàn bộ dữ liệu được sinh ra và lưu trữ trong quá trình vận hành của một tổ chức, phản ánh các hoạt động nghiệp vụ như bán hàng, kho vận, tài chính, chăm sóc khách hàng. Trong môi trường số hóa, dữ liệu được xem là một loại **tài sản**: nó ghi lại dấu vết mọi giao dịch, cho phép tổ chức nhìn lại quá khứ, đánh giá hiện tại và dự báo tương lai.

Theo cấu trúc, dữ liệu được phân thành ba nhóm: **có cấu trúc** (theo bảng hàng – cột như trong CSDL quan hệ), **bán cấu trúc** (JSON, XML, nhật ký sự kiện) và **phi cấu trúc** (văn bản, hình ảnh, âm thanh). Đề tài tập trung vào dữ liệu có cấu trúc và bán cấu trúc — nguồn chính cho phân tích định lượng. Một đặc trưng quan trọng là tính **phân mảnh**: dữ liệu nằm rải rác ở nhiều hệ thống nguồn với định dạng không đồng nhất, nên hợp nhất thành "nguồn sự thật duy nhất" (single source of truth) là tiền đề bắt buộc để phân tích nhất quán.

### 1.1.2. Vai trò của phân tích dữ liệu trong doanh nghiệp

Phân tích dữ liệu (data analytics) là quá trình khảo sát, làm sạch, biến đổi và mô hình hóa dữ liệu nhằm khám phá thông tin hữu ích và hỗ trợ ra quyết định; nó chuyển hóa dữ liệu thô — vốn không có giá trị tự thân — thành tri thức có thể hành động. Vai trò này thể hiện ở ba cấp: **chiến lược** (nhận diện xu hướng, hoạch định nguồn lực), **chiến thuật** (quản lý tồn kho, dự báo nhu cầu, phân khúc khách hàng) và **tác nghiệp** (phản ứng nhanh với sự kiện như đơn hàng bất thường). Nhờ đó dữ liệu trở thành "kim chỉ nam" cho văn hóa ra quyết định dựa trên bằng chứng (data-driven) thay vì cảm tính. Tuy nhiên, giá trị chỉ hiện thực hóa khi người dùng nghiệp vụ — hiểu bài toán kinh doanh nhưng thường không có chuyên môn kỹ thuật — tiếp cận được dữ liệu dễ dàng. Đây là khoảng trống mà đề tài hướng tới.

### 1.1.3. Business Intelligence và Dashboard

Trí tuệ doanh nghiệp (Business Intelligence — BI) là tập hợp quy trình, công nghệ và công cụ thu thập, tích hợp, phân tích và trình bày dữ liệu để hỗ trợ ra quyết định — cầu nối biến các con số rời rạc thành thông tin trực quan. Thành phần đặc trưng nhất của BI là **bảng điều khiển (dashboard)** tập hợp các chỉ số hiệu năng then chốt (KPI) và biểu đồ trên một màn hình, giúp nhà quản lý nắm nhanh "sức khỏe" doanh nghiệp. Tuy vậy, công cụ BI truyền thống có hạn chế căn bản: báo cáo phải được **dựng sẵn** bởi đội kỹ thuật; khi có câu hỏi mới ngoài báo cáo có sẵn, người dùng phải chờ đợi. Hạn chế này thúc đẩy xu hướng **phân tích tự phục vụ** và xa hơn là **hỏi dữ liệu bằng ngôn ngữ tự nhiên** — động lực trực tiếp của đề tài.

---

## 1.2. Tổng quan về trí tuệ nhân tạo và mô hình ngôn ngữ lớn

### 1.2.1. Khái niệm AI và Generative AI

**Trí tuệ nhân tạo (AI)** là lĩnh vực nghiên cứu tạo ra các hệ thống thực hiện được những nhiệm vụ vốn đòi hỏi trí thông minh của con người (nhận thức, suy luận, học hỏi, ra quyết định). Trong AI, **học máy (Machine Learning — ML)** là nhánh cho phép hệ thống "học" quy luật trực tiếp từ dữ liệu thay vì lập trình tường minh; **học sâu (Deep Learning — DL)** là nhánh con của ML dùng mạng nơ-ron nhiều lớp để tự học biểu diễn dữ liệu ở nhiều mức trừu tượng, đặc biệt hiệu quả với hình ảnh và ngôn ngữ.

[Hình 1.1: Quan hệ bao hàm giữa AI, Machine Learning, Deep Learning và Generative AI]

**Trí tuệ nhân tạo tạo sinh (Generative AI)** là nhóm mô hình có khả năng *tạo ra* nội dung mới — văn bản, hình ảnh, mã nguồn — thay vì chỉ phân loại hay dự đoán. Khác mô hình phân biệt (chỉ học ranh giới giữa các lớp), mô hình tạo sinh học phân phối xác suất của dữ liệu rồi lấy mẫu sinh ra mẫu mới. Quan hệ mang tính bao hàm: AI ⊃ ML ⊃ DL, và Generative AI là ứng dụng nổi bật của học sâu trong miền sinh nội dung.

### 1.2.2. Large Language Model (LLM)

**Mô hình ngôn ngữ lớn (LLM)** là đại diện tiêu biểu nhất của Generative AI trong miền ngôn ngữ. Về bản chất, nó học cách dự đoán từ (token) tiếp theo dựa trên ngữ cảnh phía trước; khi huấn luyện trên khối văn bản khổng lồ với hàng tỉ tham số, mô hình thu nạp tri thức ngôn ngữ, kiến thức thế giới và khả năng suy luận, cho phép trả lời câu hỏi, tóm tắt, dịch thuật và **sinh mã nguồn** — bao gồm sinh SQL từ mô tả ngôn ngữ tự nhiên.

Bên cạnh năng lực đó, LLM có giới hạn cố hữu. Quan trọng nhất là **ảo giác (hallucination)**: mô hình có thể sinh thông tin nghe hợp lý nhưng sai sự thật, do vận hành theo xác suất ngôn ngữ chứ không "biết" sự thật. Ngoài ra tri thức bị giới hạn tại thời điểm huấn luyện và không truy cập được dữ liệu riêng của doanh nghiệp. Hai giới hạn này khiến đề tài không dùng LLM trực tiếp mà kết hợp truy hồi tăng cường sinh và cơ chế kiểm soát (mục 1.4).

### 1.2.3. Cơ chế hoạt động cơ bản của Transformer

Phần lớn LLM hiện đại dựa trên kiến trúc **Transformer** (2017). Đột phá cốt lõi là **cơ chế tự chú ý (self-attention)**: thay vì xử lý câu tuần tự như mạng hồi quy, Transformer cho mỗi từ "chú ý" đồng thời đến mọi từ khác và đánh trọng số mức độ liên quan. Cơ chế này giúp mô hình nắm bắt phụ thuộc ngữ cảnh ở khoảng cách xa và xử lý song song toàn chuỗi, nhờ đó huấn luyện hiệu quả và mở rộng tới hàng tỉ tham số. Kiến trúc gồm bộ mã hóa (encoder) biểu diễn đầu vào và bộ giải mã (decoder) sinh đầu ra. Khả năng hiểu ngữ cảnh sâu của Transformer là nền tảng giúp LLM ánh xạ chính xác một câu hỏi tiếng Việt phức tạp sang cấu trúc truy vấn tương ứng.

### 1.2.4. Một số mô hình LLM phổ biến

Thị trường LLM phát triển sôi động với nhiều dòng sản phẩm, mỗi dòng có thế mạnh riêng về chất lượng, tốc độ, chi phí và đa ngôn ngữ:

| Họ mô hình | Tổ chức phát triển | Đặc điểm nổi bật |
|---|---|---|
| GPT | OpenAI | Khả năng suy luận và sinh mã mạnh; hệ sinh thái công cụ phong phú |
| Gemini | Google | Đa phương thức (văn bản, hình ảnh); tối ưu chi phí – tốc độ |
| Claude | Anthropic | Tuân thủ chỉ dẫn tốt, chú trọng an toàn; xử lý ngữ cảnh dài |
| Llama | Meta | Mã nguồn mở, có thể tự triển khai và tùy biến |

Các họ mô hình này đều dựa trên Transformer và cung cấp giao diện lập trình tương tự nhau (nhận lời nhắc, trả về văn bản sinh). Sự tương đồng đó tạo điều kiện thiết kế hệ thống theo hướng **độc lập nhà cung cấp (provider-agnostic)** — thay thế linh hoạt giữa các mô hình tùy yêu cầu chất lượng và chi phí, một nguyên tắc quan trọng của đề tài.

---

## 1.3. Kho dữ liệu và truy xuất dữ liệu

### 1.3.1. Khái niệm Data Warehouse

**Kho dữ liệu (Data Warehouse)** là hệ thống lưu trữ tập trung, thiết kế chuyên cho phân tích và báo cáo, tách biệt khỏi hệ thống tác nghiệp. Trong khi CSDL tác nghiệp tối ưu cho ghi/đọc nhanh từng giao dịch (OLTP), kho dữ liệu tối ưu cho đọc và tổng hợp khối lớn dữ liệu lịch sử (OLAP). Theo Bill Inmon, một Data Warehouse có bốn đặc điểm: **hướng chủ đề** (tổ chức theo chủ đề nghiệp vụ), **tích hợp** (hợp nhất nhiều nguồn về một chuẩn), **bất biến theo thời gian** (lưu dữ liệu lịch sử) và **không biến động** (chỉ đọc và bổ sung). Bốn đặc điểm này lý giải vì sao kho dữ liệu là nền tảng phù hợp cho hệ thống hỏi – đáp: dữ liệu đã sạch, hợp nhất và tổ chức theo chủ đề giúp ánh xạ từ câu hỏi nghiệp vụ sang truy vấn trực tiếp và đáng tin cậy hơn.

### 1.3.2. Fact Table và Dimension Table

Để tối ưu cho truy vấn phân tích, dữ liệu trong kho được tổ chức theo mô hình chiều (dimensional modeling) thay vì chuẩn hóa cao. Hai thành phần cơ bản là **bảng sự kiện (Fact Table)** — lưu các số đo định lượng (số lượng bán, doanh thu) cùng khóa liên kết tới các chiều, và **bảng chiều (Dimension Table)** — lưu thuộc tính mô tả ngữ cảnh (khách hàng, sản phẩm, thời gian, địa điểm). Cách tổ chức này cho phép "cắt lát" và tổng hợp dữ liệu theo nhiều góc nhìn — chẳng hạn tính tổng doanh thu theo từng tháng và từng khu vực.

### 1.3.3. Star Schema

**Lược đồ sao (Star Schema)** là cách tổ chức fact và dimension phổ biến nhất: bảng sự kiện ở trung tâm, các bảng chiều phẳng (phi chuẩn hóa) bao quanh như cánh sao. Biến thể **lược đồ bông tuyết (Snowflake)** chuẩn hóa tiếp các chiều thành nhiều bảng phân cấp.

| Tiêu chí | Star Schema (lược đồ sao) | Snowflake Schema (lược đồ bông tuyết) |
|---|---|---|
| Cấu trúc | Fact ở trung tâm, các dimension phẳng bao quanh | Các dimension được chuẩn hóa thành nhiều bảng phân cấp |
| Mức chuẩn hóa | Thấp (phi chuẩn hóa) | Cao hơn |
| Số JOIN khi truy vấn | Ít, truy vấn nhanh | Nhiều hơn, phức tạp hơn |
| Dư thừa dữ liệu | Có | Ít hơn |
| Phù hợp | Truy vấn nhanh, dễ hiểu | Tiết kiệm lưu trữ, dữ liệu phức tạp |

Đề tài chọn **Star Schema** cho tầng phân tích, vì giảm số JOIN không chỉ tăng tốc truy vấn mà còn giảm độ phức tạp của câu SQL mà LLM phải sinh, từ đó giảm sai sót và ảo giác.

### 1.3.4. Truy vấn dữ liệu phục vụ phân tích

SQL là công cụ chuẩn để truy xuất dữ liệu phân tích. Khác truy vấn tác nghiệp đơn giản, **truy vấn phân tích (SQL Analytics)** thường tổng hợp và tính toán trên khối lớn dữ liệu, với các thành phần đặc trưng: **hàm tổng hợp** (SUM, COUNT, AVG…) gộp nhiều dòng thành một giá trị; mệnh đề **GROUP BY** tính theo từng nhóm; và **hàm cửa sổ (Window Functions)** tính trên một "cửa sổ" các dòng liên quan mà vẫn giữ chi tiết từng dòng (xếp hạng, lũy kế). Kết quả thường được trình bày dưới dạng **chỉ số (Metrics)** và **KPI** — những con số cô đọng phản ánh tình hình kinh doanh. Bài toán cốt lõi của đề tài là tự động chuyển câu hỏi nghiệp vụ thành đúng truy vấn phân tích này.

---

## 1.4. AI Agent và Retrieval-Augmented Generation (RAG)

### 1.4.1. Khái niệm AI Agent

**Tác tử thông minh (AI Agent)** là thực thể phần mềm dùng LLM làm "bộ não" để nhận thức môi trường, lập kế hoạch và thực hiện hành động nhằm đạt mục tiêu, thay vì chỉ sinh văn bản thụ động. Khác biệt căn bản giữa một tác tử và một lời gọi LLM thông thường là tác tử có khả năng **hành động** — gọi công cụ, truy vấn dữ liệu, ra quyết định nhiều bước. Một kiến trúc tác tử điển hình gồm: bộ điều khiển dựa trên LLM, bộ nhớ (ngữ cảnh hội thoại) và tập công cụ (tools); quy trình lặp theo chu trình tiếp nhận mục tiêu → suy luận → gọi công cụ → quan sát kết quả → tiếp tục. Khi bài toán phức tạp, hệ thống có thể phân rã thành nhiều tác tử chuyên biệt phối hợp dưới một tác tử giám sát — định hướng kiến trúc được áp dụng trong đề tài.

### 1.4.2. Tool Calling và Function Calling

Hai cơ chế then chốt giúp tác tử tương tác với bên ngoài là **gọi công cụ (Tool Calling)** và **gọi hàm (Function Calling)**: thay vì "bịa" kết quả, mô hình phát ra một lời gọi có cấu trúc (JSON gồm tên hàm và tham số) tới một công cụ bên ngoài. Quy trình: mô hình nhận mô tả các công cụ khả dụng; khi cần, sinh lời gọi tương ứng; hệ thống thực thi công cụ thật (chạy SQL, gọi API) và trả kết quả về; mô hình dùng kết quả thật đó hình thành câu trả lời. Nhờ vậy, năng lực LLM mở rộng từ "chỉ sinh văn bản" sang "hành động trên hệ thống thực", giúp câu trả lời bám sát dữ liệu thực tế.

### 1.4.3. Retrieval-Augmented Generation (RAG)

**Sinh tăng cường bằng truy hồi (RAG)** kết hợp khả năng sinh của LLM với một kho tri thức bên ngoài được truy hồi tại thời điểm chạy, nhằm khắc phục hai giới hạn của LLM (ảo giác và tri thức đóng băng).

[Hình 1.2: Quy trình hoạt động của Retrieval-Augmented Generation (RAG)]

Quy trình RAG gồm bốn bước: **(1) Câu hỏi người dùng** — tiếp nhận câu hỏi; **(2) Truy hồi (Retrieval)** — tìm trong kho tri thức (CSDL, tài liệu, lược đồ) các đoạn liên quan nhất; **(3) Chèn ngữ cảnh (Context Injection)** — ghép các đoạn truy hồi vào lời nhắc cùng câu hỏi gốc làm "bằng chứng"; **(4) Sinh bằng LLM** — mô hình sinh câu trả lời dựa trên ngữ cảnh được cung cấp. Lợi ích của RAG là câu trả lời **bám sát dữ liệu thực (grounded)**, giảm mạnh ảo giác và làm việc được với dữ liệu riêng, cập nhật mà không cần huấn luyện lại. Trong đề tài, RAG truy hồi đúng phần lược đồ và tri thức nghiệp vụ liên quan, giúp tác tử sinh SQL chỉ tham chiếu các bảng/cột thực sự tồn tại.

### 1.4.4. Hybrid Retrieval (BM25 và Vector Search)

Có hai họ phương pháp truy hồi, mỗi họ có điểm mạnh/yếu riêng. **Truy hồi từ khóa (BM25)** — hàm xếp hạng theo tần suất từ — hiệu quả khi câu hỏi chứa đúng thuật ngữ, tên riêng, mã định danh, nhưng kém với từ đồng nghĩa. **Tìm kiếm vector (Vector Search)** dựa trên **nhúng vector (Embedding)** biểu diễn câu hỏi và tài liệu thành vector trong không gian nhiều chiều (đối tượng tương tự nằm gần nhau, đo bằng **độ tương đồng cô-sin**); bắt tốt tương đồng ngữ nghĩa nhưng có thể bỏ sót khi cần khớp chính xác mã/tên riêng hiếm. **Truy hồi lai (Hybrid)** kết hợp cả hai, thường bằng cách hợp nhất điểm số BM25 và vector, có thể kèm bước **xếp hạng lại (Re-ranking)**.

| Tiêu chí | BM25 (từ khóa) | Vector Search (kNN) | Hybrid (lai) |
|---|---|---|---|
| Cơ sở | Tần suất từ khớp chính xác | Tương đồng ngữ nghĩa (embedding) | Kết hợp cả hai |
| Mạnh khi | Tên riêng, mã, thuật ngữ đúng | Đồng nghĩa, diễn đạt tự do | Cả hai trường hợp |
| Yếu khi | Diễn đạt khác từ | Mã/tên riêng hiếm | — |
| Kết quả | Tốt theo từ khóa | Tốt theo ý nghĩa | Toàn diện nhất |

Nhờ truy hồi lai, hệ thống vừa hiểu "khách mua nhiều nhất" liên quan bảng khách hàng và phép đo doanh thu (ngữ nghĩa), vừa khớp chính xác tên bảng/cột và mã định danh (từ khóa). Khả năng khớp ngữ nghĩa xuyên ngôn ngữ (câu hỏi tiếng Việt — lược đồ tiếng Anh) đặc biệt quan trọng với người dùng Việt Nam và được giải quyết bằng mô hình embedding đa ngôn ngữ.
