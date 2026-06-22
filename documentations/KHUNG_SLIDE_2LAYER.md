# KHUNG SLIDE BẢO VỆ — BỐ CỤC 2 LỚP (slide tổng quát, chi tiết để nói)

**Đề tài:** Xây dựng hệ thống AI hỗ trợ truy vấn dữ liệu doanh nghiệp bằng ngôn ngữ tự nhiên (DataFinch)

> **Nguyên tắc:** Trên slide chỉ để **nội dung tổng quát, ít chữ, không thuật ngữ kỹ thuật**. Phần chi tiết / kỹ thuật / code → để ở mục **🎤 Khi nói** (bạn giải thích miệng, không in lên slide).
>
> Mạch kể: Bài toán → Tác nhân → **Tổng quan luồng 2 lớp** → **Lớp 1 (dữ liệu)** → **Lớp 2 (trợ lý AI)** → Thực nghiệm → Demo → Kết.

---

## AGENDA (5 phần — khớp slide mục lục template)
1. Tổng quan đề tài
2. Kiến trúc hệ thống 2 lớp
3. Lớp chuẩn bị dữ liệu
4. Lớp trợ lý AI
5. Thực nghiệm & Demo

---

# PHẦN 1 — TỔNG QUAN

### Slide 1 — Bìa
Tên đề tài • SV • GVHD • Lớp/Khóa.

### Slide 2 — Nội dung (Agenda 5 phần)

### Slide 3 — Bài toán
**Trên slide (4 ý ngắn):**
- Dữ liệu doanh nghiệp nhiều nhưng khó khai thác.
- Muốn dùng phải biết kỹ thuật.
- AI giúp hỏi bằng lời nói thường — nhưng dễ trả lời sai.
- **DataFinch: hỏi dữ liệu bằng tiếng Việt, chính xác và an toàn.**

🎤 *Khi nói:* giải thích rào cản viết truy vấn/hiểu cấu trúc dữ liệu, hạn chế của báo cáo truyền thống, và vì sao hỏi thẳng AI một lần lại dễ bịa dữ liệu.

### Slide 4 — Mục tiêu & Phạm vi
**Trên slide:**
- Mục tiêu: hỏi tiếng Việt → trả lời đúng + dễ hiểu, an toàn.
- Phạm vi: 2 lớp — chuẩn bị dữ liệu & trợ lý AI; chạy thật trên một máy.

🎤 *Khi nói:* mục tiêu đo đạc được, phần nào làm hoàn chỉnh / phần nào ở mức demo.

### Slide 5 — Tác nhân
**Trên slide (chỉ tên + 1 dòng):**
- Người dùng nghiệp vụ — đặt câu hỏi, xem kết quả.
- Quản trị viên dữ liệu — quản lý nguồn & mô tả dữ liệu.

🎤 *Khi nói:* nhắc thêm các tác nhân hệ thống (hệ thống nguồn, bộ điều phối tự động, nhà cung cấp AI).

---

# PHẦN 2 — KIẾN TRÚC 2 LỚP  ⭐

### Slide 6 — Tổng quan luồng 2 lớp (slide bản lề)
**Trên slide: chỉ một sơ đồ + 2 nhãn lớn, gần như không chữ.**

```
NGUỒN ──►  [ LỚP 1: CHUẨN BỊ DỮ LIỆU ] ──►  [ LỚP 2: TRỢ LÝ AI ]  ◄──► NGƯỜI DÙNG
              biến dữ liệu thành                hiểu câu hỏi &
              dữ liệu sạch, sẵn dùng            trả lời an toàn
```

- **Lớp 1:** lo phần **dữ liệu**.
- **Lớp 2:** lo phần **trả lời câu hỏi**.

🎤 *Khi nói:* giải thích 2 lớp tách biệt, điểm nối là kho dữ liệu; câu hỏi tổng hợp hỏi vào dữ liệu phân tích, câu hỏi cần số liệu mới nhất hỏi vào dữ liệu nguồn.

### Slide 7 — Công nghệ sử dụng
**Trên slide: gom theo 2 lớp, chỉ tên công nghệ (logo).**
- Lớp dữ liệu: vài công nghệ tiêu biểu.
- Lớp trợ lý AI: vài công nghệ tiêu biểu.

🎤 *Khi nói:* vai trò từng công nghệ, lý do chọn, tất cả chạy chung trên một máy.

---

# PHẦN 3 — LỚP 1: CHUẨN BỊ DỮ LIỆU

### Slide 8 — Dữ liệu đi qua 3 bước
**Trên slide: 3 khối nối tiếp, mỗi khối 1 cụm từ.**

```
DỮ LIỆU THÔ  ──►  DỮ LIỆU SẠCH  ──►  DỮ LIỆU SẴN DÙNG
(lưu nguyên gốc)   (làm sạch)         (tối ưu để hỏi nhanh)
```

🎤 *Khi nói:* chi tiết từng bước (giữ nguyên gốc, loại trùng, tách lỗi, sắp xếp lại cho truy vấn nhanh).

### Slide 9 — Vì sao cách này hiệu quả  ⭐
**Trên slide: 4 cụm từ ngắn (không giải thích dài).**
- Chia bước rõ ràng — dễ kiểm soát & sửa lỗi.
- Luôn giữ dữ liệu gốc.
- Làm sạch trước khi dùng.
- Tự động & luôn cập nhật.

🎤 *Khi nói:* diễn giải từng nguyên tắc, ví dụ vì sao tách bước giúp xử lý lại dễ, vì sao tự động hóa giúp dữ liệu luôn mới và tin cậy.

---

# PHẦN 4 — LỚP 2: TRỢ LÝ AI

### Slide 10 — Trợ lý xử lý câu hỏi qua các bước
**Trên slide: dùng layout 6 bước của template, mỗi bước 1 dòng ngắn.**
1. Nhận câu hỏi tiếng Việt.
2. Hiểu & phân loại câu hỏi.
3. Tìm đúng dữ liệu liên quan.
4. Soạn câu trả lời.
5. Kiểm tra an toàn.
6. Trả kết quả + giải thích.

🎤 *Khi nói:* chi tiết từng bước, vai trò mỗi tác tử, cách tạo truy vấn và chạy trên kho dữ liệu.

### Slide 11 — Vì sao cách này hiệu quả  ⭐
**Trên slide: 4 cụm từ ngắn.**
- Chia nhỏ nhiều bước — kiểm soát tốt, ít trả lời sai.
- Tìm đúng dữ liệu trước khi trả lời.
- Nhiều lớp kiểm tra an toàn.
- Linh hoạt đổi mô hình AI.

🎤 *Khi nói:* so sánh với hỏi thẳng AI một lần; giải thích cách chống bịa dữ liệu, các lớp an toàn (chỉ đọc, chặn sửa/xóa, che thông tin nhạy cảm), hiển thị tiến trình theo thời gian thực.

---

# PHẦN 5 — THỰC NGHIỆM, KẾT QUẢ & DEMO

### Slide 12 — Kết quả thực nghiệm
**Trên slide: 4 con số lớn, không bảng phức tạp.**
- Độ chính xác **93.3%**
- Tìm đúng dữ liệu **0.94**
- Chặn an toàn **100%**
- Thành công toàn trình **88.6%**

🎤 *Khi nói:* đánh giá trên 120 câu hỏi tiếng Việt; điểm nghẽn tốc độ ở khâu xử lý dữ liệu, không phải ở AI; một số số liệu cần đo lại quy mô lớn hơn.

### Slide 13 — Kết quả đạt được (theo 2 lớp)
**Trên slide:**
- Lớp dữ liệu: nền tảng dữ liệu sạch, tự động.
- Lớp trợ lý AI: trả lời an toàn, chính xác.
- Chạy thật trên một máy.

### Slide 14 — Demo
1–2 câu hỏi minh họa + 1 câu ngoài phạm vi để cho thấy phần kiểm soát an toàn.

### Slide 15 — Hướng phát triển + Cảm ơn
**Trên slide: 3–4 cụm ngắn.**
- Đăng nhập & nhiều người dùng.
- Tăng tốc xử lý dữ liệu.
- Hệ thống tự học từ câu hỏi.
- Hoàn thiện màn hình quản trị.

🎤 *Khi nói:* nói rõ hơn từng hướng. Kết bằng lời cảm ơn.

---

## TÓM TẮT NGUYÊN TẮC TRÌNH BÀY
- **Slide:** tiêu đề + 3–4 cụm từ tổng quát hoặc 1 sơ đồ. Không thuật ngữ, không câu dài.
- **Bạn (khi nói):** giải thích chiều sâu, kỹ thuật, code.
- **15 slide**, agenda 5 phần, hai lớp mỗi lớp 1 cặp slide (đi sâu + vì sao hiệu quả).
