# DataFinch — Customer-Facing UI Plan

> Bản kế hoạch xây dựng giao diện sản phẩm cho DataFinch. **Phạm vi: frontend only**, backend integration để sau (mock data trong giai đoạn này).
>
> Mục tiêu: trong 4 tuần có site marketing + app shell có thể demo cho design partner đầu tiên.

---

## 0. Trước khi bắt đầu — Quyết định cần chốt

Đánh dấu lựa chọn trước khi code:

### 0.1 Tên & domain
- [ ] Tên sản phẩm: ☐ DataFinch ☐ HoiData.vn ☐ AskDB ☐ Vie.Data ☐ Khác: ______
- [ ] Domain đã có chưa? ☐ Có (___________) ☐ Chưa đăng ký
- [ ] Logo có chưa? ☐ Có ☐ Cần thiết kế ☐ Tạm dùng text + icon "◈"

### 0.2 Brand identity
- [ ] Primary color: ☐ `#0891B2` (cyan hiện tại) ☐ Khác: ______
- [ ] Theme: ☐ Light (đã chọn) ☐ Dark ☐ Cả 2 (toggle)
- [ ] Font: ☐ JetBrains Mono + Inter ☐ Khác: ______
- [ ] Tone of voice: ☐ Professional ☐ Friendly ☐ Cả 2 tuỳ context

### 0.3 Target audience (chọn 1 chính)
- [ ] **SME tại VN** — shop online, F&B chain, logistics nhỏ
- [ ] **Startup SaaS** — founders cần tự xem metric
- [ ] **Enterprise** — banks, insurance (yêu cầu on-prem)
- [ ] **Data team** — analyst muốn tool tăng năng suất

> Lựa chọn này quyết định copy, pricing, screenshots.

### 0.4 Tech stack
- [ ] Framework: **Next.js 15 (App Router)** _(đề xuất)_
- [ ] Styling: **Tailwind CSS 4**
- [ ] Components: **shadcn/ui** (Radix + Tailwind)
- [ ] Animation: **Framer Motion**
- [ ] Charts (cho demo): **Recharts** hoặc **Tremor**
- [ ] Hosting: **Vercel** _(đề xuất)_
- [ ] Repo riêng cho frontend? ☐ Có (`datafinch-web`) ☐ Cùng monorepo với backend

---

## 1. Cấu trúc tổng (sitemap)

```
PUBLIC SITE (chưa login)
├── /                       Landing page
├── /features               Chi tiết tính năng (sub-page từ landing)
├── /how-it-works           Giải thích kiến trúc multi-agent
├── /pricing                Bảng giá
├── /docs                   Tài liệu (link sang Docusaurus sau)
├── /blog                   Case studies (sau)
├── /login                  Form đăng nhập
└── /signup                 Onboarding wizard

APP (sau khi login)
├── /app                    Redirect → /app/ask
├── /app/ask                💬 Chat NL→SQL (cải tiến từ hiện tại)
├── /app/saved              📌 Saved queries
├── /app/reports            📊 Báo cáo / dashboard pin
├── /app/alerts             🔔 Scheduled alerts
├── /app/data               🗂 Data sources + schema catalog
├── /app/team               👥 Team management
├── /app/settings           ⚙️ Settings (Profile / AI / Integration / Bảo mật)
└── /app/billing            💳 Plan & usage
```

---

## 2. Menu navigation

### Public site (top nav)

| Item | Mục đích | Visible khi |
|---|---|---|
| Logo + tên | Về landing | Luôn |
| **Sản phẩm** ▾ | Dropdown: Tính năng, Demo, So sánh | Luôn |
| **Cách hoạt động** | Single page giải thích | Luôn |
| **Bảng giá** | Pricing table | Luôn |
| **Tài liệu** | Docs link | Luôn |
| **Đăng nhập** | Login form | Chưa login |
| **Dùng thử miễn phí** (CTA) | Signup wizard | Chưa login |
| Avatar | Dropdown profile | Đã login |

### App sidebar (logged-in)

```
┌──────────┐
│ ◈ DataFinch
├──────────
│ 💬 Hỏi              ← default landing
│ 📌 Đã lưu
│ 📊 Báo cáo
│ 🔔 Alerts
├──────────
│ 🗂 Dữ liệu
│ 👥 Team
│ ⚙️ Cài đặt
├──────────
│ 💳 Gói
│ 📚 Tài liệu
├──────────
│ [Avatar] [Tên user]
└──────────
```

**Nguyên tắc:** 8 mục max, group bằng divider, item dùng nhiều nhất ở top.

---

## 3. Page-by-page plan

### 3.1 Landing page (`/`)

Sections theo thứ tự scroll:

| Section | Content | Effort |
|---|---|---|
| **Hero** | Tagline + 2 CTA + demo widget interactive | 2 ngày |
| **Social proof** | 4 logo customer + 1 testimonial card | 0.5 ngày |
| **How it works** | 3 bước (icon + title + desc) | 1 ngày |
| **Features grid** | 4 cột (Hỏi VN, Bảo mật, Connector, Charts) | 1 ngày |
| **Use cases** | 3 thẻ vertical (Bán lẻ / Logistics / SaaS) | 1 ngày |
| **Pricing preview** | Compact table 4 tier + link to /pricing | 0.5 ngày |
| **FAQ** | Accordion 6 câu hỏi | 0.5 ngày |
| **CTA cuối** | "Sẵn sàng thử miễn phí?" + email capture | 0.5 ngày |
| **Footer** | Links + social + copyright | 0.5 ngày |

**Tổng**: ~7 ngày = 1.5 tuần.

**Tagline đề xuất** (chọn 1):
- [ ] *"Hỏi dữ liệu như hỏi người. Trả lời chính xác như chuyên gia."*
- [ ] *"AI Data Analyst của bạn — sẵn sàng 24/7, không cần biết SQL"*
- [ ] *"Biến mọi câu hỏi tiếng Việt thành insight kinh doanh"*

### 3.2 How it works page (`/how-it-works`)

1 trang chi tiết với:
- Diagram 4 agents (Supervisor → Retriever → SQL Writer → Executor) — vẽ SVG
- Giải thích từng bước bằng business language
- Bảng so sánh "DataFinch vs ChatGPT trực tiếp"
- Video demo 90s (placeholder ban đầu)

**Effort**: 3 ngày.

### 3.3 Pricing page (`/pricing`)

```
┌─────────┬─────────┬─────────┬─────────┐
│  Free   │ Starter │ Growth  │ Enterprise│
├─────────┼─────────┼─────────┼─────────┤
│  $0     │ $49/m   │ $299/m  │ Liên hệ │
│         │         │         │         │
│ 1 source│ 3 source│ ∞ source│ ∞       │
│ 100 q/m │ 1k q/m  │ 10k q/m │ ∞       │
│ 1 user  │ 5 users │ ∞ users │ ∞       │
│ ...     │ ...     │ ...     │ ...     │
│         │         │         │         │
│ [Start] │ [Subs]  │ [Subs]  │ [Contact]│
└─────────┴─────────┴─────────┴─────────┘
```

- Monthly/Annual toggle (annual giảm 20%)
- Feature matrix bên dưới (collapse table)
- FAQ riêng

**Effort**: 2 ngày.

### 3.4 Signup / Onboarding wizard (`/signup`)

5 step flow:

| Step | Screen | UI element chính |
|---|---|---|
| 1 | Welcome | Logo + "Bắt đầu" button |
| 2 | Connect DB | Grid 6 DB icon (Postgres/MySQL/Snowflake/BigQuery/DuckDB/Khác) + "Dùng demo data" link |
| 3 | Credentials form | Host/Port/DB/User/Pass + ☑️ readonly check |
| 4 | Auto-index loading | Progress + ticks ("Đã tìm 24 bảng", "Đang tạo mô tả", ...) |
| 5 | First query suggestion | 3 sample queries based on schema |

**Stage này dùng mock** — không thật sự connect DB. Chỉ trình diễn UX.

**Effort**: 4 ngày.

### 3.5 App `/app/ask` — Chat

Cải tiến từ `ai-data-assistant.jsx` hiện tại:

- [ ] History panel bên trái (collapse được) — list các câu đã hỏi
- [ ] Suggested questions thông minh hơn (mock based on schema)
- [ ] Mỗi agent message có action buttons: 💾 Lưu | 📊 Pin vào báo cáo | 🔔 Tạo alert | 📤 Share
- [ ] Chart auto-generate placeholder (đợi backend implement chart_advisor)
- [ ] Loading skeleton thay vì spinner

**Effort**: 3 ngày.

### 3.6 App `/app/saved` — Đã lưu

```
┌─────────────────────────────────────┐
│ Đã lưu                  [Filter ▾] │
├─────────────────────────────────────┤
│ ⭐ Doanh thu Q1 2026                │
│    "Tổng doanh thu Q1 theo tháng"  │
│    Lưu: 2 ngày trước                │
│    [Mở] [Pin báo cáo] [Sửa] [Xóa]  │
├─────────────────────────────────────┤
│ ⭐ Top 10 sản phẩm bán chạy         │
│    ...                              │
└─────────────────────────────────────┘
```

- Mock 5 saved queries
- Folder/tag system (optional v2)

**Effort**: 1.5 ngày.

### 3.7 App `/app/reports` — Báo cáo

```
┌───────────────────────────────────┐
│ Tab: [Sales] [Orders] [Customers] │
├───────────────────────────────────┤
│ ┌──────┐ ┌──────┐                 │
│ │ KPI  │ │ KPI  │                 │
│ │ Card │ │ Card │                 │
│ └──────┘ └──────┘                 │
│ ┌─────────────────┐               │
│ │ Chart (Recharts)│               │
│ └─────────────────┘               │
└───────────────────────────────────┘
```

- 3 tab dashboard mock
- KPI cards + charts với fake data
- Drag-rearrange (v2)

**Effort**: 3 ngày.

### 3.8 App `/app/alerts` — Alerts

List alerts với:
- Tên + condition (text)
- Schedule (cron readable)
- Channel (Slack/email)
- Status (🟢 active / 🟡 paused / 🔴 triggered recently)
- Edit/Pause/Delete actions

Form tạo alert mới:
- NL question
- Condition (threshold)
- Schedule (daily 8am, hourly, ...)
- Channels

**Effort**: 2.5 ngày.

### 3.9 App `/app/data` — Data sources

- List data sources (mock 2-3)
- Schema catalog (browse tables + columns)
- Inline edit description cho mỗi table

**Effort**: 2.5 ngày.

### 3.10 App `/app/team` — Team

- Member list (mock 5 people)
- Invite by email
- Role assign (Admin/Analyst/Viewer)

**Effort**: 1.5 ngày.

### 3.11 App `/app/settings` — Settings

4 tabs:
- [ ] **Profile**: avatar, name, email, password
- [ ] **AI Model**: LLM provider radio + model dropdown + API key + max tokens
- [ ] **Integration**: Slack, email, webhooks
- [ ] **Bảo mật**: SSO (placeholder), audit log link, 2FA

**Effort**: 3 ngày.

### 3.12 App `/app/billing` — Gói

- Plan hiện tại
- Usage chart (queries used / month)
- Upgrade button → pricing
- Invoice history (mock)

**Effort**: 1.5 ngày.

---

## 4. Components dùng chung (build trước)

| Component | Mô tả | Effort |
|---|---|---|
| `<Logo />` | Logo + tên, có size prop | 0.5h |
| `<Button variant="primary|secondary|ghost" />` | shadcn/ui base | 0.5h |
| `<Card />` | White bg + border + shadow | 0.5h |
| `<Sidebar />` | App sidebar với 8 items | 4h |
| `<TopNav />` | Public site top navigation | 3h |
| `<Footer />` | Public site footer | 2h |
| `<PricingCard />` | 1 card trong pricing table | 3h |
| `<FAQAccordion />` | Question/answer collapse | 2h |
| `<CodeSnippet />` | SQL/JSON với syntax highlight | 2h |
| `<ChartPlaceholder />` | Skeleton + dummy chart | 2h |
| `<EmptyState />` | "Chưa có dữ liệu" + CTA | 2h |
| `<LoadingState />` | Skeleton screens | 2h |

**Tổng**: 1 tuần build component library.

---

## 5. Roadmap 4 tuần

### Tuần 1: Setup + Landing page
- [ ] Day 1: Setup Next.js + Tailwind + shadcn/ui + repo + Vercel preview
- [ ] Day 2: Component library (Button, Card, Logo, TopNav, Footer)
- [ ] Day 3-4: Landing page Hero + How it works section
- [ ] Day 5: Features grid + Use cases section
- [ ] Day 6: FAQ + CTA cuối + Footer
- [ ] Day 7: Polish + mobile responsive
- **Milestone**: Landing live tại `<domain>.vercel.app`

### Tuần 2: Pricing + How-it-works detail page
- [ ] Day 1-2: Pricing page với monthly/annual toggle
- [ ] Day 3-4: How it works detail page với SVG diagram
- [ ] Day 5: FAQ page (nếu cần riêng)
- [ ] Day 6-7: 404, About, contact form
- **Milestone**: Public site hoàn chỉnh

### Tuần 3: App shell + Onboarding
- [ ] Day 1-2: App layout (Sidebar + header)
- [ ] Day 3: `/app/ask` page (cải tiến từ JSX hiện tại)
- [ ] Day 4: `/app/saved` + `/app/reports` (mock)
- [ ] Day 5-6: Signup wizard 5 steps
- [ ] Day 7: Login page + auth UI (mock, dùng Clerk sau)
- **Milestone**: User flow end-to-end (signup → onboarding → first query)

### Tuần 4: Remaining app pages + polish
- [ ] Day 1: `/app/alerts`
- [ ] Day 2: `/app/data`
- [ ] Day 3: `/app/team`
- [ ] Day 4-5: `/app/settings` (4 tabs)
- [ ] Day 6: `/app/billing` + Plan upgrade UX
- [ ] Day 7: QA, mobile responsive, loading states, error states
- **Milestone**: Full product demo-ready

---

## 6. Acceptance criteria

Sản phẩm UI được coi là "done" khi:

### Functional
- [ ] Mọi page load < 2s (Lighthouse > 90)
- [ ] Mobile responsive (320px - 1920px)
- [ ] Dark mode toggle hoạt động (optional v1.5)
- [ ] All navigation links work
- [ ] Forms validate input
- [ ] Loading/error/empty states cho mọi page
- [ ] Keyboard navigation (Tab, Esc, Enter)
- [ ] Accessibility: contrast ratio AA, aria labels

### Content
- [ ] Tagline + copy hero finalized
- [ ] 4-6 customer logo (có thể là placeholder ban đầu)
- [ ] 1+ testimonial (placeholder hoặc thật)
- [ ] FAQ ≥ 6 câu hỏi
- [ ] Pricing với feature matrix đầy đủ

### Performance
- [ ] First Contentful Paint < 1.5s
- [ ] Bundle size < 300KB gzipped (landing page)
- [ ] Images optimized (WebP, lazy load)

---

## 7. Sau khi UI done → next step

Khi giai đoạn này hoàn thành, các bước tiếp theo (KHÔNG trong scope plan này):

1. **Auth backend** — Clerk integration (1 tuần)
2. **Connect existing backend** — wire app/ask vào FastAPI hiện tại (1 tuần)
3. **Replace mock data** — saved queries, reports, alerts thật (2 tuần)
4. **Multi-tenancy backend** — tenant isolation (4 tuần)
5. **Billing integration** — Stripe (2 tuần)
6. **Onboarding backend** — auto-index schema (3 tuần)

---

## 8. Quyết định cần user duyệt trước khi start

| # | Quyết định | Lựa chọn của bạn |
|---|---|---|
| 1 | Tên sản phẩm | ____________ |
| 2 | Domain | ____________ |
| 3 | Target audience chính | ____________ |
| 4 | Primary color | ____________ |
| 5 | Tagline | ____________ |
| 6 | Tech stack confirmed? | ☐ Yes ☐ Đổi: ___ |
| 7 | Repo: monorepo / separate? | ____________ |
| 8 | Bắt đầu tuần nào? | ____________ |
| 9 | Có cần Figma mockup trước khi code không? | ☐ Có ☐ Không, code thẳng |
| 10 | Logo design: tự làm / outsource / dùng tạm? | ____________ |

---

## 9. Risks & dependencies

| Risk | Mitigation |
|---|---|
| Copywriting chậm (cần content tiếng Việt chuẩn) | Viết draft trước, polish sau khi UI xong |
| Designer chưa có | Dùng shadcn/ui — đẹp sẵn, ít cần custom design |
| Backend chưa sẵn lúc UI demo | Mock data hard-coded, đủ cho demo flow |
| Domain chưa mua | Dùng `<name>.vercel.app` tạm |
| Logo chưa có | Dùng text "◈ DataFinch" tạm |

---

## 10. Tham khảo (sites cần học design)

UI references (cho inspiration):
- **Linear** (linear.app) — sidebar app design chuẩn nhất
- **Notion** (notion.so) — onboarding flow xuất sắc
- **Vercel** (vercel.com) — landing page minimalist
- **Stripe** (stripe.com) — pricing page chuẩn mực
- **Hex** (hex.tech) — competitor trực tiếp, học cách họ positioning
- **Mintlify** (mintlify.com) — docs site đẹp
- **Cal.com** (cal.com) — open source, code clean để học

---

**Ngày tạo plan**: 2026-05-13
**Owner**: Cao Việt Anh
**Status**: ☐ Đang review ☐ Đã duyệt ☐ Đang triển khai
