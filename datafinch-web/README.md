# DataFinch Web

Frontend marketing site + app shell cho DataFinch — AI Data Analyst cho doanh nghiệp Việt.

> Đây CHỈ là frontend. Backend FastAPI sống ở `../ai-agent/` (cùng monorepo).

## Stack

- **Next.js 16** (App Router, React 19)
- **Tailwind CSS 4**
- **TypeScript**
- Fonts: Inter (UI) + JetBrains Mono (code)
- Brand color: `#0891b2` (cyan)

## Cấu trúc thư mục

```
datafinch-web/
├── app/
│   ├── globals.css         # Theme tokens + animations
│   ├── layout.tsx          # Root layout (fonts, metadata)
│   └── page.tsx            # Landing page
├── components/
│   ├── ui/                 # Primitives
│   │   ├── Button.tsx
│   │   └── Card.tsx
│   ├── sections/           # Landing page sections
│   │   ├── Hero.tsx
│   │   ├── HowItWorks.tsx
│   │   ├── Features.tsx
│   │   ├── UseCases.tsx
│   │   ├── PricingPreview.tsx
│   │   ├── FAQ.tsx
│   │   └── FinalCTA.tsx
│   ├── Logo.tsx
│   ├── TopNav.tsx
│   └── Footer.tsx
├── lib/
│   └── utils.ts            # cn() helper
└── public/                 # Static assets
```

## Dev

```bash
npm run dev   # http://localhost:3000
npm run build
npm run start
```

## Environment variables

Tạo `.env.local`:

```env
NEXT_PUBLIC_API_BASE=http://localhost:8000
NEXT_PUBLIC_SITE_URL=https://datafinch.app
```

## Deployment — Vercel (recommended)

```bash
npx vercel
```

Auto-deploy khi push lên `main` branch.

## Roadmap

Xem [PLAN.md](../PLAN.md) ở root monorepo.

- **Tuần 1 (DONE):** ✅ Landing page (Hero, How-it-works, Features, Use cases, Pricing, FAQ, CTA)
- **Tuần 2:** Pricing detail page + How-it-works detail page
- **Tuần 3:** App shell (sidebar + chat) + Onboarding wizard
- **Tuần 4:** App pages (saved/reports/alerts/data/team/settings)
