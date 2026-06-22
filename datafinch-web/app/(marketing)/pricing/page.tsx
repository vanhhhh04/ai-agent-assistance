"use client";

import { useState } from "react";
import { Button } from "@/components/ui/Button";
import { Card } from "@/components/ui/Card";
import { cn } from "@/lib/utils";

type Period = "monthly" | "annual";

const PLANS = [
  {
    id: "free",
    name: "Free",
    description: "Thử nghiệm + cá nhân",
    price: { monthly: 0, annual: 0 },
    cta: "Bắt đầu miễn phí",
    ctaHref: "/signup",
    highlighted: false,
    features: {
      "Data sources": "1",
      "Queries / tháng": "100",
      "Users": "1",
      "Saved queries": "10",
      "Scheduled alerts": "—",
      "REST API access": "—",
      "Embed widgets": "—",
      "Support": "Community",
      "SSO / SAML": "—",
      "Audit log": "—",
      "Self-hosted": "—",
      "SLA": "—",
    },
  },
  {
    id: "starter",
    name: "Starter",
    description: "Doanh nghiệp nhỏ",
    price: { monthly: 1_200_000, annual: 1_000_000 },
    cta: "Đăng ký Starter",
    ctaHref: "/signup?plan=starter",
    highlighted: true,
    badge: "Phổ biến",
    features: {
      "Data sources": "3",
      "Queries / tháng": "1,000",
      "Users": "5",
      "Saved queries": "Unlimited",
      "Scheduled alerts": "10",
      "REST API access": "—",
      "Embed widgets": "—",
      "Support": "Email (48h)",
      "SSO / SAML": "—",
      "Audit log": "30 ngày",
      "Self-hosted": "—",
      "SLA": "—",
    },
  },
  {
    id: "growth",
    name: "Growth",
    description: "Đội ngũ data",
    price: { monthly: 7_200_000, annual: 6_000_000 },
    cta: "Đăng ký Growth",
    ctaHref: "/signup?plan=growth",
    highlighted: false,
    features: {
      "Data sources": "Unlimited",
      "Queries / tháng": "10,000",
      "Users": "Unlimited",
      "Saved queries": "Unlimited",
      "Scheduled alerts": "Unlimited",
      "REST API access": "✓",
      "Embed widgets": "5",
      "Support": "Priority (12h)",
      "SSO / SAML": "—",
      "Audit log": "1 năm",
      "Self-hosted": "—",
      "SLA": "99.5%",
    },
  },
  {
    id: "enterprise",
    name: "Enterprise",
    description: "On-prem + SSO + SLA",
    price: { monthly: null, annual: null },
    cta: "Đặt lịch demo",
    ctaHref: "/contact",
    highlighted: false,
    features: {
      "Data sources": "Unlimited",
      "Queries / tháng": "Unlimited",
      "Users": "Unlimited",
      "Saved queries": "Unlimited",
      "Scheduled alerts": "Unlimited",
      "REST API access": "✓",
      "Embed widgets": "Unlimited",
      "Support": "Dedicated CSM",
      "SSO / SAML": "✓",
      "Audit log": "Unlimited",
      "Self-hosted": "✓",
      "SLA": "99.95%",
    },
  },
];

const BILLING_FAQ = [
  {
    q: "Tôi có thể đổi plan bất kỳ lúc nào không?",
    a: "Có. Upgrade: hiệu lực ngay, charge prorated cho phần còn lại của chu kỳ. Downgrade: hiệu lực từ chu kỳ kế tiếp, không refund phần đã dùng.",
  },
  {
    q: "Thanh toán bằng cách nào?",
    a: "Thẻ Visa/Mastercard/JCB, ATM nội địa (qua VNPay), chuyển khoản (cho Growth+), hoặc invoice 30-ngày cho Enterprise.",
  },
  {
    q: "Annual plan có giảm bao nhiêu %?",
    a: "Annual giảm 17% so với monthly (tương đương 2 tháng miễn phí). Thanh toán 1 lần đầu năm.",
  },
  {
    q: "Free tier có thực sự miễn phí không? Có giới hạn gì?",
    a: "Có, mãi mãi. Giới hạn duy nhất: 100 queries/tháng + 1 user + 1 data source. Đủ cho cá nhân đánh giá + small project.",
  },
  {
    q: "Query là gì? Có tính dirty query không?",
    a: "1 query = 1 lần bạn ấn submit câu hỏi (kể cả nếu LLM trả về lỗi). Schema browsing, saved queries view, dashboard refresh KHÔNG tính. Failed query do guardrail vẫn tính (vì đã consume LLM tokens).",
  },
  {
    q: "Vượt quota tháng thì sao?",
    a: "Free: query bị reject, gợi ý upgrade. Paid plans: tự động pay-as-you-go ($0.05/query thêm), hoặc bạn có thể set hard cap.",
  },
  {
    q: "Có thể test Enterprise features không?",
    a: "Có. Liên hệ sales → free 14-day Enterprise trial (full features, on-prem deploy hỗ trợ).",
  },
];

function formatPrice(vnd: number): string {
  if (vnd >= 1_000_000) return `${(vnd / 1_000_000).toFixed(1).replace(".0", "")}tr`;
  if (vnd >= 1_000) return `${(vnd / 1_000).toFixed(0)}k`;
  return `${vnd}`;
}

export default function PricingPage() {
  const [period, setPeriod] = useState<Period>("monthly");
  const [openFaq, setOpenFaq] = useState<number | null>(0);

  return (
    <>
      {/* Hero */}
      <section className="pt-20 pb-16 md:pt-28 md:pb-20 text-center">
        <div className="mx-auto max-w-3xl px-6">
          <p className="text-xs uppercase tracking-wider font-bold text-[color:var(--color-primary)] mb-3">
            Bảng giá
          </p>
          <h1 className="text-4xl md:text-6xl font-bold tracking-tight text-[color:var(--color-text)] mb-4">
            Minh bạch.{" "}
            <span className="bg-gradient-to-r from-[color:var(--color-primary)] to-[color:var(--color-purple)] bg-clip-text text-transparent">
              Không phí ẩn.
            </span>
          </h1>
          <p className="text-lg md:text-xl text-[color:var(--color-text-muted)] mb-10">
            Bắt đầu miễn phí. Trả phí khi bạn cần thêm tính năng. Hủy bất kỳ lúc nào.
          </p>

          {/* Period toggle */}
          <div className="inline-flex items-center gap-1 p-1 rounded-xl bg-[color:var(--color-bg-subtle)] border border-[color:var(--color-border)]">
            <button
              onClick={() => setPeriod("monthly")}
              className={cn(
                "px-5 py-2 rounded-lg text-sm font-medium transition-all",
                period === "monthly"
                  ? "bg-white text-[color:var(--color-text)] shadow-sm"
                  : "text-[color:var(--color-text-muted)]"
              )}
            >
              Hàng tháng
            </button>
            <button
              onClick={() => setPeriod("annual")}
              className={cn(
                "px-5 py-2 rounded-lg text-sm font-medium transition-all flex items-center gap-2",
                period === "annual"
                  ? "bg-white text-[color:var(--color-text)] shadow-sm"
                  : "text-[color:var(--color-text-muted)]"
              )}
            >
              Hàng năm
              <span className="px-2 py-0.5 rounded-full text-xs font-bold bg-[color:var(--color-primary)] text-white">
                Tiết kiệm 17%
              </span>
            </button>
          </div>
        </div>
      </section>

      {/* Pricing cards */}
      <section className="pb-20">
        <div className="mx-auto max-w-7xl px-6">
          <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-5">
            {PLANS.map((plan) => {
              const price = plan.price[period];
              return (
                <Card
                  key={plan.id}
                  className={cn(
                    "relative flex flex-col h-full",
                    plan.highlighted &&
                      "ring-2 ring-[color:var(--color-primary)] shadow-xl shadow-cyan-500/10"
                  )}
                >
                  {plan.badge && (
                    <span className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-1 rounded-full text-xs font-bold bg-[color:var(--color-primary)] text-white whitespace-nowrap">
                      {plan.badge}
                    </span>
                  )}

                  <div className="mb-5">
                    <h3 className="text-lg font-bold text-[color:var(--color-text)] mb-1">
                      {plan.name}
                    </h3>
                    <p className="text-xs text-[color:var(--color-text-subtle)]">
                      {plan.description}
                    </p>
                  </div>

                  <div className="mb-6">
                    {price === null ? (
                      <span className="text-3xl font-bold text-[color:var(--color-text)]">
                        Liên hệ
                      </span>
                    ) : price === 0 ? (
                      <>
                        <span className="text-3xl font-bold text-[color:var(--color-text)]">
                          0₫
                        </span>
                        <span className="text-sm text-[color:var(--color-text-muted)] ml-1">
                          mãi mãi
                        </span>
                      </>
                    ) : (
                      <>
                        <span className="text-3xl font-bold text-[color:var(--color-text)]">
                          {formatPrice(price)}
                        </span>
                        <span className="text-sm text-[color:var(--color-text-muted)] ml-1">
                          /{period === "monthly" ? "tháng" : "tháng (annual)"}
                        </span>
                      </>
                    )}
                  </div>

                  <ul className="space-y-2 mb-6 flex-1">
                    {Object.entries(plan.features)
                      .slice(0, 6)
                      .map(([k, v]) => (
                        <li
                          key={k}
                          className="flex items-start gap-2 text-sm text-[color:var(--color-text-muted)]"
                        >
                          <span className="text-[color:var(--color-green)] flex-shrink-0 mt-0.5">
                            {v === "—" ? "—" : "✓"}
                          </span>
                          <span>
                            <span className="text-[color:var(--color-text)] font-medium">{k}</span>
                            {v !== "✓" && v !== "—" && (
                              <span className="text-[color:var(--color-text-subtle)]">: {v}</span>
                            )}
                          </span>
                        </li>
                      ))}
                  </ul>

                  <Button
                    href={plan.ctaHref}
                    variant={plan.highlighted ? "primary" : "outline"}
                    size="md"
                    className="w-full"
                  >
                    {plan.cta}
                  </Button>
                </Card>
              );
            })}
          </div>
        </div>
      </section>

      {/* Feature comparison matrix */}
      <section className="py-20 bg-[color:var(--color-bg-muted)] border-y border-[color:var(--color-border)]">
        <div className="mx-auto max-w-7xl px-6">
          <div className="text-center mb-12 max-w-2xl mx-auto">
            <h2 className="text-3xl md:text-4xl font-bold text-[color:var(--color-text)] mb-3">
              So sánh chi tiết
            </h2>
            <p className="text-[color:var(--color-text-muted)]">
              Mọi tính năng. Mọi giới hạn. Không giấu giếm.
            </p>
          </div>

          <div className="rounded-xl border border-[color:var(--color-border)] bg-white overflow-x-auto">
            <table className="w-full text-sm min-w-[640px]">
              <thead className="bg-[color:var(--color-bg-muted)] sticky top-0">
                <tr>
                  <th className="text-left px-5 py-4 font-semibold text-[color:var(--color-text)]">
                    Feature
                  </th>
                  {PLANS.map((plan) => (
                    <th
                      key={plan.id}
                      className={cn(
                        "px-5 py-4 font-semibold text-center",
                        plan.highlighted
                          ? "text-[color:var(--color-primary)]"
                          : "text-[color:var(--color-text)]"
                      )}
                    >
                      {plan.name}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-[color:var(--color-border)]">
                {Object.keys(PLANS[0].features).map((featKey) => (
                  <tr key={featKey} className="hover:bg-[color:var(--color-bg-muted)]/50">
                    <td className="px-5 py-3 font-medium text-[color:var(--color-text)]">
                      {featKey}
                    </td>
                    {PLANS.map((plan) => {
                      const val = plan.features[featKey as keyof typeof plan.features];
                      const isCheck = val === "✓";
                      const isDash = val === "—";
                      return (
                        <td key={plan.id} className="px-5 py-3 text-center">
                          {isCheck ? (
                            <span className="text-[color:var(--color-green)] font-bold">✓</span>
                          ) : isDash ? (
                            <span className="text-[color:var(--color-text-subtle)]">—</span>
                          ) : (
                            <span className="text-[color:var(--color-text-muted)] font-mono text-xs">
                              {val}
                            </span>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* Billing FAQ */}
      <section className="py-20">
        <div className="mx-auto max-w-3xl px-6">
          <div className="text-center mb-12">
            <h2 className="text-3xl md:text-4xl font-bold text-[color:var(--color-text)] mb-3">
              Câu hỏi về thanh toán
            </h2>
            <p className="text-[color:var(--color-text-muted)]">
              Chưa thấy câu hỏi của bạn?{" "}
              <a
                href="mailto:billing@datafinch.app"
                className="text-[color:var(--color-primary)] hover:underline"
              >
                Liên hệ team billing
              </a>
              .
            </p>
          </div>

          <div className="space-y-3">
            {BILLING_FAQ.map((faq, i) => {
              const open = openFaq === i;
              return (
                <div
                  key={i}
                  className={cn(
                    "rounded-xl border bg-white transition-all overflow-hidden",
                    open
                      ? "border-[color:var(--color-primary)] shadow-sm shadow-cyan-500/10"
                      : "border-[color:var(--color-border)]"
                  )}
                >
                  <button
                    onClick={() => setOpenFaq(open ? null : i)}
                    className="w-full px-5 py-4 flex items-center justify-between text-left gap-4"
                  >
                    <span className="font-semibold text-[color:var(--color-text)]">{faq.q}</span>
                    <span
                      className={cn(
                        "flex-shrink-0 w-6 h-6 rounded-full flex items-center justify-center text-sm transition-transform",
                        open
                          ? "bg-[color:var(--color-primary)] text-white rotate-45"
                          : "bg-[color:var(--color-bg-subtle)] text-[color:var(--color-text-muted)]"
                      )}
                    >
                      +
                    </span>
                  </button>
                  <div
                    className={cn("overflow-hidden transition-all", open ? "max-h-96" : "max-h-0")}
                  >
                    <p className="px-5 pb-5 text-sm text-[color:var(--color-text-muted)] leading-relaxed">
                      {faq.a}
                    </p>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-20 border-t border-[color:var(--color-border)] bg-[color:var(--color-bg-muted)]">
        <div className="mx-auto max-w-3xl px-6 text-center">
          <h2 className="text-3xl md:text-4xl font-bold text-[color:var(--color-text)] mb-4">
            Vẫn chưa chắc plan nào phù hợp?
          </h2>
          <p className="text-lg text-[color:var(--color-text-muted)] mb-8">
            Bắt đầu free, upgrade khi cần. Hoặc đặt lịch demo 30 phút với team chúng tôi.
          </p>
          <div className="flex flex-col sm:flex-row gap-3 justify-center">
            <Button href="/signup" size="lg">
              Bắt đầu miễn phí →
            </Button>
            <Button href="/contact" variant="outline" size="lg">
              Đặt lịch demo
            </Button>
          </div>
        </div>
      </section>
    </>
  );
}
