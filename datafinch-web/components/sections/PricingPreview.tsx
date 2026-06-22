import { Button } from "../ui/Button";
import { Card } from "../ui/Card";

const PLANS = [
  {
    name: "Free",
    price: "0₫",
    period: "mãi mãi",
    description: "Thử nghiệm + cá nhân",
    features: ["1 data source", "100 queries / tháng", "1 user", "Community support"],
    cta: "Bắt đầu miễn phí",
    href: "/signup",
    highlighted: false,
  },
  {
    name: "Starter",
    price: "1.2tr",
    period: "/tháng",
    description: "Doanh nghiệp nhỏ",
    features: [
      "3 data sources",
      "1,000 queries / tháng",
      "5 users",
      "Slack/email alerts",
      "Saved queries + reports",
      "Email support",
    ],
    cta: "Đăng ký Starter",
    href: "/signup?plan=starter",
    highlighted: true,
    badge: "Phổ biến",
  },
  {
    name: "Growth",
    price: "7.2tr",
    period: "/tháng",
    description: "Đội ngũ data",
    features: [
      "Unlimited sources",
      "10,000 queries",
      "Unlimited users",
      "REST API + Webhooks",
      "Embed widgets",
      "Priority support",
    ],
    cta: "Đăng ký Growth",
    href: "/signup?plan=growth",
    highlighted: false,
  },
  {
    name: "Enterprise",
    price: "Liên hệ",
    period: "",
    description: "On-prem + SSO + SLA",
    features: [
      "Self-hosted / VPC",
      "Unlimited everything",
      "SSO / SAML",
      "Audit log + SOC2",
      "Dedicated CSM",
      "Custom SLA",
    ],
    cta: "Đặt lịch demo",
    href: "/contact",
    highlighted: false,
  },
];

export function PricingPreview() {
  return (
    <section className="py-24 md:py-32 bg-[color:var(--color-bg-muted)]" id="pricing">
      <div className="mx-auto max-w-7xl px-6">
        <div className="text-center max-w-3xl mx-auto mb-16">
          <p className="text-xs uppercase tracking-wider font-bold text-[color:var(--color-primary)] mb-3">
            Bảng giá
          </p>
          <h2 className="text-3xl md:text-5xl font-bold tracking-tight text-[color:var(--color-text)] mb-4">
            Đơn giản. Minh bạch. Không phí ẩn.
          </h2>
          <p className="text-lg text-[color:var(--color-text-muted)]">
            Bắt đầu miễn phí. Trả phí khi bạn cần thêm features.
          </p>
        </div>

        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-5">
          {PLANS.map((plan) => (
            <Card
              key={plan.name}
              className={`relative flex flex-col h-full ${
                plan.highlighted
                  ? "ring-2 ring-[color:var(--color-primary)] shadow-xl shadow-cyan-500/10"
                  : ""
              }`}
            >
              {plan.badge && (
                <span className="absolute -top-3 left-1/2 -translate-x-1/2 px-3 py-1 rounded-full text-xs font-bold bg-[color:var(--color-primary)] text-white">
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
                <span className="text-3xl font-bold text-[color:var(--color-text)]">
                  {plan.price}
                </span>
                {plan.period && (
                  <span className="text-sm text-[color:var(--color-text-muted)] ml-1">
                    {plan.period}
                  </span>
                )}
              </div>

              <ul className="space-y-2.5 mb-6 flex-1">
                {plan.features.map((f, i) => (
                  <li
                    key={i}
                    className="flex items-start gap-2 text-sm text-[color:var(--color-text-muted)]"
                  >
                    <span className="text-[color:var(--color-green)] flex-shrink-0 mt-0.5">✓</span>
                    {f}
                  </li>
                ))}
              </ul>

              <Button
                href={plan.href}
                variant={plan.highlighted ? "primary" : "outline"}
                size="md"
                className="w-full"
              >
                {plan.cta}
              </Button>
            </Card>
          ))}
        </div>

        <div className="mt-12 text-center">
          <Button href="/pricing" variant="ghost" size="md">
            Xem chi tiết tất cả features →
          </Button>
        </div>
      </div>
    </section>
  );
}
