import { Card } from "../ui/Card";

const CASES = [
  {
    industry: "🛒  Bán lẻ / E-commerce",
    color: "var(--color-orange)",
    persona: "Founder & ops manager",
    quote:
      "\"Sáng nào tôi cũng hỏi DataFinch doanh thu hôm qua, top sản phẩm, đơn nào trễ giao. Không phải ping data team nữa.\"",
    questions: [
      "Doanh thu hôm qua theo brand?",
      "Top 10 SKU bán chạy tuần này?",
      "Đơn nào trễ giao quá 3 ngày?",
      "Khách hàng nào đặt nhiều nhất tháng này?",
    ],
    saving: "Tiết kiệm 6h/tuần cho data team",
  },
  {
    industry: "🚚  Logistics / Vận hành",
    color: "var(--color-blue)",
    persona: "Ops director & dispatcher",
    quote:
      "\"Trước đây 1 query về delivery SLA mất 2 ngày. Giờ team field tự hỏi mọi lúc trên mobile.\"",
    questions: [
      "% giao đúng SLA tuần này?",
      "Vùng nào có tỷ lệ trả hàng cao nhất?",
      "Carrier nào delay nhiều nhất?",
      "Route nào cần optimize?",
    ],
    saving: "Giảm 40% ad-hoc data requests",
  },
  {
    industry: "📈  SaaS / Startup",
    color: "var(--color-purple)",
    persona: "CEO & growth lead",
    quote:
      "\"MRR, churn, LTV, cohort retention — tất cả 1 prompt. Không cần Hex/Mode đắt đỏ.\"",
    questions: [
      "MRR theo gói tháng này?",
      "Churn rate cohort Q1 2026?",
      "Top features dùng nhiều nhất?",
      "LTV theo source acquisition?",
    ],
    saving: "$2000/tháng vs Hex/Mode",
  },
];

export function UseCases() {
  return (
    <section className="py-24 md:py-32" id="use-cases">
      <div className="mx-auto max-w-7xl px-6">
        <div className="text-center max-w-3xl mx-auto mb-16">
          <p className="text-xs uppercase tracking-wider font-bold text-[color:var(--color-primary)] mb-3">
            Use cases
          </p>
          <h2 className="text-3xl md:text-5xl font-bold tracking-tight text-[color:var(--color-text)] mb-4">
            Cho mọi team. Cho mọi câu hỏi.
          </h2>
          <p className="text-lg text-[color:var(--color-text-muted)]">
            Không quan trọng bạn dùng Postgres, BigQuery, hay Hive — DataFinch nói được tiếng VN của
            ngành bạn.
          </p>
        </div>

        <div className="grid md:grid-cols-3 gap-6">
          {CASES.map((c, i) => (
            <Card key={i} hover className="flex flex-col h-full">
              <div className="mb-5">
                <h3 className="text-lg font-bold text-[color:var(--color-text)] mb-1">
                  {c.industry}
                </h3>
                <p className="text-xs text-[color:var(--color-text-subtle)]">
                  {c.persona}
                </p>
              </div>

              <blockquote
                className="text-sm italic text-[color:var(--color-text-muted)] mb-5 pl-3 border-l-2"
                style={{ borderColor: c.color }}
              >
                {c.quote}
              </blockquote>

              <p className="text-xs font-semibold uppercase tracking-wide text-[color:var(--color-text-subtle)] mb-2">
                Câu hỏi điển hình
              </p>
              <ul className="space-y-2 mb-5 flex-1">
                {c.questions.map((q, j) => (
                  <li
                    key={j}
                    className="text-sm px-3 py-2 rounded-lg bg-[color:var(--color-bg-muted)] border border-[color:var(--color-border)] text-[color:var(--color-text)] font-medium"
                  >
                    {q}
                  </li>
                ))}
              </ul>

              <div
                className="mt-auto pt-4 border-t border-[color:var(--color-border)] flex items-center gap-2 text-sm font-semibold"
                style={{ color: c.color }}
              >
                <span>💰</span>
                {c.saving}
              </div>
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
}
