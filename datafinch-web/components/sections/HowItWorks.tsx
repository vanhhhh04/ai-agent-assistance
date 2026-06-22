import { Card } from "../ui/Card";

const STEPS = [
  {
    icon: "💬",
    color: "var(--color-primary)",
    title: "1. Bạn hỏi",
    subtitle: "bằng tiếng Việt tự nhiên",
    description:
      "Không cần biết SQL hay tên bảng. Chỉ cần hỏi như cách bạn hỏi đồng nghiệp: \"Doanh thu tháng này\", \"Khách VIP nhiều đơn nhất\", \"Sản phẩm nào sắp hết hàng\".",
    example: '"Top 5 sản phẩm bán chạy tuần này?"',
  },
  {
    icon: "🧠",
    color: "var(--color-purple)",
    title: "2. AI hiểu ngữ cảnh",
    subtitle: "qua 4 agents phối hợp",
    description:
      "Supervisor phân loại câu hỏi → Retriever tìm bảng đúng trong catalog → SQL Writer viết câu lệnh chính xác → Guardrails kiểm tra an toàn trước khi chạy.",
    example: "Supervisor → Metadata → SQL Writer → Guardrails",
  },
  {
    icon: "📊",
    color: "var(--color-green)",
    title: "3. Có ngay kết quả",
    subtitle: "+ biểu đồ tự động",
    description:
      "Kết quả trả về trực tiếp từ database của bạn — không qua server thứ 3. Kèm biểu đồ phù hợp, giải thích bằng tiếng Việt, và SQL để bạn audit.",
    example: "Bảng + Chart + Giải thích trong < 10s",
  },
];

export function HowItWorks() {
  return (
    <section className="py-24 md:py-32 border-t border-[color:var(--color-border)]" id="how-it-works">
      <div className="mx-auto max-w-7xl px-6">
        {/* Section header */}
        <div className="text-center max-w-3xl mx-auto mb-16">
          <p className="text-xs uppercase tracking-wider font-bold text-[color:var(--color-primary)] mb-3">
            Cách hoạt động
          </p>
          <h2 className="text-3xl md:text-5xl font-bold tracking-tight text-[color:var(--color-text)] mb-4">
            3 bước. Không lập trình. Không đợi data team.
          </h2>
          <p className="text-lg text-[color:var(--color-text-muted)]">
            Hệ thống multi-agent inspired by Uber Finch — biến câu hỏi tiếng Việt thành insight kinh
            doanh trong vài giây.
          </p>
        </div>

        {/* Steps grid */}
        <div className="grid md:grid-cols-3 gap-6 relative">
          {/* Connecting line on desktop */}
          <div
            className="hidden md:block absolute top-12 left-[16.67%] right-[16.67%] h-px"
            style={{
              background:
                "linear-gradient(to right, var(--color-primary), var(--color-purple), var(--color-green))",
              opacity: 0.3,
            }}
          />

          {STEPS.map((step, i) => (
            <Card key={i} hover className="relative bg-white">
              {/* Icon circle */}
              <div
                className="w-16 h-16 rounded-2xl flex items-center justify-center text-3xl mb-5 relative z-10"
                style={{
                  background: `linear-gradient(135deg, ${step.color}15, ${step.color}05)`,
                  border: `2px solid ${step.color}30`,
                }}
              >
                {step.icon}
              </div>

              <h3 className="text-xl font-bold text-[color:var(--color-text)] mb-1">
                {step.title}
              </h3>
              <p
                className="text-sm font-medium mb-4"
                style={{ color: step.color }}
              >
                {step.subtitle}
              </p>
              <p className="text-sm text-[color:var(--color-text-muted)] leading-relaxed mb-5">
                {step.description}
              </p>

              <div className="pt-4 border-t border-[color:var(--color-border)]">
                <p className="text-xs text-[color:var(--color-text-subtle)] mb-1.5 font-semibold uppercase tracking-wide">
                  Ví dụ
                </p>
                <code
                  className="text-xs font-mono block"
                  style={{ color: step.color }}
                >
                  {step.example}
                </code>
              </div>
            </Card>
          ))}
        </div>

        {/* Comparison with ChatGPT */}
        <div className="mt-20 max-w-4xl mx-auto">
          <h3 className="text-2xl md:text-3xl font-bold text-center text-[color:var(--color-text)] mb-3">
            Khác gì so với ChatGPT trực tiếp?
          </h3>
          <p className="text-center text-[color:var(--color-text-muted)] mb-10">
            ChatGPT viết SQL ngữ pháp đúng nhưng <em>không biết database của bạn</em>. DataFinch thì có.
          </p>

          <div className="rounded-xl border border-[color:var(--color-border)] overflow-hidden bg-white shadow-sm">
            <table className="w-full text-sm">
              <thead className="bg-[color:var(--color-bg-muted)]">
                <tr>
                  <th className="text-left px-5 py-3 font-semibold text-[color:var(--color-text)]"></th>
                  <th className="text-center px-5 py-3 font-semibold text-[color:var(--color-text-muted)]">
                    ChatGPT trực tiếp
                  </th>
                  <th className="text-center px-5 py-3 font-semibold text-[color:var(--color-primary)]">
                    DataFinch
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[color:var(--color-border)]">
                {[
                  ["Biết schema của bạn?", false, "Auto-index khi onboarding"],
                  ["Chạy SQL thực tế?", false, "Execute + trả result"],
                  ["Chặn DELETE/UPDATE?", false, "Guardrails block 100%"],
                  ["Kết nối DB nội bộ?", false, "VPN/connector hỗ trợ"],
                  ["Tối ưu tiếng Việt?", "Trung bình", "Tối ưu domain VN"],
                  ["Học theo feedback?", false, "Self-improving"],
                ].map(([label, gpt, df], i) => (
                  <tr key={i}>
                    <td className="px-5 py-3 font-medium text-[color:var(--color-text)]">{label}</td>
                    <td className="px-5 py-3 text-center">
                      {gpt === false ? (
                        <span className="text-[color:var(--color-danger)]">✗</span>
                      ) : (
                        <span className="text-[color:var(--color-text-muted)]">{gpt as string}</span>
                      )}
                    </td>
                    <td className="px-5 py-3 text-center">
                      <span className="inline-flex items-center gap-1 text-[color:var(--color-green)] font-medium">
                        ✓ {df as string}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </section>
  );
}
