import { Card } from "../ui/Card";

const FEATURES = [
  {
    icon: "🇻🇳",
    color: "var(--color-primary)",
    title: "Tiếng Việt native",
    description:
      "Không phải tool US dịch sang. Hệ thống được train với business terms Việt: \"doanh thu\", \"đơn hàng\", \"khách VIP\", \"bán chạy\", \"đắt nhất\".",
    bullet: ["Hiểu lóng và viết tắt", "Format số 1.5 tỷ / 200k VND", "Date formats VN (dd/mm/yyyy)"],
  },
  {
    icon: "🛡️",
    color: "var(--color-green)",
    title: "An toàn dữ liệu",
    description:
      "Database của bạn ở lại với bạn. Chúng tôi không copy, không cache, không train model trên data của bạn. Read-only by default.",
    bullet: ["Guardrails chặn DELETE/UPDATE", "Row-level security", "PII masking tự động"],
  },
  {
    icon: "🔌",
    color: "var(--color-purple)",
    title: "Kết nối mọi DB",
    description:
      "PostgreSQL, MySQL, BigQuery, Snowflake, Hive, Spark, MongoDB, DuckDB. Setup trong 5 phút — không cần thay đổi data warehouse hiện tại.",
    bullet: ["8+ connectors", "Self-hosted option", "VPC peering / SSH tunnel"],
  },
  {
    icon: "📊",
    color: "var(--color-orange)",
    title: "Insight + Visualize",
    description:
      "Không chỉ trả về table — AI tự chọn loại biểu đồ phù hợp (bar/line/pie), highlight số bất thường, giải thích trend.",
    bullet: ["Auto chart selection", "Anomaly detection", "Drill-down 1 click"],
  },
  {
    icon: "💬",
    color: "var(--color-pink)",
    title: "Tích hợp Slack/Teams",
    description:
      "Hỏi DataFinch ngay trong workspace của team. Schedule alerts tự động khi metric vượt threshold.",
    bullet: ["Slack bot", "Email digest", "Webhook + Zapier"],
  },
  {
    icon: "🧠",
    color: "var(--color-blue)",
    title: "Self-improving",
    description:
      "Mỗi 👍 / 👎 của user được học vào hệ thống. Tool càng dùng càng đúng — đặc biệt với jargon và metric riêng của doanh nghiệp bạn.",
    bullet: ["Feedback loop", "Custom metrics layer", "Synonym dictionary"],
  },
];

export function Features() {
  return (
    <section className="py-24 md:py-32 bg-[color:var(--color-bg-muted)]" id="features">
      <div className="mx-auto max-w-7xl px-6">
        <div className="text-center max-w-3xl mx-auto mb-16">
          <p className="text-xs uppercase tracking-wider font-bold text-[color:var(--color-primary)] mb-3">
            Tính năng
          </p>
          <h2 className="text-3xl md:text-5xl font-bold tracking-tight text-[color:var(--color-text)] mb-4">
            Mọi thứ bạn cần. Không thêm. Không bớt.
          </h2>
          <p className="text-lg text-[color:var(--color-text-muted)]">
            Built for VN businesses từ ngày đầu, không phải US tool dịch sang.
          </p>
        </div>

        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-5">
          {FEATURES.map((feat, i) => (
            <Card key={i} hover>
              <div
                className="w-12 h-12 rounded-xl flex items-center justify-center text-2xl mb-4"
                style={{
                  background: `${feat.color}15`,
                  border: `1px solid ${feat.color}30`,
                }}
              >
                {feat.icon}
              </div>
              <h3 className="text-lg font-bold text-[color:var(--color-text)] mb-2">
                {feat.title}
              </h3>
              <p className="text-sm text-[color:var(--color-text-muted)] leading-relaxed mb-4">
                {feat.description}
              </p>
              <ul className="space-y-1.5">
                {feat.bullet.map((b, j) => (
                  <li
                    key={j}
                    className="flex items-center gap-2 text-xs text-[color:var(--color-text-muted)]"
                  >
                    <span style={{ color: feat.color }}>✓</span>
                    {b}
                  </li>
                ))}
              </ul>
            </Card>
          ))}
        </div>
      </div>
    </section>
  );
}
