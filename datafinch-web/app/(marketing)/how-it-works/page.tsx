import { AgentDiagram } from "@/components/AgentDiagram";
import { Button } from "@/components/ui/Button";
import { Card } from "@/components/ui/Card";

export default function HowItWorksPage() {
  return (
    <>
      {/* Hero */}
      <section className="pt-20 pb-16 md:pt-28 md:pb-20 text-center relative overflow-hidden">
        <div
          className="absolute inset-0 -z-10"
          style={{
            background:
              "radial-gradient(ellipse 60% 50% at 50% 0%, rgba(8,145,178,0.1), transparent 70%)",
          }}
        />
        <div className="mx-auto max-w-3xl px-6">
          <p className="text-xs uppercase tracking-wider font-bold text-[color:var(--color-primary)] mb-3">
            Kiến trúc
          </p>
          <h1 className="text-4xl md:text-6xl font-bold tracking-tight text-[color:var(--color-text)] mb-4">
            Cách DataFinch{" "}
            <span className="bg-gradient-to-r from-[color:var(--color-primary)] to-[color:var(--color-purple)] bg-clip-text text-transparent">
              hoạt động
            </span>
          </h1>
          <p className="text-lg md:text-xl text-[color:var(--color-text-muted)]">
            Inspired by Uber Finch. 5 AI agents phối hợp để biến câu hỏi tiếng Việt thành SQL chính
            xác — chạy trên database của bạn, không qua server thứ 3.
          </p>
        </div>
      </section>

      {/* Interactive agent diagram */}
      <section className="pb-20">
        <AgentDiagram />
      </section>

      {/* Tech stack */}
      <section className="py-20 bg-[color:var(--color-bg-muted)] border-y border-[color:var(--color-border)]">
        <div className="mx-auto max-w-6xl px-6">
          <div className="text-center mb-12 max-w-2xl mx-auto">
            <h2 className="text-3xl md:text-4xl font-bold text-[color:var(--color-text)] mb-3">
              Tech stack — Open & extensible
            </h2>
            <p className="text-[color:var(--color-text-muted)]">
              Không lock-in vendor. Swap LLM provider, đổi database, self-host bất kỳ lúc nào.
            </p>
          </div>

          <div className="grid md:grid-cols-3 gap-5">
            {[
              {
                title: "LLM Providers",
                items: [
                  { name: "Anthropic Claude", desc: "Opus, Sonnet, Haiku 4.5+" },
                  { name: "OpenAI", desc: "GPT-5, GPT-5-mini, o-series" },
                  { name: "Google Gemini", desc: "2.5 Flash / Pro" },
                  { name: "Self-hosted (Enterprise)", desc: "Ollama, vLLM" },
                ],
                color: "var(--color-primary)",
              },
              {
                title: "Databases",
                items: [
                  { name: "PostgreSQL, MySQL", desc: "OLTP" },
                  { name: "BigQuery, Snowflake", desc: "Cloud DWH" },
                  { name: "Hive, Spark SQL", desc: "Big data" },
                  { name: "DuckDB, MongoDB", desc: "Sắp ra" },
                ],
                color: "var(--color-purple)",
              },
              {
                title: "Retrieval Stack",
                items: [
                  { name: "OpenSearch", desc: "Hybrid kNN + BM25" },
                  { name: "Sentence Transformers", desc: "768-d multilingual" },
                  { name: "FastAPI", desc: "Async streaming SSE" },
                  { name: "Airflow", desc: "Schedule + alerts" },
                ],
                color: "var(--color-green)",
              },
            ].map((group) => (
              <Card key={group.title} className="bg-white">
                <h3
                  className="text-lg font-bold mb-4 pb-3 border-b border-[color:var(--color-border)]"
                  style={{ color: group.color }}
                >
                  {group.title}
                </h3>
                <ul className="space-y-3">
                  {group.items.map((item) => (
                    <li key={item.name} className="flex items-start gap-3">
                      <span className="text-xs mt-1.5" style={{ color: group.color }}>
                        ●
                      </span>
                      <div>
                        <div className="font-semibold text-sm text-[color:var(--color-text)]">
                          {item.name}
                        </div>
                        <div className="text-xs text-[color:var(--color-text-subtle)]">
                          {item.desc}
                        </div>
                      </div>
                    </li>
                  ))}
                </ul>
              </Card>
            ))}
          </div>
        </div>
      </section>

      {/* Security & Privacy */}
      <section className="py-20">
        <div className="mx-auto max-w-5xl px-6">
          <div className="text-center mb-12 max-w-2xl mx-auto">
            <p className="text-xs uppercase tracking-wider font-bold text-[color:var(--color-green)] mb-3">
              An toàn dữ liệu
            </p>
            <h2 className="text-3xl md:text-4xl font-bold text-[color:var(--color-text)] mb-3">
              Database của bạn{" "}
              <span className="text-[color:var(--color-green)]">không bao giờ rời khỏi bạn</span>
            </h2>
            <p className="text-[color:var(--color-text-muted)]">
              Chúng tôi gửi câu lệnh SQL đến DB của bạn, không kéo data về server. Mọi câu lệnh đều
              qua Guardrails.
            </p>
          </div>

          <div className="grid md:grid-cols-2 gap-5">
            {[
              {
                icon: "🔒",
                title: "Read-only by default",
                desc: "DataFinch khuyến nghị (và Enterprise enforce) việc kết nối bằng read-only DB user. Guardrails layer cũng tự chặn DELETE/UPDATE/DROP/TRUNCATE 100% — không có cách nào bypass.",
              },
              {
                icon: "🌐",
                title: "VPN / SSH tunnel / VPC peering",
                desc: "Database nội bộ không expose ra internet? Setup tunnel/connector trong VPC của bạn. DataFinch agent (Go binary < 20MB) connect outbound đến cloud — không cần mở port firewall.",
              },
              {
                icon: "🔐",
                title: "PII auto-masking",
                desc: "Columns được mark là PII (email, phone, ID) sẽ tự động hash hoặc redact trong response. Admin có thể override per-role qua RBAC settings.",
              },
              {
                icon: "📋",
                title: "Audit log đầy đủ",
                desc: "Mỗi câu hỏi: ai hỏi (user_id) — hỏi gì (NL question) — generate SQL nào — chạy ở đâu — bao nhiêu rows trả về — thumbs up/down. Export cho SOC2 / ISO27001 audits.",
              },
              {
                icon: "🏢",
                title: "Self-hosted on-prem",
                desc: "Enterprise plan: deploy toàn bộ DataFinch trong VPC/on-prem của bạn qua Helm chart. Air-gapped option với LLM tự host (Ollama) — không gọi ra ngoài internet.",
              },
              {
                icon: "🛡️",
                title: "Compliance ready",
                desc: "Đang hoàn thiện SOC2 Type 1. Roadmap: ISO27001, GDPR, HIPAA mode (cho healthcare vertical). Vietnam: tuân thủ Nghị định 13/2023/NĐ-CP về bảo vệ dữ liệu cá nhân.",
              },
            ].map((item) => (
              <Card key={item.title} hover>
                <div className="flex items-start gap-4">
                  <div className="text-3xl flex-shrink-0">{item.icon}</div>
                  <div>
                    <h3 className="font-bold text-[color:var(--color-text)] mb-2">{item.title}</h3>
                    <p className="text-sm text-[color:var(--color-text-muted)] leading-relaxed">
                      {item.desc}
                    </p>
                  </div>
                </div>
              </Card>
            ))}
          </div>
        </div>
      </section>

      {/* End-to-end flow */}
      <section className="py-20 bg-[color:var(--color-bg-muted)]">
        <div className="mx-auto max-w-5xl px-6">
          <div className="text-center mb-12">
            <h2 className="text-3xl md:text-4xl font-bold text-[color:var(--color-text)] mb-3">
              End-to-end trong &lt; 10 giây
            </h2>
            <p className="text-[color:var(--color-text-muted)]">
              Timeline thực tế từ lúc bạn gõ câu hỏi đến khi có kết quả.
            </p>
          </div>

          <div className="rounded-2xl border border-[color:var(--color-border)] bg-white p-6 md:p-8 font-mono text-sm overflow-x-auto">
            <div className="space-y-1.5 min-w-[600px]">
              {[
                ["00:00.0", "var(--color-text-subtle)", "User submit", '"Top 5 khách hàng đặt nhiều đơn nhất Q1?"'],
                ["00:00.2", "var(--color-primary)", "→ Supervisor", "intent=DATA_QUERY · backend=hive_gold · confidence=0.95"],
                ["00:00.5", "var(--color-purple)", "→ Retriever", "8 catalog hits · 0 docs · 2 similar past queries"],
                ["00:00.6", "var(--color-purple)", "→ Schema augment", "Full columns of dim_customers, fact_sales loaded"],
                ["00:02.8", "var(--color-green)", "→ SQL Writer", "Generated 234-token SQL · complexity=medium · 2 tables"],
                ["00:02.9", "var(--color-orange)", "→ Guardrails", "✓ Read-only · ✓ 2 JOINs · ✓ LIMIT 100 · No PII exposed"],
                ["00:03.0", "var(--color-pink)", "→ Executor", "Hive thrift execute…"],
                ["00:08.4", "var(--color-pink)", "← Result", "5 rows × 6 cols · 5.4s exec time"],
                ["00:08.5", "var(--color-text-subtle)", "Stream to UI", "SSE events + chart spec generated"],
                ["00:08.6", "var(--color-green)", "✓ Done", "Total: 8.6s · Visible to user since 0.2s"],
              ].map(([time, color, label, detail], i) => (
                <div key={i} className="flex items-center gap-3">
                  <span className="text-[color:var(--color-text-subtle)] tabular-nums w-16">
                    {time}
                  </span>
                  <span className="font-bold w-32" style={{ color: color as string }}>
                    {label}
                  </span>
                  <span className="text-[color:var(--color-text-muted)]">{detail}</span>
                </div>
              ))}
            </div>
          </div>

          <p className="text-center text-xs text-[color:var(--color-text-subtle)] mt-6">
            ↑ Log thực tế từ production. 70% thời gian là Hive thrift execute (varies theo data
            size). LLM agents chỉ tốn ~3 giây.
          </p>
        </div>
      </section>

      {/* CTA */}
      <section className="py-20 border-t border-[color:var(--color-border)]">
        <div className="mx-auto max-w-3xl px-6 text-center">
          <h2 className="text-3xl md:text-4xl font-bold text-[color:var(--color-text)] mb-4">
            Sẵn sàng trải nghiệm?
          </h2>
          <p className="text-lg text-[color:var(--color-text-muted)] mb-8">
            Setup 5 phút. Không cần thẻ tín dụng. Tự đánh giá xem có đúng cho team bạn không.
          </p>
          <div className="flex flex-col sm:flex-row gap-3 justify-center">
            <Button href="/signup" size="lg">
              Dùng thử miễn phí →
            </Button>
            <Button href="/pricing" variant="outline" size="lg">
              Xem bảng giá
            </Button>
          </div>
        </div>
      </section>
    </>
  );
}
