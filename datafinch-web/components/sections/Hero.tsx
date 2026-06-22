"use client";

import { useEffect, useState } from "react";
import { Button } from "../ui/Button";

const DEMO_QUESTIONS = [
  "Top 5 sản phẩm bán chạy tháng này?",
  "Doanh thu Q1 2026 so với Q4 2025?",
  "Khách hàng nào mua nhiều nhất tuần này?",
  "Đơn hàng nào đang trễ giao quá 3 ngày?",
];

export function Hero() {
  const [questionIdx, setQuestionIdx] = useState(0);
  const [typed, setTyped] = useState("");

  // Typewriter effect for demo questions
  useEffect(() => {
    const q = DEMO_QUESTIONS[questionIdx];
    let i = 0;
    setTyped("");
    const interval = setInterval(() => {
      if (i <= q.length) {
        setTyped(q.slice(0, i));
        i++;
      } else {
        clearInterval(interval);
        setTimeout(() => {
          setQuestionIdx((idx) => (idx + 1) % DEMO_QUESTIONS.length);
        }, 2200);
      }
    }, 45);
    return () => clearInterval(interval);
  }, [questionIdx]);

  return (
    <section className="relative overflow-hidden">
      {/* Background gradient */}
      <div
        className="absolute inset-0 -z-10"
        style={{
          background:
            "radial-gradient(ellipse 80% 50% at 50% -20%, rgba(8,145,178,0.12), transparent 70%)",
        }}
      />

      <div className="mx-auto max-w-7xl px-6 pt-20 pb-24 md:pt-28 md:pb-32">
        {/* Badge */}
        <div className="flex justify-center mb-8 animate-fade-up" style={{ animationDelay: "0ms" }}>
          <span className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-medium bg-[color:var(--color-primary-faded)] text-[color:var(--color-primary)] border border-[color:var(--color-primary-subtle)]">
            <span className="w-1.5 h-1.5 rounded-full bg-[color:var(--color-primary)] animate-pulse" />
            Hỗ trợ tiếng Việt · GPT-5 · Claude · Gemini
          </span>
        </div>

        {/* Headline */}
        <h1
          className="text-center text-4xl md:text-6xl lg:text-7xl font-bold tracking-tight text-[color:var(--color-text)] max-w-4xl mx-auto leading-[1.1] animate-fade-up"
          style={{ animationDelay: "100ms" }}
        >
          Hỏi dữ liệu{" "}
          <span className="relative inline-block">
            <span className="bg-gradient-to-r from-[color:var(--color-primary)] to-[color:var(--color-purple)] bg-clip-text text-transparent">
              như hỏi người
            </span>
          </span>
          .<br />
          Trả lời chính xác{" "}
          <span className="text-[color:var(--color-text-muted)]">như chuyên gia.</span>
        </h1>

        {/* Subhead */}
        <p
          className="mt-6 text-center text-lg md:text-xl text-[color:var(--color-text-muted)] max-w-2xl mx-auto animate-fade-up"
          style={{ animationDelay: "200ms" }}
        >
          AI Data Analyst cho doanh nghiệp Việt. Hỏi bằng tiếng Việt, có ngay câu trả lời từ
          database của bạn. <strong className="text-[color:var(--color-text)]">Không cần biết SQL.</strong>
        </p>

        {/* CTAs */}
        <div
          className="mt-8 flex flex-col sm:flex-row gap-3 justify-center animate-fade-up"
          style={{ animationDelay: "300ms" }}
        >
          <Button href="/signup" size="lg">
            Dùng thử miễn phí
            <span aria-hidden>→</span>
          </Button>
          <Button href="/#demo" variant="outline" size="lg">
            <span aria-hidden>▶</span>
            Xem demo 90 giây
          </Button>
        </div>

        <p
          className="mt-4 text-center text-xs text-[color:var(--color-text-subtle)] animate-fade-up"
          style={{ animationDelay: "400ms" }}
        >
          Không cần thẻ tín dụng · Setup trong 5 phút · Cancel bất kỳ lúc nào
        </p>

        {/* Interactive demo widget */}
        <div
          className="mt-16 mx-auto max-w-3xl animate-fade-up"
          style={{ animationDelay: "500ms" }}
          id="demo"
        >
          <div className="rounded-2xl border border-[color:var(--color-border)] bg-white shadow-2xl shadow-cyan-500/5 overflow-hidden">
            {/* Browser chrome */}
            <div className="px-4 py-2.5 border-b border-[color:var(--color-border)] bg-[color:var(--color-bg-muted)] flex items-center gap-2">
              <div className="flex gap-1.5">
                <span className="w-3 h-3 rounded-full bg-red-400" />
                <span className="w-3 h-3 rounded-full bg-yellow-400" />
                <span className="w-3 h-3 rounded-full bg-green-400" />
              </div>
              <div className="flex-1 text-center text-xs text-[color:var(--color-text-subtle)] font-mono">
                datafinch.app/ask
              </div>
            </div>

            {/* Chat content */}
            <div className="p-6 md:p-8 min-h-[280px]">
              {/* User question */}
              <div className="flex justify-end mb-4">
                <div className="max-w-[80%] px-4 py-2.5 rounded-2xl rounded-br-md bg-[color:var(--color-primary)] text-white text-sm md:text-base font-medium shadow-sm shadow-cyan-500/20">
                  {typed}
                  <span className="inline-block w-0.5 h-4 bg-white/80 ml-0.5 animate-pulse align-middle" />
                </div>
              </div>

              {/* Agent response */}
              <div className="flex items-start gap-3">
                <div className="w-8 h-8 rounded-full bg-gradient-to-br from-[color:var(--color-primary)] to-[color:var(--color-purple)] flex items-center justify-center text-white text-sm font-bold flex-shrink-0">
                  ◈
                </div>
                <div className="flex-1 space-y-3">
                  <div className="flex items-center gap-2 text-xs text-[color:var(--color-text-subtle)]">
                    <span className="inline-flex items-center gap-1.5">
                      <span className="w-1.5 h-1.5 rounded-full bg-[color:var(--color-green)] animate-pulse" />
                      Đang xử lý
                    </span>
                    <span>·</span>
                    <span className="font-mono">supervisor → metadata → sql_writer</span>
                  </div>
                  <div className="p-3 rounded-lg bg-[color:var(--color-bg-muted)] border border-[color:var(--color-border)]">
                    <div className="text-xs font-mono text-[color:var(--color-green)] whitespace-pre">
                      {`SELECT product_name, SUM(quantity) AS sold
FROM gold.fact_sales
WHERE order_month = 5 AND order_year = 2026
GROUP BY product_name
ORDER BY sold DESC LIMIT 5;`}
                    </div>
                  </div>
                  <div className="text-sm text-[color:var(--color-text-muted)]">
                    Tìm top 5 sản phẩm có số lượng bán cao nhất tháng này...
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Floating "live" indicator */}
          <p className="mt-4 text-center text-xs text-[color:var(--color-text-subtle)]">
            ↑ Demo trực tiếp · Câu hỏi và SQL tự động đổi mỗi vài giây
          </p>
        </div>

        {/* Social proof */}
        <div className="mt-20 animate-fade-up" style={{ animationDelay: "700ms" }}>
          <p className="text-center text-xs uppercase tracking-wider font-semibold text-[color:var(--color-text-subtle)] mb-6">
            Tin dùng bởi các doanh nghiệp
          </p>
          <div className="flex flex-wrap justify-center items-center gap-x-12 gap-y-6 opacity-60">
            {["TechVN", "ShopVN", "LogiCorp", "FoodChain", "FinTech.vn"].map((logo) => (
              <span
                key={logo}
                className="font-mono text-lg font-bold text-[color:var(--color-text-muted)]"
              >
                {logo}
              </span>
            ))}
          </div>
        </div>
      </div>
    </section>
  );
}
