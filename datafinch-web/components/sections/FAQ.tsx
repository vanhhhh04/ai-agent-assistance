"use client";

import { useState } from "react";
import { cn } from "@/lib/utils";

const FAQS = [
  {
    q: "Tôi có cần biết SQL để dùng DataFinch không?",
    a: "Không. Đó chính là lý do DataFinch tồn tại. Bạn chỉ cần hỏi bằng tiếng Việt tự nhiên: \"Doanh thu tháng này\", \"Top khách VIP\", \"Đơn nào trễ giao\". AI sẽ tự viết SQL, chạy trên database của bạn và trả về kết quả + biểu đồ.",
  },
  {
    q: "DataFinch có copy dữ liệu của tôi về server không?",
    a: "Không. Dữ liệu của bạn KHÔNG bao giờ rời khỏi database của bạn. DataFinch chỉ gửi câu lệnh SQL (đã được Guardrails kiểm tra là read-only) đến DB của bạn, kết quả trả về trực tiếp trình duyệt user. Cloud version có option VPN/SSH tunnel. Enterprise có option self-host trong VPC riêng.",
  },
  {
    q: "Hỗ trợ database nào?",
    a: "Hiện tại: PostgreSQL, MySQL, BigQuery, Snowflake, Hive, Spark SQL. Sắp ra: MongoDB, DuckDB, Elasticsearch, Google Sheets. Nếu DB của bạn chưa support, contact chúng tôi — connector mới chỉ mất 1-2 tuần.",
  },
  {
    q: "AI có viết SQL sai không? Có nguy hiểm không?",
    a: "Có thể sai (~5% câu phức tạp), NHƯNG: (1) Guardrails CHẶN 100% DELETE/UPDATE/DROP — không bao giờ phá data; (2) Mọi câu được audit log, bạn xem được SQL trước khi execute; (3) Có thumbs up/down để học cải thiện; (4) Connection mặc định read-only user.",
  },
  {
    q: "Tiếng Việt thực sự hiểu tốt đến đâu?",
    a: "DataFinch tối ưu cho domain VN từ ngày đầu — không phải US tool dịch sang. Hiểu được lóng (\"bán chạy\" = quantity, không phải revenue), format số VND (1.5tr / 200k), date format dd/mm/yyyy, và business terms phổ biến (đơn hàng, khách VIP, doanh thu, lợi nhuận, tồn kho...).",
  },
  {
    q: "Setup mất bao lâu?",
    a: "Free tier: 5 phút (kết nối DB → AI auto-index schema → hỏi câu đầu tiên). Enterprise on-prem: 1-2 ngày (setup Helm chart + SSO + initial training data team).",
  },
  {
    q: "Có cancel bất kỳ lúc nào không?",
    a: "Có. Free → trả phí: instant upgrade. Trả phí → free: cancel xong vẫn dùng đến hết chu kỳ thanh toán hiện tại, không gia hạn. Không phí cancel, không charge bất ngờ.",
  },
  {
    q: "Khác gì so với ChatGPT/Claude trực tiếp?",
    a: "ChatGPT viết SQL ngữ pháp đúng nhưng KHÔNG BIẾT schema của bạn — sẽ bịa tên bảng/cột. DataFinch index sẵn schema, chạy SQL thực tế, có guardrails, học theo feedback của team bạn. Đặc biệt, ChatGPT không thể kết nối DB nội bộ qua VPN.",
  },
];

export function FAQ() {
  const [openIdx, setOpenIdx] = useState<number | null>(0);

  return (
    <section className="py-24 md:py-32" id="faq">
      <div className="mx-auto max-w-3xl px-6">
        <div className="text-center mb-12">
          <p className="text-xs uppercase tracking-wider font-bold text-[color:var(--color-primary)] mb-3">
            FAQ
          </p>
          <h2 className="text-3xl md:text-5xl font-bold tracking-tight text-[color:var(--color-text)] mb-4">
            Câu hỏi thường gặp
          </h2>
          <p className="text-lg text-[color:var(--color-text-muted)]">
            Chưa thấy câu hỏi của bạn?{" "}
            <a
              href="mailto:hello@datafinch.app"
              className="text-[color:var(--color-primary)] hover:underline"
            >
              Gửi email
            </a>{" "}
            cho team.
          </p>
        </div>

        <div className="space-y-3">
          {FAQS.map((faq, i) => {
            const open = openIdx === i;
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
                  onClick={() => setOpenIdx(open ? null : i)}
                  className="w-full px-5 py-4 flex items-center justify-between text-left gap-4"
                >
                  <span className="font-semibold text-[color:var(--color-text)]">
                    {faq.q}
                  </span>
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
                  className={cn(
                    "overflow-hidden transition-all",
                    open ? "max-h-96" : "max-h-0"
                  )}
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
  );
}
