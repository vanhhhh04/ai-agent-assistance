"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/Button";
import { Card } from "@/components/ui/Card";
import { cn } from "@/lib/utils";
import { setUser } from "@/lib/auth";

type Step = 0 | 1 | 2 | 3 | 4;

const STEPS = [
  { id: 0, label: "Welcome" },
  { id: 1, label: "Database" },
  { id: 2, label: "Kết nối" },
  { id: 3, label: "Index" },
  { id: 4, label: "Hỏi" },
];

const DB_OPTIONS = [
  { id: "postgres", icon: "🐘", name: "PostgreSQL", desc: "9.6 trở lên" },
  { id: "mysql", icon: "🐬", name: "MySQL / MariaDB", desc: "5.7+" },
  { id: "snowflake", icon: "❄️", name: "Snowflake", desc: "Cloud DWH" },
  { id: "bigquery", icon: "🟢", name: "BigQuery", desc: "Google Cloud" },
  { id: "hive", icon: "🏛", name: "Hive / Spark", desc: "Hadoop stack" },
  { id: "duckdb", icon: "🦆", name: "DuckDB", desc: "Local files" },
];

const INDEX_PROGRESS = [
  { ms: 0, text: "Đang kết nối database…", done: false },
  { ms: 800, text: "Đã tìm thấy 24 bảng", done: true },
  { ms: 1800, text: "Đang scan schema chi tiết…", done: false },
  { ms: 3200, text: "Đã đọc 156 columns + 12 indexes", done: true },
  { ms: 4500, text: "AI đang tạo mô tả cho từng bảng…", done: false },
  { ms: 7000, text: "Đã generate descriptions (24 tables, 156 columns)", done: true },
  { ms: 8000, text: "Đang index vào finch_catalog…", done: false },
  { ms: 9500, text: "Index complete — sẵn sàng truy vấn ✓", done: true },
];

export default function SignupPage() {
  const router = useRouter();
  const [step, setStep] = useState<Step>(0);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [selectedDb, setSelectedDb] = useState("");
  const [creds, setCreds] = useState({ host: "", port: "5432", db: "", user: "", pass: "" });
  const [readOnly, setReadOnly] = useState(true);
  const [indexedSteps, setIndexedSteps] = useState(0);

  const enterApp = () => {
    // Mock: save the signup info as the logged-in user and redirect to /app
    setUser({
      username: email.split("@")[0] || "user",
      name: name || "New User",
      email: email || "user@example.com",
      plan: "free",
    });
    router.push("/app/ask");
  };

  // Auto-progress through indexing animation in step 3
  useEffect(() => {
    if (step !== 3) return;
    setIndexedSteps(0);
    const timers = INDEX_PROGRESS.map((s, i) =>
      setTimeout(() => setIndexedSteps(i + 1), s.ms)
    );
    return () => timers.forEach(clearTimeout);
  }, [step]);

  const next = () => setStep((s) => Math.min(4, s + 1) as Step);
  const back = () => setStep((s) => Math.max(0, s - 1) as Step);

  return (
    <Card className="w-full max-w-2xl p-0 overflow-hidden shadow-xl">
      {/* Progress bar */}
      <div className="px-8 pt-6 pb-4 border-b border-[color:var(--color-border)] bg-[color:var(--color-bg-muted)]">
        <div className="flex items-center justify-between mb-3">
          <p className="text-xs uppercase tracking-wider font-bold text-[color:var(--color-text-muted)]">
            Bước {step + 1} / {STEPS.length}
          </p>
          <span className="text-xs text-[color:var(--color-text-subtle)]">
            ~{(STEPS.length - step - 1) * 30 + 30}s còn lại
          </span>
        </div>
        <div className="flex gap-1.5">
          {STEPS.map((s) => (
            <div
              key={s.id}
              className={cn(
                "h-1.5 flex-1 rounded-full transition-all",
                s.id < step && "bg-[color:var(--color-primary)]",
                s.id === step && "bg-[color:var(--color-primary)] animate-pulse",
                s.id > step && "bg-[color:var(--color-border)]"
              )}
            />
          ))}
        </div>
        <div className="hidden sm:flex justify-between mt-2">
          {STEPS.map((s) => (
            <span
              key={s.id}
              className={cn(
                "text-xs font-medium",
                s.id <= step
                  ? "text-[color:var(--color-primary)]"
                  : "text-[color:var(--color-text-subtle)]"
              )}
            >
              {s.label}
            </span>
          ))}
        </div>
      </div>

      <div className="p-8 md:p-10 min-h-[420px] flex flex-col">
        {/* STEP 0 — Welcome */}
        {step === 0 && (
          <div className="flex-1 flex flex-col items-center justify-center text-center animate-fade-up">
            <div className="text-5xl mb-5">👋</div>
            <h1 className="text-3xl font-bold text-[color:var(--color-text)] mb-3">
              Chào mừng đến DataFinch!
            </h1>
            <p className="text-[color:var(--color-text-muted)] mb-8 max-w-md">
              3 phút để bạn có thể hỏi dữ liệu bằng tiếng Việt. Bắt đầu với thông tin cơ bản.
            </p>
            <div className="w-full max-w-sm space-y-4">
              <div className="text-left">
                <label className="block text-sm font-medium text-[color:var(--color-text)] mb-1.5">
                  Họ tên
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="Cao Việt Anh"
                  className="w-full h-11 px-3.5 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30"
                />
              </div>
              <div className="text-left">
                <label className="block text-sm font-medium text-[color:var(--color-text)] mb-1.5">
                  Email công việc
                </label>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="you@company.vn"
                  className="w-full h-11 px-3.5 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30"
                />
              </div>
            </div>
          </div>
        )}

        {/* STEP 1 — Database selection */}
        {step === 1 && (
          <div className="flex-1 animate-fade-up">
            <h2 className="text-2xl font-bold text-[color:var(--color-text)] mb-2">
              Bạn dùng database nào?
            </h2>
            <p className="text-sm text-[color:var(--color-text-muted)] mb-6">
              DataFinch hỗ trợ các database phổ biến nhất. Chọn 1 để bắt đầu — bạn có thể thêm
              sources khác sau.
            </p>
            <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
              {DB_OPTIONS.map((db) => (
                <button
                  key={db.id}
                  onClick={() => setSelectedDb(db.id)}
                  className={cn(
                    "p-4 rounded-xl border-2 text-left transition-all",
                    selectedDb === db.id
                      ? "border-[color:var(--color-primary)] bg-[color:var(--color-primary-faded)] shadow-sm"
                      : "border-[color:var(--color-border)] bg-white hover:border-[color:var(--color-border-strong)]"
                  )}
                >
                  <div className="text-3xl mb-2">{db.icon}</div>
                  <div className="font-semibold text-sm text-[color:var(--color-text)]">
                    {db.name}
                  </div>
                  <div className="text-xs text-[color:var(--color-text-subtle)]">{db.desc}</div>
                </button>
              ))}
            </div>
            <div className="mt-5 p-3 rounded-lg bg-[color:var(--color-bg-muted)] border border-[color:var(--color-border)] text-center">
              <p className="text-xs text-[color:var(--color-text-muted)]">
                Chưa muốn connect DB?{" "}
                <button
                  onClick={() => {
                    setSelectedDb("demo");
                    next();
                  }}
                  className="text-[color:var(--color-primary)] font-medium hover:underline"
                >
                  Dùng demo data →
                </button>
              </p>
            </div>
          </div>
        )}

        {/* STEP 2 — Credentials */}
        {step === 2 && (
          <div className="flex-1 animate-fade-up">
            <h2 className="text-2xl font-bold text-[color:var(--color-text)] mb-2">
              Kết nối {DB_OPTIONS.find((d) => d.id === selectedDb)?.name || "database"}
            </h2>
            <p className="text-sm text-[color:var(--color-text-muted)] mb-6">
              Credentials chỉ được lưu encrypted. Khuyến nghị tạo user read-only riêng.
            </p>
            <div className="space-y-3">
              <div className="grid grid-cols-3 gap-3">
                <div className="col-span-2">
                  <label className="block text-xs font-medium text-[color:var(--color-text)] mb-1">
                    Host
                  </label>
                  <input
                    type="text"
                    value={creds.host}
                    onChange={(e) => setCreds({ ...creds, host: e.target.value })}
                    placeholder="db.company.com"
                    className="w-full h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-[color:var(--color-text)] mb-1">
                    Port
                  </label>
                  <input
                    type="text"
                    value={creds.port}
                    onChange={(e) => setCreds({ ...creds, port: e.target.value })}
                    className="w-full h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30"
                  />
                </div>
              </div>
              <div>
                <label className="block text-xs font-medium text-[color:var(--color-text)] mb-1">
                  Database name
                </label>
                <input
                  type="text"
                  value={creds.db}
                  onChange={(e) => setCreds({ ...creds, db: e.target.value })}
                  placeholder="production"
                  className="w-full h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30"
                />
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs font-medium text-[color:var(--color-text)] mb-1">
                    User
                  </label>
                  <input
                    type="text"
                    value={creds.user}
                    onChange={(e) => setCreds({ ...creds, user: e.target.value })}
                    placeholder="readonly_user"
                    className="w-full h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-[color:var(--color-text)] mb-1">
                    Password
                  </label>
                  <input
                    type="password"
                    value={creds.pass}
                    onChange={(e) => setCreds({ ...creds, pass: e.target.value })}
                    placeholder="••••••••"
                    className="w-full h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30"
                  />
                </div>
              </div>

              <label className="flex items-start gap-3 pt-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={readOnly}
                  onChange={(e) => setReadOnly(e.target.checked)}
                  className="mt-0.5 w-4 h-4 rounded border-[color:var(--color-border-strong)]"
                />
                <span className="text-sm">
                  <span className="font-medium text-[color:var(--color-text)]">
                    Chỉ quyền đọc (khuyến nghị)
                  </span>
                  <span className="block text-xs text-[color:var(--color-text-subtle)]">
                    Guardrails sẽ tự chặn DELETE/UPDATE, nhưng read-only user là defense-in-depth
                  </span>
                </span>
              </label>

              <div className="p-3 rounded-lg bg-[color:var(--color-primary-faded)] border border-[color:var(--color-primary-subtle)] text-xs text-[color:var(--color-text-muted)] flex gap-2">
                <span>🔒</span>
                <span>
                  DB không expose internet?{" "}
                  <a href="#" className="text-[color:var(--color-primary)] font-medium hover:underline">
                    Cài DataFinch connector trong VPC →
                  </a>
                </span>
              </div>
            </div>
          </div>
        )}

        {/* STEP 3 — Auto-index */}
        {step === 3 && (
          <div className="flex-1 flex flex-col items-center justify-center animate-fade-up">
            <div className="text-5xl mb-5">⚡</div>
            <h2 className="text-2xl font-bold text-[color:var(--color-text)] mb-2 text-center">
              Đang quét schema của bạn…
            </h2>
            <p className="text-sm text-[color:var(--color-text-muted)] mb-8 text-center">
              AI đang đọc và tạo mô tả cho từng bảng. Khoảng 1-2 phút cho 100 bảng.
            </p>

            <div className="w-full max-w-md space-y-2.5">
              {INDEX_PROGRESS.slice(0, indexedSteps).map((s, i) => (
                <div
                  key={i}
                  className="flex items-center gap-3 text-sm animate-fade-up"
                >
                  {s.done ? (
                    <span className="text-[color:var(--color-green)] flex-shrink-0">✓</span>
                  ) : (
                    <span
                      className="w-4 h-4 rounded-full border-2 border-[color:var(--color-primary)] border-t-transparent animate-spin flex-shrink-0"
                      style={{ animationDuration: "0.8s" }}
                    />
                  )}
                  <span
                    className={cn(
                      s.done
                        ? "text-[color:var(--color-text)]"
                        : "text-[color:var(--color-text-muted)]"
                    )}
                  >
                    {s.text}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* STEP 4 — First query */}
        {step === 4 && (
          <div className="flex-1 flex flex-col items-center justify-center animate-fade-up">
            <div className="text-5xl mb-5">🎉</div>
            <h2 className="text-2xl font-bold text-[color:var(--color-text)] mb-2 text-center">
              Sẵn sàng! Hãy hỏi câu đầu tiên
            </h2>
            <p className="text-sm text-[color:var(--color-text-muted)] mb-6 text-center max-w-md">
              Dựa trên schema của bạn, đây là vài câu hỏi gợi ý. Click 1 câu hoặc tự nhập.
            </p>

            <div className="w-full max-w-md space-y-2 mb-6">
              {[
                "Có bao nhiêu khách hàng hiện tại?",
                "Top 10 sản phẩm bán chạy tháng này",
                "Đơn hàng nào đang trễ giao quá 3 ngày?",
                "Doanh thu Q1 2026 theo brand",
              ].map((q) => (
                <button
                  key={q}
                  className="w-full text-left px-4 py-3 rounded-lg border border-[color:var(--color-border)] bg-white hover:border-[color:var(--color-primary)] hover:bg-[color:var(--color-primary-faded)] transition-all text-sm text-[color:var(--color-text)]"
                >
                  <span className="text-[color:var(--color-text-subtle)] mr-2">→</span>
                  {q}
                </button>
              ))}
            </div>

            <Button onClick={enterApp} size="lg" className="w-full max-w-sm">
              Vào ứng dụng →
            </Button>
          </div>
        )}

        {/* Navigation */}
        {step !== 4 && (
          <div className="mt-8 pt-5 border-t border-[color:var(--color-border)] flex items-center justify-between">
            {step > 0 ? (
              <button
                onClick={back}
                className="text-sm font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)]"
              >
                ← Quay lại
              </button>
            ) : (
              <span />
            )}
            <Button
              onClick={next}
              disabled={
                (step === 0 && (!name || !email)) ||
                (step === 1 && !selectedDb) ||
                (step === 2 && selectedDb !== "demo" && (!creds.host || !creds.user || !creds.pass)) ||
                (step === 3 && indexedSteps < INDEX_PROGRESS.length)
              }
              size="md"
            >
              {step === 2 ? "Kết nối & tiếp tục" : step === 3 ? "Tiếp tục" : "Tiếp tục"} →
            </Button>
          </div>
        )}
      </div>

      {/* Footer */}
      {step === 0 && (
        <div className="px-8 py-4 border-t border-[color:var(--color-border)] bg-[color:var(--color-bg-muted)] text-center text-xs text-[color:var(--color-text-subtle)]">
          Đã có tài khoản?{" "}
          <Link
            href="/login"
            className="font-medium text-[color:var(--color-primary)] hover:underline"
          >
            Đăng nhập
          </Link>
        </div>
      )}
    </Card>
  );
}
