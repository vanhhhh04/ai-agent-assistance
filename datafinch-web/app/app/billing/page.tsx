"use client";

import { useEffect, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { AppHeader } from "@/components/app/AppHeader";
import { getUser, type User } from "@/lib/auth";
import { cn } from "@/lib/utils";

const USAGE_DATA = Array.from({ length: 30 }, (_, i) => ({
  day: `${i + 1}`,
  queries: Math.round(20 + 60 * Math.sin(i * 0.3) + Math.random() * 40),
}));

const totalQueries = USAGE_DATA.reduce((s, d) => s + d.queries, 0);
const quotaLimit = 10_000;
const usagePercent = (totalQueries / quotaLimit) * 100;

const INVOICES = [
  { id: "INV-2026-005", date: "01/05/2026", amount: 7_200_000, status: "paid",    plan: "Growth · Monthly" },
  { id: "INV-2026-004", date: "01/04/2026", amount: 7_200_000, status: "paid",    plan: "Growth · Monthly" },
  { id: "INV-2026-003", date: "01/03/2026", amount: 7_200_000, status: "paid",    plan: "Growth · Monthly" },
  { id: "INV-2026-002", date: "01/02/2026", amount: 1_200_000, status: "paid",    plan: "Starter · Monthly" },
  { id: "INV-2026-001", date: "01/01/2026", amount: 1_200_000, status: "paid",    plan: "Starter · Monthly" },
];

const PLAN_FEATURES: Record<string, string[]> = {
  free:       ["1 source", "100 q/m", "1 user", "Community support"],
  starter:    ["3 sources", "1k q/m", "5 users", "Email support", "Alerts"],
  growth:     ["∞ sources", "10k q/m", "∞ users", "REST API", "Priority support", "SLA 99.5%"],
  enterprise: ["∞", "∞", "∞", "SSO", "On-prem", "Dedicated CSM", "SLA 99.95%"],
};

function formatVnd(amount: number): string {
  return new Intl.NumberFormat("vi-VN").format(amount) + "₫";
}

export default function BillingPage() {
  const [user, setUser] = useState<User | null>(null);

  useEffect(() => {
    setUser(getUser());
  }, []);

  const plan = user?.plan ?? "free";
  const features = PLAN_FEATURES[plan];

  return (
    <div className="min-h-screen">
      <AppHeader
        title="Gói & thanh toán"
        subtitle="Quản lý plan, xem usage và lịch sử hoá đơn"
      />

      <div className="p-6 md:p-8 space-y-6 max-w-5xl">
        {/* Current plan */}
        <div className="rounded-xl bg-gradient-to-br from-[color:var(--color-primary)] to-[color:var(--color-purple)] text-white p-6 md:p-8 shadow-lg">
          <div className="flex items-start justify-between flex-wrap gap-4">
            <div>
              <p className="text-xs font-bold uppercase tracking-wider opacity-80 mb-2">
                Gói hiện tại
              </p>
              <div className="flex items-baseline gap-3 mb-3">
                <h2 className="text-3xl md:text-4xl font-bold capitalize">{plan}</h2>
                {plan === "growth" && (
                  <span className="text-lg font-semibold opacity-80">7.2tr ₫ / tháng</span>
                )}
                {plan === "starter" && (
                  <span className="text-lg font-semibold opacity-80">1.2tr ₫ / tháng</span>
                )}
                {plan === "free" && <span className="text-lg font-semibold opacity-80">Miễn phí</span>}
              </div>
              <div className="flex flex-wrap gap-2">
                {features.map((f) => (
                  <span
                    key={f}
                    className="text-xs px-2.5 py-1 rounded-md bg-white/20 backdrop-blur-sm font-medium"
                  >
                    ✓ {f}
                  </span>
                ))}
              </div>
            </div>
            <div className="flex flex-col gap-2">
              <a
                href="/pricing"
                className="h-10 px-5 rounded-lg bg-white text-[color:var(--color-primary)] text-sm font-semibold hover:bg-gray-100 inline-flex items-center justify-center"
              >
                {plan === "enterprise" ? "Quản lý plan" : "Nâng cấp →"}
              </a>
              {plan !== "free" && (
                <button className="h-10 px-5 rounded-lg border border-white/30 text-white text-sm font-medium hover:bg-white/10">
                  Hủy plan
                </button>
              )}
            </div>
          </div>
        </div>

        {/* Usage stats */}
        <div className="grid md:grid-cols-3 gap-4">
          <div className="md:col-span-2 rounded-xl bg-white border border-[color:var(--color-border)] p-5">
            <div className="flex items-start justify-between mb-4">
              <div>
                <h3 className="font-bold text-[color:var(--color-text)]">Queries tháng này</h3>
                <p className="text-xs text-[color:var(--color-text-subtle)] mt-0.5">
                  Reset vào 01/06/2026
                </p>
              </div>
              <select className="h-8 px-2.5 rounded-md border border-[color:var(--color-border-strong)] bg-white text-xs">
                <option>30 ngày qua</option>
                <option>7 ngày qua</option>
                <option>Tháng trước</option>
              </select>
            </div>

            {/* Usage bar */}
            <div className="mb-4">
              <div className="flex items-baseline justify-between mb-2">
                <span className="text-2xl font-bold text-[color:var(--color-text)]">
                  {totalQueries.toLocaleString()}
                </span>
                <span className="text-sm text-[color:var(--color-text-muted)]">
                  / {quotaLimit.toLocaleString()} queries
                </span>
              </div>
              <div className="h-2 rounded-full bg-[color:var(--color-bg-subtle)] overflow-hidden">
                <div
                  className={cn(
                    "h-full rounded-full transition-all",
                    usagePercent < 70
                      ? "bg-[color:var(--color-green)]"
                      : usagePercent < 90
                      ? "bg-[color:var(--color-orange)]"
                      : "bg-[color:var(--color-danger)]"
                  )}
                  style={{ width: `${Math.min(100, usagePercent)}%` }}
                />
              </div>
              <p className="text-xs text-[color:var(--color-text-subtle)] mt-1.5">
                {usagePercent.toFixed(1)}% đã dùng · ~{Math.round((quotaLimit - totalQueries) / (30 - 25))}{" "}
                queries/ngày để không vượt quota
              </p>
            </div>

            {/* Chart */}
            <ResponsiveContainer width="100%" height={140}>
              <AreaChart data={USAGE_DATA} margin={{ top: 5, right: 5, left: -10, bottom: 0 }}>
                <defs>
                  <linearGradient id="usageGradient" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="#0891b2" stopOpacity={0.3} />
                    <stop offset="100%" stopColor="#0891b2" stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="day" stroke="#94a3b8" fontSize={10} interval={4} />
                <YAxis stroke="#94a3b8" fontSize={10} />
                <Tooltip
                  contentStyle={{
                    background: "white",
                    border: "1px solid #e2e8f0",
                    borderRadius: 8,
                    fontSize: 12,
                  }}
                />
                <Area
                  type="monotone"
                  dataKey="queries"
                  stroke="#0891b2"
                  strokeWidth={2}
                  fill="url(#usageGradient)"
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>

          {/* Side stats */}
          <div className="space-y-3">
            <div className="rounded-xl bg-white border border-[color:var(--color-border)] p-4">
              <p className="text-xs font-semibold uppercase tracking-wider text-[color:var(--color-text-subtle)] mb-1">
                Active users
              </p>
              <p className="text-2xl font-bold text-[color:var(--color-text)]">5</p>
              <p className="text-xs text-[color:var(--color-text-subtle)] mt-1">
                ∞ trong gói Growth
              </p>
            </div>
            <div className="rounded-xl bg-white border border-[color:var(--color-border)] p-4">
              <p className="text-xs font-semibold uppercase tracking-wider text-[color:var(--color-text-subtle)] mb-1">
                Saved queries
              </p>
              <p className="text-2xl font-bold text-[color:var(--color-text)]">23</p>
              <p className="text-xs text-[color:var(--color-text-subtle)] mt-1">
                Unlimited
              </p>
            </div>
            <div className="rounded-xl bg-white border border-[color:var(--color-border)] p-4">
              <p className="text-xs font-semibold uppercase tracking-wider text-[color:var(--color-text-subtle)] mb-1">
                Data sources
              </p>
              <p className="text-2xl font-bold text-[color:var(--color-text)]">2</p>
              <p className="text-xs text-[color:var(--color-text-subtle)] mt-1">
                Unlimited
              </p>
            </div>
          </div>
        </div>

        {/* Payment method */}
        <div className="rounded-xl bg-white border border-[color:var(--color-border)] p-5">
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-bold text-[color:var(--color-text)]">Phương thức thanh toán</h3>
            <button className="text-xs font-medium text-[color:var(--color-primary)] hover:underline">
              + Thêm phương thức
            </button>
          </div>
          <div className="flex items-center gap-4 p-4 rounded-lg bg-[color:var(--color-bg-muted)] border border-[color:var(--color-border)]">
            <div className="w-12 h-8 rounded-md bg-gradient-to-br from-blue-600 to-blue-800 flex items-center justify-center text-white text-xs font-bold">
              VISA
            </div>
            <div className="flex-1">
              <p className="font-medium text-[color:var(--color-text)]">
                Visa ending in <span className="font-mono">•• 4242</span>
              </p>
              <p className="text-xs text-[color:var(--color-text-subtle)]">
                Hết hạn 12/2027 · Cao Việt Anh
              </p>
            </div>
            <span className="text-xs font-semibold text-[color:var(--color-green)] px-2 py-1 rounded-md bg-green-50">
              Mặc định
            </span>
            <button className="text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] px-2">
              ⋯
            </button>
          </div>
        </div>

        {/* Invoice history */}
        <div className="rounded-xl bg-white border border-[color:var(--color-border)] overflow-hidden">
          <div className="px-5 py-4 border-b border-[color:var(--color-border)] flex items-center justify-between">
            <h3 className="font-bold text-[color:var(--color-text)]">Lịch sử hoá đơn</h3>
            <button className="text-xs font-medium text-[color:var(--color-primary)] hover:underline">
              ⬇ Tải tất cả (PDF)
            </button>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-[color:var(--color-bg-muted)] border-b border-[color:var(--color-border)]">
                <tr>
                  <th className="text-left px-5 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">
                    Hoá đơn
                  </th>
                  <th className="text-left px-5 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">
                    Ngày
                  </th>
                  <th className="text-left px-5 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">
                    Plan
                  </th>
                  <th className="text-right px-5 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">
                    Số tiền
                  </th>
                  <th className="text-left px-5 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">
                    Status
                  </th>
                  <th className="w-12"></th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[color:var(--color-border)]">
                {INVOICES.map((inv) => (
                  <tr key={inv.id} className="hover:bg-[color:var(--color-bg-muted)]/50">
                    <td className="px-5 py-3 font-mono text-xs font-semibold text-[color:var(--color-text)]">
                      {inv.id}
                    </td>
                    <td className="px-5 py-3 text-[color:var(--color-text-muted)]">{inv.date}</td>
                    <td className="px-5 py-3 text-[color:var(--color-text-muted)]">{inv.plan}</td>
                    <td className="px-5 py-3 text-right font-mono font-semibold text-[color:var(--color-text)]">
                      {formatVnd(inv.amount)}
                    </td>
                    <td className="px-5 py-3">
                      <span className="text-xs font-semibold text-[color:var(--color-green)] px-2 py-1 rounded-md bg-green-50">
                        ✓ Đã thanh toán
                      </span>
                    </td>
                    <td className="px-3 py-3 text-right">
                      <button className="text-[color:var(--color-text-subtle)] hover:text-[color:var(--color-text)] px-1.5 py-1">
                        ⬇
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {/* Billing email */}
        <div className="rounded-xl bg-white border border-[color:var(--color-border)] p-5">
          <h3 className="font-bold text-[color:var(--color-text)] mb-1">
            Thông tin xuất hoá đơn
          </h3>
          <p className="text-xs text-[color:var(--color-text-subtle)] mb-4">
            Email + thông tin công ty hiển thị trên hoá đơn VAT
          </p>
          <div className="grid sm:grid-cols-2 gap-3">
            <div>
              <label className="block text-xs font-medium text-[color:var(--color-text)] mb-1">
                Email nhận hoá đơn
              </label>
              <input
                defaultValue="billing@datafinch.app"
                className="w-full h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm"
              />
            </div>
            <div>
              <label className="block text-xs font-medium text-[color:var(--color-text)] mb-1">
                Mã số thuế (MST)
              </label>
              <input
                placeholder="0123456789"
                className="w-full h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm"
              />
            </div>
            <div className="sm:col-span-2">
              <label className="block text-xs font-medium text-[color:var(--color-text)] mb-1">
                Tên công ty
              </label>
              <input
                placeholder="Công ty TNHH ABC"
                className="w-full h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm"
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
