"use client";

import { useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { cn } from "@/lib/utils";

const TABS = [
  { id: "sales",     icon: "📊", label: "Sales" },
  { id: "orders",    icon: "🛒", label: "Orders" },
  { id: "customers", icon: "👥", label: "Customers" },
  { id: "shipping",  icon: "🚚", label: "Shipping" },
];

// Mock data
const MONTHLY_REVENUE = [
  { month: "T1", revenue: 4.2, orders: 320 },
  { month: "T2", revenue: 3.8, orders: 290 },
  { month: "T3", revenue: 5.1, orders: 410 },
  { month: "T4", revenue: 5.9, orders: 475 },
  { month: "T5", revenue: 6.8, orders: 520 },
];

const TOP_PRODUCTS = [
  { name: "iPhone 15 Pro",    sold: 245, revenue: 2.45 },
  { name: "Samsung Galaxy S24", sold: 198, revenue: 1.87 },
  { name: "MacBook Air M3",   sold: 142, revenue: 3.21 },
  { name: "AirPods Pro",      sold: 312, revenue: 1.56 },
  { name: "iPad Air",         sold: 87,  revenue: 1.32 },
];

const BRAND_SHARE = [
  { name: "Apple",     value: 38, color: "#0891b2" },
  { name: "Samsung",   value: 24, color: "#7c3aed" },
  { name: "Xiaomi",    value: 15, color: "#059669" },
  { name: "LG",        value: 12, color: "#ea580c" },
  { name: "Khác",      value: 11, color: "#94a3b8" },
];

const HOURLY_ORDERS = Array.from({ length: 24 }, (_, h) => ({
  hour: `${h.toString().padStart(2, "0")}h`,
  orders: Math.round(50 + 80 * Math.sin((h - 6) * 0.4) + Math.random() * 30),
}));

export default function ReportsPage() {
  const [tab, setTab] = useState("sales");

  return (
    <div className="min-h-screen md:pl-0 pl-16">
      {/* Header */}
      <header className="px-6 md:px-8 py-5 border-b border-[color:var(--color-border)] bg-white">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h1 className="text-lg font-bold text-[color:var(--color-text)]">Báo cáo</h1>
            <p className="text-sm text-[color:var(--color-text-subtle)]">
              Dashboard tổng quan · Auto-refresh mỗi 5 phút
            </p>
          </div>
          <div className="flex items-center gap-2">
            <select className="h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30">
              <option>30 ngày qua</option>
              <option>7 ngày qua</option>
              <option>Quý này</option>
              <option>Năm 2026</option>
            </select>
            <button className="h-10 px-4 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)] shadow-sm shadow-cyan-500/20">
              + Pin báo cáo
            </button>
          </div>
        </div>

        {/* Tabs */}
        <div className="flex items-center gap-1 p-1 rounded-lg bg-[color:var(--color-bg-subtle)] w-fit">
          {TABS.map((t) => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={cn(
                "px-4 py-2 rounded-md text-sm font-medium transition-all",
                tab === t.id
                  ? "bg-white text-[color:var(--color-text)] shadow-sm"
                  : "text-[color:var(--color-text-muted)]"
              )}
            >
              <span className="mr-2">{t.icon}</span>
              {t.label}
            </button>
          ))}
        </div>
      </header>

      {/* Content */}
      <div className="p-6 md:p-8 space-y-6">
        {/* KPI Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <KpiCard label="Doanh thu tháng" value="6.8 tỷ" delta="+15.2%" trend="up" color="var(--color-primary)" />
          <KpiCard label="Đơn hàng" value="520" delta="+8.7%" trend="up" color="var(--color-purple)" />
          <KpiCard label="AOV" value="13.1tr" delta="+5.9%" trend="up" color="var(--color-green)" />
          <KpiCard label="Tỷ lệ huỷ" value="3.2%" delta="-1.1%" trend="down" color="var(--color-orange)" />
        </div>

        {/* Main chart */}
        <ChartCard
          title="Doanh thu hàng tháng (tỷ VND)"
          subtitle="So sánh với cùng kỳ năm trước"
          source="fact_sales"
        >
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={MONTHLY_REVENUE} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="month" stroke="#94a3b8" fontSize={12} />
              <YAxis stroke="#94a3b8" fontSize={12} />
              <Tooltip
                contentStyle={{
                  background: "white",
                  border: "1px solid #e2e8f0",
                  borderRadius: 8,
                  fontSize: 12,
                }}
              />
              <Line
                type="monotone"
                dataKey="revenue"
                stroke="#0891b2"
                strokeWidth={3}
                dot={{ fill: "#0891b2", r: 5 }}
                activeDot={{ r: 7 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* Two charts side by side */}
        <div className="grid lg:grid-cols-2 gap-4">
          <ChartCard
            title="Top 5 sản phẩm bán chạy"
            subtitle="Theo số lượng"
            source="fact_sales"
          >
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={TOP_PRODUCTS} layout="vertical" margin={{ top: 0, right: 20, bottom: 0, left: 80 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" horizontal={false} />
                <XAxis type="number" stroke="#94a3b8" fontSize={12} />
                <YAxis type="category" dataKey="name" stroke="#94a3b8" fontSize={11} width={80} />
                <Tooltip
                  contentStyle={{
                    background: "white",
                    border: "1px solid #e2e8f0",
                    borderRadius: 8,
                    fontSize: 12,
                  }}
                />
                <Bar dataKey="sold" fill="#7c3aed" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard
            title="Thị phần theo brand"
            subtitle="Tháng này"
            source="dim_products"
          >
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie
                  data={BRAND_SHARE}
                  cx="50%"
                  cy="50%"
                  innerRadius={55}
                  outerRadius={95}
                  paddingAngle={3}
                  dataKey="value"
                >
                  {BRAND_SHARE.map((entry) => (
                    <Cell key={entry.name} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{
                    background: "white",
                    border: "1px solid #e2e8f0",
                    borderRadius: 8,
                    fontSize: 12,
                  }}
                />
                <Legend wrapperStyle={{ fontSize: 12 }} />
              </PieChart>
            </ResponsiveContainer>
          </ChartCard>
        </div>

        {/* Full width chart */}
        <ChartCard
          title="Số đơn hàng theo giờ trong ngày"
          subtitle="Trung bình 30 ngày qua · Peak 19h-22h"
          source="fact_sales"
        >
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={HOURLY_ORDERS} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="hour" stroke="#94a3b8" fontSize={11} interval={2} />
              <YAxis stroke="#94a3b8" fontSize={12} />
              <Tooltip
                contentStyle={{
                  background: "white",
                  border: "1px solid #e2e8f0",
                  borderRadius: 8,
                  fontSize: 12,
                }}
              />
              <Bar dataKey="orders" fill="#059669" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        {/* AI insight banner */}
        <div className="rounded-xl border border-[color:var(--color-primary-subtle)] bg-gradient-to-r from-[color:var(--color-primary-faded)] to-white p-5 flex items-start gap-4">
          <div className="text-3xl flex-shrink-0">🤖</div>
          <div className="flex-1">
            <h3 className="font-bold text-[color:var(--color-text)] mb-1">
              AI Insight (tự động)
            </h3>
            <p className="text-sm text-[color:var(--color-text-muted)] leading-relaxed">
              Doanh thu tháng 5 tăng <strong className="text-[color:var(--color-green)]">+15.2%</strong> so
              với tháng trước, chủ yếu nhờ <strong>iPhone 15 Pro</strong> (+45%) và{" "}
              <strong>MacBook Air M3</strong> (+38%). Peak giờ giao dịch là 19h-22h. Cảnh báo: tỷ lệ
              huỷ Samsung Galaxy S24 cao bất thường (8.4% vs trung bình 3.2%) — nên investigate.
            </p>
          </div>
          <button className="text-xs text-[color:var(--color-primary)] font-semibold hover:underline flex-shrink-0 mt-1">
            Hỏi chi tiết →
          </button>
        </div>
      </div>
    </div>
  );
}

function KpiCard({
  label,
  value,
  delta,
  trend,
  color,
}: {
  label: string;
  value: string;
  delta: string;
  trend: "up" | "down";
  color: string;
}) {
  const isPositive = trend === "up";
  return (
    <div
      className="rounded-xl bg-white border p-4 hover:shadow-md transition-shadow"
      style={{ borderColor: `${color}30` }}
    >
      <p className="text-xs font-semibold uppercase tracking-wider text-[color:var(--color-text-subtle)] mb-2">
        {label}
      </p>
      <div className="flex items-baseline justify-between">
        <span className="text-2xl md:text-3xl font-bold text-[color:var(--color-text)]">
          {value}
        </span>
        <span
          className={cn(
            "text-xs font-bold px-2 py-0.5 rounded-md",
            isPositive ? "text-[color:var(--color-green)] bg-green-50" : "text-[color:var(--color-orange)] bg-orange-50"
          )}
        >
          {isPositive ? "↑" : "↓"} {delta}
        </span>
      </div>
    </div>
  );
}

function ChartCard({
  title,
  subtitle,
  source,
  children,
}: {
  title: string;
  subtitle?: string;
  source?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-xl bg-white border border-[color:var(--color-border)] p-5 hover:shadow-md transition-shadow">
      <div className="flex items-start justify-between mb-4">
        <div>
          <h3 className="font-semibold text-[color:var(--color-text)]">{title}</h3>
          {subtitle && (
            <p className="text-xs text-[color:var(--color-text-subtle)] mt-0.5">{subtitle}</p>
          )}
        </div>
        <div className="flex items-center gap-2">
          {source && (
            <span className="text-xs font-mono text-[color:var(--color-text-subtle)] px-2 py-0.5 rounded-md bg-[color:var(--color-bg-subtle)]">
              {source}
            </span>
          )}
          <button className="text-[color:var(--color-text-subtle)] hover:text-[color:var(--color-text)] text-sm">
            ⋯
          </button>
        </div>
      </div>
      {children}
    </div>
  );
}
