"use client";

import { useState } from "react";
import { AppHeader } from "@/components/app/AppHeader";
import { cn } from "@/lib/utils";

type AlertStatus = "active" | "paused" | "triggered";

type AlertItem = {
  id: string;
  name: string;
  question: string;
  condition: string;
  schedule: string;
  channel: { type: "slack" | "email"; target: string };
  status: AlertStatus;
  lastRun: string;
  triggeredCount: number;
};

const MOCK_ALERTS: AlertItem[] = [
  {
    id: "1",
    name: "Doanh thu hôm nay giảm > 20%",
    question: "Doanh thu hôm nay so với 7 ngày trước",
    condition: "revenue_today < avg_7d * 0.8",
    schedule: "Mỗi sáng 8:00",
    channel: { type: "slack", target: "#ops-alerts" },
    status: "triggered",
    lastRun: "Hôm qua 8:00",
    triggeredCount: 3,
  },
  {
    id: "2",
    name: "Tồn kho thấp top 10 sản phẩm",
    question: "Top 10 sản phẩm có stock < 100",
    condition: "min(stock_quantity) < 100",
    schedule: "Mỗi giờ",
    channel: { type: "email", target: "ops@company.vn" },
    status: "active",
    lastRun: "5 phút trước",
    triggeredCount: 12,
  },
  {
    id: "3",
    name: "Tỷ lệ huỷ đơn cao bất thường",
    question: "Tỷ lệ huỷ đơn hôm nay",
    condition: "cancel_rate > 5%",
    schedule: "Mỗi 2 giờ",
    channel: { type: "slack", target: "#sales-team" },
    status: "active",
    lastRun: "1 giờ trước",
    triggeredCount: 0,
  },
  {
    id: "4",
    name: "Đơn hàng VIP chưa xử lý > 1h",
    question: "Đơn hàng VIP pending > 1 giờ",
    condition: "pending_vip_orders > 0",
    schedule: "Mỗi 15 phút",
    channel: { type: "slack", target: "#vip-support" },
    status: "paused",
    lastRun: "3 ngày trước",
    triggeredCount: 28,
  },
];

const STATUS_CONFIG: Record<
  AlertStatus,
  { label: string; color: string; bg: string }
> = {
  active:    { label: "Hoạt động",  color: "var(--color-green)",   bg: "#d1fae5" },
  paused:    { label: "Tạm dừng",    color: "var(--color-text-subtle)", bg: "var(--color-bg-subtle)" },
  triggered: { label: "Đã trigger",  color: "var(--color-orange)",  bg: "#fed7aa" },
};

export default function AlertsPage() {
  const [filter, setFilter] = useState<"all" | AlertStatus>("all");
  const [showCreate, setShowCreate] = useState(false);

  const filtered = MOCK_ALERTS.filter((a) => filter === "all" || a.status === filter);

  const counts = {
    all: MOCK_ALERTS.length,
    active: MOCK_ALERTS.filter((a) => a.status === "active").length,
    paused: MOCK_ALERTS.filter((a) => a.status === "paused").length,
    triggered: MOCK_ALERTS.filter((a) => a.status === "triggered").length,
  };

  return (
    <div className="min-h-screen">
      <AppHeader
        title="Alerts"
        subtitle="Scheduled queries gửi notification khi vượt threshold"
        actions={
          <button
            onClick={() => setShowCreate(true)}
            className="h-10 px-4 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)] shadow-sm shadow-cyan-500/20 transition-all"
          >
            + Tạo alert
          </button>
        }
      />

      <div className="p-6 md:p-8">
        {/* Stats row */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
          {(["all", "active", "triggered", "paused"] as const).map((k) => (
            <button
              key={k}
              onClick={() => setFilter(k)}
              className={cn(
                "p-4 rounded-xl border text-left transition-all",
                filter === k
                  ? "border-[color:var(--color-primary)] bg-[color:var(--color-primary-faded)]"
                  : "border-[color:var(--color-border)] bg-white hover:border-[color:var(--color-border-strong)]"
              )}
            >
              <p className="text-xs font-semibold uppercase tracking-wider text-[color:var(--color-text-subtle)] mb-1">
                {k === "all" ? "Tất cả" : STATUS_CONFIG[k as AlertStatus].label}
              </p>
              <p className="text-2xl font-bold text-[color:var(--color-text)]">{counts[k]}</p>
            </button>
          ))}
        </div>

        {/* Alerts list */}
        <div className="space-y-3 max-w-5xl">
          {filtered.length === 0 ? (
            <div className="text-center py-20 text-[color:var(--color-text-muted)]">
              <div className="text-4xl mb-3">🔔</div>
              <p className="font-medium">Không có alert nào</p>
            </div>
          ) : (
            filtered.map((a) => <AlertCard key={a.id} alert={a} />)
          )}
        </div>
      </div>

      {/* Create modal */}
      {showCreate && <CreateAlertModal onClose={() => setShowCreate(false)} />}
    </div>
  );
}

function AlertCard({ alert }: { alert: AlertItem }) {
  const cfg = STATUS_CONFIG[alert.status];
  return (
    <div className="rounded-xl bg-white border border-[color:var(--color-border)] hover:border-[color:var(--color-border-strong)] hover:shadow-md transition-all p-5">
      <div className="flex items-start gap-4">
        <div
          className="w-2 h-2 rounded-full mt-2 flex-shrink-0"
          style={{
            background: cfg.color,
            boxShadow: alert.status === "active" ? `0 0 0 4px ${cfg.bg}` : "none",
          }}
        />
        <div className="flex-1 min-w-0">
          <div className="flex items-start justify-between gap-3 mb-2">
            <div>
              <h3 className="font-semibold text-[color:var(--color-text)]">{alert.name}</h3>
              <p className="text-sm text-[color:var(--color-text-muted)] italic mt-0.5">
                &ldquo;{alert.question}&rdquo;
              </p>
            </div>
            <span
              className="flex-shrink-0 text-xs px-2.5 py-1 rounded-md font-semibold"
              style={{ color: cfg.color, background: cfg.bg }}
            >
              {cfg.label}
            </span>
          </div>

          {/* Metadata */}
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1.5 text-xs text-[color:var(--color-text-muted)] mb-4">
            <span className="flex items-center gap-1.5">
              ⚖️
              <code className="font-mono text-[color:var(--color-orange)]">{alert.condition}</code>
            </span>
            <span>🕐 {alert.schedule}</span>
            <span>
              {alert.channel.type === "slack" ? "💬" : "📧"}{" "}
              <code className="font-mono">{alert.channel.target}</code>
            </span>
          </div>

          <div className="flex items-center justify-between pt-3 border-t border-[color:var(--color-border)]">
            <div className="flex items-center gap-1">
              <button className="px-3 py-1.5 rounded-md text-xs font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]">
                ✏️ Sửa
              </button>
              <button className="px-3 py-1.5 rounded-md text-xs font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]">
                {alert.status === "paused" ? "▶ Resume" : "⏸ Tạm dừng"}
              </button>
              <button className="px-3 py-1.5 rounded-md text-xs font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]">
                ▶ Test ngay
              </button>
              <button className="px-3 py-1.5 rounded-md text-xs font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-danger)] hover:bg-red-50">
                🗑 Xóa
              </button>
            </div>
            <div className="text-xs text-[color:var(--color-text-subtle)]">
              Run: {alert.lastRun} · Đã trigger {alert.triggeredCount} lần
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function CreateAlertModal({ onClose }: { onClose: () => void }) {
  return (
    <div className="fixed inset-0 z-50 bg-black/50 flex items-center justify-center p-4" onClick={onClose}>
      <div
        onClick={(e) => e.stopPropagation()}
        className="bg-white rounded-xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto"
      >
        <div className="px-6 py-4 border-b border-[color:var(--color-border)] flex items-center justify-between">
          <h2 className="text-lg font-bold text-[color:var(--color-text)]">Tạo alert mới</h2>
          <button
            onClick={onClose}
            className="w-8 h-8 rounded-md hover:bg-[color:var(--color-bg-subtle)] text-[color:var(--color-text-muted)]"
          >
            ✕
          </button>
        </div>

        <div className="p-6 space-y-5">
          <div>
            <label className="block text-sm font-medium text-[color:var(--color-text)] mb-1.5">
              Tên alert
            </label>
            <input
              placeholder="VD: Doanh thu hôm nay giảm > 20%"
              className="w-full h-11 px-3.5 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-[color:var(--color-text)] mb-1.5">
              Câu hỏi NL
            </label>
            <input
              placeholder="VD: Doanh thu hôm nay so với 7 ngày trước"
              className="w-full h-11 px-3.5 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-[color:var(--color-text)] mb-1.5">
              Trigger khi
            </label>
            <div className="grid grid-cols-3 gap-2">
              <select className="h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm">
                <option>Giá trị kết quả</option>
                <option>So sánh với baseline</option>
                <option>% thay đổi</option>
              </select>
              <select className="h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm">
                <option>&gt;</option>
                <option>&lt;</option>
                <option>=</option>
              </select>
              <input
                placeholder="VD: 20%"
                className="h-10 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm"
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-[color:var(--color-text)] mb-1.5">
              Schedule
            </label>
            <div className="grid grid-cols-2 gap-2">
              {["Mỗi 15 phút", "Mỗi giờ", "Mỗi 6 giờ", "Mỗi sáng 8:00"].map((s) => (
                <label
                  key={s}
                  className="flex items-center gap-2 px-3 py-2.5 rounded-lg border border-[color:var(--color-border)] bg-white cursor-pointer hover:border-[color:var(--color-primary)]"
                >
                  <input type="radio" name="schedule" className="text-[color:var(--color-primary)]" />
                  <span className="text-sm">{s}</span>
                </label>
              ))}
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-[color:var(--color-text)] mb-1.5">
              Gửi tới
            </label>
            <div className="space-y-2">
              {[
                { icon: "💬", label: "Slack", placeholder: "#channel-name" },
                { icon: "📧", label: "Email", placeholder: "email@company.vn" },
              ].map((c) => (
                <div key={c.label} className="flex items-center gap-2">
                  <input type="checkbox" />
                  <span className="text-lg">{c.icon}</span>
                  <span className="text-sm font-medium text-[color:var(--color-text)] w-16">
                    {c.label}
                  </span>
                  <input
                    placeholder={c.placeholder}
                    className="flex-1 h-9 px-3 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm"
                  />
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="px-6 py-4 border-t border-[color:var(--color-border)] flex justify-end gap-2 bg-[color:var(--color-bg-muted)]">
          <button
            onClick={onClose}
            className="h-10 px-4 rounded-lg border border-[color:var(--color-border-strong)] text-sm font-medium hover:bg-white"
          >
            Hủy
          </button>
          <button className="h-10 px-5 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)]">
            Tạo alert
          </button>
        </div>
      </div>
    </div>
  );
}
