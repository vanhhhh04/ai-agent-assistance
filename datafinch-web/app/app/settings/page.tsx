"use client";

import { useEffect, useState } from "react";
import { AppHeader } from "@/components/app/AppHeader";
import { getUser, type User } from "@/lib/auth";
import { cn } from "@/lib/utils";

type Tab = "profile" | "ai" | "integration" | "security";

const TABS: { id: Tab; label: string; icon: string }[] = [
  { id: "profile",     icon: "👤", label: "Profile" },
  { id: "ai",          icon: "🤖", label: "AI Model" },
  { id: "integration", icon: "🔌", label: "Integration" },
  { id: "security",    icon: "🔒", label: "Bảo mật" },
];

export default function SettingsPage() {
  const [tab, setTab] = useState<Tab>("profile");
  const [user, setUser] = useState<User | null>(null);

  useEffect(() => {
    setUser(getUser());
  }, []);

  return (
    <div className="min-h-screen">
      <AppHeader title="Cài đặt" subtitle="Quản lý profile, AI model, tích hợp và bảo mật" />

      <div className="p-6 md:p-8 max-w-4xl">
        {/* Tabs */}
        <div className="flex items-center gap-1 p-1 rounded-lg bg-[color:var(--color-bg-subtle)] border border-[color:var(--color-border)] w-fit mb-6 overflow-x-auto">
          {TABS.map((t) => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={cn(
                "flex items-center gap-2 px-4 py-2 rounded-md text-sm font-medium transition-all whitespace-nowrap",
                tab === t.id
                  ? "bg-white text-[color:var(--color-text)] shadow-sm"
                  : "text-[color:var(--color-text-muted)]"
              )}
            >
              <span>{t.icon}</span>
              {t.label}
            </button>
          ))}
        </div>

        {/* Tab content */}
        {tab === "profile" && <ProfileTab user={user} />}
        {tab === "ai" && <AITab />}
        {tab === "integration" && <IntegrationTab />}
        {tab === "security" && <SecurityTab />}
      </div>
    </div>
  );
}

function Section({
  title,
  description,
  children,
}: {
  title: string;
  description?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-xl bg-white border border-[color:var(--color-border)] p-6 mb-4">
      <div className="mb-5">
        <h3 className="font-bold text-[color:var(--color-text)]">{title}</h3>
        {description && (
          <p className="text-xs text-[color:var(--color-text-subtle)] mt-1">{description}</p>
        )}
      </div>
      {children}
    </div>
  );
}

function Field({
  label,
  children,
  hint,
}: {
  label: string;
  children: React.ReactNode;
  hint?: string;
}) {
  return (
    <div className="mb-4 last:mb-0">
      <label className="block text-sm font-medium text-[color:var(--color-text)] mb-1.5">
        {label}
      </label>
      {children}
      {hint && <p className="text-xs text-[color:var(--color-text-subtle)] mt-1.5">{hint}</p>}
    </div>
  );
}

function inputCls(extra?: string) {
  return cn(
    "w-full h-11 px-3.5 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30 focus:border-[color:var(--color-primary)] transition-colors",
    extra
  );
}

function SaveBar() {
  return (
    <div className="flex justify-end gap-2 mt-6 pt-5 border-t border-[color:var(--color-border)]">
      <button className="h-10 px-4 rounded-lg border border-[color:var(--color-border-strong)] text-sm font-medium hover:bg-[color:var(--color-bg-subtle)]">
        Hủy
      </button>
      <button className="h-10 px-5 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)]">
        Lưu thay đổi
      </button>
    </div>
  );
}

function ProfileTab({ user }: { user: User | null }) {
  return (
    <>
      <Section title="Thông tin cá nhân">
        <div className="flex items-center gap-5 mb-5">
          <div className="w-20 h-20 rounded-full bg-gradient-to-br from-[color:var(--color-primary)] to-[color:var(--color-purple)] flex items-center justify-center text-white text-2xl font-bold flex-shrink-0">
            {user?.name?.[0]?.toUpperCase() ?? "?"}
          </div>
          <div>
            <button className="h-9 px-3 rounded-md border border-[color:var(--color-border-strong)] text-sm font-medium hover:bg-[color:var(--color-bg-subtle)]">
              📷 Tải avatar
            </button>
            <p className="text-xs text-[color:var(--color-text-subtle)] mt-1.5">
              PNG/JPG · tối đa 2MB · khuyến nghị 256×256
            </p>
          </div>
        </div>

        <Field label="Họ tên">
          <input defaultValue={user?.name ?? ""} className={inputCls()} />
        </Field>
        <Field label="Email">
          <input
            type="email"
            defaultValue={user?.email ?? ""}
            disabled
            className={inputCls("opacity-60 cursor-not-allowed")}
          />
        </Field>
        <Field label="Ngôn ngữ">
          <select className={inputCls()}>
            <option>Tiếng Việt</option>
            <option>English</option>
          </select>
        </Field>
        <Field label="Múi giờ">
          <select className={inputCls()}>
            <option>Asia/Ho_Chi_Minh (UTC+7)</option>
            <option>Asia/Bangkok (UTC+7)</option>
            <option>Asia/Singapore (UTC+8)</option>
          </select>
        </Field>
        <SaveBar />
      </Section>

      <Section title="Đổi mật khẩu">
        <Field label="Mật khẩu hiện tại">
          <input type="password" className={inputCls()} placeholder="••••••••" />
        </Field>
        <Field
          label="Mật khẩu mới"
          hint="Tối thiểu 8 ký tự · có chữ hoa, số, ký tự đặc biệt"
        >
          <input type="password" className={inputCls()} placeholder="••••••••" />
        </Field>
        <SaveBar />
      </Section>
    </>
  );
}

function AITab() {
  const [provider, setProvider] = useState("openai");
  const [showKey, setShowKey] = useState(false);

  return (
    <>
      <Section
        title="LLM Provider"
        description="Chọn provider cho Supervisor + SQL Writer agents. Bạn có thể đổi bất kỳ lúc nào."
      >
        <div className="space-y-2.5">
          {[
            { id: "anthropic", label: "Anthropic Claude", desc: "Opus, Sonnet, Haiku 4.5+", badge: "Khuyến nghị" },
            { id: "openai",    label: "OpenAI GPT",       desc: "GPT-5, GPT-5-mini, o-series" },
            { id: "gemini",    label: "Google Gemini",    desc: "Gemini 2.5 Flash/Pro" },
          ].map((p) => (
            <label
              key={p.id}
              className={cn(
                "flex items-start gap-3 p-4 rounded-lg border cursor-pointer transition-colors",
                provider === p.id
                  ? "border-[color:var(--color-primary)] bg-[color:var(--color-primary-faded)]"
                  : "border-[color:var(--color-border)] bg-white hover:border-[color:var(--color-border-strong)]"
              )}
            >
              <input
                type="radio"
                name="provider"
                checked={provider === p.id}
                onChange={() => setProvider(p.id)}
                className="mt-1 text-[color:var(--color-primary)]"
              />
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-0.5">
                  <span className="font-semibold text-[color:var(--color-text)]">{p.label}</span>
                  {p.badge && (
                    <span className="text-xs px-2 py-0.5 rounded-md bg-[color:var(--color-primary)] text-white font-bold">
                      {p.badge}
                    </span>
                  )}
                </div>
                <p className="text-xs text-[color:var(--color-text-muted)]">{p.desc}</p>
              </div>
            </label>
          ))}
        </div>
      </Section>

      <Section
        title="Model configuration"
        description={`Cài đặt model cho ${provider === "openai" ? "OpenAI" : provider === "gemini" ? "Gemini" : "Claude"}`}
      >
        <Field label="API Key">
          <div className="relative">
            <input
              type={showKey ? "text" : "password"}
              defaultValue="sk-proj-••••••••••••••••••••"
              className={inputCls("pr-20 font-mono")}
            />
            <button
              onClick={() => setShowKey(!showKey)}
              className="absolute right-2 top-1/2 -translate-y-1/2 px-2 py-1 text-xs font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)]"
            >
              {showKey ? "Ẩn" : "Hiện"}
            </button>
          </div>
        </Field>
        <Field label="Supervisor model" hint="LLM nhỏ — phân loại intent + chọn backend">
          <select className={inputCls()}>
            {provider === "openai" && (
              <>
                <option>gpt-5-mini</option>
                <option>gpt-5-nano</option>
              </>
            )}
            {provider === "anthropic" && (
              <>
                <option>claude-haiku-4-5</option>
                <option>claude-sonnet-4-6</option>
              </>
            )}
            {provider === "gemini" && <option>gemini-2.5-flash</option>}
          </select>
        </Field>
        <Field label="SQL Writer model" hint="LLM mạnh — viết SQL từ NL question">
          <select className={inputCls()}>
            {provider === "openai" && <option>gpt-5</option>}
            {provider === "anthropic" && (
              <>
                <option>claude-sonnet-4-6</option>
                <option>claude-opus-4-7</option>
              </>
            )}
            {provider === "gemini" && <option>gemini-2.5-pro</option>}
          </select>
        </Field>
        <Field label="Max tokens" hint="Token limit cho response của SQL writer">
          <input type="number" defaultValue={5000} className={inputCls()} />
        </Field>
        <SaveBar />
      </Section>
    </>
  );
}

function IntegrationTab() {
  return (
    <>
      <Section
        title="Tích hợp gửi notification"
        description="Cho alerts + scheduled reports"
      >
        {[
          { icon: "💬", name: "Slack", status: "connected", target: "datafinch-vn.slack.com" },
          { icon: "📱", name: "Microsoft Teams", status: "disconnected", target: null },
          { icon: "📧", name: "Email", status: "connected", target: "admin@datafinch.app" },
          { icon: "🔗", name: "Webhook", status: "disconnected", target: null },
        ].map((i) => (
          <div
            key={i.name}
            className="flex items-center gap-4 p-4 rounded-lg border border-[color:var(--color-border)] mb-2 last:mb-0"
          >
            <div className="text-2xl flex-shrink-0">{i.icon}</div>
            <div className="flex-1">
              <p className="font-semibold text-[color:var(--color-text)]">{i.name}</p>
              {i.target ? (
                <p className="text-xs text-[color:var(--color-text-subtle)] font-mono">
                  {i.target}
                </p>
              ) : (
                <p className="text-xs text-[color:var(--color-text-subtle)]">Chưa kết nối</p>
              )}
            </div>
            <button
              className={cn(
                "h-9 px-3 rounded-md text-xs font-medium",
                i.status === "connected"
                  ? "text-[color:var(--color-text-muted)] hover:bg-[color:var(--color-bg-subtle)]"
                  : "text-[color:var(--color-primary)] bg-[color:var(--color-primary-faded)] hover:bg-cyan-100"
              )}
            >
              {i.status === "connected" ? "Disconnect" : "Connect"}
            </button>
          </div>
        ))}
      </Section>

      <Section title="API Key" description="Sử dụng REST API để query từ code (Growth plan)">
        <Field label="Production API Key">
          <div className="flex gap-2">
            <input
              defaultValue="df_live_••••••••••••••••••••••••••"
              disabled
              className={inputCls("font-mono opacity-60 flex-1")}
            />
            <button className="h-11 px-4 rounded-lg border border-[color:var(--color-border-strong)] text-sm font-medium hover:bg-[color:var(--color-bg-subtle)] whitespace-nowrap">
              📋 Copy
            </button>
            <button className="h-11 px-4 rounded-lg border border-[color:var(--color-border-strong)] text-sm font-medium hover:bg-[color:var(--color-bg-subtle)] whitespace-nowrap">
              🔄 Regenerate
            </button>
          </div>
        </Field>
      </Section>
    </>
  );
}

function SecurityTab() {
  return (
    <>
      <Section title="2-Factor Authentication">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="font-semibold text-[color:var(--color-text)] mb-1">
              2FA chưa bật
            </p>
            <p className="text-sm text-[color:var(--color-text-muted)]">
              Khuyến nghị bật 2FA để bảo vệ tài khoản. Hỗ trợ Authenticator apps (Google, Authy,
              1Password).
            </p>
          </div>
          <button className="h-10 px-4 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)] flex-shrink-0">
            Bật 2FA
          </button>
        </div>
      </Section>

      <Section title="Single Sign-On (SSO)" description="Yêu cầu Enterprise plan">
        <div className="p-5 rounded-lg bg-[color:var(--color-bg-muted)] border border-dashed border-[color:var(--color-border-strong)] text-center">
          <p className="text-sm font-medium text-[color:var(--color-text)] mb-1">
            ✨ SSO / SAML chỉ có ở Enterprise plan
          </p>
          <p className="text-xs text-[color:var(--color-text-muted)] mb-3">
            Tích hợp với Okta, Azure AD, Google Workspace, OneLogin, ...
          </p>
          <a
            href="/pricing"
            className="text-sm font-semibold text-[color:var(--color-primary)] hover:underline"
          >
            Xem Enterprise plan →
          </a>
        </div>
      </Section>

      <Section title="Active sessions">
        {[
          { device: "Chrome trên Windows · Hà Nội", current: true, lastActive: "Đang hoạt động" },
          { device: "Safari trên iPhone · TP.HCM", current: false, lastActive: "2 ngày trước" },
        ].map((s, i) => (
          <div
            key={i}
            className="flex items-center justify-between p-4 rounded-lg border border-[color:var(--color-border)] mb-2 last:mb-0"
          >
            <div>
              <p className="font-medium text-[color:var(--color-text)] text-sm">{s.device}</p>
              <p className="text-xs text-[color:var(--color-text-subtle)]">{s.lastActive}</p>
            </div>
            {s.current ? (
              <span className="text-xs font-semibold text-[color:var(--color-green)] px-2 py-1 rounded-md bg-green-50">
                Phiên hiện tại
              </span>
            ) : (
              <button className="text-xs font-medium text-[color:var(--color-danger)] hover:underline">
                Đăng xuất phiên này
              </button>
            )}
          </div>
        ))}
      </Section>

      <Section title="Audit log">
        <p className="text-sm text-[color:var(--color-text-muted)] mb-4">
          Mọi action trong account của bạn được log đầy đủ. Export cho SOC2 / ISO27001 audits.
        </p>
        <div className="flex gap-2">
          <button className="h-10 px-4 rounded-lg border border-[color:var(--color-border-strong)] text-sm font-medium hover:bg-[color:var(--color-bg-subtle)]">
            👁 Xem audit log
          </button>
          <button className="h-10 px-4 rounded-lg border border-[color:var(--color-border-strong)] text-sm font-medium hover:bg-[color:var(--color-bg-subtle)]">
            ⬇ Export CSV (30 ngày qua)
          </button>
        </div>
      </Section>

      <Section title="Danger zone">
        <div className="p-4 rounded-lg border border-red-200 bg-red-50">
          <p className="font-semibold text-[color:var(--color-danger)] mb-1">Xóa workspace</p>
          <p className="text-sm text-[color:var(--color-text-muted)] mb-3">
            Hành động này không thể hoàn tác. Toàn bộ saved queries, alerts, members sẽ bị xóa vĩnh
            viễn.
          </p>
          <button className="h-9 px-4 rounded-lg bg-[color:var(--color-danger)] text-white text-sm font-semibold hover:opacity-90">
            Xóa workspace
          </button>
        </div>
      </Section>
    </>
  );
}
