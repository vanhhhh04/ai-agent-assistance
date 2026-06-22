"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { Logo } from "@/components/Logo";
import { cn } from "@/lib/utils";
import { getUser, logout, type User } from "@/lib/auth";

const PRIMARY_LINKS = [
  { href: "/app/ask",     icon: "💬", label: "Hỏi" },
  { href: "/app/saved",   icon: "📌", label: "Đã lưu" },
  { href: "/app/reports", icon: "📊", label: "Báo cáo" },
  { href: "/app/alerts",  icon: "🔔", label: "Alerts" },
];

const SECONDARY_LINKS = [
  { href: "/app/data",     icon: "🗂", label: "Dữ liệu" },
  { href: "/app/team",     icon: "👥", label: "Team" },
  { href: "/app/settings", icon: "⚙️", label: "Cài đặt" },
];

const FOOTER_LINKS = [
  { href: "/app/billing", icon: "💳", label: "Gói" },
  { href: "/docs",        icon: "📚", label: "Tài liệu" },
];

function NavItem({
  href,
  icon,
  label,
  active,
  onClick,
}: {
  href: string;
  icon: string;
  label: string;
  active: boolean;
  onClick?: () => void;
}) {
  return (
    <Link
      href={href}
      onClick={onClick}
      className={cn(
        "flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-all",
        active
          ? "bg-[color:var(--color-primary-faded)] text-[color:var(--color-primary)]"
          : "text-[color:var(--color-text-muted)] hover:bg-[color:var(--color-bg-subtle)] hover:text-[color:var(--color-text)]"
      )}
    >
      <span className="text-base">{icon}</span>
      <span>{label}</span>
      {active && (
        <span className="ml-auto w-1.5 h-1.5 rounded-full bg-[color:var(--color-primary)]" />
      )}
    </Link>
  );
}

export function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();
  const [mobileOpen, setMobileOpen] = useState(false);
  const [user, setUser] = useState<User | null>(null);
  const [menuOpen, setMenuOpen] = useState(false);

  useEffect(() => {
    setUser(getUser());
  }, []);

  const close = () => setMobileOpen(false);
  const isActive = (href: string) => pathname?.startsWith(href);

  const handleLogout = () => {
    logout();
    router.push("/login");
  };

  const SidebarContent = (
    <>
      {/* Logo */}
      <div className="px-4 py-5 border-b border-[color:var(--color-border)]">
        <Logo />
      </div>

      {/* Primary nav */}
      <nav className="flex-1 px-3 py-4 overflow-y-auto">
        <div className="space-y-1">
          {PRIMARY_LINKS.map((link) => (
            <NavItem
              key={link.href}
              {...link}
              active={!!isActive(link.href)}
              onClick={close}
            />
          ))}
        </div>

        <div className="my-4 mx-3 border-t border-[color:var(--color-border)]" />

        <div className="space-y-1">
          {SECONDARY_LINKS.map((link) => (
            <NavItem
              key={link.href}
              {...link}
              active={!!isActive(link.href)}
              onClick={close}
            />
          ))}
        </div>

        <div className="my-4 mx-3 border-t border-[color:var(--color-border)]" />

        <div className="space-y-1">
          {FOOTER_LINKS.map((link) => (
            <NavItem
              key={link.href}
              {...link}
              active={!!isActive(link.href)}
              onClick={close}
            />
          ))}
        </div>
      </nav>

      {/* User card */}
      <div className="px-3 py-3 border-t border-[color:var(--color-border)] relative">
        {/* Dropdown menu */}
        {menuOpen && (
          <>
            <div
              className="fixed inset-0 z-40"
              onClick={() => setMenuOpen(false)}
            />
            <div className="absolute bottom-full left-3 right-3 mb-2 bg-white border border-[color:var(--color-border)] rounded-lg shadow-lg z-50 overflow-hidden">
              <Link
                href="/app/settings"
                onClick={() => setMenuOpen(false)}
                className="flex items-center gap-2 px-3 py-2 text-sm text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]"
              >
                <span>⚙️</span> Cài đặt
              </Link>
              <Link
                href="/app/billing"
                onClick={() => setMenuOpen(false)}
                className="flex items-center gap-2 px-3 py-2 text-sm text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]"
              >
                <span>💳</span> Gói & thanh toán
              </Link>
              <div className="border-t border-[color:var(--color-border)]" />
              <button
                onClick={handleLogout}
                className="w-full flex items-center gap-2 px-3 py-2 text-sm text-[color:var(--color-danger)] hover:bg-red-50 text-left"
              >
                <span>🚪</span> Đăng xuất
              </button>
            </div>
          </>
        )}

        <button
          onClick={() => setMenuOpen((v) => !v)}
          className="w-full flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-[color:var(--color-bg-subtle)] transition-colors"
        >
          <div className="w-8 h-8 rounded-full bg-gradient-to-br from-[color:var(--color-primary)] to-[color:var(--color-purple)] flex items-center justify-center text-white text-sm font-bold flex-shrink-0">
            {user?.name?.[0]?.toUpperCase() ?? "?"}
          </div>
          <div className="flex-1 min-w-0 text-left">
            <div className="text-sm font-medium text-[color:var(--color-text)] truncate">
              {user?.name ?? "—"}
            </div>
            <div className="text-xs text-[color:var(--color-text-subtle)] truncate capitalize">
              {user?.plan ?? "—"} plan
            </div>
          </div>
          <span className="text-[color:var(--color-text-subtle)] text-sm">⋯</span>
        </button>
      </div>
    </>
  );

  return (
    <>
      {/* Mobile toggle button (visible <md) */}
      <button
        onClick={() => setMobileOpen(true)}
        className="md:hidden fixed top-4 left-4 z-30 p-2 rounded-lg bg-white border border-[color:var(--color-border)] shadow-sm"
        aria-label="Open sidebar"
      >
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M3 12h18M3 6h18M3 18h18" strokeLinecap="round" />
        </svg>
      </button>

      {/* Mobile overlay */}
      {mobileOpen && (
        <div
          className="md:hidden fixed inset-0 z-40 bg-black/40"
          onClick={close}
        />
      )}

      {/* Sidebar (mobile drawer + desktop sticky) */}
      <aside
        className={cn(
          "fixed md:sticky top-0 left-0 z-50 md:z-auto",
          "w-64 h-screen flex-shrink-0",
          "bg-white border-r border-[color:var(--color-border)]",
          "flex flex-col",
          "transition-transform duration-200",
          mobileOpen ? "translate-x-0" : "-translate-x-full md:translate-x-0"
        )}
      >
        {SidebarContent}
      </aside>
    </>
  );
}
