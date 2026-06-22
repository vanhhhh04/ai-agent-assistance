"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { Logo } from "./Logo";
import { Button } from "./ui/Button";
import { cn } from "@/lib/utils";
import { getUser, type User } from "@/lib/auth";

const navLinks = [
  { href: "/#features", label: "Tính năng" },
  { href: "/how-it-works", label: "Cách hoạt động" },
  { href: "/pricing", label: "Bảng giá" },
  { href: "/docs", label: "Tài liệu" },
];

export function TopNav() {
  const [mobileOpen, setMobileOpen] = useState(false);
  const [user, setUser] = useState<User | null>(null);

  useEffect(() => {
    setUser(getUser());
  }, []);

  return (
    <header className="sticky top-0 z-50 backdrop-blur-md bg-white/80 border-b border-[color:var(--color-border)]">
      <div className="mx-auto max-w-7xl px-6 h-16 flex items-center justify-between">
        <Logo />

        {/* Desktop nav */}
        <nav className="hidden md:flex items-center gap-1">
          {navLinks.map((link) => (
            <Link
              key={link.href}
              href={link.href}
              className="px-3 py-2 text-sm font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] rounded-md transition-colors"
            >
              {link.label}
            </Link>
          ))}
        </nav>

        {/* CTA */}
        <div className="hidden md:flex items-center gap-2">
          {user ? (
            <Button href="/app/ask" variant="primary" size="sm">
              Vào ứng dụng →
            </Button>
          ) : (
            <>
              <Button href="/login" variant="ghost" size="sm">
                Đăng nhập
              </Button>
              <Button href="/signup" variant="primary" size="sm">
                Dùng thử miễn phí →
              </Button>
            </>
          )}
        </div>

        {/* Mobile menu button */}
        <button
          className="md:hidden p-2 rounded-md hover:bg-[color:var(--color-bg-subtle)]"
          onClick={() => setMobileOpen(!mobileOpen)}
          aria-label="Toggle menu"
        >
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            {mobileOpen ? (
              <path d="M18 6L6 18M6 6l12 12" strokeLinecap="round" />
            ) : (
              <path d="M3 12h18M3 6h18M3 18h18" strokeLinecap="round" />
            )}
          </svg>
        </button>
      </div>

      {/* Mobile menu */}
      <div
        className={cn(
          "md:hidden overflow-hidden transition-all duration-200 border-t border-[color:var(--color-border)]",
          mobileOpen ? "max-h-96" : "max-h-0"
        )}
      >
        <div className="px-6 py-4 flex flex-col gap-2 bg-white">
          {navLinks.map((link) => (
            <Link
              key={link.href}
              href={link.href}
              className="py-2 text-sm font-medium text-[color:var(--color-text-muted)]"
              onClick={() => setMobileOpen(false)}
            >
              {link.label}
            </Link>
          ))}
          <div className="flex gap-2 pt-3 mt-2 border-t border-[color:var(--color-border)]">
            <Button href="/login" variant="outline" size="sm" className="flex-1">
              Đăng nhập
            </Button>
            <Button href="/signup" variant="primary" size="sm" className="flex-1">
              Dùng thử
            </Button>
          </div>
        </div>
      </div>
    </header>
  );
}
