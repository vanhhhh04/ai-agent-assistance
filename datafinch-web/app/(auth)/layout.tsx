import Link from "next/link";
import { Logo } from "@/components/Logo";

export default function AuthLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen flex flex-col">
      <header className="px-6 py-5 border-b border-[color:var(--color-border)]">
        <div className="mx-auto max-w-7xl flex items-center justify-between">
          <Logo />
          <Link
            href="/"
            className="text-sm text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)]"
          >
            ← Về trang chủ
          </Link>
        </div>
      </header>
      <main className="flex-1 flex items-center justify-center px-6 py-12 bg-[color:var(--color-bg-muted)]">
        {children}
      </main>
      <footer className="px-6 py-5 border-t border-[color:var(--color-border)] text-center text-xs text-[color:var(--color-text-subtle)]">
        © 2026 DataFinch ·{" "}
        <Link href="/privacy" className="hover:text-[color:var(--color-text-muted)]">
          Bảo mật
        </Link>{" "}
        ·{" "}
        <Link href="/terms" className="hover:text-[color:var(--color-text-muted)]">
          Điều khoản
        </Link>
      </footer>
    </div>
  );
}
