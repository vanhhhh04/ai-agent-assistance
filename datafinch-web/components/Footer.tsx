import Link from "next/link";
import { Logo } from "./Logo";

const footerLinks = {
  "Sản phẩm": [
    { href: "/#features", label: "Tính năng" },
    { href: "/how-it-works", label: "Cách hoạt động" },
    { href: "/pricing", label: "Bảng giá" },
    { href: "/changelog", label: "Cập nhật" },
  ],
  "Tài nguyên": [
    { href: "/docs", label: "Tài liệu" },
    { href: "/blog", label: "Blog" },
    { href: "/guides", label: "Hướng dẫn" },
    { href: "/api", label: "API Reference" },
  ],
  "Công ty": [
    { href: "/about", label: "Giới thiệu" },
    { href: "/customers", label: "Khách hàng" },
    { href: "/careers", label: "Tuyển dụng" },
    { href: "/contact", label: "Liên hệ" },
  ],
  "Pháp lý": [
    { href: "/privacy", label: "Chính sách bảo mật" },
    { href: "/terms", label: "Điều khoản dịch vụ" },
    { href: "/security", label: "Bảo mật" },
    { href: "/gdpr", label: "Tuân thủ" },
  ],
};

export function Footer() {
  return (
    <footer className="border-t border-[color:var(--color-border)] bg-[color:var(--color-bg-muted)]">
      <div className="mx-auto max-w-7xl px-6 py-16">
        <div className="grid grid-cols-2 md:grid-cols-5 gap-8 mb-12">
          <div className="col-span-2 md:col-span-1">
            <Logo />
            <p className="mt-4 text-sm text-[color:var(--color-text-muted)] max-w-xs">
              AI Data Analyst cho doanh nghiệp Việt. Hỏi tiếng Việt, có ngay câu trả lời.
            </p>
          </div>

          {Object.entries(footerLinks).map(([title, links]) => (
            <div key={title}>
              <h4 className="text-xs font-bold uppercase tracking-wider text-[color:var(--color-text)] mb-4">
                {title}
              </h4>
              <ul className="space-y-3">
                {links.map((link) => (
                  <li key={link.href}>
                    <Link
                      href={link.href}
                      className="text-sm text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] transition-colors"
                    >
                      {link.label}
                    </Link>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>

        <div className="pt-8 border-t border-[color:var(--color-border)] flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
          <p className="text-sm text-[color:var(--color-text-subtle)]">
            © 2026 DataFinch. Made in Vietnam 🇻🇳
          </p>
          <div className="flex items-center gap-6 text-sm text-[color:var(--color-text-muted)]">
            <Link href="https://github.com" className="hover:text-[color:var(--color-text)]">
              GitHub
            </Link>
            <Link href="https://x.com" className="hover:text-[color:var(--color-text)]">
              X (Twitter)
            </Link>
            <Link href="mailto:hello@datafinch.app" className="hover:text-[color:var(--color-text)]">
              hello@datafinch.app
            </Link>
          </div>
        </div>
      </div>
    </footer>
  );
}
