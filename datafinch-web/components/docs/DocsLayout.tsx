"use client";

import { useEffect, useState } from "react";
import { cn } from "@/lib/utils";

export type TocItem = {
  id: string;
  label: string;
  children?: { id: string; label: string }[];
};

export function DocsLayout({
  toc,
  children,
}: {
  toc: TocItem[];
  children: React.ReactNode;
}) {
  const [active, setActive] = useState<string>("");

  useEffect(() => {
    const handler = () => {
      const allIds = toc.flatMap((t) => [t.id, ...(t.children?.map((c) => c.id) ?? [])]);
      let current = "";
      for (const id of allIds) {
        const el = document.getElementById(id);
        if (!el) continue;
        if (el.getBoundingClientRect().top < 100) current = id;
      }
      setActive(current);
    };
    handler();
    window.addEventListener("scroll", handler, { passive: true });
    return () => window.removeEventListener("scroll", handler);
  }, [toc]);

  return (
    <div className="mx-auto max-w-7xl px-6 py-12 grid lg:grid-cols-[240px_1fr] gap-12">
      {/* Sticky TOC sidebar */}
      <aside className="hidden lg:block">
        <div className="sticky top-24">
          <p className="text-xs font-bold uppercase tracking-wider text-[color:var(--color-text-subtle)] mb-3">
            Trên trang này
          </p>
          <nav className="space-y-0.5">
            {toc.map((item) => (
              <div key={item.id}>
                <a
                  href={`#${item.id}`}
                  className={cn(
                    "block py-1.5 px-3 text-sm rounded-md transition-colors border-l-2",
                    active === item.id ||
                      item.children?.some((c) => c.id === active)
                      ? "text-[color:var(--color-primary)] font-semibold border-[color:var(--color-primary)] bg-[color:var(--color-primary-faded)]"
                      : "text-[color:var(--color-text-muted)] border-transparent hover:text-[color:var(--color-text)]"
                  )}
                >
                  {item.label}
                </a>
                {item.children && (
                  <div className="ml-3 mt-0.5 space-y-0.5">
                    {item.children.map((c) => (
                      <a
                        key={c.id}
                        href={`#${c.id}`}
                        className={cn(
                          "block py-1 px-3 text-xs rounded-md transition-colors border-l-2",
                          active === c.id
                            ? "text-[color:var(--color-primary)] font-medium border-[color:var(--color-primary)]"
                            : "text-[color:var(--color-text-subtle)] border-transparent hover:text-[color:var(--color-text-muted)]"
                        )}
                      >
                        {c.label}
                      </a>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </nav>
        </div>
      </aside>

      {/* Main content */}
      <main className="min-w-0 prose-docs">{children}</main>
    </div>
  );
}

export function H2({ id, children }: { id: string; children: React.ReactNode }) {
  return (
    <h2
      id={id}
      className="scroll-mt-24 text-3xl font-bold tracking-tight text-[color:var(--color-text)] mt-12 mb-4 pb-3 border-b border-[color:var(--color-border)]"
    >
      {children}
    </h2>
  );
}

export function H3({ id, children }: { id: string; children: React.ReactNode }) {
  return (
    <h3
      id={id}
      className="scroll-mt-24 text-xl font-bold text-[color:var(--color-text)] mt-8 mb-3"
    >
      {children}
    </h3>
  );
}

export function CodeBlock({
  language,
  children,
}: {
  language?: string;
  children: string;
}) {
  return (
    <div className="my-4 rounded-lg border border-[color:var(--color-border)] bg-[color:var(--color-bg-muted)] overflow-hidden">
      {language && (
        <div className="px-4 py-1.5 border-b border-[color:var(--color-border)] bg-white text-xs font-mono text-[color:var(--color-text-subtle)] uppercase tracking-wider">
          {language}
        </div>
      )}
      <pre className="p-4 overflow-x-auto text-xs leading-relaxed">
        <code className="font-mono text-[color:var(--color-text)]">{children}</code>
      </pre>
    </div>
  );
}

export function Callout({
  type = "info",
  children,
}: {
  type?: "info" | "warn" | "success" | "danger";
  children: React.ReactNode;
}) {
  const cfg = {
    info:    { icon: "ℹ️", border: "var(--color-primary)", bg: "var(--color-primary-faded)" },
    warn:    { icon: "⚠️", border: "var(--color-orange)",  bg: "#fff7ed" },
    success: { icon: "✓",  border: "var(--color-green)",   bg: "#f0fdf4" },
    danger:  { icon: "✗",  border: "var(--color-danger)",  bg: "#fef2f2" },
  }[type];

  return (
    <div
      className="my-4 p-4 rounded-lg border-l-4 flex gap-3 text-sm"
      style={{ borderColor: cfg.border, background: cfg.bg }}
    >
      <span className="flex-shrink-0">{cfg.icon}</span>
      <div className="flex-1 text-[color:var(--color-text)]">{children}</div>
    </div>
  );
}

export function EndpointCard({
  method,
  path,
  description,
  children,
}: {
  method: "GET" | "POST" | "PUT" | "DELETE";
  path: string;
  description: string;
  children?: React.ReactNode;
}) {
  const methodColor = {
    GET: "var(--color-green)",
    POST: "var(--color-primary)",
    PUT: "var(--color-orange)",
    DELETE: "var(--color-danger)",
  }[method];

  return (
    <div className="my-5 rounded-xl border border-[color:var(--color-border)] bg-white overflow-hidden">
      <div className="px-4 py-3 border-b border-[color:var(--color-border)] bg-[color:var(--color-bg-muted)] flex items-center gap-3">
        <span
          className="text-xs font-bold px-2 py-1 rounded-md font-mono text-white"
          style={{ background: methodColor }}
        >
          {method}
        </span>
        <code className="font-mono text-sm text-[color:var(--color-text)] flex-1">{path}</code>
      </div>
      <div className="p-4">
        <p className="text-sm text-[color:var(--color-text-muted)] mb-3">{description}</p>
        {children}
      </div>
    </div>
  );
}

export function ToolCard({
  icon,
  name,
  role,
  link,
  color,
}: {
  icon: string;
  name: string;
  role: string;
  link: string;
  color: string;
}) {
  return (
    <a
      href={`#${link}`}
      className="p-4 rounded-xl border border-[color:var(--color-border)] bg-white hover:border-[color:var(--color-border-strong)] hover:shadow-md transition-all block"
    >
      <div
        className="w-10 h-10 rounded-lg flex items-center justify-center text-xl mb-3"
        style={{ background: `${color}15`, border: `1px solid ${color}40` }}
      >
        {icon}
      </div>
      <h4 className="font-bold text-sm text-[color:var(--color-text)] mb-0.5">{name}</h4>
      <p className="text-xs text-[color:var(--color-text-muted)]">{role}</p>
    </a>
  );
}
