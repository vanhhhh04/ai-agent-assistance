"use client";

import { useState } from "react";
import { AppHeader } from "@/components/app/AppHeader";
import { cn } from "@/lib/utils";

type DataSource = {
  id: string;
  type: "postgres" | "hive" | "mysql" | "bigquery" | "snowflake";
  name: string;
  host: string;
  database: string;
  tables: number;
  lastSync: string;
  status: "connected" | "syncing" | "error";
};

const MOCK_SOURCES: DataSource[] = [
  {
    id: "1",
    type: "hive",
    name: "Hive Gold Production",
    host: "hiveserver2:10000",
    database: "gold",
    tables: 10,
    lastSync: "5 phút trước",
    status: "connected",
  },
  {
    id: "2",
    type: "postgres",
    name: "Postgres ERP",
    host: "postgres:5432",
    database: "ecommerce",
    tables: 11,
    lastSync: "1 phút trước",
    status: "connected",
  },
];

const SOURCE_ICONS: Record<DataSource["type"], string> = {
  postgres: "🐘",
  hive: "🏛",
  mysql: "🐬",
  bigquery: "🟢",
  snowflake: "❄️",
};

type TableInfo = {
  name: string;
  kind: "fact" | "dim";
  rows: number;
  columns: { name: string; type: string; isPii?: boolean; isKey?: boolean }[];
  description: string;
};

const MOCK_TABLES: TableInfo[] = [
  {
    name: "fact_sales",
    kind: "fact",
    rows: 134_582,
    description: "Bảng giao dịch bán hàng theo từng order_item. 1 row = 1 line item trong 1 order.",
    columns: [
      { name: "order_key", type: "int", isKey: true },
      { name: "customer_key", type: "int", isKey: true },
      { name: "product_key", type: "int", isKey: true },
      { name: "quantity", type: "int" },
      { name: "unit_price", type: "decimal(12,2)" },
      { name: "item_total", type: "decimal(12,2)" },
      { name: "order_total", type: "decimal(12,2)" },
      { name: "order_date", type: "timestamp" },
      { name: "order_year", type: "int" },
      { name: "order_month", type: "int" },
      { name: "order_status", type: "string" },
      { name: "brand", type: "string" },
      { name: "product_name", type: "string" },
    ],
  },
  {
    name: "dim_customers",
    kind: "dim",
    rows: 96_282,
    description: "Bảng dimension khách hàng — SCD Type 1.",
    columns: [
      { name: "customer_key", type: "int", isKey: true },
      { name: "customer_name", type: "string" },
      { name: "email", type: "string", isPii: true },
      { name: "gender", type: "string" },
      { name: "date_of_birth", type: "date", isPii: true },
      { name: "customer_since", type: "timestamp" },
    ],
  },
  {
    name: "dim_products",
    kind: "dim",
    rows: 10_023,
    description: "Bảng dimension sản phẩm.",
    columns: [
      { name: "product_key", type: "int", isKey: true },
      { name: "sku", type: "string" },
      { name: "product_name", type: "string" },
      { name: "brand", type: "string" },
      { name: "category", type: "string" },
      { name: "price", type: "decimal(12,2)" },
    ],
  },
  {
    name: "dim_coupons",
    kind: "dim",
    rows: 1_482,
    description: "Bảng dimension mã giảm giá.",
    columns: [
      { name: "coupon_key", type: "int", isKey: true },
      { name: "coupon_code", type: "string" },
      { name: "discount_type", type: "string" },
      { name: "discount_value", type: "decimal" },
      { name: "valid_from", type: "date" },
      { name: "valid_until", type: "date" },
    ],
  },
  {
    name: "dim_addresses",
    kind: "dim",
    rows: 1_412,
    description: "Bảng dimension địa chỉ giao hàng.",
    columns: [
      { name: "address_key", type: "int", isKey: true },
      { name: "city", type: "string" },
      { name: "state", type: "string" },
      { name: "country", type: "string" },
    ],
  },
];

export default function DataPage() {
  const [query, setQuery] = useState("");
  const [kindFilter, setKindFilter] = useState<"all" | "fact" | "dim">("all");
  const [expandedTable, setExpandedTable] = useState<string | null>("fact_sales");

  const filteredTables = MOCK_TABLES.filter((t) => {
    if (kindFilter !== "all" && t.kind !== kindFilter) return false;
    if (query && !t.name.toLowerCase().includes(query.toLowerCase())) return false;
    return true;
  });

  return (
    <div className="min-h-screen">
      <AppHeader
        title="Dữ liệu"
        subtitle="Quản lý data sources + duyệt schema catalog đã được AI hiểu"
        actions={
          <button className="h-10 px-4 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)] shadow-sm shadow-cyan-500/20 transition-all">
            + Kết nối nguồn mới
          </button>
        }
      />

      <div className="p-6 md:p-8 space-y-8">
        {/* Data sources */}
        <section>
          <h2 className="text-base font-bold text-[color:var(--color-text)] mb-3 flex items-center gap-2">
            🔌 Data sources
            <span className="text-xs font-normal text-[color:var(--color-text-subtle)]">
              ({MOCK_SOURCES.length})
            </span>
          </h2>
          <div className="grid md:grid-cols-2 gap-3">
            {MOCK_SOURCES.map((s) => (
              <SourceCard key={s.id} source={s} />
            ))}
          </div>
        </section>

        {/* Schema catalog */}
        <section>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-base font-bold text-[color:var(--color-text)] flex items-center gap-2">
              📚 Schema catalog
              <span className="text-xs font-normal text-[color:var(--color-text-subtle)]">
                (AI đã hiểu {MOCK_TABLES.length} bảng)
              </span>
            </h2>
            <button className="text-xs text-[color:var(--color-primary)] hover:underline font-medium">
              🔄 Re-index catalog
            </button>
          </div>

          {/* Filters */}
          <div className="flex flex-wrap gap-2 mb-4">
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="🔎 Tìm bảng..."
              className="h-10 px-3.5 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30 w-full sm:w-64"
            />
            <div className="flex items-center gap-1 p-1 rounded-lg bg-[color:var(--color-bg-subtle)]">
              {(["all", "fact", "dim"] as const).map((k) => (
                <button
                  key={k}
                  onClick={() => setKindFilter(k)}
                  className={cn(
                    "px-3 py-1.5 rounded-md text-xs font-medium uppercase tracking-wide transition-all",
                    kindFilter === k
                      ? "bg-white text-[color:var(--color-text)] shadow-sm"
                      : "text-[color:var(--color-text-muted)]"
                  )}
                >
                  {k === "all" ? "Tất cả" : k}
                </button>
              ))}
            </div>
          </div>

          <div className="space-y-2">
            {filteredTables.map((t) => (
              <TableRow
                key={t.name}
                table={t}
                expanded={expandedTable === t.name}
                onToggle={() => setExpandedTable(expandedTable === t.name ? null : t.name)}
              />
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}

function SourceCard({ source }: { source: DataSource }) {
  return (
    <div className="rounded-xl bg-white border border-[color:var(--color-border)] p-4 hover:shadow-md transition-shadow">
      <div className="flex items-start gap-3 mb-3">
        <div className="text-2xl flex-shrink-0">{SOURCE_ICONS[source.type]}</div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <h3 className="font-semibold text-[color:var(--color-text)] truncate">
              {source.name}
            </h3>
            {source.status === "connected" && (
              <span className="flex items-center gap-1 text-xs text-[color:var(--color-green)] font-semibold">
                <span className="w-1.5 h-1.5 rounded-full bg-[color:var(--color-green)]" /> Live
              </span>
            )}
          </div>
          <p className="text-xs text-[color:var(--color-text-subtle)] font-mono truncate">
            {source.host}/{source.database}
          </p>
        </div>
      </div>
      <div className="flex items-center justify-between text-xs text-[color:var(--color-text-muted)] pt-3 border-t border-[color:var(--color-border)]">
        <span>
          📦 {source.tables} bảng · 🕐 Sync {source.lastSync}
        </span>
        <div className="flex gap-1">
          <button className="px-2 py-1 rounded-md hover:bg-[color:var(--color-bg-subtle)]">
            ⚙️
          </button>
          <button className="px-2 py-1 rounded-md hover:bg-[color:var(--color-bg-subtle)]">
            🔄
          </button>
        </div>
      </div>
    </div>
  );
}

function TableRow({
  table,
  expanded,
  onToggle,
}: {
  table: TableInfo;
  expanded: boolean;
  onToggle: () => void;
}) {
  const isFact = table.kind === "fact";
  return (
    <div className="rounded-xl bg-white border border-[color:var(--color-border)] overflow-hidden">
      <button
        onClick={onToggle}
        className="w-full p-4 flex items-start gap-3 text-left hover:bg-[color:var(--color-bg-muted)] transition-colors"
      >
        <span
          className="flex-shrink-0 mt-0.5"
          style={{ color: isFact ? "var(--color-orange)" : "var(--color-purple)" }}
        >
          {isFact ? "◆" : "◇"}
        </span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <code className="font-mono font-semibold text-[color:var(--color-text)]">
              {table.name}
            </code>
            <span
              className="text-xs px-2 py-0.5 rounded-md font-semibold uppercase"
              style={{
                color: isFact ? "var(--color-orange)" : "var(--color-purple)",
                background: isFact ? "#fed7aa" : "#ede9fe",
              }}
            >
              {table.kind}
            </span>
            <span className="text-xs text-[color:var(--color-text-subtle)]">
              {table.rows.toLocaleString()} rows · {table.columns.length} cols
            </span>
          </div>
          <p className="text-sm text-[color:var(--color-text-muted)]">{table.description}</p>
        </div>
        <span className="flex-shrink-0 text-[color:var(--color-text-subtle)]">
          {expanded ? "▼" : "▶"}
        </span>
      </button>

      {expanded && (
        <div className="border-t border-[color:var(--color-border)] bg-[color:var(--color-bg-muted)]/50 p-4">
          <div className="flex items-center justify-between mb-3">
            <p className="text-xs font-bold uppercase tracking-wider text-[color:var(--color-text-subtle)]">
              Columns ({table.columns.length})
            </p>
            <button className="text-xs text-[color:var(--color-primary)] hover:underline font-medium">
              ✏️ Sửa mô tả
            </button>
          </div>
          <div className="grid sm:grid-cols-2 gap-2">
            {table.columns.map((c) => (
              <div
                key={c.name}
                className="flex items-center gap-2 px-3 py-2 rounded-lg bg-white border border-[color:var(--color-border)]"
              >
                <span
                  className="w-1.5 h-1.5 rounded-full flex-shrink-0"
                  style={{
                    background: c.isKey
                      ? "var(--color-green)"
                      : c.isPii
                      ? "var(--color-orange)"
                      : "var(--color-border-strong)",
                  }}
                />
                <code className="font-mono text-sm text-[color:var(--color-text)] font-medium">
                  {c.name}
                </code>
                <span className="text-xs text-[color:var(--color-text-subtle)] ml-auto font-mono">
                  {c.type}
                </span>
                {c.isPii && (
                  <span className="text-xs px-1.5 py-0.5 rounded bg-orange-100 text-orange-700 font-semibold">
                    PII
                  </span>
                )}
              </div>
            ))}
          </div>
          <div className="mt-3 pt-3 border-t border-[color:var(--color-border)] flex gap-2">
            <button className="px-3 py-1.5 rounded-md text-xs font-medium bg-[color:var(--color-primary)] text-white">
              💬 Hỏi về bảng này
            </button>
            <button className="px-3 py-1.5 rounded-md text-xs font-medium text-[color:var(--color-text-muted)] hover:bg-[color:var(--color-bg-subtle)]">
              👁 Xem preview rows
            </button>
            <button className="px-3 py-1.5 rounded-md text-xs font-medium text-[color:var(--color-text-muted)] hover:bg-[color:var(--color-bg-subtle)]">
              📊 Thống kê
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
