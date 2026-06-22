"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { AppHeader } from "@/components/app/AppHeader";
import {
  deleteSavedQuery,
  formatRelativeTime,
  getSavedQueries,
  incrementRunCount,
  onSavedQueriesChange,
  toggleStar,
  type SavedQuery,
} from "@/lib/savedQueries";
import { cn } from "@/lib/utils";

export default function SavedPage() {
  const router = useRouter();
  const [saved, setSaved] = useState<SavedQuery[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [query, setQuery] = useState("");
  const [folder, setFolder] = useState("Tất cả");
  const [starredOnly, setStarredOnly] = useState(false);

  // Load + subscribe to changes (works across tabs too)
  useEffect(() => {
    setSaved(getSavedQueries());
    setLoaded(true);
    return onSavedQueriesChange(() => setSaved(getSavedQueries()));
  }, []);

  // Folders derived from saved data
  const folders = useMemo(() => {
    const s = new Set<string>(saved.map((q) => q.folder));
    return ["Tất cả", ...Array.from(s).sort()];
  }, [saved]);

  const filtered = useMemo(
    () =>
      saved.filter((s) => {
        if (folder !== "Tất cả" && s.folder !== folder) return false;
        if (starredOnly && !s.starred) return false;
        if (query) {
          const q = query.toLowerCase();
          if (
            !s.title.toLowerCase().includes(q) &&
            !s.question.toLowerCase().includes(q) &&
            !s.sql.toLowerCase().includes(q)
          )
            return false;
        }
        return true;
      }),
    [saved, folder, starredOnly, query]
  );

  const handleRun = (item: SavedQuery) => {
    incrementRunCount(item.id);
    router.push(`/app/ask?q=${encodeURIComponent(item.question)}`);
  };

  return (
    <div className="min-h-screen">
      <AppHeader
        title="Đã lưu"
        subtitle={
          loaded
            ? `${filtered.length} / ${saved.length} câu hỏi · Lưu trong trình duyệt (localStorage)`
            : "Đang tải..."
        }
        actions={
          <Link
            href="/app/ask"
            className="h-10 px-4 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)] shadow-sm shadow-cyan-500/20 transition-all inline-flex items-center"
          >
            + Hỏi câu mới
          </Link>
        }
      />

      <div className="p-6 md:p-8">
        {loaded && saved.length === 0 ? (
          <EmptyState />
        ) : (
          <>
            {/* Filters */}
            <div className="flex flex-wrap items-center gap-2 mb-6">
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="🔎 Tìm trong câu hỏi, SQL..."
                className="h-10 px-3.5 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30 focus:border-[color:var(--color-primary)] w-full sm:w-64"
              />

              {folders.length > 1 && (
                <div className="flex items-center gap-1 p-1 rounded-lg bg-[color:var(--color-bg-subtle)]">
                  {folders.map((f) => (
                    <button
                      key={f}
                      onClick={() => setFolder(f)}
                      className={cn(
                        "px-3 py-1.5 rounded-md text-xs font-medium transition-all",
                        folder === f
                          ? "bg-white text-[color:var(--color-text)] shadow-sm"
                          : "text-[color:var(--color-text-muted)]"
                      )}
                    >
                      {f}
                    </button>
                  ))}
                </div>
              )}

              <button
                onClick={() => setStarredOnly((v) => !v)}
                className={cn(
                  "h-10 px-3 rounded-lg text-sm font-medium transition-all border",
                  starredOnly
                    ? "bg-yellow-50 border-yellow-300 text-yellow-700"
                    : "bg-white border-[color:var(--color-border-strong)] text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)]"
                )}
              >
                {starredOnly ? "⭐" : "☆"} Đã star
              </button>
            </div>

            {/* List */}
            {filtered.length === 0 ? (
              <div className="text-center py-20 text-[color:var(--color-text-muted)]">
                <div className="text-4xl mb-3">🔍</div>
                <p className="font-medium mb-1">Không tìm thấy câu hỏi nào</p>
                <p className="text-sm">Thử đổi filter hoặc xóa search query</p>
              </div>
            ) : (
              <div className="space-y-3 max-w-5xl">
                {filtered.map((s) => (
                  <SavedCard key={s.id} saved={s} onRun={() => handleRun(s)} />
                ))}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}

function EmptyState() {
  return (
    <div className="max-w-lg mx-auto text-center py-16">
      <div className="text-6xl mb-5">📌</div>
      <h2 className="text-xl font-bold text-[color:var(--color-text)] mb-2">
        Chưa có câu hỏi nào được lưu
      </h2>
      <p className="text-[color:var(--color-text-muted)] mb-8">
        Sau khi hỏi câu nào hay, click <strong>💾 Lưu</strong> trong câu trả lời để bookmark cho lần
        sau. Câu hỏi lưu trong trình duyệt của bạn.
      </p>
      <Link
        href="/app/ask"
        className="inline-flex items-center justify-center h-11 px-5 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)] shadow-sm shadow-cyan-500/20"
      >
        Hỏi câu đầu tiên →
      </Link>
    </div>
  );
}

function SavedCard({ saved, onRun }: { saved: SavedQuery; onRun: () => void }) {
  const [expanded, setExpanded] = useState(false);

  const handleDelete = () => {
    if (confirm(`Xóa câu hỏi "${saved.title}"?`)) {
      deleteSavedQuery(saved.id);
    }
  };

  return (
    <div className="rounded-xl bg-white border border-[color:var(--color-border)] hover:border-[color:var(--color-border-strong)] hover:shadow-md transition-all">
      <div className="p-5">
        <div className="flex items-start gap-3 mb-2">
          <button
            onClick={() => toggleStar(saved.id)}
            className="mt-1 text-lg flex-shrink-0 hover:scale-110 transition-transform"
            title={saved.starred ? "Bỏ star" : "Đánh dấu star"}
          >
            {saved.starred ? "⭐" : "☆"}
          </button>
          <div className="flex-1 min-w-0">
            <div className="flex items-start justify-between gap-2 mb-1">
              <h3 className="font-semibold text-[color:var(--color-text)]">{saved.title}</h3>
              <span className="flex-shrink-0 text-xs px-2 py-0.5 rounded-md bg-[color:var(--color-bg-subtle)] text-[color:var(--color-text-muted)]">
                {saved.folder}
              </span>
            </div>
            <p className="text-sm text-[color:var(--color-text-muted)] italic mb-3">
              &ldquo;{saved.question}&rdquo;
            </p>
            <div className="flex flex-wrap items-center gap-3 text-xs text-[color:var(--color-text-subtle)]">
              <span>🕐 Lưu {formatRelativeTime(saved.savedAt)}</span>
              {saved.lastRunAt && (
                <>
                  <span>·</span>
                  <span>▶ Chạy lần cuối {formatRelativeTime(saved.lastRunAt)}</span>
                </>
              )}
              <span>·</span>
              <span>📈 {saved.runs} lần chạy</span>
              {saved.tables.length > 0 && (
                <>
                  <span>·</span>
                  <span className="font-mono">{saved.tables.join(", ")}</span>
                </>
              )}
              <span>·</span>
              <span className="font-mono uppercase">{saved.backend}</span>
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="flex items-center justify-between mt-4 pt-3 border-t border-[color:var(--color-border)]">
          <div className="flex items-center gap-1">
            <button
              onClick={onRun}
              className="px-3 py-1.5 rounded-md text-xs font-semibold bg-[color:var(--color-primary)] text-white hover:bg-[color:var(--color-primary-hover)]"
            >
              ▶ Chạy lại
            </button>
            <button
              onClick={() => setExpanded((v) => !v)}
              className="px-3 py-1.5 rounded-md text-xs font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]"
            >
              {expanded ? "▼" : "▶"} SQL
            </button>
            <button
              onClick={async () => {
                try {
                  await navigator.clipboard.writeText(saved.sql);
                } catch {}
              }}
              className="px-3 py-1.5 rounded-md text-xs font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]"
              title="Copy SQL vào clipboard"
            >
              📋 Copy SQL
            </button>
          </div>
          <div className="flex items-center gap-1">
            <button
              onClick={handleDelete}
              className="p-1.5 rounded-md text-[color:var(--color-text-subtle)] hover:text-[color:var(--color-danger)] hover:bg-red-50"
              title="Xóa"
            >
              🗑
            </button>
          </div>
        </div>

        {expanded && (
          <div className="mt-3 space-y-2">
            {saved.explanation && (
              <div className="p-3 rounded-lg bg-[color:var(--color-primary-faded)] border border-[color:var(--color-primary-subtle)] text-sm text-[color:var(--color-text)]">
                <p className="text-xs font-bold uppercase tracking-wider text-[color:var(--color-primary)] mb-1">
                  Giải thích
                </p>
                {saved.explanation}
              </div>
            )}
            <pre className="p-3 rounded-lg bg-[color:var(--color-bg-muted)] border border-[color:var(--color-border)] text-xs font-mono text-[color:var(--color-green)] whitespace-pre-wrap leading-relaxed overflow-x-auto">
              {saved.sql || "(Không có SQL)"}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
}
