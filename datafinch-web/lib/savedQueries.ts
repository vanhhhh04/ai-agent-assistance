/**
 * Local-storage CRUD for saved queries.
 * One key holds the full array; UI re-reads on every mount and after every
 * mutation via the `notify()` event so multiple components stay in sync.
 */

import type { QueryResult } from "./api";

export type SavedQuery = {
  id: string;
  title: string;
  question: string;
  sql: string;
  explanation: string;
  tables: string[];
  backend: string;
  rowCount: number;
  complexity: string;
  savedAt: string;       // ISO timestamp
  starred: boolean;
  folder: string;
  runs: number;
  lastRunAt?: string;
};

const STORAGE_KEY = "datafinch:saved-queries";
const EVENT_NAME = "datafinch:saved-queries-changed";

// ─── Read ──────────────────────────────────────────────────────────

export function getSavedQueries(): SavedQuery[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

export function getSavedQuery(id: string): SavedQuery | null {
  return getSavedQueries().find((q) => q.id === id) ?? null;
}

// ─── Mutate ────────────────────────────────────────────────────────

function persist(queries: SavedQuery[]): void {
  if (typeof window === "undefined") return;
  localStorage.setItem(STORAGE_KEY, JSON.stringify(queries));
  window.dispatchEvent(new CustomEvent(EVENT_NAME));
}

function genId(): string {
  if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID();
  return `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
}

/**
 * Save a query. Returns the new SavedQuery. If an entry with the same
 * question+sql exists, returns it without creating a duplicate.
 */
export function saveQuery(question: string, result: QueryResult): SavedQuery {
  const all = getSavedQueries();
  const existing = all.find((q) => q.question === question && q.sql === (result.sql ?? ""));
  if (existing) return existing;

  const saved: SavedQuery = {
    id: genId(),
    title: question.length > 60 ? question.slice(0, 57).trimEnd() + "…" : question,
    question,
    sql: result.sql ?? "",
    explanation: result.explanation ?? "",
    tables: Array.isArray(result.tables_used) ? result.tables_used : [],
    backend: result.backend ?? "unknown",
    rowCount: result.row_count ?? 0,
    complexity: result.complexity ?? "unknown",
    savedAt: new Date().toISOString(),
    starred: false,
    folder: "Mặc định",
    runs: 1,
  };
  persist([saved, ...all]);
  return saved;
}

export function deleteSavedQuery(id: string): void {
  persist(getSavedQueries().filter((q) => q.id !== id));
}

export function toggleStar(id: string): void {
  persist(getSavedQueries().map((q) => (q.id === id ? { ...q, starred: !q.starred } : q)));
}

export function updateSavedQuery(id: string, updates: Partial<SavedQuery>): void {
  persist(getSavedQueries().map((q) => (q.id === id ? { ...q, ...updates } : q)));
}

export function incrementRunCount(id: string): void {
  persist(
    getSavedQueries().map((q) =>
      q.id === id ? { ...q, runs: q.runs + 1, lastRunAt: new Date().toISOString() } : q
    )
  );
}

export function clearAll(): void {
  persist([]);
}

// ─── Helpers ───────────────────────────────────────────────────────

export function isQuerySaved(question: string, sql: string): boolean {
  return getSavedQueries().some((q) => q.question === question && q.sql === sql);
}

export function formatRelativeTime(iso: string): string {
  const now = Date.now();
  const then = new Date(iso).getTime();
  const diff = Math.max(0, now - then);
  const min = Math.floor(diff / 60_000);
  if (min < 1) return "vừa xong";
  if (min < 60) return `${min} phút trước`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr} giờ trước`;
  const day = Math.floor(hr / 24);
  if (day < 7) return `${day} ngày trước`;
  const week = Math.floor(day / 7);
  if (week < 4) return `${week} tuần trước`;
  return new Date(iso).toLocaleDateString("vi-VN");
}

// ─── Subscription (for cross-component sync) ───────────────────────

export function onSavedQueriesChange(handler: () => void): () => void {
  if (typeof window === "undefined") return () => {};
  window.addEventListener(EVENT_NAME, handler);
  // Also listen to native storage event for cross-tab sync
  const storageHandler = (e: StorageEvent) => {
    if (e.key === STORAGE_KEY) handler();
  };
  window.addEventListener("storage", storageHandler);
  return () => {
    window.removeEventListener(EVENT_NAME, handler);
    window.removeEventListener("storage", storageHandler);
  };
}
