"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useSearchParams } from "next/navigation";
import { askQuery, checkHealth, type QueryResult, type SSEEvent } from "@/lib/api";
import { cn } from "@/lib/utils";
import { isQuerySaved, saveQuery } from "@/lib/savedQueries";

const AGENT_STEPS = [
  { id: "supervisor",       label: "Supervisor",      icon: "⚡", color: "#0891b2" },
  { id: "metadata",         label: "Metadata",        icon: "🗂️", color: "#7c3aed" },
  { id: "sql_writer",       label: "SQL Writer",      icon: "✍️", color: "#059669" },
  { id: "execution",        label: "Execute",         icon: "⚙️", color: "#ea580c" },
  { id: "result_formatter", label: "Format",          icon: "📊", color: "#db2777" },
];

const SUGGESTED = [
  "Top 5 khách hàng đặt nhiều đơn nhất",
  "Doanh thu Q1 2026 theo brand",
  "Sản phẩm bán chạy nhất tháng này",
  "Có bao nhiêu đơn hàng chưa giao?",
];

type Message =
  | { id: number; role: "user"; text: string }
  | {
      id: number;
      role: "agent";
      status: "thinking" | "done";
      question: string;
      result?: QueryResult;
      error?: string;
    };

export default function AskPage() {
  const searchParams = useSearchParams();
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [steps, setSteps] = useState<Record<string, "running" | "done" | "error">>({});
  const [stepMsgs, setStepMsgs] = useState<Record<string, string>>({});
  const [running, setRunning] = useState(false);
  const [backendOk, setBackendOk] = useState<boolean | null>(null);
  const bottomRef = useRef<HTMLDivElement | null>(null);
  const historyRef = useRef<Array<{ role: string; content: string }>>([]);
  const autoSentRef = useRef(false);

  useEffect(() => {
    checkHealth().then(setBackendOk);
  }, []);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Auto-send question pre-filled from saved page (e.g. /app/ask?q=Top%205%20...)
  useEffect(() => {
    if (autoSentRef.current) return;
    const q = searchParams?.get("q");
    if (q && !running) {
      autoSentRef.current = true;
      setInput(q);
      // Defer one tick so React commits the input value first
      setTimeout(() => send(q), 50);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams]);

  const send = useCallback(
    async (q?: string) => {
      const question = (q ?? input).trim();
      if (!question || running) return;
      setInput("");
      setRunning(true);
      setSteps({});
      setStepMsgs({});

      const userMsg: Message = { id: Date.now(), role: "user", text: question };
      const agentId = Date.now() + 1;
      const agentMsg: Message = {
        id: agentId,
        role: "agent",
        status: "thinking",
        question,
      };
      setMessages((p) => [...p, userMsg, agentMsg]);

      let result: QueryResult | undefined;
      let err: string | undefined;

      try {
        for await (const ev of askQuery(question, historyRef.current) as AsyncGenerator<SSEEvent>) {
          if (ev.type === "step") {
            setSteps((p) => ({ ...p, [ev.step]: ev.status }));
            if (ev.message) setStepMsgs((p) => ({ ...p, [ev.step]: ev.message! }));
          } else if (ev.type === "result") {
            result = ev.data;
          } else if (ev.type === "error") {
            err = ev.message;
          }
        }
      } catch (e: unknown) {
        err = e instanceof Error ? e.message : String(e);
      }

      setMessages((p) =>
        p.map((m) =>
          m.id === agentId && m.role === "agent"
            ? { ...m, status: "done", result, error: err }
            : m
        )
      );

      if (result) {
        historyRef.current = [
          ...historyRef.current,
          { role: "user", content: question },
          { role: "assistant", content: result.explanation || "" },
        ].slice(-10);
      }

      setRunning(false);
      setTimeout(() => setSteps({}), 2500);
    },
    [input, running]
  );

  return (
    <div className="flex h-screen flex-col">
      {/* Header */}
      <header className="px-6 md:px-8 h-16 border-b border-[color:var(--color-border)] bg-white flex items-center justify-between md:pl-8 pl-16">
        <div>
          <h1 className="text-base font-semibold text-[color:var(--color-text)]">Hỏi dữ liệu</h1>
          <p className="text-xs text-[color:var(--color-text-subtle)]">
            Hỏi bằng tiếng Việt, có ngay câu trả lời từ database
          </p>
        </div>
        <div className="flex items-center gap-2">
          {backendOk === true && (
            <span className="inline-flex items-center gap-1.5 text-xs font-medium text-[color:var(--color-green)] px-2 py-1 rounded-md bg-green-50 border border-green-200">
              <span className="w-1.5 h-1.5 rounded-full bg-[color:var(--color-green)] animate-pulse" />
              Backend live
            </span>
          )}
          {backendOk === false && (
            <span className="inline-flex items-center gap-1.5 text-xs font-medium text-[color:var(--color-warning)] px-2 py-1 rounded-md bg-orange-50 border border-orange-200">
              <span className="w-1.5 h-1.5 rounded-full bg-[color:var(--color-warning)]" />
              Backend offline
            </span>
          )}
        </div>
      </header>

      {/* Agent pipeline (visible when running) */}
      {Object.keys(steps).length > 0 && (
        <div className="px-6 md:px-8 py-3 border-b border-[color:var(--color-border)] bg-[color:var(--color-bg-muted)]">
          <div className="flex items-center gap-1.5 overflow-x-auto">
            {AGENT_STEPS.map((s, i) => {
              const st = steps[s.id];
              return (
                <div key={s.id} className="flex items-center gap-1.5 flex-shrink-0">
                  <div
                    className={cn(
                      "flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-medium transition-all",
                      !st && "bg-white text-[color:var(--color-text-subtle)] border border-[color:var(--color-border)]",
                      st === "running" && "shadow-sm",
                      st === "done" && "shadow-sm",
                      st === "error" && "bg-red-50 text-[color:var(--color-danger)] border border-red-200"
                    )}
                    style={
                      st === "running" || st === "done"
                        ? { background: `${s.color}15`, color: s.color, border: `1px solid ${s.color}40` }
                        : undefined
                    }
                  >
                    <span>{s.icon}</span>
                    <span>{s.label}</span>
                    {st === "running" && (
                      <span className="w-1 h-1 rounded-full bg-current animate-pulse" />
                    )}
                    {st === "done" && <span>✓</span>}
                  </div>
                  {i < AGENT_STEPS.length - 1 && (
                    <span className="text-[color:var(--color-text-subtle)] text-xs">→</span>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Messages area */}
      <div className="flex-1 overflow-y-auto px-4 md:px-8 py-6">
        {messages.length === 0 && (
          <EmptyState onPick={send} backendOk={backendOk} />
        )}

        {messages.map((m) =>
          m.role === "user" ? (
            <div key={m.id} className="mb-5 flex justify-end animate-fade-up">
              <div className="max-w-[78%] px-4 py-2.5 rounded-2xl rounded-br-md bg-[color:var(--color-primary)] text-white text-sm md:text-base font-medium shadow-sm">
                {m.text}
              </div>
            </div>
          ) : (
            <AgentBubble key={m.id} message={m} stepMsgs={stepMsgs} />
          )
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="px-4 md:px-8 py-4 border-t border-[color:var(--color-border)] bg-white">
        <div className="flex items-center gap-2 max-w-4xl mx-auto">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && send()}
            placeholder="Nhập câu hỏi về dữ liệu..."
            disabled={running}
            className="flex-1 h-12 px-4 rounded-xl border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30 focus:border-[color:var(--color-primary)] disabled:opacity-60"
          />
          <button
            onClick={() => send()}
            disabled={running || !input.trim()}
            className="h-12 px-5 rounded-xl bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)] disabled:opacity-40 disabled:cursor-not-allowed shadow-sm shadow-cyan-500/20 transition-all"
          >
            {running ? (
              <span className="inline-block animate-spin">⟳</span>
            ) : (
              <>Gửi →</>
            )}
          </button>
        </div>
        <p className="text-center text-xs text-[color:var(--color-text-subtle)] mt-2">
          Enter để gửi · Hỗ trợ tiếng Việt · Câu hỏi nối tiếp được hỗ trợ
        </p>
      </div>
    </div>
  );
}

function EmptyState({
  onPick,
  backendOk,
}: {
  onPick: (q: string) => void;
  backendOk: boolean | null;
}) {
  return (
    <div className="max-w-2xl mx-auto pt-12 md:pt-20 text-center">
      <div className="text-5xl mb-5">◈</div>
      <h2 className="text-2xl md:text-3xl font-bold text-[color:var(--color-text)] mb-3">
        Xin chào! Tôi là DataFinch
      </h2>
      <p className="text-[color:var(--color-text-muted)] mb-8">
        Hỏi tôi bất kỳ câu hỏi nào về dữ liệu của bạn
      </p>
      {backendOk === false && (
        <div className="mb-6 p-3 rounded-lg bg-orange-50 border border-orange-200 text-sm text-[color:var(--color-warning)]">
          ⚠️ Backend FastAPI chưa kết nối. Khởi động:{" "}
          <code className="font-mono text-xs bg-white px-1.5 py-0.5 rounded">
            docker compose up ai-agent
          </code>
        </div>
      )}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-2.5">
        {SUGGESTED.map((q) => (
          <button
            key={q}
            onClick={() => onPick(q)}
            className="text-left p-4 rounded-xl border border-[color:var(--color-border)] bg-white hover:border-[color:var(--color-primary)] hover:bg-[color:var(--color-primary-faded)] transition-all text-sm text-[color:var(--color-text)] font-medium"
          >
            <span className="text-[color:var(--color-text-subtle)] mr-2">→</span>
            {q}
          </button>
        ))}
      </div>
    </div>
  );
}

function AgentBubble({
  message,
  stepMsgs,
}: {
  message: Extract<Message, { role: "agent" }>;
  stepMsgs: Record<string, string>;
}) {
  const [showSql, setShowSql] = useState(false);
  const [saveState, setSaveState] = useState<"idle" | "saved">("idle");
  const [shareState, setShareState] = useState<"idle" | "copied">("idle");
  const [vote, setVote] = useState<"up" | "down" | null>(null);

  // Check if already in localStorage on mount (so reopening shows persisted state)
  useEffect(() => {
    if (message.result?.sql && isQuerySaved(message.question, message.result.sql)) {
      setSaveState("saved");
    }
  }, [message]);

  const handleSave = () => {
    if (!message.result || saveState === "saved") return;
    saveQuery(message.question, message.result);
    setSaveState("saved");
  };

  const handleShare = async () => {
    if (!message.result?.sql) return;
    const text = `Câu hỏi: ${message.question}\n\nSQL:\n${message.result.sql}`;
    try {
      await navigator.clipboard.writeText(text);
      setShareState("copied");
      setTimeout(() => setShareState("idle"), 2000);
    } catch {
      // Fallback for browsers without clipboard API
    }
  };

  if (message.status === "thinking") {
    const lastMsg = Object.values(stepMsgs).pop();
    return (
      <div className="mb-5 flex items-start gap-3 animate-fade-up">
        <Avatar />
        <div className="flex-1 max-w-[88%]">
          <div className="rounded-2xl rounded-tl-md bg-white border border-[color:var(--color-border)] p-4 shadow-sm">
            <div className="flex items-center gap-2 text-sm text-[color:var(--color-text-muted)]">
              <span className="inline-block w-4 h-4 rounded-full border-2 border-[color:var(--color-primary)] border-t-transparent animate-spin" />
              <span>{lastMsg || "Đang xử lý..."}</span>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (message.error || !message.result) {
    return (
      <div className="mb-5 flex items-start gap-3 animate-fade-up">
        <Avatar />
        <div className="flex-1 max-w-[88%]">
          <div className="rounded-2xl rounded-tl-md bg-red-50 border border-red-200 p-4">
            <p className="text-sm font-semibold text-[color:var(--color-danger)] mb-1">
              ✗ Không thể xử lý câu hỏi
            </p>
            <p className="text-sm text-[color:var(--color-text-muted)]">
              {message.error || "Có lỗi không xác định"}
            </p>
          </div>
        </div>
      </div>
    );
  }

  const r = message.result;

  return (
    <div className="mb-5 flex items-start gap-3 animate-fade-up">
      <Avatar />
      <div className="flex-1 max-w-[88%] space-y-3">
        {/* Explanation */}
        <div className="rounded-2xl rounded-tl-md bg-white border border-[color:var(--color-border)] p-4 shadow-sm">
          <p className="text-sm md:text-base text-[color:var(--color-text)] leading-relaxed">
            {r.explanation}
          </p>

          {/* Chips */}
          <div className="flex flex-wrap gap-1.5 mt-3">
            {r.intent && <Chip>{r.intent}</Chip>}
            {r.complexity && <Chip>{r.complexity}</Chip>}
            {r.row_count !== undefined && (
              <Chip color="var(--color-green)" bg="#d1fae5">
                {r.row_count} rows
              </Chip>
            )}
            {r.tables_used?.map((t) => (
              <Chip key={t} color="var(--color-purple)" bg="#ede9fe">
                {t}
              </Chip>
            ))}
            {r.exec_ms !== undefined && (
              <Chip color="var(--color-text-subtle)" bg="var(--color-bg-subtle)">
                {r.exec_ms}ms
              </Chip>
            )}
          </div>

          {/* SQL toggle */}
          {r.sql && (
            <>
              <button
                onClick={() => setShowSql((v) => !v)}
                className="mt-3 text-xs font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)]"
              >
                {showSql ? "▼" : "▶"} Xem SQL
              </button>
              {showSql && (
                <pre className="mt-2 p-3 rounded-lg bg-[color:var(--color-bg-muted)] border border-[color:var(--color-border)] text-xs font-mono text-[color:var(--color-green)] whitespace-pre-wrap leading-relaxed overflow-x-auto">
                  {r.sql}
                </pre>
              )}
            </>
          )}
        </div>

        {/* Result table */}
        {r.rows && r.rows.length > 0 && (
          <ResultTable cols={r.columns} rows={r.rows} />
        )}

        {/* Schema info (for SCHEMA_INFO intent) */}
        {r.schema_info && (
          <div className="rounded-xl bg-white border border-[color:var(--color-border)] p-4 shadow-sm">
            <p className="text-xs font-bold uppercase tracking-wider text-[color:var(--color-text-subtle)] mb-3">
              Schema có sẵn
            </p>
            <div className="space-y-2">
              {Object.entries(r.schema_info).map(([t, cols]) => (
                <div key={t} className="text-sm">
                  <span className="font-semibold text-[color:var(--color-purple)] font-mono">
                    {t}
                  </span>
                  <span className="text-[color:var(--color-text-subtle)] ml-2">
                    ({cols.length} cols)
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Actions — only Save / Share / Feedback (no Report/Alert since
            those features aren't wired up yet). */}
        <div className="flex items-center gap-1 text-xs">
          <button
            onClick={handleSave}
            disabled={saveState === "saved"}
            className={cn(
              "px-2.5 py-1 rounded-md flex items-center gap-1.5 transition-all font-medium",
              saveState === "saved"
                ? "text-[color:var(--color-green)] bg-green-50 cursor-default"
                : "text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]"
            )}
            title="Lưu câu hỏi này vào danh sách đã lưu (localStorage)"
          >
            {saveState === "saved" ? (
              <>
                <span>✓</span>
                <span>Đã lưu</span>
              </>
            ) : (
              <>
                <span>💾</span>
                <span>Lưu</span>
              </>
            )}
          </button>

          <button
            onClick={handleShare}
            className={cn(
              "px-2.5 py-1 rounded-md flex items-center gap-1.5 transition-all font-medium",
              shareState === "copied"
                ? "text-[color:var(--color-green)] bg-green-50"
                : "text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]"
            )}
            title="Copy câu hỏi + SQL vào clipboard"
          >
            <span>{shareState === "copied" ? "✓" : "📤"}</span>
            <span>{shareState === "copied" ? "Đã copy" : "Share"}</span>
          </button>

          <div className="ml-auto flex items-center gap-1">
            <button
              onClick={() => setVote(vote === "up" ? null : "up")}
              className={cn(
                "px-2 py-1 rounded-md transition-all",
                vote === "up"
                  ? "bg-green-50 text-[color:var(--color-green)]"
                  : "text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]"
              )}
              title="Câu trả lời chính xác"
            >
              👍
            </button>
            <button
              onClick={() => setVote(vote === "down" ? null : "down")}
              className={cn(
                "px-2 py-1 rounded-md transition-all",
                vote === "down"
                  ? "bg-red-50 text-[color:var(--color-danger)]"
                  : "text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] hover:bg-[color:var(--color-bg-subtle)]"
              )}
              title="Câu trả lời sai"
            >
              👎
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function Avatar() {
  return (
    <div className="w-9 h-9 rounded-full bg-gradient-to-br from-[color:var(--color-primary)] to-[color:var(--color-purple)] flex items-center justify-center text-white font-bold flex-shrink-0 shadow-sm">
      ◈
    </div>
  );
}

function Chip({
  children,
  color = "var(--color-text-muted)",
  bg = "var(--color-bg-subtle)",
}: {
  children: React.ReactNode;
  color?: string;
  bg?: string;
}) {
  return (
    <span
      className="inline-block text-xs px-2 py-0.5 rounded-md font-medium"
      style={{ background: bg, color, border: `1px solid ${color}25` }}
    >
      {children}
    </span>
  );
}

function ResultTable({ cols, rows }: { cols: string[]; rows: Record<string, unknown>[] }) {
  return (
    <div className="rounded-xl bg-white border border-[color:var(--color-border)] overflow-hidden shadow-sm">
      <div className="px-4 py-2.5 bg-[color:var(--color-bg-muted)] border-b border-[color:var(--color-border)] text-xs font-bold uppercase tracking-wider text-[color:var(--color-text-muted)] flex items-center justify-between">
        <span>📊 Kết quả · {rows.length} rows</span>
        <button className="text-[color:var(--color-text-subtle)] hover:text-[color:var(--color-text)]">
          ⬇ Export CSV
        </button>
      </div>
      <div className="overflow-x-auto max-h-96">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-white border-b border-[color:var(--color-border)]">
            <tr>
              {cols.map((c) => (
                <th
                  key={c}
                  className="px-4 py-2.5 text-left font-mono text-xs font-bold uppercase tracking-wide text-[color:var(--color-text-subtle)] whitespace-nowrap"
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-[color:var(--color-border)]">
            {rows.map((row, i) => (
              <tr key={i} className="hover:bg-[color:var(--color-bg-muted)]/50">
                {cols.map((c) => (
                  <td
                    key={c}
                    className="px-4 py-2.5 text-[color:var(--color-text)] font-mono whitespace-nowrap"
                  >
                    {String(row[c] ?? "—")}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
