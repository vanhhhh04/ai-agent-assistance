/**
 * Lightweight API client for the DataFinch backend (FastAPI).
 * Backend runs separately on port 8000 by default. Override via NEXT_PUBLIC_API_BASE.
 */

export const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

export type SSEEvent =
  | { type: "step"; step: string; status: "running" | "done" | "error"; message?: string; data?: unknown }
  | { type: "result"; data: QueryResult }
  | { type: "error"; message: string };

export type QueryResult = {
  intent: string;
  backend: string;
  sql: string | null;
  explanation: string;
  tables_used: string[];
  complexity: string;
  warnings: string[];
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  exec_ms?: number;
  total_ms?: number;
  query_id?: string;
  retrieval?: { catalog: number; docs: number; history: number; tables?: string[] };
  aggregated?: boolean;
  schema_info?: Record<string, string[]>;
};

/**
 * Stream the /api/query/ask SSE endpoint. Yields each event as it arrives.
 * The backend already chunks `data: {...}\n\n` lines per SSE spec.
 */
export async function* askQuery(
  question: string,
  history: Array<{ role: string; content: string }> = [],
  signal?: AbortSignal
): AsyncGenerator<SSEEvent> {
  const res = await fetch(`${API_BASE}/api/query/ask`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, conversation_history: history }),
    signal,
  });

  if (!res.ok || !res.body) {
    throw new Error(`Backend returned HTTP ${res.status}`);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });

    const chunks = buf.split("\n\n");
    buf = chunks.pop() ?? "";

    for (const chunk of chunks) {
      const line = chunk.trim();
      if (!line.startsWith("data: ")) continue;
      try {
        yield JSON.parse(line.slice(6)) as SSEEvent;
      } catch {
        // Malformed JSON — skip
      }
    }
  }
}

export async function checkHealth(): Promise<boolean> {
  try {
    const res = await fetch(`${API_BASE}/api/health/ping`, { signal: AbortSignal.timeout(3000) });
    return res.ok;
  } catch {
    return false;
  }
}
