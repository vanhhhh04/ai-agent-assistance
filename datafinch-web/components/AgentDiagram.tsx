"use client";

import { useState } from "react";
import { cn } from "@/lib/utils";

const AGENTS = [
  {
    id: "supervisor",
    name: "Supervisor",
    icon: "⚡",
    color: "#0891b2",
    role: "Phân loại câu hỏi",
    detail:
      "LLM nhỏ (Claude Haiku / GPT-5-nano) phân loại intent của câu hỏi: DATA_QUERY (truy vấn data), SCHEMA_INFO (hỏi về cấu trúc DB), OUT_OF_SCOPE (chitchat, off-topic), FOLLOWUP (câu hỏi nối tiếp). Đồng thời chọn backend phù hợp (Hive Gold cho aggregation, Postgres Bronze cho realtime).",
  },
  {
    id: "retriever",
    name: "Metadata Retriever",
    icon: "🗂️",
    color: "#7c3aed",
    role: "Tìm bảng đúng",
    detail:
      "Hybrid retrieval qua OpenSearch — kết hợp kNN (semantic embedding 768-d multilingual) và BM25 (keyword). Tìm top-K bảng/cột liên quan trong catalog đã được index. Augment với full schema từ Hive Metastore để LLM thấy đầy đủ columns.",
  },
  {
    id: "sql_writer",
    name: "SQL Writer",
    icon: "✍️",
    color: "#059669",
    role: "Viết câu lệnh SQL",
    detail:
      "LLM mạnh hơn (Claude Sonnet / GPT-5 / Gemini 2.5 Flash) nhận: question + retrieved schema + dialect rules (HiveQL/PostgreSQL) + similar past queries. Output JSON {sql, explanation, tables_used, complexity, warnings}. Hỗ trợ tiếng Việt domain-specific.",
  },
  {
    id: "guardrails",
    name: "Guardrails",
    icon: "🛡️",
    color: "#ea580c",
    role: "Kiểm tra an toàn",
    detail:
      "Static analysis: chặn DELETE/UPDATE/DROP/TRUNCATE, kiểm tra LIMIT, đếm JOINs (max 5), block subquery patterns không tương thích Hive, PII column masking. Reject queries vi phạm trước khi gọi database.",
  },
  {
    id: "executor",
    name: "Executor",
    icon: "⚙️",
    color: "#db2777",
    role: "Thực thi & format",
    detail:
      "Dispatch SQL đến đúng backend (Hive via thrift, Postgres via asyncpg). Stream kết quả về frontend qua SSE. Log query vào OpenSearch query_log để self-improvement loop. Generate chart spec từ result shape.",
  },
];

export function AgentDiagram() {
  const [selected, setSelected] = useState(0);

  return (
    <div className="my-12">
      {/* SVG diagram */}
      <div className="relative max-w-5xl mx-auto px-6">
        <svg
          viewBox="0 0 800 200"
          className="w-full h-auto"
          xmlns="http://www.w3.org/2000/svg"
        >
          {/* Connecting line with gradient */}
          <defs>
            <linearGradient id="flow" x1="0%" y1="0%" x2="100%" y2="0%">
              {AGENTS.map((a, i) => (
                <stop
                  key={a.id}
                  offset={`${(i / (AGENTS.length - 1)) * 100}%`}
                  stopColor={a.color}
                />
              ))}
            </linearGradient>
            <filter id="glow">
              <feGaussianBlur stdDeviation="3" result="blur" />
              <feMerge>
                <feMergeNode in="blur" />
                <feMergeNode in="SourceGraphic" />
              </feMerge>
            </filter>
          </defs>

          {/* Line */}
          <line
            x1="80"
            y1="100"
            x2="720"
            y2="100"
            stroke="url(#flow)"
            strokeWidth="3"
            strokeLinecap="round"
            opacity="0.4"
          />

          {/* Arrows */}
          {AGENTS.slice(0, -1).map((_, i) => {
            const x = 80 + (i + 1) * (640 / AGENTS.length) - 30;
            return (
              <polygon
                key={i}
                points={`${x},94 ${x + 10},100 ${x},106`}
                fill={AGENTS[i + 1].color}
                opacity="0.6"
              />
            );
          })}

          {/* Agent nodes */}
          {AGENTS.map((agent, i) => {
            const cx = 80 + i * (640 / (AGENTS.length - 1));
            const isSelected = selected === i;
            return (
              <g
                key={agent.id}
                onClick={() => setSelected(i)}
                style={{ cursor: "pointer" }}
              >
                {/* Outer glow on select */}
                {isSelected && (
                  <circle
                    cx={cx}
                    cy={100}
                    r="48"
                    fill={agent.color}
                    opacity="0.15"
                    filter="url(#glow)"
                  >
                    <animate
                      attributeName="r"
                      values="42;48;42"
                      dur="2s"
                      repeatCount="indefinite"
                    />
                  </circle>
                )}
                {/* Main circle */}
                <circle
                  cx={cx}
                  cy={100}
                  r="34"
                  fill="white"
                  stroke={agent.color}
                  strokeWidth={isSelected ? 3 : 2}
                  className="transition-all"
                />
                {/* Icon */}
                <text
                  x={cx}
                  y={108}
                  textAnchor="middle"
                  fontSize="28"
                >
                  {agent.icon}
                </text>
                {/* Number label */}
                <circle
                  cx={cx + 24}
                  cy={76}
                  r="10"
                  fill={agent.color}
                />
                <text
                  x={cx + 24}
                  y={80}
                  textAnchor="middle"
                  fontSize="11"
                  fill="white"
                  fontWeight="700"
                >
                  {i + 1}
                </text>
                {/* Name below */}
                <text
                  x={cx}
                  y={158}
                  textAnchor="middle"
                  fontSize="12"
                  fontWeight={isSelected ? "700" : "500"}
                  fill={isSelected ? agent.color : "var(--color-text-muted)"}
                  className="select-none"
                >
                  {agent.name}
                </text>
              </g>
            );
          })}
        </svg>
      </div>

      {/* Detail panel */}
      <div className="mt-8 max-w-4xl mx-auto px-6">
        <div
          key={selected}
          className="rounded-2xl border-2 bg-white p-6 md:p-8 shadow-lg animate-fade-up"
          style={{ borderColor: AGENTS[selected].color }}
        >
          <div className="flex items-start gap-5">
            <div
              className="w-16 h-16 rounded-2xl flex items-center justify-center text-3xl flex-shrink-0"
              style={{
                background: `${AGENTS[selected].color}15`,
                border: `2px solid ${AGENTS[selected].color}40`,
              }}
            >
              {AGENTS[selected].icon}
            </div>
            <div className="flex-1">
              <div className="flex items-center gap-2 mb-1">
                <span
                  className="text-xs font-bold uppercase tracking-wider px-2 py-0.5 rounded"
                  style={{
                    background: `${AGENTS[selected].color}15`,
                    color: AGENTS[selected].color,
                  }}
                >
                  Bước {selected + 1} / {AGENTS.length}
                </span>
              </div>
              <h3 className="text-2xl font-bold text-[color:var(--color-text)] mb-1">
                {AGENTS[selected].name}
              </h3>
              <p
                className="text-sm font-medium mb-3"
                style={{ color: AGENTS[selected].color }}
              >
                {AGENTS[selected].role}
              </p>
              <p className="text-[color:var(--color-text-muted)] leading-relaxed">
                {AGENTS[selected].detail}
              </p>
            </div>
          </div>

          {/* Nav buttons */}
          <div className="flex items-center justify-between mt-6 pt-5 border-t border-[color:var(--color-border)]">
            <button
              onClick={() => setSelected(Math.max(0, selected - 1))}
              disabled={selected === 0}
              className="text-sm font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] disabled:opacity-30 disabled:cursor-not-allowed"
            >
              ← Trước
            </button>
            <div className="flex gap-1.5">
              {AGENTS.map((_, i) => (
                <button
                  key={i}
                  onClick={() => setSelected(i)}
                  className={cn(
                    "w-2 h-2 rounded-full transition-all",
                    i === selected
                      ? "w-8"
                      : "bg-[color:var(--color-border-strong)] hover:bg-[color:var(--color-text-subtle)]"
                  )}
                  style={i === selected ? { background: AGENTS[i].color } : {}}
                />
              ))}
            </div>
            <button
              onClick={() => setSelected(Math.min(AGENTS.length - 1, selected + 1))}
              disabled={selected === AGENTS.length - 1}
              className="text-sm font-medium text-[color:var(--color-text-muted)] hover:text-[color:var(--color-text)] disabled:opacity-30 disabled:cursor-not-allowed"
            >
              Sau →
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
