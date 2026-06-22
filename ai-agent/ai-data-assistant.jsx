import { useState, useRef, useEffect, useCallback } from "react";

const API_BASE = "http://localhost:8000/api";

const AGENT_STEPS = [
  { id: "supervisor",       label: "Supervisor Agent",  icon: "⚡", color: "#0891B2" },
  { id: "metadata",         label: "Metadata Lookup",   icon: "🗂️", color: "#7C3AED" },
  { id: "sql_writer",       label: "SQL Writer Agent",  icon: "✍️", color: "#059669" },
  { id: "execution",        label: "Query Execution",   icon: "⚙️", color: "#EA580C" },
  { id: "result_formatter", label: "Result Formatter",  icon: "📊", color: "#DB2777" },
];

const SUGGESTED = [
  "Tổng doanh thu tháng 4 năm 2026 là bao nhiêu?",
  "Top 5 sản phẩm bán chạy nhất?",
  "So sánh doanh thu Q1 2025 và Q1 2026",
  "Có bao nhiêu khách hàng mua hàng tháng này?",
];

async function* streamPipeline(question, history) {
  const res = await fetch(`${API_BASE}/query/ask`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, conversation_history: history }),
  });
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let buf = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    const chunks = buf.split("\n\n");
    buf = chunks.pop();
    for (const chunk of chunks) {
      if (chunk.startsWith("data: ")) {
        try { yield JSON.parse(chunk.slice(6)); } catch {}
      }
    }
  }
}

export default function App() {
  const [messages, setMessages]       = useState([]);
  const [input, setInput]             = useState("");
  const [steps, setSteps]             = useState({});
  const [stepMsgs, setStepMsgs]       = useState({});
  const [running, setRunning]         = useState(false);
  const [schema, setSchema]           = useState({});
  const [tab, setTab]                 = useState("chat");
  const [backendOk, setBackendOk]     = useState(null);
  const bottomRef  = useRef(null);
  const historyRef = useRef([]);

  useEffect(() => {
    fetch(`${API_BASE}/health`)
      .then(() => setBackendOk(true))
      .catch(() => setBackendOk(false));
    fetch(`${API_BASE}/schema/full`)
      .then(r => r.json())
      .then(d => setSchema(d.schema || {}))
      .catch(() => {});
  }, []);

  useEffect(() => { bottomRef.current?.scrollIntoView({ behavior: "smooth" }); }, [messages]);

  const send = useCallback(async (q) => {
    const question = (q || input).trim();
    if (!question || running) return;
    setInput(""); setRunning(true); setSteps({}); setStepMsgs({});

    setMessages(p => [...p, { role: "user", text: question }]);
    const id = Date.now();
    setMessages(p => [...p, { role: "agent", status: "thinking", id }]);

    let result = null, err = null;

    try {
      for await (const ev of streamPipeline(question, historyRef.current)) {
        if (ev.type === "step") {
          setSteps(p => ({ ...p, [ev.step]: ev.status }));
          if (ev.message) setStepMsgs(p => ({ ...p, [ev.step]: ev.message }));
        } else if (ev.type === "result") {
          result = ev.data;
        } else if (ev.type === "error") {
          err = ev.message;
        }
      }
    } catch (e) { err = `Cannot reach backend: ${e.message}`; }

    setMessages(p => p.map(m => m.id === id
      ? { role: "agent", status: "done", id, result, error: err }
      : m
    ));

    if (result) {
      historyRef.current = [
        ...historyRef.current,
        { role: "user", content: question },
        { role: "assistant", content: result.explanation || "" },
      ].slice(-10);
    }

    setRunning(false);
    setTimeout(() => setSteps({}), 3500);
  }, [input, running]);

  return (
    <div style={S.root}>
      {/* ── Sidebar ── */}
      <aside style={S.sidebar}>
        <div style={S.logo}>
          <span style={{ fontSize: 22, color: "#0891B2" }}>◈</span>
          <span style={{ fontSize: 13, fontWeight: 700, letterSpacing: 2, color: "#0f172a" }}>DataFinch</span>
          <span style={S.pill}>{backendOk === true ? "● LIVE" : backendOk === false ? "○ OFFLINE" : "…"}</span>
        </div>

        <Section label="STACK">
          {[["🗄","PostgreSQL","#2563EB"],["⚡","NiFi + Kafka","#7C3AED"],
            ["🔥","Spark + HDFS","#059669"],["🏛","Hive DWH","#D97706"],
            ["🚀","FastAPI","#DB2777"],["🤖","Claude Agents","#0891B2"]
          ].map(([icon, name, color], i, arr) => (
            <div key={name}>
              <div style={{ display:"flex", alignItems:"center", gap:5,
                padding:"4px 7px", borderRadius:4, background:`${color}15`, border:`1px solid ${color}30` }}>
                <span style={{ fontSize:10 }}>{icon}</span>
                <span style={{ fontSize:10, color, fontFamily:"monospace", fontWeight:600 }}>{name}</span>
              </div>
              {i < arr.length-1 && <div style={{ width:1, height:4, background:"#cbd5e1", marginLeft:12 }} />}
            </div>
          ))}
        </Section>

        <Section label="AGENT PIPELINE">
          {AGENT_STEPS.map(step => {
            const st = steps[step.id], msg = stepMsgs[step.id];
            return (
              <div key={step.id} style={{ display:"flex", alignItems:"flex-start", gap:7,
                opacity: st ? 1 : 0.45, transition:"opacity 0.4s", padding:"2px 0" }}>
                <div style={{ width:8, height:8, borderRadius:"50%", marginTop:3, flexShrink:0,
                  background: st ? step.color : "#cbd5e1",
                  boxShadow: st==="running" ? `0 0 10px ${step.color}` : "none",
                  animation: st==="running" ? "blink 0.8s infinite" : "none", transition:"all 0.3s" }} />
                <div style={{ flex:1 }}>
                  <div style={{ fontSize:10, color: st ? "#0f172a" : "#94a3b8", fontWeight: st ? 600 : 400 }}>
                    {step.icon} {step.label}
                  </div>
                  {msg && st==="running" && <div style={{ fontSize:9, color:step.color, marginTop:1, fontWeight:600 }}>{msg}</div>}
                </div>
                {st==="done"  && <span style={{ color:step.color, fontSize:11, fontWeight:700 }}>✓</span>}
                {st==="error" && <span style={{ color:"#DC2626", fontSize:11, fontWeight:700 }}>✗</span>}
                {st==="running" && <span style={{ color:step.color, fontSize:8, animation:"blink 0.5s infinite" }}>●●</span>}
              </div>
            );
          })}
        </Section>

        <Section label={`SCHEMA ${Object.keys(schema).length > 0 ? "● "+Object.keys(schema).length+" tables" : ""}`}>
          {Object.keys(schema).length === 0
            ? <p style={{ fontSize:9, color:"#94a3b8" }}>Offline — start backend</p>
            : Object.entries(schema).slice(0,6).map(([t, cols]) => (
                <div key={t} style={{ padding:"4px 7px", background:"#f8fafc",
                  borderRadius:4, border:"1px solid #e2e8f0", marginBottom:3 }}>
                  <p style={{ fontSize:10, color:"#7C3AED", fontFamily:"monospace", fontWeight:600 }}>{t}</p>
                  <p style={{ fontSize:9, color:"#64748b" }}>
                    {(cols||[]).slice(0,3).map(c=>c.column||c).join(", ")}…
                  </p>
                </div>
              ))
          }
        </Section>
      </aside>

      {/* ── Main ── */}
      <main style={S.main}>
        <header style={S.topbar}>
          <div>
            <h1 style={{ fontSize:15, fontWeight:700, letterSpacing:1, color:"#0f172a" }}>AI Data Assistant</h1>
            <p style={{ fontSize:10, color:"#64748b", marginTop:2 }}>
              Multi-Agent · FastAPI + PostgreSQL · Anthropic Claude Sonnet
            </p>
          </div>
          <div style={{ display:"flex", gap:3, background:"#f1f5f9", padding:3,
            borderRadius:6, border:"1px solid #e2e8f0" }}>
            {["chat","schema"].map(t => (
              <button key={t} onClick={() => setTab(t)}
                style={{ padding:"5px 12px", borderRadius:4, border:"none", fontFamily:"inherit",
                  background: tab===t ? "#ffffff" : "transparent",
                  color: tab===t ? "#0f172a" : "#64748b",
                  boxShadow: tab===t ? "0 1px 3px rgba(15,23,42,0.08)" : "none",
                  cursor:"pointer", fontSize:11, fontWeight: tab===t ? 600 : 400 }}>
                {t === "chat" ? "💬 Chat" : "🗄️ Schema"}
              </button>
            ))}
          </div>
        </header>

        {tab === "chat" ? (
          <>
            <div style={S.chatArea}>
              {messages.length === 0 && (
                <div style={{ display:"flex", flexDirection:"column", alignItems:"center",
                  justifyContent:"center", flex:1, gap:14, paddingTop:40 }}>
                  <div style={{ fontSize:48, color:"#0891B2" }}>◈</div>
                  <h2 style={{ fontSize:20, fontWeight:700, color:"#0f172a" }}>Xin chào! Tôi là DataFinch</h2>
                  <p style={{ fontSize:12, color:"#64748b" }}>Hỏi tôi bất kỳ câu hỏi về dữ liệu thương mại điện tử</p>
                  <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:8, maxWidth:560 }}>
                    {SUGGESTED.map(q => (
                      <button key={q} onClick={() => send(q)} style={S.suggestBtn}>{q}</button>
                    ))}
                  </div>
                </div>
              )}

              {messages.map((msg, i) => (
                <div key={i} style={{ display:"flex",
                  justifyContent: msg.role==="user" ? "flex-end" : "flex-start",
                  animation:"fadeSlide 0.25s ease" }}>
                  {msg.role === "user"
                    ? <div style={S.userBubble}>{msg.text}</div>
                    : <AgentMsg msg={msg} />
                  }
                </div>
              ))}
              <div ref={bottomRef} />
            </div>

            <div style={{ padding:"10px 20px 14px", borderTop:"1px solid #e2e8f0", background:"#ffffff" }}>
              <div style={{ display:"flex", gap:7 }}>
                <input style={S.input} value={input}
                  onChange={e => setInput(e.target.value)}
                  onKeyDown={e => e.key==="Enter" && send()}
                  placeholder="Nhập câu hỏi về dữ liệu... (Enter để gửi)"
                  disabled={running} />
                <button style={{ ...S.sendBtn, opacity: running ? 0.4 : 1 }}
                  onClick={() => send()} disabled={running}>
                  {running ? "⟳" : "→"}
                </button>
              </div>
              <p style={{ fontSize:9, color:"#94a3b8", marginTop:5 }}>
                Hỗ trợ tiếng Việt · Câu hỏi nối tiếp (follow-up) được hỗ trợ
              </p>
            </div>
          </>
        ) : (
          <SchemaTab schema={schema} />
        )}
      </main>

      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap');
        @keyframes blink{0%,100%{opacity:1}50%{opacity:0.3}}
        @keyframes spin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}
        @keyframes fadeSlide{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
        *{box-sizing:border-box;margin:0;padding:0}
        ::-webkit-scrollbar{width:6px;height:6px}
        ::-webkit-scrollbar-track{background:#f1f5f9}
        ::-webkit-scrollbar-thumb{background:#cbd5e1;border-radius:3px}
        ::-webkit-scrollbar-thumb:hover{background:#94a3b8}
        button:hover{filter:brightness(0.97)}
        input::placeholder{color:#94a3b8}
      `}</style>
    </div>
  );
}

function Section({ label, children }) {
  return (
    <div style={{ display:"flex", flexDirection:"column", gap:4 }}>
      <p style={{ fontSize:9, color:"#64748b", letterSpacing:2, fontWeight:700, marginBottom:3 }}>{label}</p>
      {children}
    </div>
  );
}

function AgentMsg({ msg }) {
  const [showSql, setShowSql] = useState(false);
  if (msg.status === "thinking") return (
    <div style={S.agentBubble}>
      <span style={{ fontSize:15, color:"#0891B2", animation:"spin 1.2s linear infinite", display:"inline-block" }}>⟳</span>
      <span style={{ color:"#64748b", fontSize:12, marginLeft:8 }}>Agent đang xử lý...</span>
    </div>
  );
  const r = msg.result;
  if (!r) return <div style={S.agentBubble}><p style={{ color:"#DC2626", fontSize:13 }}>{msg.error || "Lỗi hệ thống"}</p></div>;
  return (
    <div style={S.agentBubble}>
      <p style={{ fontSize:13, color:"#1e293b", lineHeight:1.65 }}>{r.explanation}</p>
      <div style={{ display:"flex", gap:5, flexWrap:"wrap" }}>
        {r.intent && <Chip>{r.intent}</Chip>}
        {r.complexity && <Chip>{r.complexity}</Chip>}
        {r.row_count !== undefined && <Chip color="#059669" bg="#D1FAE5">{r.row_count} rows</Chip>}
        {r.tables_used?.map(t => <Chip key={t} color="#7C3AED" bg="#EDE9FE">{t}</Chip>)}
      </div>
      {r.sql && (
        <>
          <button onClick={() => setShowSql(v=>!v)}
            style={{ fontSize:10, color:"#64748b", background:"none", border:"none",
              cursor:"pointer", fontFamily:"inherit", textAlign:"left", fontWeight:600 }}>
            {showSql ? "▼" : "▶"} Xem SQL
          </button>
          {showSql && (
            <div style={{ background:"#f8fafc", border:"1px solid #e2e8f0", borderRadius:5, padding:10 }}>
              <pre style={{ fontSize:11, color:"#059669", fontFamily:"monospace", whiteSpace:"pre-wrap", lineHeight:1.65 }}>
                {r.sql}
              </pre>
            </div>
          )}
        </>
      )}
      {r.rows?.length > 0 && <ResultTable cols={r.columns} rows={r.rows} />}
    </div>
  );
}

function Chip({ children, color="#475569", bg="#f1f5f9" }) {
  return <span style={{ fontSize:9, padding:"3px 8px", background:bg,
    borderRadius:10, color, border:`1px solid ${color}30`, fontWeight:600 }}>{children}</span>;
}

function ResultTable({ cols, rows }) {
  if (!rows?.length) return null;
  return (
    <div style={{ background:"#f8fafc", border:"1px solid #e2e8f0", borderRadius:5, padding:10 }}>
      <p style={{ fontSize:9, color:"#64748b", marginBottom:6, letterSpacing:1, fontWeight:700 }}>📊 KẾT QUẢ</p>
      <div style={{ overflowX:"auto" }}>
        <table style={{ width:"100%", borderCollapse:"collapse", fontSize:11 }}>
          <thead>
            <tr>{cols.map(c=><th key={c} style={{ padding:"6px 10px", textAlign:"left",
              color:"#64748b", borderBottom:"2px solid #e2e8f0", fontFamily:"monospace", fontSize:9, fontWeight:700 }}>{c}</th>)}</tr>
          </thead>
          <tbody>
            {rows.map((row,i)=>(
              <tr key={i} style={{ background: i%2===0 ? "transparent" : "#ffffff" }}>
                {cols.map(c=><td key={c} style={{ padding:"6px 10px", color:"#1e293b",
                  fontFamily:"monospace", borderBottom:"1px solid #f1f5f9" }}>{row[c]??"-"}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p style={{ fontSize:9, color:"#94a3b8", marginTop:5 }}>{rows.length} rows · PostgreSQL · LIMIT 100</p>
    </div>
  );
}

function SchemaTab({ schema }) {
  const empty = Object.keys(schema).length === 0;
  return (
    <div style={{ padding:22, overflowY:"auto", flex:1, background:"#f8fafc" }}>
      <h2 style={{ fontSize:16, marginBottom:4, color:"#0f172a" }}>Live Schema — PostgreSQL</h2>
      <p style={{ fontSize:11, color:"#64748b", marginBottom:18 }}>
        {empty ? "⚠️ Backend offline" : `${Object.keys(schema).length} tables · Gold Layer · Star Schema`}
      </p>
      {empty
        ? <code style={{ color:"#059669", fontSize:12 }}>uvicorn main:app --reload</code>
        : <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:11 }}>
            {Object.entries(schema).map(([t, cols]) => (
              <div key={t} style={{ background:"#ffffff", border:"1px solid #e2e8f0", borderRadius:8, padding:12,
                boxShadow:"0 1px 2px rgba(15,23,42,0.04)" }}>
                <p style={{ color: t.startsWith("fact") ? "#EA580C" : "#7C3AED",
                  fontFamily:"monospace", fontSize:10, marginBottom:8, letterSpacing:1, fontWeight:700 }}>
                  {t.startsWith("fact") ? "◆ FACT" : "◇ DIM"} · {t}
                </p>
                {(cols||[]).map(c => {
                  const col = typeof c==="string" ? c : c.column;
                  const type = typeof c==="object" ? c.type : "";
                  return (
                    <div key={col} style={{ display:"flex", alignItems:"center", gap:6,
                      padding:"3px 0", borderBottom:"1px solid #f1f5f9" }}>
                      <span style={{ width:6, height:6, borderRadius:"50%", flexShrink:0,
                        background: col?.includes("id") || col?.includes("key") ? "#059669" : "#cbd5e1" }} />
                      <span style={{ fontSize:10, fontFamily:"monospace",
                        color: col?.includes("id") || col?.includes("key") ? "#059669" : "#1e293b", fontWeight: 500 }}>{col}</span>
                      {type && <span style={{ fontSize:8, color:"#94a3b8", marginLeft:"auto" }}>{type}</span>}
                    </div>
                  );
                })}
              </div>
            ))}
          </div>
      }
    </div>
  );
}

const S = {
  root: { display:"flex", height:"100vh", background:"#f8fafc", color:"#0f172a",
    fontFamily:"'JetBrains Mono',monospace", overflow:"hidden" },
  sidebar: { width:230, background:"#ffffff", borderRight:"1px solid #e2e8f0",
    display:"flex", flexDirection:"column", padding:14, gap:18, overflowY:"auto", flexShrink:0,
    boxShadow:"1px 0 3px rgba(15,23,42,0.03)" },
  logo: { display:"flex", alignItems:"center", gap:7, padding:"5px 0" },
  pill: { fontSize:8, padding:"2px 7px", borderRadius:10, background:"#CFFAFE",
    color:"#0891B2", border:"1px solid #0891B240", marginLeft:"auto", fontWeight:700 },
  main: { flex:1, display:"flex", flexDirection:"column", overflow:"hidden", background:"#f8fafc" },
  topbar: { display:"flex", justifyContent:"space-between", alignItems:"center",
    padding:"14px 22px", borderBottom:"1px solid #e2e8f0", background:"#ffffff" },
  chatArea: { flex:1, overflowY:"auto", padding:20, display:"flex", flexDirection:"column", gap:14,
    background:"#f8fafc" },
  userBubble: { background:"#0891B2", color:"#ffffff", borderRadius:"12px 12px 2px 12px",
    padding:"10px 14px", maxWidth:"65%", fontSize:13, fontWeight:500,
    boxShadow:"0 2px 4px rgba(8,145,178,0.15)" },
  agentBubble: { background:"#ffffff", border:"1px solid #e2e8f0",
    borderRadius:"12px 12px 12px 2px", padding:14, maxWidth:"83%",
    display:"flex", flexDirection:"column", gap:10,
    boxShadow:"0 1px 3px rgba(15,23,42,0.05)" },
  suggestBtn: { padding:"10px 13px", background:"#ffffff", border:"1px solid #e2e8f0",
    borderRadius:7, color:"#1e293b", cursor:"pointer", fontSize:11, textAlign:"left",
    fontFamily:"inherit", fontWeight:500,
    boxShadow:"0 1px 2px rgba(15,23,42,0.04)", transition:"all 0.15s" },
  input: { flex:1, background:"#ffffff", border:"1px solid #cbd5e1", borderRadius:7,
    padding:"10px 14px", color:"#0f172a", fontSize:12, fontFamily:"inherit", outline:"none",
    transition:"border 0.15s" },
  sendBtn: { width:44, background:"#0891B2", border:"none", borderRadius:7,
    color:"#ffffff", fontSize:18, cursor:"pointer", fontWeight:700,
    boxShadow:"0 2px 4px rgba(8,145,178,0.25)" },
};
