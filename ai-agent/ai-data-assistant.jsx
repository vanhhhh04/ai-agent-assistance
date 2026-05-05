import { useState, useRef, useEffect, useCallback } from "react";

const API_BASE = "http://localhost:8000/api";

const AGENT_STEPS = [
  { id: "supervisor",       label: "Supervisor Agent",  icon: "⚡", color: "#00D4FF" },
  { id: "metadata",         label: "Metadata Lookup",   icon: "🗂️", color: "#A78BFA" },
  { id: "sql_writer",       label: "SQL Writer Agent",  icon: "✍️", color: "#34D399" },
  { id: "execution",        label: "Query Execution",   icon: "⚙️", color: "#FB923C" },
  { id: "result_formatter", label: "Result Formatter",  icon: "📊", color: "#F472B6" },
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
          <span style={{ fontSize: 20, color: "#00D4FF" }}>◈</span>
          <span style={{ fontSize: 13, fontWeight: 700, letterSpacing: 2 }}>DataFinch</span>
          <span style={S.pill}>{backendOk === true ? "● LIVE" : backendOk === false ? "○ OFFLINE" : "…"}</span>
        </div>

        <Section label="STACK">
          {[["🗄","PostgreSQL","#3B82F6"],["⚡","NiFi + Kafka","#8B5CF6"],
            ["🔥","Spark + HDFS","#10B981"],["🏛","Hive DWH","#F59E0B"],
            ["🚀","FastAPI","#EC4899"],["🤖","Claude Agents","#00D4FF"]
          ].map(([icon, name, color], i, arr) => (
            <div key={name}>
              <div style={{ display:"flex", alignItems:"center", gap:5,
                padding:"3px 6px", borderRadius:3, background:`${color}10`, border:`1px solid ${color}20` }}>
                <span style={{ fontSize:9 }}>{icon}</span>
                <span style={{ fontSize:9, color, fontFamily:"monospace" }}>{name}</span>
              </div>
              {i < arr.length-1 && <div style={{ width:1, height:4, background:"#1e293b", marginLeft:12 }} />}
            </div>
          ))}
        </Section>

        <Section label="AGENT PIPELINE">
          {AGENT_STEPS.map(step => {
            const st = steps[step.id], msg = stepMsgs[step.id];
            return (
              <div key={step.id} style={{ display:"flex", alignItems:"flex-start", gap:7,
                opacity: st ? 1 : 0.3, transition:"opacity 0.4s", padding:"2px 0" }}>
                <div style={{ width:7, height:7, borderRadius:"50%", marginTop:3, flexShrink:0,
                  background: st ? step.color : "#1e293b",
                  boxShadow: st==="running" ? `0 0 8px ${step.color}` : "none",
                  animation: st==="running" ? "blink 0.8s infinite" : "none", transition:"all 0.3s" }} />
                <div style={{ flex:1 }}>
                  <div style={{ fontSize:10, color: st ? "#e2e8f0" : "#475569" }}>
                    {step.icon} {step.label}
                  </div>
                  {msg && st==="running" && <div style={{ fontSize:9, color:step.color, marginTop:1 }}>{msg}</div>}
                </div>
                {st==="done"  && <span style={{ color:step.color, fontSize:10 }}>✓</span>}
                {st==="error" && <span style={{ color:"#EF4444", fontSize:10 }}>✗</span>}
                {st==="running" && <span style={{ color:step.color, fontSize:8, animation:"blink 0.5s infinite" }}>●●</span>}
              </div>
            );
          })}
        </Section>

        <Section label={`SCHEMA ${Object.keys(schema).length > 0 ? "● "+Object.keys(schema).length+" tables" : ""}`}>
          {Object.keys(schema).length === 0
            ? <p style={{ fontSize:9, color:"#334155" }}>Offline — start backend</p>
            : Object.entries(schema).slice(0,6).map(([t, cols]) => (
                <div key={t} style={{ padding:"3px 6px", background:"#0c0f1f",
                  borderRadius:3, border:"1px solid #141829", marginBottom:2 }}>
                  <p style={{ fontSize:9, color:"#A78BFA", fontFamily:"monospace" }}>{t}</p>
                  <p style={{ fontSize:8, color:"#334155" }}>
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
            <h1 style={{ fontSize:15, fontWeight:700, letterSpacing:1 }}>AI Data Assistant</h1>
            <p style={{ fontSize:9, color:"#334155", marginTop:2 }}>
              Multi-Agent · FastAPI + PostgreSQL · Anthropic Claude Sonnet
            </p>
          </div>
          <div style={{ display:"flex", gap:3, background:"#0c0f1f", padding:3,
            borderRadius:5, border:"1px solid #141829" }}>
            {["chat","schema"].map(t => (
              <button key={t} onClick={() => setTab(t)}
                style={{ padding:"4px 11px", borderRadius:4, border:"none", fontFamily:"inherit",
                  background: tab===t ? "#1e293b" : "transparent",
                  color: tab===t ? "#e2e8f0" : "#475569", cursor:"pointer", fontSize:10 }}>
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
                  <div style={{ fontSize:44, color:"#00D4FF" }}>◈</div>
                  <h2 style={{ fontSize:20, fontWeight:700 }}>Xin chào! Tôi là DataFinch</h2>
                  <p style={{ fontSize:12, color:"#64748b" }}>Hỏi tôi bất kỳ câu hỏi về dữ liệu thương mại điện tử</p>
                  <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:7, maxWidth:560 }}>
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

            <div style={{ padding:"10px 20px 14px", borderTop:"1px solid #141829" }}>
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
              <p style={{ fontSize:9, color:"#334155", marginTop:5 }}>
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
        @keyframes blink{0%,100%{opacity:1}50%{opacity:0.2}}
        @keyframes spin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}
        @keyframes fadeSlide{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
        *{box-sizing:border-box;margin:0;padding:0}
        ::-webkit-scrollbar{width:3px}
        ::-webkit-scrollbar-thumb{background:#1e293b;border-radius:2px}
        button:hover{filter:brightness(1.2)}
      `}</style>
    </div>
  );
}

function Section({ label, children }) {
  return (
    <div style={{ display:"flex", flexDirection:"column", gap:4 }}>
      <p style={{ fontSize:8, color:"#334155", letterSpacing:2, fontWeight:700, marginBottom:2 }}>{label}</p>
      {children}
    </div>
  );
}

function AgentMsg({ msg }) {
  const [showSql, setShowSql] = useState(false);
  if (msg.status === "thinking") return (
    <div style={S.agentBubble}>
      <span style={{ fontSize:15, color:"#00D4FF", animation:"spin 1.2s linear infinite", display:"inline-block" }}>⟳</span>
      <span style={{ color:"#64748b", fontSize:12, marginLeft:8 }}>Agent đang xử lý...</span>
    </div>
  );
  const r = msg.result;
  if (!r) return <div style={S.agentBubble}><p style={{ color:"#EF4444", fontSize:13 }}>{msg.error || "Lỗi hệ thống"}</p></div>;
  return (
    <div style={S.agentBubble}>
      <p style={{ fontSize:13, color:"#cbd5e1", lineHeight:1.65 }}>{r.explanation}</p>
      <div style={{ display:"flex", gap:5, flexWrap:"wrap" }}>
        {r.intent && <Chip>{r.intent}</Chip>}
        {r.complexity && <Chip>{r.complexity}</Chip>}
        {r.row_count !== undefined && <Chip color="#34D399">{r.row_count} rows</Chip>}
        {r.tables_used?.map(t => <Chip key={t} color="#A78BFA">{t}</Chip>)}
      </div>
      {r.sql && (
        <>
          <button onClick={() => setShowSql(v=>!v)}
            style={{ fontSize:10, color:"#475569", background:"none", border:"none",
              cursor:"pointer", fontFamily:"inherit", textAlign:"left" }}>
            {showSql ? "▼" : "▶"} Xem SQL
          </button>
          {showSql && (
            <div style={{ background:"#080b14", border:"1px solid #1e293b", borderRadius:5, padding:10 }}>
              <pre style={{ fontSize:11, color:"#34D399", fontFamily:"monospace", whiteSpace:"pre-wrap", lineHeight:1.65 }}>
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

function Chip({ children, color="#64748b" }) {
  return <span style={{ fontSize:9, padding:"2px 7px", background:"#1e293b",
    borderRadius:10, color, border:"1px solid #334155" }}>{children}</span>;
}

function ResultTable({ cols, rows }) {
  if (!rows?.length) return null;
  return (
    <div style={{ background:"#080b14", border:"1px solid #1e293b", borderRadius:5, padding:10 }}>
      <p style={{ fontSize:9, color:"#475569", marginBottom:6, letterSpacing:1 }}>📊 KẾT QUẢ</p>
      <div style={{ overflowX:"auto" }}>
        <table style={{ width:"100%", borderCollapse:"collapse", fontSize:11 }}>
          <thead>
            <tr>{cols.map(c=><th key={c} style={{ padding:"5px 9px", textAlign:"left",
              color:"#475569", borderBottom:"1px solid #1e293b", fontFamily:"monospace", fontSize:9 }}>{c}</th>)}</tr>
          </thead>
          <tbody>
            {rows.map((row,i)=>(
              <tr key={i} style={{ background: i%2===0 ? "transparent" : "#ffffff03" }}>
                {cols.map(c=><td key={c} style={{ padding:"5px 9px", color:"#cbd5e1",
                  fontFamily:"monospace", borderBottom:"1px solid #0f111700" }}>{row[c]??"-"}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p style={{ fontSize:9, color:"#334155", marginTop:5 }}>{rows.length} rows · PostgreSQL · LIMIT 100</p>
    </div>
  );
}

function SchemaTab({ schema }) {
  const empty = Object.keys(schema).length === 0;
  return (
    <div style={{ padding:22, overflowY:"auto", flex:1 }}>
      <h2 style={{ fontSize:16, marginBottom:4 }}>Live Schema — PostgreSQL</h2>
      <p style={{ fontSize:11, color:"#64748b", marginBottom:18 }}>
        {empty ? "⚠️ Backend offline" : `${Object.keys(schema).length} tables · Gold Layer · Star Schema`}
      </p>
      {empty
        ? <code style={{ color:"#34D399", fontSize:12 }}>uvicorn main:app --reload</code>
        : <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:11 }}>
            {Object.entries(schema).map(([t, cols]) => (
              <div key={t} style={{ background:"#0c0f1f", border:"1px solid #141829", borderRadius:7, padding:12 }}>
                <p style={{ color: t.startsWith("fact") ? "#FB923C" : "#A78BFA",
                  fontFamily:"monospace", fontSize:10, marginBottom:7, letterSpacing:1 }}>
                  {t.startsWith("fact") ? "◆ FACT" : "◇ DIM"} · {t}
                </p>
                {(cols||[]).map(c => {
                  const col = typeof c==="string" ? c : c.column;
                  const type = typeof c==="object" ? c.type : "";
                  return (
                    <div key={col} style={{ display:"flex", alignItems:"center", gap:6,
                      padding:"2px 0", borderBottom:"1px solid #14182900" }}>
                      <span style={{ width:5, height:5, borderRadius:"50%", flexShrink:0,
                        background: col?.includes("id") ? "#34D399" : "#334155" }} />
                      <span style={{ fontSize:10, fontFamily:"monospace",
                        color: col?.includes("id") ? "#34D399" : "#94a3b8" }}>{col}</span>
                      {type && <span style={{ fontSize:8, color:"#334155", marginLeft:"auto" }}>{type}</span>}
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
  root: { display:"flex", height:"100vh", background:"#080b14", color:"#e2e8f0",
    fontFamily:"'JetBrains Mono',monospace", overflow:"hidden" },
  sidebar: { width:225, background:"#090c1b", borderRight:"1px solid #141829",
    display:"flex", flexDirection:"column", padding:13, gap:17, overflowY:"auto", flexShrink:0 },
  logo: { display:"flex", alignItems:"center", gap:7, padding:"5px 0" },
  pill: { fontSize:8, padding:"1px 6px", borderRadius:10, background:"#0c1a2e",
    color:"#00D4FF", border:"1px solid #00D4FF30", marginLeft:"auto" },
  main: { flex:1, display:"flex", flexDirection:"column", overflow:"hidden" },
  topbar: { display:"flex", justifyContent:"space-between", alignItems:"center",
    padding:"13px 20px", borderBottom:"1px solid #141829" },
  chatArea: { flex:1, overflowY:"auto", padding:20, display:"flex", flexDirection:"column", gap:13 },
  userBubble: { background:"#1a2035", borderRadius:"10px 10px 2px 10px", padding:"9px 13px",
    maxWidth:"65%", fontSize:13, border:"1px solid #1e293b" },
  agentBubble: { background:"#090c1b", border:"1px solid #141829", borderRadius:"10px 10px 10px 2px",
    padding:14, maxWidth:"83%", display:"flex", flexDirection:"column", gap:9 },
  suggestBtn: { padding:"9px 12px", background:"#0c0f1f", border:"1px solid #1e293b",
    borderRadius:6, color:"#94a3b8", cursor:"pointer", fontSize:11, textAlign:"left", fontFamily:"inherit" },
  input: { flex:1, background:"#0c0f1f", border:"1px solid #1e293b", borderRadius:6,
    padding:"9px 13px", color:"#e2e8f0", fontSize:12, fontFamily:"inherit", outline:"none" },
  sendBtn: { width:42, background:"#00D4FF", border:"none", borderRadius:6,
    color:"#080b14", fontSize:17, cursor:"pointer", fontWeight:700 },
};
