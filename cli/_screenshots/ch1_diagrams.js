// Render 3 hình lý thuyết Chương 1 -> PNG (Playwright + Mermaid CDN).
const { chromium } = require("playwright");
const path = require("path");
const fs = require("fs");
const OUT = path.resolve(__dirname, "../../documentations/diagrams");
fs.mkdirSync(OUT, { recursive: true });

const DIAGRAMS = {
  "hinh_1_1_ai_ml_dl": `flowchart TB
  subgraph AI["Trí tuệ nhân tạo (AI)"]
    subgraph ML["Học máy (Machine Learning)"]
      subgraph DL["Học sâu (Deep Learning)"]
        GA["Generative AI<br/>Mô hình ngôn ngữ lớn (LLM)"]
      end
    end
  end`,

  "hinh_1_2_datawarehouse": `flowchart LR
  src["Hệ thống nguồn<br/>(ERP, bán hàng, kho vận)"] --> etl["ETL / ELT<br/>(trích xuất – biến đổi – nạp)"]
  etl --> stg["Tầng trung chuyển<br/>(Staging)"]
  stg --> dw[("Kho dữ liệu<br/>(Data Warehouse)")]
  dw --> dm["Kho chuyên đề<br/>(Data Mart)"]
  dm --> bi["Tầng phân tích & BI<br/>(báo cáo, dashboard)"]
  dw --> bi`,

  "hinh_1_3_rag": `flowchart LR
  q["(1) Câu hỏi<br/>người dùng"] --> r["(2) Truy hồi<br/>(Retrieval)"]
  kb[("Kho tri thức<br/>lược đồ / tài liệu")] --> r
  r --> ci["(3) Chèn ngữ cảnh<br/>(Context Injection)"]
  ci --> llm["(4) Sinh bằng LLM<br/>(Generation)"]
  llm --> ans["Câu trả lời<br/>bám sát dữ liệu"]`,
};

(async () => {
  const browser = await chromium.launch();
  const ctx = await browser.newContext({ deviceScaleFactor: 2 });
  const page = await ctx.newPage();
  await page.setContent(`<!doctype html><html><head><meta charset="utf-8">
    <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
    <style>body{margin:0;background:#fff;font-family:'Segoe UI',Arial,sans-serif}
    #box{display:inline-block;padding:18px;background:#fff}</style>
    </head><body><div id="box"></div></body></html>`, { waitUntil: "networkidle" });
  await page.waitForFunction(() => !!window.mermaid, { timeout: 30000 });
  await page.evaluate(() => window.mermaid.initialize({ startOnLoad: false, theme: "default", flowchart: { htmlLabels: true, curve: "basis" }, securityLevel: "loose" }));
  for (const [name, code] of Object.entries(DIAGRAMS)) {
    try {
      await page.evaluate(async (c) => {
        const { svg } = await window.mermaid.render("g_" + Math.random().toString(36).slice(2), c);
        document.getElementById("box").innerHTML = svg;
      }, code);
      await page.waitForTimeout(400);
      const el = await page.$("#box");
      await el.screenshot({ path: path.join(OUT, name + ".png") });
      console.log("  ok:", name + ".png");
    } catch (e) { console.log("  LỖI", name, "-", e.message.split("\n")[0]); }
  }
  await browser.close();
  console.log("DONE");
})().catch((e) => { console.error("FATAL:", e); process.exit(1); });
