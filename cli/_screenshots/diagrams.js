// Render 5 sơ đồ Mermaid (Chương 2) -> PNG, dùng Playwright + Mermaid CDN.
const { chromium } = require("playwright");
const path = require("path");
const fs = require("fs");

const OUT = path.resolve(__dirname, "../../documentations/diagrams");
fs.mkdirSync(OUT, { recursive: true });

const DIAGRAMS = {
  "hinh_2_1_usecase": `flowchart LR
  user(["👤 Người dùng nghiệp vụ"])
  admin(["👤 Quản trị viên"])
  subgraph HT["Hệ thống DataFinch"]
    uc1(["Hỏi dữ liệu bằng<br/>ngôn ngữ tự nhiên (NL→SQL)"])
    uc2(["Xem kết quả & biểu đồ"])
    uc3(["Lưu / mở lại truy vấn"])
    uc4(["Quản lý nguồn dữ liệu<br/>& catalog"])
    uc5(["Cấu hình nhà cung cấp LLM"])
  end
  user --> uc1
  user --> uc2
  user --> uc3
  admin --> uc4
  admin --> uc5
  admin --> uc1`,

  "hinh_2_2_kientruc": `flowchart TB
  fe["Giao diện người dùng<br/>(Next.js / React)"]
  subgraph P2["Trụ cột 2 — AI Agent Service (FastAPI)"]
    direction TB
    gw["API Gateway (SSE)"]
    sup["Supervisor"]
    ret["Metadata Retriever"]
    sw["SQL Writer"]
    gr["Guardrails"]
    ex["Executor"]
    gw --> sup --> ret --> sw --> gr --> ex
    os[("OpenSearch<br/>finch_catalog / table_docs / query_log")]
    ret <--> os
  end
  subgraph P1["Trụ cột 1 — Data Pipeline (Medallion)"]
    direction TB
    src["Nguồn: ERP / Kho vận / Thanh toán"]
    ing["CDC (Debezium) + NiFi"]
    kfk["Apache Kafka"]
    sp["Spark: Bronze → Silver → Gold<br/>(HDFS)"]
    hv[("Hive Metastore<br/>Gold star schema")]
    src --> ing --> kfk --> sp --> hv
  end
  fe --> gw
  ex --> hv
  ci["Catalog Indexer"]
  hv --> ci --> os`,

  "hinh_2_3_pipeline": `flowchart TB
  q["Câu hỏi tiếng Việt"] --> s1["[1] Supervisor<br/>phân loại intent + chọn backend"]
  s1 -->|OUT_OF_SCOPE| eo["Trả lời từ chối lịch sự"]
  s1 -->|SCHEMA_INFO| es["Trả lược đồ từ cache"]
  s1 -->|DATA_QUERY / FOLLOWUP| s2["[2] Metadata Retriever<br/>hybrid BM25 + kNN"]
  s2 --> s3["[3] SQL Writer<br/>sinh SQL + schema augmentation"]
  s3 --> s4["[4] Guardrails<br/>7 lớp kiểm tra an toàn"]
  s4 -->|hợp lệ| s5["[5] Executor<br/>thực thi Hive / Postgres"]
  s4 -->|chặn| err["Báo lỗi an toàn"]
  s5 --> s6["[6] Định dạng kết quả + biểu đồ"]
  s6 --> s7["[7] Ghi nhật ký query_log"]
  s7 --> s8["[8] Trả kết quả qua SSE"]`,

  "hinh_2_4_starschema": `erDiagram
  fact_sales }o--|| dim_customers : "customer_key"
  fact_sales }o--|| dim_products  : "product_key"
  fact_sales }o--|| dim_coupons   : "coupon_key"
  fact_sales }o--|| dim_addresses : "address_key"
  fact_sales }o--|| dim_shipping  : "shipping_key"
  fact_sales }o--|| dim_date      : "order_date"
  dim_products }o--|| dim_categories : "category_key"
  fact_reviews  }o--|| dim_products  : "product_key"
  fact_feedback }o--|| dim_customers : "customer_key"
  fact_sales {
    bigint order_item_key PK
    bigint order_key
    bigint customer_key FK
    bigint product_key FK
    int quantity
    decimal item_total
    int order_year
    int order_month
  }
  dim_customers {
    bigint customer_key PK
    string customer_name
    string email
  }
  dim_products {
    bigint product_key PK
    string product_name
    string brand
    bigint category_key FK
  }`,

  "hinh_2_5_sse": `sequenceDiagram
  participant FE as Frontend (Next.js)
  participant GW as API Gateway (FastAPI)
  participant AG as Cụm tác tử
  FE->>GW: POST /api/query/ask (câu hỏi)
  GW-->>FE: data: step supervisor (running)
  GW->>AG: Supervisor phân loại
  GW-->>FE: data: step supervisor (done)
  GW-->>FE: data: step metadata (running)
  GW->>AG: Retrieval + SQL Writer
  GW-->>FE: data: step sql_writer (done, sql)
  GW->>AG: Guardrails + Executor
  GW-->>FE: data: step execution (done)
  GW-->>FE: data: result (rows, sql, explanation)`,
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
    } catch (e) {
      console.log("  LỖI render", name, "-", e.message.split("\n")[0]);
    }
  }
  await browser.close();
  console.log("DONE -> documentations/diagrams/");
})().catch((e) => { console.error("FATAL:", e); process.exit(1); });
