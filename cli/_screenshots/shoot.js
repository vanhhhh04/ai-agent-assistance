// Chụp ảnh giao diện DataFinch cho đồ án. Stack đã chạy: FE :3000, BE :8000.
const { chromium } = require("playwright");
const path = require("path");

const OUT = path.resolve(__dirname, "../../documentations/screenshots");
const BASE = "http://localhost:3000";

// Bypass mock AuthGate: bơm sẵn user vào localStorage trước khi trang load.
const USER = {
  username: "admin",
  name: "Admin Demo",
  email: "admin@datafinch.app",
  plan: "growth",
};

async function shoot(page, name, { full = true } = {}) {
  await page.waitForTimeout(1200); // để animation/biểu đồ ổn định
  const file = path.join(OUT, `${name}.png`);
  try {
    await page.screenshot({ path: file, fullPage: full });
  } catch (e) {
    console.log("  (fullPage lỗi, thử viewport):", e.message.split("\n")[0]);
    await page.screenshot({ path: file, fullPage: false });
  }
  console.log("  saved:", name + ".png");
}

(async () => {
  const browser = await chromium.launch();
  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 1000 },
    deviceScaleFactor: 1.5, // nét vừa phải, tránh lỗi ảnh quá lớn khi fullPage
    locale: "vi-VN",
  });
  ctx.setDefaultTimeout(200000);
  await ctx.addInitScript((u) => {
    localStorage.setItem("datafinch:user", JSON.stringify(u));
  }, USER);

  const page = await ctx.newPage();

  const goto = async (url, wait = "networkidle") => {
    console.log("→", url);
    await page.goto(BASE + url, { waitUntil: "domcontentloaded", timeout: 60000 });
    try { await page.waitForLoadState(wait, { timeout: 20000 }); } catch {}
  };

  // 1. Landing (marketing) — viewport, không full để gọn hero
  await goto("/");
  await shoot(page, "01_landing", { full: false });

  // 2. Login
  await goto("/login");
  await shoot(page, "02_login", { full: false });

  // 3. Signup wizard (bước Welcome)
  await goto("/signup");
  await shoot(page, "03_signup_step1", { full: false });
  // thử sang bước 2 (chọn CSDL) nếu có nút tiếp
  try {
    const btn = page.getByRole("button", { name: /bắt đầu|tiếp|next|continue/i }).first();
    if (await btn.isVisible({ timeout: 2000 })) { await btn.click(); await page.waitForTimeout(800); await shoot(page, "03b_signup_step2", { full: false }); }
  } catch {}

  // 4. Chat — trạng thái rỗng
  await goto("/app/ask");
  await shoot(page, "04_ask_empty", { full: false });

  // 5. Chat — kết quả LIVE (auto-send qua ?q=)
  const q = "Top 5 khách hàng đặt nhiều đơn nhất";
  await goto("/app/ask?q=" + encodeURIComponent(q), "domcontentloaded");
  console.log("  chờ kết quả pipeline (Hive cold start có thể >1 phút)...");
  try {
    await page.waitForFunction(
      () => /Xem SQL|Kết quả ·|\d+ rows/i.test(document.body.innerText),
      { timeout: 190000, polling: 1000 }
    );
    // mở SQL nếu đang ẩn
    try {
      const sqlToggle = page.getByText(/Xem SQL/i).first();
      if (await sqlToggle.isVisible({ timeout: 2000 })) { await sqlToggle.click(); await page.waitForTimeout(600); }
    } catch {}
    await page.waitForTimeout(1500);
    await shoot(page, "05_ask_result", { full: true });
  } catch (e) {
    console.log("  (không bắt được kết quả trong thời gian chờ — chụp trạng thái hiện tại)", e.message);
    await shoot(page, "05_ask_result_timeout", { full: true });
  }

  // 6–10. Các trang app (demo)
  const pages = [
    ["/app/saved",    "06_saved"],
    ["/app/reports",  "07_reports"],
    ["/app/data",     "08_data"],
    ["/app/settings", "09_settings"],
    ["/app/billing",  "10_billing"],
  ];
  for (const [url, name] of pages) {
    await goto(url);
    await shoot(page, name, { full: true });
  }

  await browser.close();
  console.log("\nXONG. Ảnh tại documentations/screenshots/");
})().catch((e) => { console.error("LỖI:", e); process.exit(1); });
