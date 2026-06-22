import type { Metadata } from "next";
import { Inter, JetBrains_Mono } from "next/font/google";
import "./globals.css";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin", "vietnamese"],
  display: "swap",
});

const jetbrains = JetBrains_Mono({
  variable: "--font-jetbrains-mono",
  subsets: ["latin"],
  display: "swap",
});

export const metadata: Metadata = {
  title: "DataFinch — Hỏi dữ liệu như hỏi người",
  description:
    "AI Data Analyst cho doanh nghiệp Việt. Hỏi bằng tiếng Việt, có ngay câu trả lời từ database của bạn. Không cần biết SQL.",
  keywords: ["data analytics", "natural language SQL", "AI", "tiếng Việt", "business intelligence"],
  openGraph: {
    title: "DataFinch — Hỏi dữ liệu như hỏi người",
    description: "AI Data Analyst cho doanh nghiệp Việt. Hỏi bằng tiếng Việt, có ngay câu trả lời.",
    type: "website",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="vi" className={`${inter.variable} ${jetbrains.variable} antialiased`}>
      <body>{children}</body>
    </html>
  );
}
