import { Button } from "../ui/Button";

export function FinalCTA() {
  return (
    <section className="py-24 md:py-32 border-t border-[color:var(--color-border)]">
      <div className="mx-auto max-w-4xl px-6 text-center">
        <div className="relative inline-block mb-6">
          <span className="text-6xl">◈</span>
          <span
            className="absolute inset-0 -z-10 blur-3xl opacity-30"
            style={{ background: "var(--color-primary)" }}
          />
        </div>
        <h2 className="text-4xl md:text-6xl font-bold tracking-tight text-[color:var(--color-text)] mb-6 leading-tight">
          Sẵn sàng hỏi
          <br />
          <span className="bg-gradient-to-r from-[color:var(--color-primary)] to-[color:var(--color-purple)] bg-clip-text text-transparent">
            câu đầu tiên?
          </span>
        </h2>
        <p className="text-lg md:text-xl text-[color:var(--color-text-muted)] mb-10 max-w-2xl mx-auto">
          5 phút setup. Không cần thẻ tín dụng. Có data team trong tay ngay hôm nay.
        </p>

        <div className="flex flex-col sm:flex-row gap-3 justify-center">
          <Button href="/signup" size="lg">
            Dùng thử miễn phí
            <span aria-hidden>→</span>
          </Button>
          <Button href="/contact" variant="outline" size="lg">
            Đặt lịch demo
          </Button>
        </div>

        <div className="mt-12 flex flex-wrap justify-center gap-x-8 gap-y-3 text-sm text-[color:var(--color-text-muted)]">
          {[
            "✓ Không thẻ tín dụng",
            "✓ Setup 5 phút",
            "✓ Hủy bất kỳ lúc nào",
            "✓ Made in Vietnam 🇻🇳",
          ].map((item) => (
            <span key={item} className="font-medium">
              {item}
            </span>
          ))}
        </div>
      </div>
    </section>
  );
}
