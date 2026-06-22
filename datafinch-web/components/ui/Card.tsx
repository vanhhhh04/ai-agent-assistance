import { cn } from "@/lib/utils";

export function Card({
  className,
  children,
  hover = false,
}: {
  className?: string;
  children: React.ReactNode;
  hover?: boolean;
}) {
  return (
    <div
      className={cn(
        "rounded-xl border border-[color:var(--color-border)] bg-white p-6",
        "shadow-[0_1px_3px_rgba(15,23,42,0.04)]",
        hover && "transition-all duration-200 hover:shadow-[0_8px_24px_rgba(15,23,42,0.08)] hover:border-[color:var(--color-border-strong)] hover:-translate-y-0.5",
        className
      )}
    >
      {children}
    </div>
  );
}
