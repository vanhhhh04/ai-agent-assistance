import Link from "next/link";
import { cn } from "@/lib/utils";

export function Logo({ className, href = "/" }: { className?: string; href?: string }) {
  return (
    <Link href={href} className={cn("inline-flex items-center gap-2 group", className)}>
      <span className="text-2xl text-[color:var(--color-primary)] group-hover:rotate-12 transition-transform">
        ◈
      </span>
      <span className="font-bold tracking-tight text-lg text-[color:var(--color-text)]">
        DataFinch
      </span>
    </Link>
  );
}
