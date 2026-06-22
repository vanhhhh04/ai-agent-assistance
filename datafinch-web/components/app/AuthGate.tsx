"use client";

import { useEffect, useState } from "react";
import { useRouter, usePathname } from "next/navigation";
import { getUser, type User } from "@/lib/auth";

/**
 * Client-side guard. If no user in localStorage, redirect to /login.
 * Renders a tiny loading state during the first client tick to avoid flash.
 *
 * Mock auth — replace with proper middleware + session check when wiring
 * Clerk/Supabase/JWT.
 */
export function AuthGate({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const [user, setUser] = useState<User | null | "loading">("loading");

  useEffect(() => {
    const u = getUser();
    if (!u) {
      const redirect = pathname ? `?next=${encodeURIComponent(pathname)}` : "";
      router.replace(`/login${redirect}`);
      return;
    }
    setUser(u);
  }, [router, pathname]);

  if (user === "loading" || user === null) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-[color:var(--color-bg-muted)]">
        <div className="flex flex-col items-center gap-3 text-[color:var(--color-text-muted)]">
          <div className="w-8 h-8 rounded-full border-2 border-[color:var(--color-primary)] border-t-transparent animate-spin" />
          <span className="text-sm">Đang xác thực…</span>
        </div>
      </div>
    );
  }

  return <>{children}</>;
}
