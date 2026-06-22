import { Sidebar } from "@/components/app/Sidebar";
import { AuthGate } from "@/components/app/AuthGate";

export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <AuthGate>
      <div className="flex min-h-screen bg-[color:var(--color-bg-muted)]">
        <Sidebar />
        <div className="flex-1 min-w-0">{children}</div>
      </div>
    </AuthGate>
  );
}
