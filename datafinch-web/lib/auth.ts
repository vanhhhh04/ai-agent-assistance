/**
 * Mock auth — single hardcoded credential pair (admin/admin) saved to localStorage.
 * Replace with real auth (Clerk/Supabase/JWT) when wiring backend.
 */

export type User = {
  username: string;
  name: string;
  email: string;
  plan: "free" | "starter" | "growth" | "enterprise";
};

const STORAGE_KEY = "datafinch:user";

// Demo credentials — accept admin/admin only
const DEMO_USERS: Record<string, { password: string; user: User }> = {
  admin: {
    password: "admin",
    user: {
      username: "admin",
      name: "Admin Demo",
      email: "admin@datafinch.app",
      plan: "growth",
    },
  },
};

export function login(username: string, password: string): User | null {
  const entry = DEMO_USERS[username.trim().toLowerCase()];
  if (!entry || entry.password !== password) return null;
  if (typeof window !== "undefined") {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(entry.user));
  }
  return entry.user;
}

export function logout(): void {
  if (typeof window !== "undefined") {
    localStorage.removeItem(STORAGE_KEY);
  }
}

export function getUser(): User | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as User) : null;
  } catch {
    return null;
  }
}

export function setUser(user: User): void {
  if (typeof window !== "undefined") {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(user));
  }
}
