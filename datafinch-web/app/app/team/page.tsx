"use client";

import { useState } from "react";
import { AppHeader } from "@/components/app/AppHeader";
import { cn } from "@/lib/utils";

type Role = "owner" | "admin" | "analyst" | "viewer";

type Member = {
  id: string;
  name: string;
  email: string;
  avatar: string;
  role: Role;
  lastActive: string;
  joinedAt: string;
  queriesThisMonth: number;
};

const MOCK_MEMBERS: Member[] = [
  {
    id: "1",
    name: "Admin Demo",
    email: "admin@datafinch.app",
    avatar: "A",
    role: "owner",
    lastActive: "Đang online",
    joinedAt: "2026-01-15",
    queriesThisMonth: 234,
  },
  {
    id: "2",
    name: "Nguyễn Thị B",
    email: "nguyen.b@company.vn",
    avatar: "N",
    role: "admin",
    lastActive: "2 giờ trước",
    joinedAt: "2026-02-03",
    queriesThisMonth: 156,
  },
  {
    id: "3",
    name: "Trần Văn C",
    email: "tran.c@company.vn",
    avatar: "T",
    role: "analyst",
    lastActive: "1 ngày trước",
    joinedAt: "2026-03-10",
    queriesThisMonth: 89,
  },
  {
    id: "4",
    name: "Lê Thị D",
    email: "le.d@company.vn",
    avatar: "L",
    role: "analyst",
    lastActive: "3 giờ trước",
    joinedAt: "2026-03-22",
    queriesThisMonth: 67,
  },
  {
    id: "5",
    name: "Phạm Văn E",
    email: "pham.e@company.vn",
    avatar: "P",
    role: "viewer",
    lastActive: "1 tuần trước",
    joinedAt: "2026-04-05",
    queriesThisMonth: 12,
  },
];

const PENDING_INVITES = [
  { email: "vu.f@company.vn", role: "analyst" as Role, sentAt: "2 giờ trước" },
  { email: "do.g@partner.com", role: "viewer" as Role, sentAt: "1 ngày trước" },
];

const ROLE_CONFIG: Record<Role, { label: string; desc: string; color: string; bg: string }> = {
  owner:   { label: "Owner",   desc: "Toàn quyền + billing", color: "var(--color-orange)", bg: "#fed7aa" },
  admin:   { label: "Admin",   desc: "Quản lý team + settings", color: "var(--color-purple)", bg: "#ede9fe" },
  analyst: { label: "Analyst", desc: "Hỏi + tạo alerts/saves",   color: "var(--color-primary)", bg: "var(--color-primary-faded)" },
  viewer:  { label: "Viewer",  desc: "Chỉ xem báo cáo",        color: "var(--color-text-subtle)", bg: "var(--color-bg-subtle)" },
};

export default function TeamPage() {
  const [showInvite, setShowInvite] = useState(false);

  return (
    <div className="min-h-screen">
      <AppHeader
        title="Team"
        subtitle={`${MOCK_MEMBERS.length} thành viên · ${PENDING_INVITES.length} lời mời chờ phản hồi`}
        actions={
          <button
            onClick={() => setShowInvite(true)}
            className="h-10 px-4 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)] shadow-sm shadow-cyan-500/20"
          >
            + Mời thành viên
          </button>
        }
      />

      <div className="p-6 md:p-8 space-y-8">
        {/* Stats */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {(["owner", "admin", "analyst", "viewer"] as Role[]).map((r) => {
            const count = MOCK_MEMBERS.filter((m) => m.role === r).length;
            const cfg = ROLE_CONFIG[r];
            return (
              <div
                key={r}
                className="rounded-xl bg-white border border-[color:var(--color-border)] p-4"
              >
                <p
                  className="text-xs font-bold uppercase tracking-wider mb-1"
                  style={{ color: cfg.color }}
                >
                  {cfg.label}
                </p>
                <p className="text-2xl font-bold text-[color:var(--color-text)]">{count}</p>
                <p className="text-xs text-[color:var(--color-text-subtle)] mt-1">{cfg.desc}</p>
              </div>
            );
          })}
        </div>

        {/* Members table */}
        <section>
          <h2 className="text-base font-bold text-[color:var(--color-text)] mb-3">
            Thành viên ({MOCK_MEMBERS.length})
          </h2>
          <div className="rounded-xl bg-white border border-[color:var(--color-border)] overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-[color:var(--color-bg-muted)] border-b border-[color:var(--color-border)]">
                  <tr>
                    <th className="text-left px-4 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">
                      Thành viên
                    </th>
                    <th className="text-left px-4 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">
                      Role
                    </th>
                    <th className="text-left px-4 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">
                      Queries tháng này
                    </th>
                    <th className="text-left px-4 py-3 font-semibold text-[color:var(--color-text-subtle)] uppercase text-xs tracking-wider">
                      Last active
                    </th>
                    <th className="w-12"></th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-[color:var(--color-border)]">
                  {MOCK_MEMBERS.map((m) => (
                    <MemberRow key={m.id} member={m} />
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </section>

        {/* Pending invites */}
        {PENDING_INVITES.length > 0 && (
          <section>
            <h2 className="text-base font-bold text-[color:var(--color-text)] mb-3">
              ⏳ Đang chờ phản hồi ({PENDING_INVITES.length})
            </h2>
            <div className="space-y-2">
              {PENDING_INVITES.map((inv, i) => (
                <div
                  key={i}
                  className="flex items-center gap-4 p-4 rounded-xl bg-white border border-[color:var(--color-border)]"
                >
                  <div className="w-10 h-10 rounded-full bg-[color:var(--color-bg-subtle)] border-2 border-dashed border-[color:var(--color-border-strong)] flex items-center justify-center text-[color:var(--color-text-subtle)]">
                    ?
                  </div>
                  <div className="flex-1">
                    <p className="font-medium text-[color:var(--color-text)]">{inv.email}</p>
                    <p className="text-xs text-[color:var(--color-text-subtle)]">
                      Đã gửi {inv.sentAt} · Role: {ROLE_CONFIG[inv.role].label}
                    </p>
                  </div>
                  <div className="flex gap-2">
                    <button className="px-3 py-1.5 rounded-md text-xs font-medium text-[color:var(--color-text-muted)] hover:bg-[color:var(--color-bg-subtle)]">
                      Gửi lại
                    </button>
                    <button className="px-3 py-1.5 rounded-md text-xs font-medium text-[color:var(--color-danger)] hover:bg-red-50">
                      Hủy
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </section>
        )}
      </div>

      {showInvite && <InviteModal onClose={() => setShowInvite(false)} />}
    </div>
  );
}

function MemberRow({ member }: { member: Member }) {
  const cfg = ROLE_CONFIG[member.role];
  return (
    <tr className="hover:bg-[color:var(--color-bg-muted)]/50">
      <td className="px-4 py-3">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-full bg-gradient-to-br from-[color:var(--color-primary)] to-[color:var(--color-purple)] flex items-center justify-center text-white font-bold text-sm flex-shrink-0">
            {member.avatar}
          </div>
          <div className="min-w-0">
            <p className="font-medium text-[color:var(--color-text)] truncate">{member.name}</p>
            <p className="text-xs text-[color:var(--color-text-subtle)] truncate">{member.email}</p>
          </div>
        </div>
      </td>
      <td className="px-4 py-3">
        <span
          className="inline-block text-xs px-2.5 py-1 rounded-md font-semibold"
          style={{ color: cfg.color, background: cfg.bg }}
        >
          {cfg.label}
        </span>
      </td>
      <td className="px-4 py-3 text-[color:var(--color-text-muted)] font-mono text-xs">
        {member.queriesThisMonth.toLocaleString()}
      </td>
      <td className="px-4 py-3 text-[color:var(--color-text-muted)] text-xs">
        {member.lastActive === "Đang online" ? (
          <span className="inline-flex items-center gap-1.5 text-[color:var(--color-green)] font-medium">
            <span className="w-1.5 h-1.5 rounded-full bg-[color:var(--color-green)]" />
            Đang online
          </span>
        ) : (
          member.lastActive
        )}
      </td>
      <td className="px-4 py-3 text-right">
        <button className="px-2 py-1 rounded-md text-[color:var(--color-text-subtle)] hover:bg-[color:var(--color-bg-subtle)] hover:text-[color:var(--color-text)]">
          ⋯
        </button>
      </td>
    </tr>
  );
}

function InviteModal({ onClose }: { onClose: () => void }) {
  const [emails, setEmails] = useState("");
  const [role, setRole] = useState<Role>("analyst");

  return (
    <div className="fixed inset-0 z-50 bg-black/50 flex items-center justify-center p-4" onClick={onClose}>
      <div
        onClick={(e) => e.stopPropagation()}
        className="bg-white rounded-xl shadow-2xl w-full max-w-lg"
      >
        <div className="px-6 py-4 border-b border-[color:var(--color-border)] flex items-center justify-between">
          <h2 className="text-lg font-bold text-[color:var(--color-text)]">Mời thành viên</h2>
          <button
            onClick={onClose}
            className="w-8 h-8 rounded-md hover:bg-[color:var(--color-bg-subtle)] text-[color:var(--color-text-muted)]"
          >
            ✕
          </button>
        </div>

        <div className="p-6 space-y-5">
          <div>
            <label className="block text-sm font-medium text-[color:var(--color-text)] mb-1.5">
              Email (phân cách bằng dấu phẩy hoặc xuống dòng)
            </label>
            <textarea
              value={emails}
              onChange={(e) => setEmails(e.target.value)}
              rows={3}
              placeholder="email1@company.vn, email2@company.vn"
              className="w-full px-3.5 py-2.5 rounded-lg border border-[color:var(--color-border-strong)] bg-white text-sm focus:outline-none focus:ring-2 focus:ring-[color:var(--color-primary)]/30"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-[color:var(--color-text)] mb-2">
              Role
            </label>
            <div className="space-y-2">
              {(["admin", "analyst", "viewer"] as Role[]).map((r) => {
                const cfg = ROLE_CONFIG[r];
                return (
                  <label
                    key={r}
                    className={cn(
                      "flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-colors",
                      role === r
                        ? "border-[color:var(--color-primary)] bg-[color:var(--color-primary-faded)]"
                        : "border-[color:var(--color-border)] bg-white hover:border-[color:var(--color-border-strong)]"
                    )}
                  >
                    <input
                      type="radio"
                      name="role"
                      checked={role === r}
                      onChange={() => setRole(r)}
                      className="mt-1 text-[color:var(--color-primary)]"
                    />
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="font-semibold text-sm" style={{ color: cfg.color }}>
                          {cfg.label}
                        </span>
                      </div>
                      <p className="text-xs text-[color:var(--color-text-muted)]">{cfg.desc}</p>
                    </div>
                  </label>
                );
              })}
            </div>
          </div>
        </div>

        <div className="px-6 py-4 border-t border-[color:var(--color-border)] flex justify-end gap-2 bg-[color:var(--color-bg-muted)]">
          <button
            onClick={onClose}
            className="h-10 px-4 rounded-lg border border-[color:var(--color-border-strong)] text-sm font-medium hover:bg-white"
          >
            Hủy
          </button>
          <button className="h-10 px-5 rounded-lg bg-[color:var(--color-primary)] text-white text-sm font-semibold hover:bg-[color:var(--color-primary-hover)]">
            Gửi lời mời
          </button>
        </div>
      </div>
    </div>
  );
}
