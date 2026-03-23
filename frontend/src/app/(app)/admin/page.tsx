"use client";

import { FormEvent, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@/context/auth-context";
import { adminCreateUser, adminListUsers, type AdminUser } from "@/lib/api";

export default function AdminPage() {
  const { token, user } = useAuth();
  const router = useRouter();
  const [rows, setRows] = useState<AdminUser[]>([]);
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState<"user" | "admin">("user");
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (user && user.role !== "admin") router.replace("/dashboard");
  }, [user, router]);

  useEffect(() => {
    if (!token || user?.role !== "admin") return;
    void adminListUsers(token).then(setRows).catch(() => setErr("Could not load users."));
  }, [token, user?.role]);

  async function onCreate(e: FormEvent) {
    e.preventDefault();
    if (!token) return;
    setErr(null);
    try {
      await adminCreateUser(token, { username, password, role });
      setUsername("");
      setPassword("");
      setRows(await adminListUsers(token));
    } catch {
      setErr("Create failed (duplicate username?).");
    }
  }

  if (!user || user.role !== "admin") {
    return null;
  }

  return (
    <div>
      <h1 className="text-2xl font-semibold">Admin</h1>
      <p className="mt-2 text-sm text-zinc-400">Create and manage users.</p>

      <form className="mt-6 max-w-md space-y-3" onSubmit={onCreate}>
        <h2 className="text-sm font-medium text-zinc-300">New user</h2>
        <input
          className="w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
          placeholder="Username"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          required
        />
        <input
          type="password"
          className="w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
          placeholder="Password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
        />
        <select
          className="w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
          value={role}
          onChange={(e) => setRole(e.target.value as "user" | "admin")}
        >
          <option value="user">user</option>
          <option value="admin">admin</option>
        </select>
        <button type="submit" className="rounded-lg bg-emerald-600 px-4 py-2 text-sm">
          Create user
        </button>
        {err && <p className="text-sm text-red-400">{err}</p>}
      </form>

      <div className="mt-10 overflow-x-auto rounded-xl border border-zinc-800">
        <table className="min-w-full text-left text-sm">
          <thead className="border-b border-zinc-800 bg-zinc-900/80 text-xs uppercase text-zinc-500">
            <tr>
              <th className="px-3 py-2">ID</th>
              <th className="px-3 py-2">Username</th>
              <th className="px-3 py-2">Role</th>
              <th className="px-3 py-2">Created</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id} className="border-b border-zinc-800/80">
                <td className="px-3 py-2">{r.id}</td>
                <td className="px-3 py-2">{r.username}</td>
                <td className="px-3 py-2">{r.role}</td>
                <td className="px-3 py-2 text-zinc-400">{new Date(r.created_at).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
