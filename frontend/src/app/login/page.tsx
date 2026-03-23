"use client";

import { useRouter } from "next/navigation";
import { FormEvent, useEffect, useState } from "react";
import { useAuth } from "@/context/auth-context";
import { login } from "@/lib/api";

export default function LoginPage() {
  const { setSession, token, ready } = useAuth();
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (ready && token) router.replace("/dashboard");
  }, [ready, token, router]);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    try {
      const res = await login(username, password);
      setSession(res.access_token, res.user);
      router.replace("/dashboard");
    } catch {
      setError("Invalid username or password.");
    }
  }

  return (
    <div className="flex min-h-screen items-center justify-center p-6">
      <div className="w-full max-w-sm rounded-xl border border-zinc-800 bg-zinc-900/80 p-8 shadow-xl">
        <h1 className="text-xl font-semibold tracking-tight">Deal Dashboard</h1>
        <p className="mt-1 text-sm text-zinc-400">Sign in to continue.</p>
        <form className="mt-6 space-y-4" onSubmit={onSubmit}>
          <div>
            <label className="block text-xs font-medium text-zinc-400">Username</label>
            <input
              className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm outline-none ring-emerald-500/30 focus:ring-2"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoComplete="username"
              required
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-zinc-400">Password</label>
            <input
              type="password"
              className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm outline-none ring-emerald-500/30 focus:ring-2"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete="current-password"
              required
            />
          </div>
          {error && <p className="text-sm text-red-400">{error}</p>}
          <button
            type="submit"
            className="w-full rounded-lg bg-emerald-600 py-2 text-sm font-medium text-white hover:bg-emerald-500"
          >
            Sign in
          </button>
        </form>
      </div>
    </div>
  );
}
