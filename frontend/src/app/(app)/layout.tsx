"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect } from "react";
import { useAuth } from "@/context/auth-context";

const nav = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/listings", label: "Listings" },
  { href: "/settings", label: "Settings" },
  { href: "/admin", label: "Admin", adminOnly: true },
];

export default function AppShellLayout({ children }: { children: React.ReactNode }) {
  const { token, user, ready, logout } = useAuth();
  const pathname = usePathname();
  const router = useRouter();

  useEffect(() => {
    if (ready && !token) router.replace("/login");
  }, [ready, token, router]);

  if (!ready || !token || !user) {
    return (
      <div className="flex min-h-screen items-center justify-center text-zinc-500">
        Loading…
      </div>
    );
  }

  return (
    <div className="flex min-h-screen">
      <aside className="flex w-56 flex-col border-r border-zinc-800 bg-zinc-900/50">
        <div className="border-b border-zinc-800 p-4">
          <p className="text-xs uppercase tracking-wide text-zinc-500">Deal finder</p>
          <p className="mt-1 font-medium text-zinc-100">{user.username}</p>
          <p className="text-xs text-zinc-500">{user.role}</p>
        </div>
        <nav className="flex flex-1 flex-col gap-1 p-2">
          {nav
            .filter((item) => !item.adminOnly || user.role === "admin")
            .map((item) => {
              const active = pathname === item.href || pathname.startsWith(`${item.href}/`);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={`rounded-lg px-3 py-2 text-sm ${
                    active ? "bg-zinc-800 text-white" : "text-zinc-400 hover:bg-zinc-800/60"
                  }`}
                >
                  {item.label}
                </Link>
              );
            })}
        </nav>
        <div className="border-t border-zinc-800 p-2">
          <button
            type="button"
            onClick={() => {
              logout();
              router.replace("/login");
            }}
            className="w-full rounded-lg px-3 py-2 text-left text-sm text-zinc-400 hover:bg-zinc-800"
          >
            Log out
          </button>
        </div>
      </aside>
      <main className="flex-1 overflow-auto p-8">{children}</main>
    </div>
  );
}
