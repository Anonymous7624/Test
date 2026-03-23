"use client";

import { useAuth } from "@/context/auth-context";

export default function DashboardPage() {
  const { user } = useAuth();
  return (
    <div>
      <h1 className="text-2xl font-semibold">Dashboard</h1>
      <p className="mt-2 max-w-xl text-zinc-400">
        Welcome, {user?.username}. Use <strong>Listings</strong> to review normalized deals,{" "}
        <strong>Settings</strong> to configure monitoring and Telegram targets, and{" "}
        <strong>Admin</strong> (admins only) to manage users.
      </p>
    </div>
  );
}
