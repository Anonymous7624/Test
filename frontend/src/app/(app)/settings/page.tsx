"use client";

import { FormEvent, useEffect, useState } from "react";
import { useAuth } from "@/context/auth-context";
import {
  fetchCategories,
  fetchSettings,
  updateSettings,
  workerRun,
  workerStatus,
  workerStop,
  type UserSettings,
} from "@/lib/api";

export default function SettingsPage() {
  const { token } = useAuth();
  const [settings, setSettings] = useState<UserSettings | null>(null);
  const [categoryOptions, setCategoryOptions] = useState<{ id: string; label: string }[]>([]);
  const [msg, setMsg] = useState<string | null>(null);
  const [workerMsg, setWorkerMsg] = useState<string>("");

  useEffect(() => {
    if (!token) return;
    void (async () => {
      const [{ categories }, s, st] = await Promise.all([
        fetchCategories(),
        fetchSettings(token),
        workerStatus(token),
      ]);
      setCategoryOptions(categories.map((c) => ({ id: c.id, label: c.label })));
      setSettings(s);
      setWorkerMsg(st.monitoring_enabled ? "Monitoring on" : "Monitoring off");
    })();
  }, [token]);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    if (!token || !settings) return;
    setMsg(null);
    try {
      const next = await updateSettings(token, settings);
      setSettings(next);
      setMsg("Saved.");
    } catch {
      setMsg("Save failed.");
    }
  }

  if (!settings) {
    return <p className="text-zinc-500">Loading…</p>;
  }

  return (
    <div>
      <h1 className="text-2xl font-semibold">Settings</h1>
      <form className="mt-6 max-w-lg space-y-4" onSubmit={onSubmit}>
        <div>
          <label className="block text-xs text-zinc-500">Location</label>
          <input
            className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
            value={settings.location}
            onChange={(e) => setSettings({ ...settings, location: e.target.value })}
          />
        </div>
        <div>
          <label className="block text-xs text-zinc-500">Radius (km)</label>
          <input
            type="number"
            className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
            value={settings.radius_km}
            onChange={(e) => setSettings({ ...settings, radius_km: Number(e.target.value) })}
          />
        </div>
        <div>
          <label className="block text-xs text-zinc-500">Category</label>
          <select
            className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
            value={settings.category_id}
            onChange={(e) => setSettings({ ...settings, category_id: e.target.value })}
          >
            {categoryOptions.map((c) => (
              <option key={c.id} value={c.id}>
                {c.label}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-xs text-zinc-500">Max price</label>
          <input
            type="number"
            className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
            value={settings.max_price}
            onChange={(e) => setSettings({ ...settings, max_price: Number(e.target.value) })}
          />
        </div>
        <div>
          <label className="block text-xs text-zinc-500">Telegram bot token (optional)</label>
          <input
            className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
            value={settings.telegram_bot_token ?? ""}
            onChange={(e) =>
              setSettings({
                ...settings,
                telegram_bot_token: e.target.value || null,
              })
            }
            placeholder="Set in backend env for worker stub, or store per-user later"
          />
        </div>
        <div>
          <label className="block text-xs text-zinc-500">Telegram chat id / target (optional)</label>
          <input
            className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
            value={settings.telegram_chat_id ?? ""}
            onChange={(e) =>
              setSettings({
                ...settings,
                telegram_chat_id: e.target.value || null,
              })
            }
          />
        </div>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            className="rounded-lg bg-emerald-700 px-4 py-2 text-sm"
            onClick={async () => {
              if (!token) return;
              await workerRun(token);
              const st = await workerStatus(token);
              setWorkerMsg(st.monitoring_enabled ? "Monitoring on" : "Monitoring off");
            }}
          >
            Run monitoring
          </button>
          <button
            type="button"
            className="rounded-lg bg-zinc-800 px-4 py-2 text-sm"
            onClick={async () => {
              if (!token) return;
              await workerStop(token);
              const st = await workerStatus(token);
              setWorkerMsg(st.monitoring_enabled ? "Monitoring on" : "Monitoring off");
            }}
          >
            Stop monitoring
          </button>
        </div>
        <p className="text-xs text-zinc-500">{workerMsg}</p>
        <button type="submit" className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium">
          Save settings
        </button>
        {msg && <p className="text-sm text-emerald-400">{msg}</p>}
      </form>
    </div>
  );
}
