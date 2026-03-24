"use client";

import { FormEvent, useEffect, useState } from "react";
import { useAuth } from "@/context/auth-context";
import {
  fetchCategories,
  fetchSettings,
  sendTelegramTest,
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
  const [telegramMsg, setTelegramMsg] = useState<string | null>(null);
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
        <div className="rounded-lg border border-zinc-800 bg-zinc-950/50 p-4">
          <h2 className="text-sm font-medium text-zinc-200">Telegram</h2>
          <p className="mt-1 text-xs text-zinc-500">
            Set the bot token only in backend <code className="text-zinc-400">TELEGRAM_BOT_TOKEN</code>. Your
            personal chat id is stored here.
          </p>
          <div className="mt-3">
            <label className="block text-xs text-zinc-500">Chat ID</label>
            <input
              className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
              value={settings.telegram_chat_id ?? ""}
              onChange={(e) =>
                setSettings({
                  ...settings,
                  telegram_chat_id: e.target.value || null,
                })
              }
              placeholder="e.g. numeric ID from @userinfobot"
            />
          </div>
          <p className="mt-2 text-xs text-zinc-400">
            Status:{" "}
            {settings.telegram_connected ? (
              <span className="text-emerald-400">Connected (chat id saved)</span>
            ) : (
              <span className="text-zinc-500">Not connected</span>
            )}
          </p>
          <div className="mt-3 flex flex-wrap gap-2">
            <button
              type="button"
              className="rounded-lg bg-emerald-700 px-4 py-2 text-sm"
              onClick={async () => {
                if (!token) return;
                setTelegramMsg(null);
                try {
                  const next = await updateSettings(token, {
                    telegram_chat_id: settings.telegram_chat_id,
                  });
                  setSettings(next);
                  setTelegramMsg("Telegram settings saved.");
                } catch {
                  setTelegramMsg("Save failed.");
                }
              }}
            >
              Save Telegram
            </button>
            <button
              type="button"
              className="rounded-lg bg-zinc-800 px-4 py-2 text-sm"
              onClick={async () => {
                if (!token) return;
                setTelegramMsg(null);
                try {
                  await sendTelegramTest(token);
                  setTelegramMsg("Test message sent.");
                } catch (e) {
                  setTelegramMsg(e instanceof Error ? e.message : "Test failed.");
                }
              }}
            >
              Send test message
            </button>
          </div>
          {telegramMsg && <p className="mt-2 text-sm text-zinc-400">{telegramMsg}</p>}
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
