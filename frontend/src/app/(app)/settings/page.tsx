"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { useAuth } from "@/context/auth-context";
import { GeoapifyLocationInput } from "@/components/geoapify-location-input";
import {
  fetchCategories,
  fetchMonitoringReadiness,
  fetchSettings,
  sendTelegramTest,
  startTelegramVerification,
  updateSettings,
  workerRun,
  workerStatus,
  workerStop,
  type MonitoringReadiness,
  type UserSettings,
  type WorkerStatusPayload,
} from "@/lib/api";

export default function SettingsPage() {
  const { token } = useAuth();
  const [settings, setSettings] = useState<UserSettings | null>(null);
  const [categoryOptions, setCategoryOptions] = useState<{ id: string; label: string }[]>([]);
  const [msg, setMsg] = useState<string | null>(null);
  const [telegramMsg, setTelegramMsg] = useState<string | null>(null);
  const [verifyInfo, setVerifyInfo] = useState<{ code: string; instructions: string; expiresAt: string } | null>(
    null,
  );
  const [readiness, setReadiness] = useState<MonitoringReadiness | null>(null);
  const [worker, setWorker] = useState<WorkerStatusPayload | null>(null);
  const [runErr, setRunErr] = useState<string | null>(null);

  const loadAll = useCallback(async () => {
    if (!token) return;
    const [{ categories }, s, st, rd] = await Promise.all([
      fetchCategories(),
      fetchSettings(token),
      workerStatus(token),
      fetchMonitoringReadiness(token),
    ]);
    setCategoryOptions(categories.map((c) => ({ id: c.id, label: c.label })));
    setSettings(s);
    setWorker(st);
    setReadiness(rd);
  }, [token]);

  useEffect(() => {
    void loadAll();
  }, [loadAll]);

  useEffect(() => {
    if (!token) return;
    const id = window.setInterval(() => {
      void (async () => {
        try {
          const [st, rd] = await Promise.all([workerStatus(token), fetchMonitoringReadiness(token)]);
          setWorker(st);
          setReadiness(rd);
        } catch {
          /* ignore */
        }
      })();
    }, 4000);
    return () => window.clearInterval(id);
  }, [token]);

  async function onSubmit(e: FormEvent) {
    e.preventDefault();
    if (!token || !settings) return;
    setMsg(null);
    try {
      const { radius_km, ...rest } = settings;
      void radius_km;
      const next = await updateSettings(token, { ...rest, radius_miles: settings.radius_miles });
      setSettings(next);
      const rd = await fetchMonitoringReadiness(token);
      setReadiness(rd);
      setMsg("Saved.");
    } catch {
      setMsg("Save failed.");
    }
  }

  const checklist = useMemo(() => {
    const errs = readiness?.errors ?? [];
    return errs.length === 0 ? null : errs;
  }, [readiness]);

  if (!settings) {
    return <p className="text-zinc-500">Loading…</p>;
  }

  const busyStates = new Set(["starting", "searching", "monitoring"]);
  const workerBusy = settings.monitoring_enabled && busyStates.has(worker?.monitoring_state ?? "");
  const canRun = (readiness?.ready ?? false) && !settings.monitoring_enabled;

  return (
    <div>
      <h1 className="text-2xl font-semibold">Settings</h1>

      <div className="mt-6 max-w-2xl space-y-6">
        <div className="rounded-xl border border-zinc-800 bg-zinc-950/40 p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-sm font-medium text-zinc-200">Monitoring status</h2>
              <p className="mt-1 text-xs text-zinc-500">
                The worker process must be running separately; it polls your saved settings and writes listings.
              </p>
            </div>
            <div className="flex items-center gap-2">
              {workerBusy ? (
                <span
                  className="inline-flex h-8 w-8 animate-spin rounded-full border-2 border-zinc-600 border-t-emerald-400"
                  aria-hidden
                />
              ) : null}
              <span
                className={`rounded-full px-3 py-1 text-xs font-medium ${
                  settings.monitoring_enabled
                    ? "bg-emerald-900/50 text-emerald-300"
                    : "bg-zinc-800 text-zinc-400"
                }`}
              >
                {settings.monitoring_enabled ? (worker?.monitoring_state ?? "…").toUpperCase() : "IDLE"}
              </span>
            </div>
          </div>
          <dl className="mt-4 grid gap-2 text-xs text-zinc-400 sm:grid-cols-2">
            <div>
              <dt className="text-zinc-500">Last checked</dt>
              <dd className="font-mono text-zinc-300">
                {worker?.last_checked_at ? new Date(worker.last_checked_at).toLocaleString() : "—"}
              </dd>
            </div>
            <div>
              <dt className="text-zinc-500">Listings stored</dt>
              <dd className="font-mono text-zinc-300">{worker?.listings_found_count ?? "—"}</dd>
            </div>
            <div>
              <dt className="text-zinc-500">Alerts sent</dt>
              <dd className="font-mono text-zinc-300">{worker?.alerts_sent_count ?? "—"}</dd>
            </div>
            <div>
              <dt className="text-zinc-500">Backfill</dt>
              <dd className="font-mono text-zinc-300">
                {worker?.backfill_complete === false && settings.monitoring_enabled ? "In progress" : "Done / n/a"}
              </dd>
            </div>
          </dl>
          {worker?.last_error ? (
            <p className="mt-3 rounded-lg border border-red-900/50 bg-red-950/30 px-3 py-2 text-xs text-red-200">
              {worker.last_error}
            </p>
          ) : null}
        </div>

        <form className="max-w-lg space-y-4" onSubmit={onSubmit}>
          <div>
            <label className="block text-xs text-zinc-500">Location</label>
            <GeoapifyLocationInput
              location_text={settings.location_text}
              center_lat={settings.center_lat}
              center_lon={settings.center_lon}
              geoapify_place_id={settings.geoapify_place_id}
              onChange={(next) =>
                setSettings({
                  ...settings,
                  location_text: next.location_text,
                  center_lat: next.center_lat,
                  center_lon: next.center_lon,
                  geoapify_place_id: next.geoapify_place_id,
                })
              }
              inputClassName="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
            />
            <p className="mt-1 text-[11px] text-zinc-500">Select a suggestion so the place is validated (not free text only).</p>
          </div>
          <div>
            <label className="block text-xs text-zinc-500">Search radius (miles)</label>
            <input
              type="number"
              min={5}
              step={0.1}
              className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
              value={settings.radius_miles}
              onChange={(e) =>
                setSettings({ ...settings, radius_miles: Number(e.target.value) })
              }
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
            <label className="block text-xs text-zinc-500">Max price (USD)</label>
            <input
              type="number"
              min={10}
              step={1}
              className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
              value={settings.max_price}
              onChange={(e) => setSettings({ ...settings, max_price: Number(e.target.value) })}
            />
            <p className="mt-1 text-[11px] text-zinc-500">Prices are shown and stored in US dollars.</p>
          </div>

          <div className="rounded-lg border border-zinc-800 bg-zinc-950/50 p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h2 className="text-sm font-medium text-zinc-200">Telegram</h2>
              <span
                className={`rounded-full px-2 py-0.5 text-[11px] font-medium ${
                  settings.telegram_connected
                    ? "bg-emerald-900/60 text-emerald-200"
                    : "bg-zinc-800 text-zinc-400"
                }`}
              >
                {settings.telegram_connected ? "Connected" : "Not connected"}
              </span>
            </div>
            <p className="mt-2 text-xs text-zinc-500">
              Bot token stays in backend <code className="text-zinc-400">TELEGRAM_BOT_TOKEN</code> only. Link your
              chat with a one-time code.
            </p>
            {settings.telegram_verify_pending && !settings.telegram_connected ? (
              <p className="mt-2 text-xs text-amber-400/90">Verification code pending — complete in Telegram before it expires.</p>
            ) : null}
            {verifyInfo ? (
              <div className="mt-3 rounded-lg border border-emerald-900/40 bg-emerald-950/20 px-3 py-2 text-xs text-zinc-300">
                <p className="font-mono text-sm text-emerald-300">/start {verifyInfo.code}</p>
                <p className="mt-1 text-zinc-400">{verifyInfo.instructions}</p>
                <p className="mt-1 text-[11px] text-zinc-500">Expires {new Date(verifyInfo.expiresAt).toLocaleString()}</p>
              </div>
            ) : null}
            <div className="mt-3 flex flex-wrap gap-2">
              <button
                type="button"
                className="rounded-lg bg-emerald-700 px-4 py-2 text-sm"
                onClick={async () => {
                  if (!token) return;
                  setTelegramMsg(null);
                  try {
                    const v = await startTelegramVerification(token);
                    setVerifyInfo({
                      code: v.code,
                      instructions: v.instructions,
                      expiresAt: v.expires_at,
                    });
                    const next = await fetchSettings(token);
                    setSettings(next);
                    setTelegramMsg("Code generated. Send the command to your bot in Telegram.");
                  } catch (e) {
                    setTelegramMsg(e instanceof Error ? e.message : "Could not start verification.");
                  }
                }}
              >
                Generate verification code
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
                disabled={!settings.telegram_connected}
              >
                Send test message
              </button>
            </div>
            {telegramMsg && <p className="mt-2 text-sm text-zinc-400">{telegramMsg}</p>}
          </div>

          {checklist ? (
            <div className="rounded-lg border border-amber-900/40 bg-amber-950/20 px-3 py-2 text-xs text-amber-100/90">
              <p className="font-medium text-amber-200">Before Run, complete:</p>
              <ul className="mt-2 list-inside list-disc space-y-1">
                {checklist.map((c) => (
                  <li key={c}>{c}</li>
                ))}
              </ul>
            </div>
          ) : (
            <p className="text-xs text-emerald-500/90">All required checks pass — you can start monitoring.</p>
          )}

          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              className="inline-flex items-center gap-2 rounded-lg bg-emerald-700 px-4 py-2 text-sm disabled:cursor-not-allowed disabled:opacity-40"
              disabled={!canRun}
              title={!canRun ? "Fix checklist items or stop monitoring first" : "Start persistent monitoring"}
              onClick={async () => {
                if (!token) return;
                setRunErr(null);
                try {
                  await workerRun(token);
                  const [st, s, rd] = await Promise.all([
                    workerStatus(token),
                    fetchSettings(token),
                    fetchMonitoringReadiness(token),
                  ]);
                  setWorker(st);
                  setSettings(s);
                  setReadiness(rd);
                } catch (e) {
                  setRunErr(e instanceof Error ? e.message : "Failed to start");
                }
              }}
            >
              {workerBusy ? (
                <span className="inline-block h-4 w-4 animate-spin rounded-full border-2 border-zinc-300 border-t-transparent" />
              ) : null}
              Run monitoring
            </button>
            <button
              type="button"
              className="rounded-lg bg-zinc-800 px-4 py-2 text-sm disabled:opacity-40"
              disabled={!settings.monitoring_enabled}
              onClick={async () => {
                if (!token) return;
                setRunErr(null);
                await workerStop(token);
                const [st, s] = await Promise.all([workerStatus(token), fetchSettings(token)]);
                setWorker(st);
                setSettings(s);
              }}
            >
              Stop monitoring
            </button>
          </div>
          {runErr ? (
            <p className="text-sm text-red-400">
              {runErr}
            </p>
          ) : null}

          <button type="submit" className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium">
            Save settings
          </button>
          {msg && <p className="text-sm text-emerald-400">{msg}</p>}
        </form>
      </div>
    </div>
  );
}
