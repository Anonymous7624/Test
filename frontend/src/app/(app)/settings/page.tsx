"use client";

import { FormEvent, useCallback, useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@/context/auth-context";
import { GeoapifyLocationInput } from "@/components/geoapify-location-input";
import {
  deleteAccount,
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
  const router = useRouter();
  const { token, logout } = useAuth();
  const [settings, setSettings] = useState<UserSettings | null>(null);
  const [categoryOptions, setCategoryOptions] = useState<{ id: string; label: string }[]>([]);
  const [msg, setMsg] = useState<string | null>(null);
  const [telegramMsg, setTelegramMsg] = useState<string | null>(null);
  const [verifyInfo, setVerifyInfo] = useState<{
    code: string;
    instructions: string;
    expiresAt: string;
    startCommand: string;
  } | null>(null);
  const [readiness, setReadiness] = useState<MonitoringReadiness | null>(null);
  const [worker, setWorker] = useState<WorkerStatusPayload | null>(null);
  const [runErr, setRunErr] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deletePassword, setDeletePassword] = useState("");
  const [deleteErr, setDeleteErr] = useState<string | null>(null);

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
    if (settings?.telegram_connected) setVerifyInfo(null);
  }, [settings?.telegram_connected]);

  useEffect(() => {
    if (!token) return;
    const id = window.setInterval(() => {
      void (async () => {
        try {
          const [st, rd, s] = await Promise.all([
            workerStatus(token),
            fetchMonitoringReadiness(token),
            fetchSettings(token),
          ]);
          setWorker(st);
          setReadiness(rd);
          setSettings(s);
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
    setSaving(true);
    try {
      const { radius_km, ...rest } = settings;
      void radius_km;
      const next = await updateSettings(token, { ...rest, radius_miles: settings.radius_miles });
      setSettings(next);
      const rd = await fetchMonitoringReadiness(token);
      setReadiness(rd);
      setMsg("Saved.");
    } catch (err) {
      setMsg(err instanceof Error ? err.message : "Save failed.");
    } finally {
      setSaving(false);
    }
  }

  const readinessRows = readiness?.checks ?? [];

  if (!settings) {
    return <p className="text-zinc-500">Loading…</p>;
  }

  const busyStates = new Set(["starting", "searching", "monitoring"]);
  const workerBusy = settings.monitoring_enabled && busyStates.has(worker?.monitoring_state ?? "");
  const canRun = (readiness?.ready ?? false) && !settings.monitoring_enabled;
  const canSave = Boolean(token && settings && !saving);
  const saveDisabledReason = !token
    ? "Sign in to save settings."
    : !settings
      ? "Loading settings…"
      : saving
        ? "Saving…"
        : null;
  let runDisabledReason: string | null = null;
  if (!readiness) runDisabledReason = "Loading readiness…";
  else if (settings.monitoring_enabled) runDisabledReason = "Stop monitoring before starting again.";
  else if (!readiness.ready) runDisabledReason = "Run is blocked until every item below passes (save settings after edits).";

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

          <div className="rounded-lg border border-zinc-800 bg-zinc-950/50 px-3 py-3">
            <p className="text-xs text-zinc-400">
              Save stores location, radius, category, price, and Telegram fields. This does not depend on monitoring
              readiness or Telegram — you can save partial progress anytime.
            </p>
            <div className="mt-3 flex flex-wrap items-center gap-2">
              <button
                type="submit"
                disabled={!canSave}
                title={!canSave ? saveDisabledReason ?? undefined : undefined}
                aria-label="Save settings"
                className="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-medium disabled:cursor-not-allowed disabled:opacity-40"
              >
                {saving ? "Saving…" : "Save settings"}
              </button>
              {!canSave && saveDisabledReason ? (
                <span className="text-xs text-amber-200/90">{saveDisabledReason}</span>
              ) : null}
            </div>
            {msg && <p className="mt-2 text-sm text-emerald-400">{msg}</p>}
          </div>

          <div className="rounded-lg border border-zinc-800 bg-zinc-950/50 p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h2 className="text-sm font-medium text-zinc-200">Telegram alerts</h2>
              <span
                className={`rounded-full px-2 py-0.5 text-[11px] font-medium ${
                  settings.telegram_connected
                    ? "bg-emerald-900/60 text-emerald-200"
                    : settings.telegram_verify_pending
                      ? "bg-amber-900/50 text-amber-200"
                      : "bg-zinc-800 text-zinc-400"
                }`}
              >
                {settings.telegram_connected
                  ? "Connected"
                  : settings.telegram_verify_pending
                    ? "Verification pending"
                    : "Not connected"}
              </span>
            </div>
            <p className="mt-2 text-xs leading-relaxed text-zinc-400">
              Profit alerts are sent by our Telegram bot. The bot token is only stored on the server (
              <code className="text-zinc-500">TELEGRAM_BOT_TOKEN</code>). You link your personal Telegram chat with a
              one-time code — no need to paste a chat ID unless you use the optional fallback below.
            </p>
            <div className="mt-3 rounded-lg border border-zinc-700/80 bg-zinc-900/40 px-3 py-2 text-xs text-zinc-300">
              <p className="font-medium text-zinc-200">Link Telegram (recommended)</p>
              <ol className="mt-2 list-decimal space-y-1.5 pl-4 text-zinc-400">
                <li>Open the Telegram app (phone or desktop).</li>
                <li>
                  Search for the bot{" "}
                  <span className="font-mono text-emerald-300/95">{settings.telegram_bot_username}</span> and open the
                  chat.
                </li>
                <li>
                  Tap Start or use the exact command shown below (copy it). The server will save your chat
                  automatically.
                </li>
              </ol>
            </div>
            {settings.telegram_verify_pending && !settings.telegram_connected ? (
              <p className="mt-2 text-xs text-amber-400/90">
                Code pending — open the bot and send the command before it expires.
              </p>
            ) : null}
            {verifyInfo && !settings.telegram_verify_pending && !settings.telegram_connected ? (
              <p className="mt-2 text-xs text-zinc-500">
                This verification code is no longer active (expired or replaced). Generate a new one.
              </p>
            ) : null}
            {verifyInfo ? (
              <div className="mt-3 space-y-2 rounded-lg border border-emerald-900/40 bg-emerald-950/25 px-3 py-3">
                <p className="text-[11px] font-medium uppercase tracking-wide text-emerald-400/90">Send this command</p>
                <div className="flex flex-wrap items-center gap-2">
                  <code className="block flex-1 min-w-[12rem] rounded border border-emerald-900/50 bg-zinc-950 px-3 py-2 font-mono text-sm text-emerald-200">
                    {verifyInfo.startCommand}
                  </code>
                  <button
                    type="button"
                    className="shrink-0 rounded-lg bg-emerald-800 px-3 py-2 text-xs font-medium text-white"
                    onClick={() => {
                      void navigator.clipboard.writeText(verifyInfo.startCommand);
                      setTelegramMsg("Command copied to clipboard.");
                    }}
                  >
                    Copy
                  </button>
                </div>
                <p className="text-xs text-zinc-500">{verifyInfo.instructions}</p>
                <p className="text-[11px] text-zinc-500">Expires {new Date(verifyInfo.expiresAt).toLocaleString()}</p>
              </div>
            ) : null}
            <div className="mt-4 border-t border-zinc-800 pt-4">
              <label className="block text-xs text-zinc-500">
                Manual chat ID <span className="text-zinc-600">(optional fallback / debug)</span>
              </label>
              <input
                type="text"
                inputMode="numeric"
                autoComplete="off"
                placeholder="Only if you cannot use verification — paste numeric chat id"
                className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 font-mono text-sm"
                value={settings.telegram_chat_id ?? ""}
                onChange={(e) =>
                  setSettings({
                    ...settings,
                    telegram_chat_id: e.target.value.trim() === "" ? null : e.target.value.trim(),
                  })
                }
              />
              <p className="mt-1 text-[11px] text-zinc-600">
                Save settings after editing. If set, Run treats Telegram as configured without the /start code flow.
              </p>
            </div>
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
                      startCommand: v.start_command,
                    });
                    const next = await fetchSettings(token);
                    setSettings(next);
                    const rd = await fetchMonitoringReadiness(token);
                    setReadiness(rd);
                    setTelegramMsg(`Code ready — send it to ${next.telegram_bot_username} in Telegram.`);
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

          <div className="rounded-lg border border-zinc-800 bg-zinc-950/40 px-3 py-3">
            <p className="text-xs font-medium text-zinc-300">Run requirements</p>
            {!readiness ? (
              <p className="mt-2 text-xs text-zinc-500">Loading readiness…</p>
            ) : readinessRows.length > 0 ? (
              <ul className="mt-2 space-y-1.5">
                {readinessRows.map((c) => (
                  <li key={c.id} className="flex items-start gap-2 text-xs">
                    <span className="select-none" aria-hidden>
                      {c.ok ? "✅" : "❌"}
                    </span>
                    <span className={c.ok ? "text-zinc-400" : "text-amber-200/95"}>{c.label}</span>
                  </li>
                ))}
              </ul>
            ) : (
              <ul className="mt-2 space-y-1 text-xs text-amber-100/90">
                {(readiness.errors ?? []).map((err) => (
                  <li key={err}>• {err}</li>
                ))}
              </ul>
            )}
            {readiness?.ready ? (
              <p className="mt-3 text-xs text-emerald-500/90">All checks pass — you can run monitoring.</p>
            ) : readiness ? (
              <p className="mt-3 text-xs text-amber-200/80">Fix every item marked ❌ (and save settings if you changed them).</p>
            ) : null}
          </div>

          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              className={`inline-flex items-center gap-2 rounded-lg px-4 py-2 text-sm disabled:cursor-not-allowed disabled:opacity-40 ${
                canRun ? "bg-emerald-700" : "border border-zinc-600 bg-zinc-800"
              }`}
              disabled={!canRun}
              title={!canRun ? (runDisabledReason ?? "Run disabled") : "Start persistent monitoring"}
              aria-label="Run monitoring (requires readiness)"
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
          {!canRun && runDisabledReason ? (
            <p className="text-xs text-zinc-500">{runDisabledReason}</p>
          ) : null}
        </form>

        <div className="mt-10 max-w-lg rounded-xl border border-red-900/40 bg-red-950/20 px-4 py-4">
          <h2 className="text-sm font-medium text-red-200/95">Danger zone</h2>
          <p className="mt-1 text-xs text-zinc-500">
            Permanently delete your account and your listings. This cannot be undone.
          </p>
          {!deleteOpen ? (
            <button
              type="button"
              className="mt-3 rounded-lg border border-red-900/60 bg-red-950/40 px-3 py-2 text-sm text-red-200"
              onClick={() => {
                setDeleteOpen(true);
                setDeleteErr(null);
                setDeletePassword("");
              }}
            >
              Delete account…
            </button>
          ) : (
            <div className="mt-3 space-y-2">
              <label className="block text-xs text-zinc-500">Confirm with your password</label>
              <input
                type="password"
                autoComplete="current-password"
                className="w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm"
                value={deletePassword}
                onChange={(e) => setDeletePassword(e.target.value)}
              />
              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  className="rounded-lg bg-red-800 px-3 py-2 text-sm"
                  onClick={async () => {
                    if (!token) return;
                    setDeleteErr(null);
                    try {
                      await deleteAccount(token, deletePassword);
                      logout();
                      router.replace("/login");
                    } catch (e) {
                      setDeleteErr(e instanceof Error ? e.message : "Delete failed.");
                    }
                  }}
                >
                  Delete my account
                </button>
                <button
                  type="button"
                  className="rounded-lg bg-zinc-800 px-3 py-2 text-sm"
                  onClick={() => {
                    setDeleteOpen(false);
                    setDeletePassword("");
                    setDeleteErr(null);
                  }}
                >
                  Cancel
                </button>
              </div>
              {deleteErr ? <p className="text-sm text-red-400">{deleteErr}</p> : null}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
