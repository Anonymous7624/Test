"use client";

import { FormEvent, useCallback, useEffect, useRef, useState } from "react";
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
import { copyTextSafe } from "@/lib/clipboard";

const MONITORING_BUSY_STATES = new Set(["starting", "backfill", "polling"]);

function editableSnapshot(s: UserSettings): string {
  return JSON.stringify({
    location_text: s.location_text,
    center_lat: s.center_lat,
    center_lon: s.center_lon,
    geoapify_place_id: s.geoapify_place_id,
    radius_miles: s.radius_miles,
    search_mode: s.search_mode,
    marketplace_category_slug: s.marketplace_category_slug,
    custom_keywords: s.custom_keywords,
    telegram_chat_id: s.telegram_chat_id ?? null,
    telegram_alert_mode: s.telegram_alert_mode,
  });
}

type SaveUiState = "unsaved" | "saving" | "saved" | "error";

export default function SettingsPage() {
  const router = useRouter();
  const { token, logout, user } = useAuth();
  const [settings, setSettings] = useState<UserSettings | null>(null);
  const [categoryOptions, setCategoryOptions] = useState<{ slug: string; label: string }[]>([]);
  const [keywordDraft, setKeywordDraft] = useState("");
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
  const [saveUi, setSaveUi] = useState<SaveUiState>("saved");
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deletePassword, setDeletePassword] = useState("");
  const [deleteErr, setDeleteErr] = useState<string | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  const lastSavedSnapshotRef = useRef<string>("");
  const isHydratedRef = useRef(false);
  const editableDirtyRef = useRef(false);

  const loadAll = useCallback(async () => {
    if (!token) return;
    setLoadError(null);
    try {
      const [{ categories }, s, st, rd] = await Promise.all([
        fetchCategories().catch(() => ({ categories: [] as { slug: string; label: string }[] })),
        fetchSettings(token),
        workerStatus(token),
        fetchMonitoringReadiness(token).catch(
          (): MonitoringReadiness => ({
            ready: false,
            errors: ["Could not load readiness"],
            checks: [],
          }),
        ),
      ]);
      setCategoryOptions(categories.map((c) => ({ slug: c.slug, label: c.label })));
      setSettings(s);
      lastSavedSnapshotRef.current = editableSnapshot(s);
      isHydratedRef.current = true;
      editableDirtyRef.current = false;
      setSaveUi("saved");
      setWorker(st);
      setReadiness(rd);
    } catch (e) {
      setLoadError(e instanceof Error ? e.message : "Failed to load settings");
    }
  }, [token]);

  useEffect(() => {
    void loadAll();
  }, [loadAll]);

  useEffect(() => {
    if (settings?.telegram_connected) setVerifyInfo(null);
  }, [settings?.telegram_connected]);

  useEffect(() => {
    if (!token || !settings || !isHydratedRef.current) return;
    if (settings.monitoring_enabled) return;
    const snap = editableSnapshot(settings);
    if (snap === lastSavedSnapshotRef.current) return;
    editableDirtyRef.current = true;
    setSaveUi("unsaved");
    const t = window.setTimeout(() => {
      void (async () => {
        if (!token || !settings) return;
        const snapBeforeSave = editableSnapshot(settings);
        if (snapBeforeSave === lastSavedSnapshotRef.current) return;
        setSaveUi("saving");
        try {
          const { radius_km, ...rest } = settings;
          void radius_km;
          const next = await updateSettings(token, { ...rest, radius_miles: settings.radius_miles });
          if (editableSnapshot(settings) !== snapBeforeSave) {
            setReadiness(await fetchMonitoringReadiness(token));
            setSaveUi("unsaved");
            return;
          }
          setSettings(next);
          lastSavedSnapshotRef.current = editableSnapshot(next);
          editableDirtyRef.current = false;
          setSaveUi("saved");
          const rd = await fetchMonitoringReadiness(token);
          setReadiness(rd);
        } catch {
          setSaveUi("error");
        }
      })();
    }, 700);
    return () => window.clearTimeout(t);
  }, [settings, token]);

  useEffect(() => {
    if (!token) return;
    const id = window.setInterval(() => {
      void (async () => {
        const st = await workerStatus(token);
        setWorker(st);
        try {
          const rd = await fetchMonitoringReadiness(token);
          setReadiness(rd);
        } catch {
          /* transient readiness failure */
        }
        if (!editableDirtyRef.current) {
          try {
            const s = await fetchSettings(token);
            setSettings(s);
            lastSavedSnapshotRef.current = editableSnapshot(s);
            setSaveUi("saved");
          } catch {
            /* keep UI stable if settings poll fails */
          }
        }
      })();
    }, 4000);
    return () => window.clearInterval(id);
  }, [token]);

  function onFormSubmit(e: FormEvent) {
    e.preventDefault();
  }

  const readinessRows = readiness?.checks ?? [];

  if (!settings) {
    if (loadError) {
      return (
        <div>
          <h1 className="text-2xl font-semibold">Settings</h1>
          <p className="mt-4 rounded-lg border border-red-900/50 bg-red-950/30 px-3 py-2 text-sm text-red-200">
            {loadError}
          </p>
        </div>
      );
    }
    return <p className="text-zinc-500">Loading…</p>;
  }

  const msSettings = (settings.monitoring_state ?? "").toLowerCase();
  const msLive = (() => {
    const w = worker?.monitoring_state ?? "";
    const wl = w.toLowerCase();
    if (worker?.status_fetch_error || wl === "" || wl === "unknown") return msSettings;
    return wl;
  })();
  const settingsLocked = settings.monitoring_enabled;
  const workerBusy = settings.monitoring_enabled && MONITORING_BUSY_STATES.has(msLive);
  const canRun = (readiness?.ready ?? false) && !settings.monitoring_enabled;

  let runDisabledReason: string | null = null;
  if (!readiness) runDisabledReason = "Loading readiness…";
  else if (settings.monitoring_enabled) runDisabledReason = "Stop monitoring before starting again.";
  else if (!readiness.ready)
    runDisabledReason =
      "Run is blocked until every item below passes (finish editing and wait until settings show Saved).";

  const saveUiLabel =
    saveUi === "saving"
      ? "Saving…"
      : saveUi === "saved"
        ? "Saved"
        : saveUi === "unsaved"
          ? "Unsaved changes"
          : "Error saving";

  const saveUiClass =
    saveUi === "error"
      ? "text-red-400"
      : saveUi === "unsaved"
        ? "text-amber-200/95"
        : saveUi === "saving"
          ? "text-zinc-300"
          : "text-emerald-400/95";

  return (
    <div>
      <h1 className="text-2xl font-semibold">Settings</h1>

      <div
        className="mt-3 flex min-h-[2rem] items-center rounded-lg border border-zinc-800 bg-zinc-950/50 px-3 py-2 text-sm"
        role="status"
        aria-live="polite"
      >
        <span className={`font-medium ${saveUiClass}`}>{saveUiLabel}</span>
      </div>

      {settingsLocked ? (
        <p className="mt-4 rounded-lg border border-amber-900/50 bg-amber-950/30 px-3 py-2 text-sm text-amber-100/95">
          Monitoring is running. Stop it before changing settings.
        </p>
      ) : null}

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
                {settings.monitoring_enabled ? (msLive || "…").toUpperCase() : "IDLE"}
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
          {worker?.status_fetch_error ? (
            <p className="mt-3 rounded-lg border border-amber-900/50 bg-amber-950/25 px-3 py-2 text-xs text-amber-100/90">
              Cannot fetch worker status: {worker.status_fetch_error}
            </p>
          ) : null}
          {worker?.pipeline_message && !worker?.configuration_error ? (
            <p className="mt-2 text-xs text-zinc-400">
              <span className="text-zinc-500">Pipeline: </span>
              {worker.pipeline_message}
            </p>
          ) : null}
          {worker?.pipeline_counts ? (
            <div className="mt-2">
              <p className="text-[10px] uppercase tracking-wide text-zinc-600">
                Last completed batch (Steps 1–4) — not lifetime totals
              </p>
              <dl className="mt-1 grid gap-1 font-mono text-[11px] text-zinc-500 sm:grid-cols-2">
                <div>
                  <dt className="inline text-zinc-600">Raw collected </dt>
                  <dd className="inline text-zinc-400">{worker.pipeline_counts.raw_collected}</dd>
                </div>
                <div>
                  <dt className="inline text-zinc-600">After prefilter </dt>
                  <dd className="inline text-zinc-400">{worker.pipeline_counts.step1_kept}</dd>
                </div>
                <div>
                  <dt className="inline text-zinc-600">Matched </dt>
                  <dd className="inline text-zinc-400">{worker.pipeline_counts.step2_matched}</dd>
                </div>
                <div>
                  <dt className="inline text-zinc-600">Scored </dt>
                  <dd className="inline text-zinc-400">{worker.pipeline_counts.step3_scored}</dd>
                </div>
                <div>
                  <dt className="inline text-zinc-600">Saved / alerts </dt>
                  <dd className="inline text-zinc-400">
                    {worker.pipeline_counts.step4_saved} / {worker.pipeline_counts.alerts_sent}
                  </dd>
                </div>
              </dl>
            </div>
          ) : null}
          {worker?.configuration_error ? (
            <p className="mt-3 rounded-lg border border-amber-900/50 bg-amber-950/25 px-3 py-2 text-xs text-amber-100/95">
              <span className="font-medium text-amber-200/95">Search settings — </span>
              {worker.configuration_error}
            </p>
          ) : null}
          {worker?.last_error ? (
            <p className="mt-3 rounded-lg border border-red-900/50 bg-red-950/30 px-3 py-2 text-xs text-red-200">
              <span className="font-medium text-red-300/95">Collector / pipeline error — </span>
              {worker.last_error}
            </p>
          ) : null}
          {worker?.pipeline_error ? (
            <p className="mt-2 rounded-lg border border-amber-900/40 bg-amber-950/20 px-3 py-2 text-xs text-amber-100/95">
              <span className="font-medium text-amber-200/95">In-pipeline notice — </span>
              {worker.pipeline_error}
              <span className="mt-1 block text-[10px] text-zinc-500">
                Clears when the current batch finishes successfully or a new cycle starts.
              </span>
            </p>
          ) : null}
          {user?.role === "admin" && worker?.admin_pipeline_snapshot ? (
            <details className="mt-3 rounded-lg border border-violet-900/40 bg-violet-950/20 px-3 py-2">
              <summary className="cursor-pointer text-xs font-medium text-violet-200/95">
                Admin · pipeline debug (live DB fields)
              </summary>
              <dl className="mt-2 grid gap-1.5 text-[11px] text-zinc-400 sm:grid-cols-2">
                <div>
                  <dt className="text-zinc-600">Current stage</dt>
                  <dd className="font-mono text-zinc-300">
                    step {(worker.admin_pipeline_snapshot.worker_current_step as number) ?? "—"} ·{" "}
                    {String(worker.admin_pipeline_snapshot.worker_current_state ?? "—")}
                  </dd>
                </div>
                <div>
                  <dt className="text-zinc-600">Stored listings (DB)</dt>
                  <dd className="font-mono text-zinc-300">
                    {typeof worker.admin_pipeline_snapshot.stored_listings_count === "number"
                      ? worker.admin_pipeline_snapshot.stored_listings_count
                      : worker.listings_found_count}
                  </dd>
                </div>
                <div className="sm:col-span-2">
                  <dt className="text-zinc-600">Message</dt>
                  <dd className="font-mono text-xs text-zinc-300">
                    {(worker.admin_pipeline_snapshot.worker_pipeline_message as string) || "—"}
                  </dd>
                </div>
                <div>
                  <dt className="text-zinc-600">Last batch started</dt>
                  <dd className="font-mono text-zinc-300">
                    {worker.admin_pipeline_snapshot.worker_last_batch_started_at
                      ? new Date(String(worker.admin_pipeline_snapshot.worker_last_batch_started_at)).toLocaleString()
                      : "—"}
                  </dd>
                </div>
                <div>
                  <dt className="text-zinc-600">Last successful batch</dt>
                  <dd className="font-mono text-zinc-300">
                    {worker.admin_pipeline_snapshot.worker_last_success_at
                      ? new Date(String(worker.admin_pipeline_snapshot.worker_last_success_at)).toLocaleString()
                      : "—"}
                  </dd>
                </div>
                <div className="sm:col-span-2">
                  <dt className="text-zinc-600">Last fatal error (raw DB)</dt>
                  <dd className="text-xs text-red-300/90">
                    {(worker.admin_pipeline_snapshot.last_error_db as string | null) || "—"}
                  </dd>
                </div>
                <div className="sm:col-span-2">
                  <dt className="text-zinc-600">Last fatal error (active / API)</dt>
                  <dd className="text-xs text-emerald-200/90">
                    {(worker.admin_pipeline_snapshot.last_error_active as string | null) || "—"}
                  </dd>
                </div>
                <div className="sm:col-span-2">
                  <dt className="text-zinc-600">Pipeline error (DB)</dt>
                  <dd className="text-xs text-amber-200/90">
                    {(worker.admin_pipeline_snapshot.worker_pipeline_error as string | null) || "—"}
                  </dd>
                </div>
              </dl>
              <pre className="mt-2 max-h-40 overflow-auto text-[10px] text-zinc-500">
                {JSON.stringify(worker.admin_pipeline_snapshot, null, 2)}
              </pre>
            </details>
          ) : null}
        </div>

        <form className="max-w-lg space-y-4" onSubmit={onFormSubmit}>
          <div className={`space-y-4 ${settingsLocked ? "opacity-55" : ""}`} aria-disabled={settingsLocked}>
          <div>
            <label className="block text-xs text-zinc-500">Location</label>
            <GeoapifyLocationInput
              location_text={settings.location_text}
              center_lat={settings.center_lat}
              center_lon={settings.center_lon}
              geoapify_place_id={settings.geoapify_place_id}
              disabled={settingsLocked}
              onChange={(next) =>
                setSettings({
                  ...settings,
                  location_text: next.location_text,
                  center_lat: next.center_lat,
                  center_lon: next.center_lon,
                  geoapify_place_id: next.geoapify_place_id,
                })
              }
              inputClassName="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm disabled:cursor-not-allowed"
            />
            <p className="mt-1 text-[11px] text-zinc-500">Select a suggestion so the place is validated (not free text only).</p>
          </div>
          <div>
            <label className="block text-xs text-zinc-500">Search radius (miles)</label>
            <input
              type="number"
              min={5}
              step={0.1}
              disabled={settingsLocked}
              className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm disabled:cursor-not-allowed"
              value={settings.radius_miles}
              onChange={(e) =>
                setSettings({ ...settings, radius_miles: Number(e.target.value) })
              }
            />
          </div>
          <div className="space-y-3">
            <div>
              <label className="block text-xs text-zinc-500">Search mode</label>
              <select
                disabled={settingsLocked}
                className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm disabled:cursor-not-allowed"
                value={settings.search_mode}
                onChange={(e) => {
                  const v = e.target.value as "marketplace_category" | "custom_keywords";
                  if (v === "marketplace_category") {
                    const first = categoryOptions[0]?.slug ?? "electronics";
                    const slug = settings.marketplace_category_slug ?? first;
                    setSettings({
                      ...settings,
                      search_mode: v,
                      marketplace_category_slug: slug,
                      marketplace_category_label:
                        categoryOptions.find((c) => c.slug === slug)?.label ?? null,
                    });
                  } else {
                    setSettings({
                      ...settings,
                      search_mode: v,
                      marketplace_category_slug: null,
                      marketplace_category_label: null,
                    });
                  }
                }}
              >
                <option value="marketplace_category">Built-in Marketplace category</option>
                <option value="custom_keywords">Custom keywords</option>
              </select>
              <p className="mt-1 text-[11px] text-zinc-500">
                Category mode scrolls the official Marketplace category feed. Keyword mode runs a Marketplace search
                for each phrase (not global Facebook search).
              </p>
            </div>

            {settings.search_mode === "marketplace_category" ? (
              <div>
                <label className="block text-xs text-zinc-500">Marketplace category</label>
                <select
                  disabled={settingsLocked}
                  className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm disabled:cursor-not-allowed"
                  value={settings.marketplace_category_slug ?? ""}
                  onChange={(e) => {
                    const slug = e.target.value;
                    const lab = categoryOptions.find((c) => c.slug === slug)?.label ?? slug;
                    setSettings({
                      ...settings,
                      marketplace_category_slug: slug,
                      marketplace_category_label: lab,
                    });
                  }}
                >
                  {categoryOptions.map((c) => (
                    <option key={c.slug} value={c.slug}>
                      {c.label}
                    </option>
                  ))}
                </select>
              </div>
            ) : (
              <div>
                <label className="block text-xs text-zinc-500">
                  Custom keywords <span className="text-zinc-600">({settings.custom_keywords.length}/15)</span>
                </label>
                <div className="mt-1 flex flex-wrap gap-1.5">
                  {settings.custom_keywords.map((kw, i) => (
                    <span
                      key={`${kw}-${i}`}
                      className="inline-flex items-center gap-1 rounded-full border border-zinc-700 bg-zinc-900 px-2.5 py-1 text-xs text-zinc-200"
                    >
                      {kw}
                      <button
                        type="button"
                        disabled={settingsLocked}
                        className="rounded px-1 text-zinc-500 hover:text-zinc-200 disabled:cursor-not-allowed"
                        onClick={() =>
                          setSettings({
                            ...settings,
                            custom_keywords: settings.custom_keywords.filter((_, j) => j !== i),
                          })
                        }
                        aria-label={`Remove ${kw}`}
                      >
                        ×
                      </button>
                    </span>
                  ))}
                </div>
                <div className="mt-2 flex gap-2">
                  <input
                    type="text"
                    disabled={settingsLocked || settings.custom_keywords.length >= 15}
                    placeholder="e.g. iphone, herman miller"
                    className="min-w-0 flex-1 rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm disabled:cursor-not-allowed"
                    value={keywordDraft}
                    onChange={(e) => setKeywordDraft(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") {
                        e.preventDefault();
                        const t = keywordDraft.trim();
                        if (!t || settings.custom_keywords.length >= 15) return;
                        if (
                          settings.custom_keywords.some((k) => k.toLowerCase() === t.toLowerCase())
                        ) {
                          setKeywordDraft("");
                          return;
                        }
                        setSettings({
                          ...settings,
                          custom_keywords: [...settings.custom_keywords, t],
                        });
                        setKeywordDraft("");
                      }
                    }}
                  />
                  <button
                    type="button"
                    disabled={
                      settingsLocked ||
                      settings.custom_keywords.length >= 15 ||
                      !keywordDraft.trim()
                    }
                    className="shrink-0 rounded-lg bg-zinc-800 px-3 py-2 text-sm disabled:cursor-not-allowed disabled:opacity-40"
                    onClick={() => {
                      const t = keywordDraft.trim();
                      if (!t || settings.custom_keywords.length >= 15) return;
                      if (settings.custom_keywords.some((k) => k.toLowerCase() === t.toLowerCase())) {
                        setKeywordDraft("");
                        return;
                      }
                      setSettings({
                        ...settings,
                        custom_keywords: [...settings.custom_keywords, t],
                      });
                      setKeywordDraft("");
                    }}
                  >
                    Add
                  </button>
                </div>
                <p className="mt-1 text-[11px] text-zinc-500">
                  Up to 15 phrases; duplicates and extra spaces are removed on save.
                </p>
              </div>
            )}
          </div>

          <div className="rounded-lg border border-zinc-800 bg-zinc-950/50 px-3 py-3">
            <p className="text-xs text-zinc-400">
              Location, radius, search settings, and Telegram fields save automatically shortly after you stop editing.
              Partial progress is kept when validation fails for a field (check the status above).
            </p>
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
                    disabled={settingsLocked}
                    className="shrink-0 rounded-lg bg-emerald-800 px-3 py-2 text-xs font-medium text-white disabled:cursor-not-allowed disabled:opacity-40"
                    onClick={() => {
                      void copyTextSafe(verifyInfo.startCommand).then(({ message }) => setTelegramMsg(message));
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
                disabled={settingsLocked}
                placeholder="Only if you cannot use verification — paste numeric chat id"
                className="mt-1 w-full rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 font-mono text-sm disabled:cursor-not-allowed"
                value={settings.telegram_chat_id ?? ""}
                onChange={(e) =>
                  setSettings({
                    ...settings,
                    telegram_chat_id: e.target.value.trim() === "" ? null : e.target.value.trim(),
                  })
                }
              />
              <p className="mt-1 text-[11px] text-zinc-600">
                Changes save automatically. If set, Run treats Telegram as configured without the /start code flow.
              </p>
            </div>
            {settings.telegram_connected ? (
              <div className="mt-4 rounded-lg border border-zinc-800 bg-zinc-900/30 px-3 py-3">
                <label className="block text-xs font-medium text-zinc-300">Telegram alert mode</label>
                <p className="mt-1 text-[11px] text-zinc-500">
                  Controls which saved listings trigger a Telegram message (templates are fixed; not AI-written).
                </p>
                <select
                  className="mt-2 w-full max-w-md rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-200 disabled:cursor-not-allowed"
                  disabled={settingsLocked}
                  value={settings.telegram_alert_mode}
                  onChange={(e) =>
                    setSettings({
                      ...settings,
                      telegram_alert_mode: e.target.value as UserSettings["telegram_alert_mode"],
                    })
                  }
                >
                  <option value="any_listing">Every matched listing</option>
                  <option value="profitable_only">Profitable listings only</option>
                  <option value="none">None (no Telegram alerts)</option>
                </select>
              </div>
            ) : null}
            <div className="mt-3 flex flex-wrap gap-2">
              <button
                type="button"
                disabled={settingsLocked}
                className="rounded-lg bg-emerald-700 px-4 py-2 text-sm disabled:cursor-not-allowed disabled:opacity-40"
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
                    lastSavedSnapshotRef.current = editableSnapshot(next);
                    editableDirtyRef.current = false;
                    setSaveUi("saved");
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
                className="rounded-lg bg-zinc-800 px-4 py-2 text-sm disabled:cursor-not-allowed disabled:opacity-40"
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
                disabled={settingsLocked || !settings.telegram_connected}
              >
                Send test message
              </button>
            </div>
            {telegramMsg && <p className="mt-2 text-sm text-zinc-400">{telegramMsg}</p>}
          </div>
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
              <p className="mt-3 text-xs text-amber-200/80">
                Fix every item marked ❌ (edits save automatically — wait for Saved before Run if you changed settings).
              </p>
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
                  lastSavedSnapshotRef.current = editableSnapshot(s);
                  editableDirtyRef.current = false;
                  setSaveUi("saved");
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
                lastSavedSnapshotRef.current = editableSnapshot(s);
                editableDirtyRef.current = false;
                setSaveUi("saved");
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
