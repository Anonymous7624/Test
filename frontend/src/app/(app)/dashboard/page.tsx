"use client";

import { useCallback, useEffect, useState } from "react";
import { useAuth } from "@/context/auth-context";
import { ListingDetailModal } from "@/components/listing-detail-modal";
import { fetchListings, workerStatus, type ListingRow, type WorkerStatusPayload } from "@/lib/api";
import Link from "next/link";

const MONITORING_BUSY_STATES = new Set(["starting", "backfill", "polling"]);

export default function DashboardPage() {
  const { user, token } = useAuth();
  const [worker, setWorker] = useState<WorkerStatusPayload | null>(null);
  const [listings, setListings] = useState<ListingRow[]>([]);
  const [detail, setDetail] = useState<ListingRow | null>(null);

  const load = useCallback(async () => {
    if (!token) return;
    try {
      const st = await workerStatus(token);
      setWorker(st);
      const rows = await fetchListings(token, {}).catch(() => [] as ListingRow[]);
      setListings(rows);
    } catch {
      setListings([]);
    }
  }, [token]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (!token) return;
    const id = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(id);
  }, [token, load]);

  const statusOk = !worker?.status_fetch_error;
  const ms = (worker?.monitoring_state ?? "").toLowerCase();
  const workerBusy =
    statusOk && Boolean(worker?.monitoring_enabled && MONITORING_BUSY_STATES.has(ms));
  const isAdmin = user?.role === "admin";
  const pc = worker?.pipeline_counts;
  const cur = worker?.current_pipeline_counts;
  const batchActive = Boolean(worker?.batch_is_active);
  const storedCount = worker?.listings_found_count ?? listings.length;

  // Show a warning if monitoring is active but the worker hasn't sent a recent heartbeat.
  const workerNotRunning =
    statusOk &&
    Boolean(worker?.monitoring_enabled) &&
    worker?.worker_is_alive === false;

  // Human-readable labels for pipeline stage states.
  const STAGE_LABELS: Record<string, string> = {
    collecting_listings: "Step 1 · Collecting listings",
    step2_normalize: "Step 2 · Normalizing",
    step3_match: "Step 3 · Matching filters",
    step4_save_alert: "Step 4 · Saving & alerting",
    batch_complete: "Batch complete",
    no_listings_this_cycle: "No listings this cycle",
    batch_interrupted: "Interrupted",
    collector_error: "Collector error",
    configuration_error: "Configuration error",
    idle: "Idle",
    starting: "Starting",
  };

  function formatStageLabel(step: number | undefined, state: string | undefined): string {
    if (!state || state === "idle") return "Idle";
    if (STAGE_LABELS[state]) return STAGE_LABELS[state];
    return step != null && step > 0 ? `Step ${step} · ${state}` : state;
  }

  return (
    <div>
      <ListingDetailModal listing={detail} onClose={() => setDetail(null)} />
      <h1 className="text-2xl font-semibold">Dashboard</h1>
      <p className="mt-2 max-w-2xl text-zinc-400">
        Welcome, {user?.username}. This overview refreshes every few seconds while the app is open.
      </p>

      {worker?.status_fetch_error ? (
        <div className="mt-6 rounded-xl border border-amber-900/50 bg-amber-950/25 px-4 py-3 text-sm text-amber-100/95">
          <p className="font-medium text-amber-200/95">Worker status unavailable</p>
          <p className="mt-1 text-xs text-amber-100/85">{worker.status_fetch_error}</p>
          <p className="mt-2 text-xs text-zinc-500">
            Listings below may still load if the listings API is reachable. Fix the API URL or start the backend.
          </p>
        </div>
      ) : null}

      <div className="mt-8 grid gap-4 md:grid-cols-3">
        <div className="rounded-xl border border-zinc-800 bg-zinc-950/40 p-4">
          <div className="flex items-center justify-between gap-2">
            <h2 className="text-sm font-medium text-zinc-200">Worker activity</h2>
            {workerBusy ? (
              <span
                className="inline-flex h-6 w-6 animate-spin rounded-full border-2 border-zinc-600 border-t-emerald-400"
                aria-label="Working"
              />
            ) : (
              <span className="rounded-full bg-zinc-800 px-2 py-0.5 text-[11px] text-zinc-400">Idle</span>
            )}
          </div>
          <p className="mt-2 text-xs text-zinc-500">
            Status:{" "}
            <span className="font-mono text-zinc-200">
              {worker?.monitoring_enabled ? (worker.monitoring_state ?? "—").toUpperCase() : "IDLE"}
            </span>
          </p>
          <p className="mt-1 text-xs text-zinc-500">
            Last checked:{" "}
            {worker?.last_checked_at ? new Date(worker.last_checked_at).toLocaleString() : "—"}
          </p>
          {workerNotRunning ? (
            <div className="mt-2 rounded-md border border-red-900/60 bg-red-950/30 px-2.5 py-2 text-xs">
              <p className="font-medium text-red-200">Worker not running</p>
              <p className="mt-0.5 text-red-100/75">
                Monitoring is {ms.toUpperCase()} but no worker heartbeat received.
                Start the worker: <span className="font-mono select-all">python worker/main.py</span>
              </p>
            </div>
          ) : null}
          {!workerNotRunning && worker?.pipeline_message && !worker?.configuration_error ? (
            <p className="mt-2 text-xs leading-snug text-zinc-400">
              <span className={batchActive ? "text-emerald-600" : "text-zinc-600"}>
                {batchActive ? "Running: " : "Last: "}
              </span>
              {worker.pipeline_message}
            </p>
          ) : null}
          {batchActive && cur ? (
            <div className="mt-2">
              <p className="text-[10px] uppercase tracking-wide text-emerald-700">
                Active batch progress
              </p>
              <p className="mt-1 font-mono text-[11px] text-zinc-400">
                Collected {cur.raw_collected} · kept {cur.step1_kept} · matched {cur.step2_matched} · saved {cur.step4_saved} · alerts {cur.alerts_sent}
              </p>
            </div>
          ) : worker?.monitoring_enabled && !batchActive ? (
            <p className="mt-2 text-[11px] text-zinc-600 italic">No batch currently running</p>
          ) : null}
          {pc ? (
            <div className="mt-2">
              <p className="text-[10px] uppercase tracking-wide text-zinc-600">
                Last completed batch
              </p>
              <p className="mt-1 font-mono text-[11px] text-zinc-500">
                Collected {pc.raw_collected} · kept {pc.step1_kept} · matched {pc.step2_matched} · saved {pc.step4_saved} · alerts {pc.alerts_sent}
              </p>
            </div>
          ) : null}
          {worker?.last_successful_run_at ? (
            <p className="mt-1 text-[11px] text-zinc-600">
              Last successful batch: {new Date(worker.last_successful_run_at).toLocaleString()}
            </p>
          ) : null}
          {worker?.configuration_error ? (
            <p className="mt-2 text-xs text-amber-100/95">
              <span className="font-medium text-amber-200/95">Search settings — </span>
              {worker.configuration_error}
            </p>
          ) : null}
          {worker?.last_error ? (
            <p className="mt-2 text-xs text-red-300">
              <span className="font-medium text-red-200/95">Last collector error — </span>
              {worker.last_error}
            </p>
          ) : null}
          {worker?.pipeline_error ? (
            <p className="mt-2 text-xs text-amber-100/95">
              <span className="font-medium text-amber-200/95">In-pipeline notice — </span>
              {worker.pipeline_error}
            </p>
          ) : null}
          {worker?.collector_warning ? (
            <p className="mt-2 rounded-md border border-amber-800/60 bg-amber-950/30 px-2 py-1.5 text-xs text-amber-100/95">
              <span className="font-medium text-amber-300">Collector warning (last batch) — </span>
              {worker.collector_warning}
            </p>
          ) : null}
        </div>

        <div className="rounded-xl border border-zinc-800 bg-zinc-950/40 p-4">
          <h2 className="text-sm font-medium text-zinc-200">Listings</h2>
          <p className="mt-2 text-3xl font-semibold text-zinc-100">{storedCount}</p>
          <p className="mt-1 text-xs text-zinc-500">Total stored in the database for your account</p>
          <Link href="/listings" className="mt-3 inline-block text-sm text-emerald-400 hover:underline">
            Open listings →
          </Link>
        </div>

        <div className="rounded-xl border border-zinc-800 bg-zinc-950/40 p-4">
          <h2 className="text-sm font-medium text-zinc-200">Alerts</h2>
          <p className="mt-2 text-3xl font-semibold text-zinc-100">{worker?.alerts_sent_count ?? "—"}</p>
          <p className="mt-1 text-xs text-zinc-500">Telegram alerts sent (depends on alert mode in Settings)</p>
        </div>
      </div>

      {isAdmin && worker && !worker.status_fetch_error ? (
        <div className="mt-6 rounded-xl border border-violet-900/50 bg-violet-950/20 p-4">
          <h2 className="text-sm font-medium text-violet-200/95">Admin · worker pipeline</h2>
          <p className="mt-1 text-xs text-zinc-500">
            Live DB snapshot. Last completed vs current batch counts are separate; listing total is all rows stored for
            the account.
          </p>
          {worker.admin_pipeline_snapshot ? (
            <>
              <dl className="mt-3 grid gap-2 text-[11px] text-zinc-400 sm:grid-cols-2">
                <div>
                  <dt className="text-zinc-600">Stage</dt>
                  <dd className="font-mono text-zinc-300">
                    {formatStageLabel(
                      worker.admin_pipeline_snapshot.worker_current_step as number | undefined,
                      worker.admin_pipeline_snapshot.worker_current_state as string | undefined,
                    )}
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
                  <dt className="text-zinc-600">Current message</dt>
                  <dd className="text-xs text-zinc-300">
                    {(worker.admin_pipeline_snapshot.worker_pipeline_message as string) || "—"}
                  </dd>
                </div>
                <div>
                  <dt className="text-zinc-600">Last fatal error</dt>
                  <dd className="text-xs text-red-300/90">
                    {(worker.admin_pipeline_snapshot.last_error_active as string | null) || "—"}
                  </dd>
                </div>
                <div>
                  <dt className="text-zinc-600">Collector / pipeline warning</dt>
                  <dd className="text-xs text-amber-200/90">
                    {(worker.admin_pipeline_snapshot.worker_collector_warning as string | null) || "—"}
                  </dd>
                </div>
              </dl>
              {(() => {
                const snap = worker.admin_pipeline_snapshot;
                const last = snap.counts as Record<string, number> | null | undefined;
                const curr = snap.current_counts as Record<string, number> | null | undefined;
                if (!last && !curr) return null;
                const rows: Array<{ label: string; key: string }> = [
                  { label: "Raw collected", key: "raw_collected" },
                  { label: "Step 1 kept", key: "step1_kept" },
                  { label: "Step 2 matched", key: "step2_matched" },
                  { label: "Step 3 scored", key: "step3_scored" },
                  { label: "Step 4 saved", key: "step4_saved" },
                  { label: "Alerts sent", key: "alerts_sent" },
                ];
                return (
                  <div className="mt-3 overflow-hidden rounded-lg border border-zinc-800">
                    <table className="w-full text-[10px] text-zinc-400">
                      <thead>
                        <tr className="border-b border-zinc-800 bg-zinc-900/60">
                          <th className="py-1.5 pl-3 pr-2 text-left font-normal text-zinc-600">Batch step</th>
                          <th className="py-1.5 pr-3 text-right font-normal text-zinc-500">
                            Last completed
                          </th>
                          <th className="py-1.5 pr-3 text-right font-normal text-zinc-500">
                            In progress
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {rows.map(({ label, key }) => (
                          <tr key={key} className="border-b border-zinc-800/50 last:border-0">
                            <td className="py-1 pl-3 pr-2 text-zinc-500">{label}</td>
                            <td className="py-1 pr-3 text-right font-mono text-zinc-300">
                              {last?.[key] ?? "—"}
                            </td>
                            <td className="py-1 pr-3 text-right font-mono text-zinc-400">
                              {curr?.[key] ?? "—"}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                );
              })()}
              <details className="mt-3">
                <summary className="cursor-pointer text-[11px] text-violet-300/90">Raw JSON</summary>
                <pre className="mt-2 max-h-48 overflow-auto rounded-lg border border-zinc-800 bg-zinc-950/80 p-3 text-[10px] leading-relaxed text-zinc-500">
                  {JSON.stringify(worker.admin_pipeline_snapshot, null, 2)}
                </pre>
              </details>
            </>
          ) : (
            <p className="mt-2 text-xs text-zinc-500">No admin snapshot (unexpected).</p>
          )}
        </div>
      ) : null}

      <p className="mt-8 text-sm text-zinc-500">
        Configure radius in <strong className="text-zinc-300">miles</strong>, prices in{" "}
        <strong className="text-zinc-300">USD</strong>, and Telegram in{" "}
        <Link href="/settings" className="text-emerald-400 hover:underline">
          Settings
        </Link>
        .
      </p>

      <div className="mt-10">
        <div className="flex items-center justify-between gap-2">
          <h2 className="text-lg font-medium text-zinc-200">Recent matches</h2>
          <Link href="/listings" className="text-sm text-emerald-400 hover:underline">
            All listings →
          </Link>
        </div>
        <p className="mt-2 text-xs text-zinc-500">Click a row for full description and details.</p>
        <div className="mt-4 overflow-x-auto rounded-xl border border-zinc-800">
          <table className="min-w-full text-left text-sm">
            <thead className="border-b border-zinc-800 bg-zinc-900/80 text-xs uppercase text-zinc-500">
              <tr>
                <th className="px-3 py-2">Title</th>
                <th className="px-3 py-2">Price</th>
                <th className="px-3 py-2">Est. profit (heuristic)</th>
                <th className="px-3 py-2">Mode</th>
                <th className="px-3 py-2">Alert</th>
                <th className="px-3 py-2">Found</th>
              </tr>
            </thead>
            <tbody>
              {listings.slice(0, 8).map((r) => (
                <tr
                  key={r.id}
                  className="cursor-pointer border-b border-zinc-800/80 hover:bg-zinc-900/40"
                  onClick={() => setDetail(r)}
                >
                  <td className="max-w-xs truncate px-3 py-2">{r.title}</td>
                  <td className="whitespace-nowrap px-3 py-2">${r.price.toFixed(2)}</td>
                  <td className={`px-3 py-2 ${r.profitable ? "text-emerald-400" : ""}`}>
                    ${r.estimated_profit.toFixed(2)}
                  </td>
                  <td className="px-3 py-2 text-zinc-400">
                    {r.origin_type === "backfill" ? "Backfill" : "Live"}
                  </td>
                  <td className="max-w-[12rem] px-3 py-2 text-xs text-zinc-400">
                    {r.alert_sent && r.alert_sent_at
                      ? `Sent ${new Date(r.alert_sent_at).toLocaleString()}`
                      : `${r.alert_status}${r.alert_last_error ? ` — ${r.alert_last_error}` : ""}`}
                  </td>
                  <td className="whitespace-nowrap px-3 py-2 text-zinc-500">
                    {new Date(r.found_at).toLocaleString()}
                  </td>
                </tr>
              ))}
              {listings.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-3 py-8 text-center text-zinc-500">
                    No listings yet — enable monitoring in Settings and keep the worker running. Matches are saved automatically after filtering.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
