"use client";

import { useCallback, useEffect, useState } from "react";
import { useAuth } from "@/context/auth-context";
import { fetchCategories, fetchListings, type Category, type ListingRow } from "@/lib/api";

export default function ListingsPage() {
  const { token } = useAuth();
  const [rows, setRows] = useState<ListingRow[]>([]);
  const [categories, setCategories] = useState<Category[]>([]);
  const [profitableOnly, setProfitableOnly] = useState(false);
  const [category, setCategory] = useState<string>("");
  const [err, setErr] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!token) return;
    setErr(null);
    try {
      const [list, cats] = await Promise.all([
        fetchListings(token, {
          profitable_only: profitableOnly || undefined,
          category: category || undefined,
        }),
        fetchCategories(),
      ]);
      setRows(list);
      setCategories(cats.categories);
    } catch {
      setErr("Could not load listings.");
    }
  }, [token, profitableOnly, category]);

  useEffect(() => {
    void load();
  }, [load]);

  return (
    <div>
      <h1 className="text-2xl font-semibold">Listings</h1>
      <div className="mt-4 flex flex-wrap items-end gap-4">
        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={profitableOnly}
            onChange={(e) => setProfitableOnly(e.target.checked)}
            className="rounded border-zinc-600"
          />
          Profitable only
        </label>
        <div>
          <label className="block text-xs text-zinc-500">Category</label>
          <select
            className="mt-1 rounded-lg border border-zinc-700 bg-zinc-900 px-2 py-1.5 text-sm"
            value={category}
            onChange={(e) => setCategory(e.target.value)}
          >
            <option value="">All</option>
            {categories.map((c) => (
              <option key={c.id} value={c.id}>
                {c.label}
              </option>
            ))}
          </select>
        </div>
        <button
          type="button"
          onClick={() => void load()}
          className="rounded-lg bg-zinc-800 px-3 py-1.5 text-sm hover:bg-zinc-700"
        >
          Refresh
        </button>
      </div>
      {err && <p className="mt-4 text-sm text-red-400">{err}</p>}
      <div className="mt-6 overflow-x-auto rounded-xl border border-zinc-800">
        <table className="min-w-full text-left text-sm">
          <thead className="border-b border-zinc-800 bg-zinc-900/80 text-xs uppercase text-zinc-500">
            <tr>
              <th className="px-3 py-2">Title</th>
              <th className="px-3 py-2">Price (USD)</th>
              <th className="px-3 py-2">Est. resale</th>
              <th className="px-3 py-2">Est. profit</th>
              <th className="px-3 py-2">Category</th>
              <th className="px-3 py-2">Location</th>
              <th className="px-3 py-2">Mode</th>
              <th className="px-3 py-2">Confidence</th>
              <th className="px-3 py-2">AI reasoning</th>
              <th className="px-3 py-2">Found</th>
              <th className="px-3 py-2">Alert</th>
              <th className="px-3 py-2">Link</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id} className="border-b border-zinc-800/80">
                <td className="max-w-xs truncate px-3 py-2">{r.title}</td>
                <td className="px-3 py-2">${r.price.toFixed(2)}</td>
                <td className="px-3 py-2">${r.estimated_resale.toFixed(2)}</td>
                <td className={`px-3 py-2 ${r.profitable ? "text-emerald-400" : ""}`}>
                  ${r.estimated_profit.toFixed(2)}
                </td>
                <td className="px-3 py-2">{r.category_id || r.category_slug}</td>
                <td className="px-3 py-2">{r.location_text}</td>
                <td className="px-3 py-2 text-zinc-400">
                  {r.origin_type === "backfill" ? "Backfill" : "Live"}
                </td>
                <td className="px-3 py-2 text-zinc-400">
                  {r.confidence != null ? `${(r.confidence * 100).toFixed(0)}%` : "—"}
                </td>
                <td className="max-w-xs truncate px-3 py-2 text-zinc-400" title={r.reasoning ?? ""}>
                  {r.reasoning ?? "—"}
                </td>
                <td className="whitespace-nowrap px-3 py-2 text-zinc-400">
                  {new Date(r.found_at).toLocaleString()}
                </td>
                <td className="px-3 py-2">
                  {r.alert_sent ? "sent" : r.alert_status}
                  {r.should_alert != null ? ` (${r.should_alert ? "AI yes" : "AI no"})` : ""}
                </td>
                <td className="px-3 py-2">
                  <a
                    href={r.source_link}
                    target="_blank"
                    rel="noreferrer"
                    className="text-emerald-400 hover:underline"
                  >
                    Open
                  </a>
                </td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={12} className="px-3 py-12 text-center text-zinc-500">
                  <p className="text-sm font-medium text-zinc-400">No listings yet</p>
                  <p className="mt-2 max-w-md text-xs text-zinc-500">
                    Save valid settings in Settings, verify Telegram, click Run monitoring, and keep the worker process
                    running. Matching deals appear here with USD pricing and backfill vs live labels.
                  </p>
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
