"use client";

import { useEffect } from "react";
import type { ListingRow } from "@/lib/api";

type Props = {
  listing: ListingRow | null;
  onClose: () => void;
};

function fmtConfidence(c: ListingRow["confidence"]): string {
  if (c == null) return "—";
  if (typeof c === "number") {
    if (c >= 0 && c <= 1) return `${(c * 100).toFixed(0)}%`;
    return String(c);
  }
  return String(c);
}

export function ListingDetailModal({ listing, onClose }: Props) {
  useEffect(() => {
    if (!listing) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [listing, onClose]);

  if (!listing) return null;

  const sm = listing.scrape_metadata as
    | {
        brand?: string | null;
        condition?: string | null;
        listing_location_detail?: string | null;
        image_urls?: string[];
        detail_enriched?: boolean;
      }
    | null
    | undefined;

  const detailImages = Array.isArray(sm?.image_urls) ? sm.image_urls : [];

  const url = (listing.source_url && listing.source_url.trim()) || listing.source_link;
  const alertLine =
    listing.alert_sent && listing.alert_sent_at
      ? `Sent ${new Date(listing.alert_sent_at).toLocaleString()}`
      : `${listing.alert_status}${listing.alert_last_error ? ` — ${listing.alert_last_error}` : ""}`;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="listing-detail-title"
      onClick={onClose}
    >
      <div
        className="max-h-[90vh] w-full max-w-2xl overflow-y-auto rounded-xl border border-zinc-800 bg-zinc-950 p-6 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-3">
          <h2 id="listing-detail-title" className="text-lg font-semibold text-zinc-100">
            {listing.title}
          </h2>
          <button
            type="button"
            className="shrink-0 rounded-lg border border-zinc-700 px-2 py-1 text-xs text-zinc-400 hover:bg-zinc-900"
            onClick={onClose}
          >
            Close
          </button>
        </div>

        <dl className="mt-4 space-y-3 text-sm">
          <div>
            <dt className="text-xs uppercase text-zinc-500">Description</dt>
            <dd className="mt-1 whitespace-pre-wrap text-zinc-300">{listing.description?.trim() || "—"}</dd>
          </div>
          <div className="grid gap-3 sm:grid-cols-2">
          <div>
            <dt className="text-xs uppercase text-zinc-500">Location</dt>
            <dd className="mt-1 text-zinc-300">{listing.location_text || "—"}</dd>
          </div>
          {sm?.listing_location_detail ? (
            <div>
              <dt className="text-xs uppercase text-zinc-500">Listing location (detail)</dt>
              <dd className="mt-1 text-zinc-300">{sm.listing_location_detail}</dd>
            </div>
          ) : null}
          {sm?.brand ? (
            <div>
              <dt className="text-xs uppercase text-zinc-500">Brand</dt>
              <dd className="mt-1 text-zinc-300">{sm.brand}</dd>
            </div>
          ) : null}
          {sm?.condition ? (
            <div>
              <dt className="text-xs uppercase text-zinc-500">Condition</dt>
              <dd className="mt-1 text-zinc-300">{sm.condition}</dd>
            </div>
          ) : null}
            <div>
              <dt className="text-xs uppercase text-zinc-500">Source URL</dt>
              <dd className="mt-1 break-all">
                <a href={url} target="_blank" rel="noreferrer" className="text-emerald-400 hover:underline">
                  {url}
                </a>
              </dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-zinc-500">Price (USD)</dt>
              <dd className="mt-1 font-mono text-zinc-200">${listing.price.toFixed(2)}</dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-zinc-500">Est. retail</dt>
              <dd className="mt-1 font-mono text-zinc-200">${listing.estimated_resale.toFixed(2)}</dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-zinc-500">Est. profit</dt>
              <dd className={`mt-1 font-mono ${listing.profitable ? "text-emerald-400" : "text-zinc-200"}`}>
                ${listing.estimated_profit.toFixed(2)}
              </dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-zinc-500">Confidence</dt>
              <dd className="mt-1 text-zinc-300">{fmtConfidence(listing.confidence)}</dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-zinc-500">Alert status</dt>
              <dd className="mt-1 text-zinc-300">{alertLine}</dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-zinc-500">AI should_alert</dt>
              <dd className="mt-1 text-zinc-300">
                {listing.should_alert == null ? "—" : listing.should_alert ? "yes" : "no"}
              </dd>
            </div>
          </div>
          <div>
            <dt className="text-xs uppercase text-zinc-500">AI reasoning</dt>
            <dd className="mt-1 whitespace-pre-wrap text-zinc-300">{listing.reasoning?.trim() || "—"}</dd>
          </div>
          {(listing.matched_keywords?.length ?? 0) > 0 ? (
            <div>
              <dt className="text-xs uppercase text-zinc-500">Matched keywords</dt>
              <dd className="mt-1 text-zinc-400">{(listing.matched_keywords ?? []).join(", ")}</dd>
            </div>
          ) : null}
          <div>
            <dt className="text-xs uppercase text-zinc-500">Category / mode</dt>
            <dd className="mt-1 text-zinc-400">
              {listing.category_id || listing.category_slug} · {listing.origin_type === "backfill" ? "Backfill" : "Live"}
            </dd>
          </div>
          <div>
            <dt className="text-xs uppercase text-zinc-500">Found</dt>
            <dd className="mt-1 text-zinc-400">{new Date(listing.found_at).toLocaleString()}</dd>
          </div>
          {listing.scraped_at ? (
            <div>
              <dt className="text-xs uppercase text-zinc-500">Scraped</dt>
              <dd className="mt-1 text-zinc-400">{new Date(listing.scraped_at).toLocaleString()}</dd>
            </div>
          ) : null}
          {sm?.detail_enriched ? (
            <div>
              <dt className="text-xs uppercase text-zinc-500">Detail page scraped</dt>
              <dd className="mt-1 text-zinc-400">yes</dd>
            </div>
          ) : null}
          {detailImages.length > 0 ? (
            <div>
              <dt className="text-xs uppercase text-zinc-500">Images</dt>
              <dd className="mt-2 flex flex-wrap gap-2">
                {detailImages.slice(0, 6).map((u) => (
                  <a
                    key={u}
                    href={u}
                    target="_blank"
                    rel="noreferrer"
                    className="block overflow-hidden rounded border border-zinc-800"
                  >
                    {/* eslint-disable-next-line @next/next/no-img-element */}
                    <img src={u} alt="" className="h-24 w-24 object-cover" loading="lazy" />
                  </a>
                ))}
              </dd>
            </div>
          ) : null}
          {listing.ai_result && Object.keys(listing.ai_result).length > 0 ? (
            <div>
              <dt className="text-xs uppercase text-zinc-500">AI result (raw)</dt>
              <dd className="mt-2">
                <pre className="max-h-48 overflow-auto rounded-lg border border-zinc-800 bg-zinc-900/80 p-3 text-[11px] leading-relaxed text-zinc-500">
                  {JSON.stringify(listing.ai_result, null, 2)}
                </pre>
              </dd>
            </div>
          ) : null}
        </dl>
      </div>
    </div>
  );
}
