"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export type GeoapifySuggestion = {
  location_text: string;
  center_lat: number;
  center_lon: number;
  geoapify_place_id: string | null;
};

type AutocompleteHit = {
  formatted?: string;
  address_line1?: string;
  lat?: number;
  lon?: number;
  place_id?: string;
};

type Props = {
  location_text: string;
  center_lat: number | null;
  center_lon: number | null;
  geoapify_place_id: string | null;
  onChange: (next: GeoapifySuggestion) => void;
  inputClassName?: string;
  disabled?: boolean;
};

const DEBOUNCE_MS = 350;

export function GeoapifyLocationInput({
  location_text,
  center_lat,
  center_lon,
  geoapify_place_id,
  onChange,
  inputClassName,
  disabled = false,
}: Props) {
  const apiKey = process.env.NEXT_PUBLIC_GEOAPIFY_API_KEY?.trim() ?? "";
  const [query, setQuery] = useState(location_text);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [items, setItems] = useState<AutocompleteHit[]>([]);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const wrapRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setQuery(location_text);
  }, [location_text, center_lat, center_lon, geoapify_place_id]);

  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (wrapRef.current && !wrapRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, []);

  const runAutocomplete = useCallback(
    (text: string) => {
      if (!apiKey || text.trim().length < 2) {
        setItems([]);
        return;
      }
      setLoading(true);
      const u = new URL("https://api.geoapify.com/v1/geocode/autocomplete");
      u.searchParams.set("text", text.trim());
      u.searchParams.set("limit", "8");
      u.searchParams.set("format", "json");
      u.searchParams.set("apiKey", apiKey);
      void fetch(u.toString())
        .then((r) => r.json())
        .then((data: { results?: AutocompleteHit[] }) => {
          setItems(data.results ?? []);
        })
        .catch(() => setItems([]))
        .finally(() => setLoading(false));
    },
    [apiKey],
  );

  function onInputChange(v: string) {
    if (disabled) return;
    setQuery(v);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      runAutocomplete(v);
      setOpen(true);
    }, DEBOUNCE_MS);
  }

  function pick(hit: AutocompleteHit) {
    if (disabled) return;
    if (hit.lat == null || hit.lon == null) return;
    const label = (hit.formatted || hit.address_line1 || "").trim() || query;
    const pid = hit.place_id ?? null;
    onChange({
      location_text: label,
      center_lat: hit.lat,
      center_lon: hit.lon,
      geoapify_place_id: pid,
    });
    setQuery(label);
    setOpen(false);
    setItems([]);
  }

  if (!apiKey) {
    return (
      <p className="text-xs text-amber-500/90">
        Set <code className="text-zinc-400">NEXT_PUBLIC_GEOAPIFY_API_KEY</code> in{" "}
        <code className="text-zinc-400">frontend/.env.local</code> to enable location search.
      </p>
    );
  }

  return (
    <div ref={wrapRef} className="relative">
      <input
        className={inputClassName}
        value={query}
        disabled={disabled}
        aria-disabled={disabled}
        onChange={(e) => onInputChange(e.target.value)}
        onFocus={() => {
          if (disabled) return;
          if (query.trim().length >= 2) {
            runAutocomplete(query);
            setOpen(true);
          }
        }}
        autoComplete="off"
        placeholder="Search address or place…"
      />
      {loading && (
        <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-[10px] text-zinc-500">
          …
        </span>
      )}
      {open && items.length > 0 && (
        <ul className="absolute z-20 mt-1 max-h-56 w-full overflow-auto rounded-lg border border-zinc-700 bg-zinc-950 py-1 text-sm shadow-lg">
          {items.map((hit, i) => {
            const label = (hit.formatted || hit.address_line1 || "").trim() || `Result ${i + 1}`;
            return (
              <li key={`${hit.place_id ?? label}-${i}`}>
                <button
                  type="button"
                  className="w-full px-3 py-2 text-left hover:bg-zinc-800"
                  onMouseDown={(e) => e.preventDefault()}
                  onClick={() => pick(hit)}
                >
                  {label}
                </button>
              </li>
            );
          })}
        </ul>
      )}
      {center_lat != null && center_lon != null ? (
        <p className="mt-1 text-[11px] text-zinc-500">
          Selected · {center_lat.toFixed(4)}, {center_lon.toFixed(4)}
          {geoapify_place_id ? ` · id ${geoapify_place_id.slice(0, 12)}…` : null}
        </p>
      ) : null}
    </div>
  );
}
