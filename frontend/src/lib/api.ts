/** API origin (no trailing slash). Set NEXT_PUBLIC_API_BASE_URL in .env.local for LAN dev. */
function resolveApiBaseUrl(): string {
  const fromBase = process.env.NEXT_PUBLIC_API_BASE_URL?.replace(/\/$/, "");
  if (fromBase) return `${fromBase}/api`;

  // Legacy: full URL including /api (e.g. http://host:8000/api)
  const legacy = process.env.NEXT_PUBLIC_API_URL?.replace(/\/$/, "");
  if (legacy) {
    return legacy.endsWith("/api") ? legacy : `${legacy}/api`;
  }

  if (process.env.NODE_ENV === "development") {
    console.warn(
      "[api] Set NEXT_PUBLIC_API_BASE_URL in frontend/.env.local (e.g. http://192.168.1.181:8000)",
    );
  }
  return "";
}

const API_BASE = resolveApiBaseUrl();

export type User = {
  id: number;
  username: string;
  role: string;
};

export type LoginResponse = {
  access_token: string;
  token_type: string;
  user: User;
};

export type UserSettings = {
  location_text: string;
  center_lat: number | null;
  center_lon: number | null;
  geoapify_place_id: string | null;
  boundary_context: Record<string, unknown> | null;
  radius_km: number;
  category_id: string;
  max_price: number;
  telegram_chat_id: string | null;
  telegram_connected: boolean;
  monitoring_enabled: boolean;
};

export type ListingRow = {
  id: number;
  title: string;
  price: number;
  estimated_resale: number;
  estimated_profit: number;
  category_slug: string;
  location: string;
  found_at: string;
  alert_status: string;
  source_link: string;
  source: string;
  profitable: boolean;
};

export type Category = { id: string; label: string; keywords: string[] };

function headers(token: string | null, init?: HeadersInit): HeadersInit {
  const h: Record<string, string> = { "Content-Type": "application/json" };
  if (token) h.Authorization = `Bearer ${token}`;
  return { ...h, ...init };
}

export async function login(username: string, password: string): Promise<LoginResponse> {
  const res = await fetch(`${API_BASE}/auth/login`, {
    method: "POST",
    headers: headers(null),
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) throw new Error("Login failed");
  return res.json() as Promise<LoginResponse>;
}

export async function fetchMe(token: string): Promise<User> {
  const res = await fetch(`${API_BASE}/auth/me`, { headers: headers(token) });
  if (!res.ok) throw new Error("Unauthorized");
  return res.json() as Promise<User>;
}

export async function fetchSettings(token: string): Promise<UserSettings> {
  const res = await fetch(`${API_BASE}/settings/me`, { headers: headers(token) });
  if (!res.ok) throw new Error("Failed to load settings");
  return res.json() as Promise<UserSettings>;
}

export async function updateSettings(
  token: string,
  body: Partial<UserSettings>,
): Promise<UserSettings> {
  const res = await fetch(`${API_BASE}/settings/me`, {
    method: "PUT",
    headers: headers(token),
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error("Failed to save settings");
  return res.json() as Promise<UserSettings>;
}

export async function sendTelegramTest(token: string): Promise<{ ok: boolean; message: string }> {
  const res = await fetch(`${API_BASE}/settings/telegram/test`, {
    method: "POST",
    headers: headers(token),
  });
  if (!res.ok) {
    const err = (await res.json().catch(() => null)) as {
      detail?: string | { msg: string }[];
    } | null;
    let detail = res.statusText;
    if (typeof err?.detail === "string") detail = err.detail;
    else if (Array.isArray(err?.detail) && err.detail[0]?.msg) detail = err.detail[0].msg;
    throw new Error(detail);
  }
  return res.json() as Promise<{ ok: boolean; message: string }>;
}

export async function fetchCategories(): Promise<{ categories: Category[] }> {
  const res = await fetch(`${API_BASE}/categories`);
  if (!res.ok) throw new Error("Failed to load categories");
  return res.json() as Promise<{ categories: Category[] }>;
}

export async function fetchListings(
  token: string,
  params: { profitable_only?: boolean; category?: string },
): Promise<ListingRow[]> {
  const sp = new URLSearchParams();
  if (params.profitable_only) sp.set("profitable_only", "true");
  if (params.category) sp.set("category", params.category);
  const q = sp.toString();
  const res = await fetch(`${API_BASE}/listings${q ? `?${q}` : ""}`, {
    headers: headers(token),
  });
  if (!res.ok) throw new Error("Failed to load listings");
  return res.json() as Promise<ListingRow[]>;
}

export async function workerRun(token: string) {
  const res = await fetch(`${API_BASE}/worker/run`, {
    method: "POST",
    headers: headers(token),
  });
  if (!res.ok) throw new Error("Failed to start");
  return res.json();
}

export async function workerStop(token: string) {
  const res = await fetch(`${API_BASE}/worker/stop`, {
    method: "POST",
    headers: headers(token),
  });
  if (!res.ok) throw new Error("Failed to stop");
  return res.json();
}

export async function workerStatus(token: string) {
  const res = await fetch(`${API_BASE}/worker/status`, { headers: headers(token) });
  if (!res.ok) throw new Error("Failed status");
  return res.json() as Promise<{ monitoring_enabled: boolean; message: string }>;
}

export type AdminUser = { id: number; username: string; role: string; created_at: string };

export async function adminListUsers(token: string): Promise<AdminUser[]> {
  const res = await fetch(`${API_BASE}/admin/users`, { headers: headers(token) });
  if (!res.ok) throw new Error("Forbidden");
  return res.json() as Promise<AdminUser[]>;
}

export async function adminCreateUser(
  token: string,
  body: { username: string; password: string; role: string },
): Promise<AdminUser> {
  const res = await fetch(`${API_BASE}/admin/users`, {
    method: "POST",
    headers: headers(token),
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error("Create failed");
  return res.json() as Promise<AdminUser>;
}
