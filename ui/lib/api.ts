// Thin client for the rSearch API. Token lives in localStorage; every
// call goes through rq() so auth and errors are handled once.

declare global {
  interface Window {
    /** Set by /env.js at runtime to repoint a built UI at another API. */
    __RSEARCH_API__?: string;
  }
}

// Resolved per call, not baked in at build: /env.js can set
// window.__RSEARCH_API__ ("" = same origin) so one built console image
// serves any cluster; the NEXT_PUBLIC_RSEARCH_API build arg and the
// localhost dev default remain as fallbacks (#6).
export function apiBase(): string {
  if (typeof window !== "undefined" && window.__RSEARCH_API__ !== undefined) {
    return window.__RSEARCH_API__;
  }
  return process.env.NEXT_PUBLIC_RSEARCH_API ?? "http://localhost:9200";
}

export function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem("rsearch_token");
}

export function setToken(token: string | null) {
  if (token) window.localStorage.setItem("rsearch_token", token);
  else window.localStorage.removeItem("rsearch_token");
}

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export async function rq<T = unknown>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const headers = new Headers(init.headers);
  if (!headers.has("content-type") && init.body)
    headers.set("content-type", "application/json");
  const token = getToken();
  if (token) headers.set("authorization", `Bearer ${token}`);
  const response = await fetch(`${apiBase()}${path}`, { ...init, headers });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const reason =
      (body as { error?: { reason?: string } })?.error?.reason ??
      `request failed (${response.status})`;
    throw new ApiError(response.status, reason);
  }
  return body as T;
}

export interface StreamInfo {
  index: string;
  "docs.count": string;
  splits: number;
  retention_hours: number | null;
}

export interface Hit {
  _id: string;
  _source: Record<string, unknown>;
  sort: number[];
}

export async function search(
  index: string,
  query: string,
  fromMillis: number | null,
  size = 100,
  signal?: AbortSignal,
): Promise<{ total: number; hits: Hit[] }> {
  const filters: unknown[] = [];
  if (fromMillis)
    filters.push({ range: { "@timestamp": { gte: fromMillis, lte: "now" } } });
  if (query.trim())
    filters.push({ query_string: { query: query.trim() } });
  const body = {
    size,
    query: filters.length ? { bool: { filter: filters } } : { match_all: {} },
  };
  const result = await rq<{
    hits: { total: { value: number }; hits: Hit[] };
  }>(`/${encodeURIComponent(index)}/_search`, {
    method: "POST",
    body: JSON.stringify(body),
    signal,
  });
  return { total: result.hits.total.value, hits: result.hits.hits };
}
