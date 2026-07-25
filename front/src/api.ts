export interface HealthResponse {
  ok: boolean;
  local_upstreams: string[];
  remote_upstreams: string[];
  tools: number;
  configs: string[];
}

// The dev server proxies /api -> the back/ gateway's own routes (see
// vite.config.ts); in production the front/ static build is typically
// served behind the same reverse proxy that fronts back/, so this stays a
// same-origin relative path either way.
const BASE = "/api";

export async function getHealth(): Promise<HealthResponse> {
  const res = await fetch(`${BASE}/healthz`);
  if (!res.ok) throw new Error(`healthz failed: ${res.status}`);
  return res.json();
}
