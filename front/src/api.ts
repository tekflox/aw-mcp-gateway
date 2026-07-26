export interface HealthResponse {
  ok: boolean;
  local_upstreams: string[];
  remote_upstreams: string[];
  tools: number;
  configs: string[];
  gateway_id: string;
  federation_chain: string[];
  federated_gateways: string[];
}

export interface LinkTokenSummary {
  id: string;
  label: string;
  scopes: string[];
  created_at: number;
  revoked: boolean;
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

// Token-authenticated endpoints (minting a link token needs the gateway's
// own bearer token — same one used for /mcp — so this is meant to be used
// from an already-authenticated admin session, not exposed publicly).
export async function listLinkTokens(bearerToken: string): Promise<LinkTokenSummary[]> {
  const res = await fetch(`${BASE}/link-tokens`, {
    headers: { Authorization: `Bearer ${bearerToken}` },
  });
  if (!res.ok) throw new Error(`list link tokens failed: ${res.status}`);
  return (await res.json()).tokens;
}

export async function mintLinkToken(
  bearerToken: string,
  label: string,
  scopes?: string[],
): Promise<{ token: string } & LinkTokenSummary> {
  const res = await fetch(`${BASE}/link-tokens`, {
    method: "POST",
    headers: { Authorization: `Bearer ${bearerToken}`, "Content-Type": "application/json" },
    body: JSON.stringify({ label, scopes }),
  });
  if (!res.ok) throw new Error(`mint link token failed: ${res.status}`);
  return res.json();
}

export async function revokeLinkToken(bearerToken: string, tokenId: string): Promise<void> {
  const res = await fetch(`${BASE}/link-tokens/${tokenId}/revoke`, {
    method: "POST",
    headers: { Authorization: `Bearer ${bearerToken}` },
  });
  if (!res.ok) throw new Error(`revoke link token failed: ${res.status}`);
}
