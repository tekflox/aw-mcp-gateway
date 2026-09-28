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

// External upstreams — hand-registered via Settings, distinct from the
// scanned (app-contributed) upstreams reported in HealthResponse. A spec's
// env/header values may hold a "${secret:<name>}" reference instead of a
// literal — see back/gateway/config.py's resolve_secret_refs().
export interface ExternalUpstreamSpec {
  type: "stdio" | "http" | "gateway";
  command?: string;
  args?: string[];
  env?: Record<string, string>;
  url?: string;
  headers?: Record<string, string>;
  enabled?: boolean;
  // "gateway" only — the peer's bearer token, and the subset of ITS tools
  // to aggregate into this gateway. Absent allowed_tools means "everything
  // the peer publishes" (the old behaviour); an explicit list is how a
  // federation to another workspace's gateway stays scoped to what was
  // actually picked in the UI instead of pulling that peer's whole pool.
  token?: string;
  allowed_tools?: string[];
}

export interface GatewayProbeTool {
  name: string;
  description?: string;
}

export interface GatewayProbeResult {
  gateway_id: string;
  federation_chain: string[];
  tools: GatewayProbeTool[];
}

export interface ExternalUpstreamStatus {
  name: string;
  spec: ExternalUpstreamSpec;
  enabled: boolean;
  status: "running" | "stopped" | "error";
  error: string | null;
  tools: number;
}

export interface PutExternalUpstreamResult extends ExternalUpstreamStatus {
  reload: unknown;
  warning?: string;
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

// Admin/config-surface endpoints (external upstreams below) are guarded by
// _check_admin_auth on the back end, which accepts the trusted
// X-AW-Identity-Sub header the workspace reverse proxy injects for a logged
// in browser session — so, unlike the link-token endpoints above, these
// don't need a bearer token passed in from the caller.
async function _errorMessage(res: Response, fallback: string): Promise<string> {
  try {
    const body = await res.json();
    return body?.error || fallback;
  } catch {
    return fallback;
  }
}

export async function listExternalUpstreams(): Promise<ExternalUpstreamStatus[]> {
  const res = await fetch(`${BASE}/admin/external-upstreams`);
  if (!res.ok) throw new Error(await _errorMessage(res, `list external upstreams failed: ${res.status}`));
  return (await res.json()).upstreams;
}

export async function putExternalUpstream(
  name: string,
  spec: ExternalUpstreamSpec,
  secrets?: Record<string, string>,
): Promise<PutExternalUpstreamResult> {
  const res = await fetch(`${BASE}/admin/external-upstreams/${encodeURIComponent(name)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ spec, secrets }),
  });
  if (!res.ok) throw new Error(await _errorMessage(res, `save external upstream failed: ${res.status}`));
  return res.json();
}

// Preview what a candidate `type: gateway` peer would publish — nothing is
// saved by this call. Powers the tool picker: enter URL+token, fetch the
// real tool names, THEN choose which ones to allow before saving.
export async function probeGatewayUpstream(url: string, token?: string): Promise<GatewayProbeResult> {
  const res = await fetch(`${BASE}/admin/external-upstreams/probe-gateway`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url, token }),
  });
  if (!res.ok) throw new Error(await _errorMessage(res, `probe gateway failed: ${res.status}`));
  return res.json();
}

export async function deleteExternalUpstream(name: string): Promise<void> {
  const res = await fetch(`${BASE}/admin/external-upstreams/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
  if (!res.ok) throw new Error(await _errorMessage(res, `delete external upstream failed: ${res.status}`));
}
