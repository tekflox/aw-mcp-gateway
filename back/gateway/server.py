"""Gateway — owns the upstream pool (local stdio + HTTP + remote/WS) and the
aggregated tool routing table. Ported from the in-repo mcp-gateway's
``Gateway``/``build_app``, stripped of AW-internal couplings (OTel/SigNoz
tracing, warm-container Redis, X-Aw-Context-* header injection, caller-run-id
propagation to agents-platform) — none of those are part of the generic
gateway mechanism itself.
"""

from __future__ import annotations

import argparse
import logging
import os
import time

from fastapi import FastAPI, Header, Request, WebSocket

from . import caller_context
from fastapi.responses import JSONResponse, Response

from . import config, metrics
from .config_gateway import ConfigGateway, policy_upstreams
from .remote_upstream import RemoteUpstream, link_endpoint
from .token_store import FileTokenStore, TokenStore
from .upstream import (
    FederationCycleError,
    FederationDepthExceeded,
    GatewayUpstream,
    HttpUpstream,
    Upstream,
    public_name,
)

log = logging.getLogger("aw-mcp-gateway")

DEFAULT_ALLOW: list[str] = []  # empty = nothing local unless config/mcp.json + gateway.json say so

# How long a parked upstream (routes kept published, but not actually live)
# is retried before its routes are dropped for real. ~10x runtime.py's
# DEFAULT_MCP_RESCAN_INTERVAL_S=60 — long enough to ride out a core
# restart/deploy blip, short enough that a genuinely dead upstream doesn't
# haunt tools/list forever and become the orphan-route problem tracked by
# degraded:app-uninstall-leaves-stale-gateway-mcp-entry.
PARK_TTL_SECONDS = 600


class Gateway:
    def __init__(self, allow: list[str], gateway_id: str | None = None, max_federation_depth: int | None = None,
                 workspace_name: str | None = None):
        self.allow = allow
        self.gateway_id = gateway_id or config.gateway_id()
        self.max_federation_depth = max_federation_depth or config.max_federation_depth()
        # Namespaces every published tool name — see config.workspace_name().
        self.workspace_name = workspace_name if workspace_name is not None else config.workspace_name()
        self.upstreams: dict[str, Upstream | HttpUpstream | GatewayUpstream] = {}
        self.remotes: dict[str, RemoteUpstream] = {}  # public route_name -> RemoteUpstream (live only)
        self.routes: dict[str, tuple[str, str]] = {}  # public name -> (server, tool)
        # public name -> that tool's raw `annotations` dict (idempotentHint
        # etc.), for the proof-gated retry policy in HttpUpstream.call_tool.
        # Kept as its own dict rather than widening the `routes` tuple above
        # — every `_drop_local_routes`/`_withdraw_remote` filter on it
        # indexes by tuple position (`v[0] != server`), and a third element
        # would silently corrupt that filter instead of erroring.
        self.route_annotations: dict[str, dict] = {}
        self.agg_tools: list[dict] = []
        # A local upstream that HAD published routes and then failed to
        # (re)start — never touches self.routes/self.agg_tools shape (see the
        # tuple-index warning on _drop_local_routes below); tracked entirely
        # separately so a route being "parked" is invisible to every other
        # reader of self.routes. name -> {"error": str, "parked_at": float
        # (time.monotonic), "tools": list[dict] (raw, pre-namespacing, for
        # restoring routes on repeated failure)}.
        self.unavailable: dict[str, dict] = {}
        # Reconnect-safe / collision-safe remote naming (see register_remote):
        self._remote_by_token: dict[str, RemoteUpstream] = {}  # token_id -> RemoteUpstream, survives disconnects
        self._remote_name_groups: dict[str, list[str]] = {}  # workspace+base_name -> [token_id, ...] order

    def _load_specs(self) -> dict[str, dict]:
        """Which upstreams actually get started.

        Two trust paths, so installing an app is enough on its own —
        no manual ``gateway.json`` edit required:

        * **scanned** — every server discovered by ``config.scan_app_mcp_servers()``
          under ``AW_APP_SCAN_ROOTS`` (i.e. contributed by an app the
          workspace's own install flow already vetted through
          permissions/dependencies) is auto-trusted and always loaded.
        * **custom** — anything only present via ``config/mcp.custom.json``
          (hand-authored, not reviewed by the app framework) still needs
          an explicit ``self.allow`` entry — the one remaining use of the
          allowlist.
        """
        servers = config.load_mcp_servers()
        scanned, _sources = config.scan_app_mcp_servers()
        out = {}
        for name, spec in servers.items():
            if name not in scanned and name not in self.allow:
                continue
            if spec.get("enabled") is False:
                log.warning("upstream '%s' is disabled in config/mcp.json — skipping", name)
                continue
            kind = spec.get("type", "stdio")
            if kind not in ("stdio", "http", "gateway"):
                log.warning("upstream '%s' has unsupported type=%s — skipping", name, kind)
                continue
            out[name] = spec
        for name in self.allow:
            if name not in servers:
                log.warning("allowlisted upstream '%s' not found in config/mcp.json", name)
        return out

    async def start(self) -> None:
        specs = self._load_specs()
        for name, spec in specs.items():
            error = await self._start_one(name, spec)
            if error:
                continue
            log.info("upstream %s (%s) — %d tools", name, spec.get("type", "stdio"),
                     len(self.upstreams[name].tools))
        log.info("gateway ready: %d local upstreams, %d tools",
                 len(self.upstreams), len(self.agg_tools))

    async def _start_one(self, name: str, spec: dict) -> str | None:
        """Construct, start, and register ONE local upstream. Returns an
        error string on failure (already logged), None on success — shared
        by start() (every upstream, at boot) and reload() (only the
        added/changed ones)."""
        kind = spec.get("type", "stdio")
        up: Upstream | HttpUpstream | GatewayUpstream
        if kind == "gateway":
            up = GatewayUpstream(name, spec, self.gateway_id, self.max_federation_depth)
        elif kind == "http":
            up = HttpUpstream(name, spec)
        else:
            up = Upstream(name, spec)
        try:
            await up.start()
        except (FederationCycleError, FederationDepthExceeded) as exc:
            log.error("federation upstream '%s' rejected: %s", name, exc)
            return str(exc)
        except Exception as exc:
            log.exception("failed to start upstream %s", name)
            return str(exc)
        self.upstreams[name] = up
        for tool in up.tools:
            self._add_route(name, tool)
        return None

    def _drop_local_routes(self, server: str) -> None:
        """Remove every published route/tool that dispatches to `server` —
        the local-upstream counterpart of _withdraw_remote(), used by
        reload() before restarting/removing a local upstream so stale
        routes don't linger pointing at a torn-down or replaced process."""
        self.agg_tools = [t for t in self.agg_tools
                          if self.routes.get(t["name"], ("",))[0] != server]
        self.routes = {k: v for k, v in self.routes.items() if v[0] != server}
        self.route_annotations = {k: v for k, v in self.route_annotations.items() if k in self.routes}

    def _park(self, name: str, error: str, tools: list[dict]) -> None:
        """Mark `name` unavailable — the caller has ALREADY restored its
        routes via `_add_route` before calling this, so tools/list keeps
        listing them and tools/call gets a retryable error (see handle())
        instead of Unknown tool. Preserves the original parked_at across
        repeated failed retries, so the TTL counts from the first failure,
        not the most recent one."""
        existing = self.unavailable.get(name)
        self.unavailable[name] = {
            "error": error,
            "parked_at": existing["parked_at"] if existing else time.monotonic(),
            "tools": tools,
        }

    def _unpark(self, name: str) -> None:
        # Whatever this parking episode's total duration turned out to be —
        # counted here (recovery), not while still ongoing, since only a
        # CONCLUDED episode has a duration to report. See
        # resilience:gateway-proof-gated-retry-with-counters /
        # resilience:doctor-mcp-needs-24h-memory, the counter this feeds.
        info = self.unavailable.pop(name, None)
        if info is not None:
            metrics.counters.record(name, "upstream_unavailable_seconds",
                                    time.monotonic() - info["parked_at"])

    def _expire_parked(self) -> list[str]:
        """Give up on any upstream parked longer than PARK_TTL_SECONDS
        without recovering — drop its by-now-stale routes so tools/list
        stops claiming it's available. It still gets a fresh start attempt
        every reload regardless (falls through to the 'added' path below,
        same as an upstream nobody has parked yet) — only the parked
        bookkeeping and its TTL clock reset."""
        now = time.monotonic()
        expired = [name for name, info in self.unavailable.items()
                  if now - info["parked_at"] > PARK_TTL_SECONDS]
        for name in expired:
            info = self.unavailable.pop(name, None)
            if info is not None:
                metrics.counters.record(name, "upstream_unavailable_seconds",
                                        now - info["parked_at"])
            self._drop_local_routes(name)
        return expired

    async def reload(self) -> dict:
        """Re-scan config/mcp.json (apps + config/mcp.custom.json) and
        reconcile the running local upstreams to match it — WITHOUT
        restarting the gateway process itself, so in-flight federation
        (remote upstreams, /link connectors) is undisturbed.

        Diffs old vs new specs by name:
        * removed (no longer in config — a real uninstall/disable) —
          stopped, routes dropped immediately. Never parked, regardless of
          how it's currently doing — an intentional removal must not linger.
        * changed (same name, different spec — e.g. an app's settings
          save changed its mcp.json) — stopped then restarted fresh. On
          failure, PARKED: routes stay published and the old process is
          gone, but the name isn't left routeless (see _park below).
        * parked-retry (currently unavailable, still in config) — always
          gets a fresh start attempt, spec-equality is meaningless since
          nothing of it is actually running. This is what lets a parked
          upstream heal on its own once the core/whatever it depends on
          comes back, without this card's sibling (proof-gated retry with
          backoff) having to exist yet.
        * added (new name) — started fresh. Nothing to park on failure —
          it never had routes to begin with.
        * unchanged (live, same spec) — left running as-is, no reconnect
          churn.

        Called by aw-workspace after an app with `contributes.mcp: true`
        saves its config (that app is expected to have already rewritten
        its own mcp.json to disk BEFORE this fires) — see aw-workspace's
        save_app_config route. Also called periodically by aw-workspace's
        own rescan (src/apps/runtime.py, up to DEFAULT_MCP_RESCAN_INTERVAL_S
        seconds apart), which is what drives the parked-retry path above.
        """
        self._expire_parked()
        new_specs = self._load_specs()
        old_names = set(self.upstreams) | set(self.unavailable)
        new_names = set(new_specs)

        removed = old_names - new_names
        added = new_names - old_names
        common = old_names & new_names
        parked_retry = {n for n in common if n in self.unavailable}
        changed = {n for n in common
                  if n not in self.unavailable and self.upstreams[n].spec != new_specs[n]}
        unchanged = common - changed - parked_retry

        # Real config removal — dropped right away, never parked. Only a
        # name that FAILS to (re)start below gets parked, with a TTL.
        for name in removed:
            up = self.upstreams.pop(name, None)
            if up is not None:
                await up.stop()
            self._drop_local_routes(name)
            self._unpark(name)

        failed: list[dict] = []
        for name in sorted(added | changed | parked_retry):
            prior_tools: list[dict] | None = None
            if name in changed:
                prior_up = self.upstreams.pop(name, None)
                if prior_up is not None:
                    prior_tools = list(prior_up.tools)
                    await prior_up.stop()
                self._drop_local_routes(name)
            elif name in parked_retry:
                prior_tools = list(self.unavailable[name].get("tools") or [])
                self._drop_local_routes(name)

            error = await self._start_one(name, new_specs[name])
            if error:
                failed.append({"name": name, "error": error})
                if prior_tools is not None:
                    # Had routes published before this attempt — restore
                    # them instead of leaving the name routeless. This is
                    # the actual fix: tools/list keeps listing it, and
                    # handle()'s tools/call now returns a retryable error
                    # instead of "Unknown tool" for it.
                    for t in prior_tools:
                        self._add_route(name, t)
                    self._park(name, error, prior_tools)
            else:
                self._unpark(name)

        log.info("gateway reload: +%d -%d ~%d changed, %d unchanged, %d failed, %d parked — "
                 "%d local upstreams, %d tools now",
                 len(added), len(removed), len(changed), len(unchanged), len(failed),
                 len(self.unavailable), len(self.upstreams), len(self.agg_tools))
        return {
            "added": sorted(added), "removed": sorted(removed),
            "changed": sorted(changed), "unchanged": sorted(unchanged),
            "failed": failed, "parked": sorted(self.unavailable),
            "upstreams": sorted(self.upstreams), "tools": len(self.agg_tools),
        }

    def _add_route(self, server: str, tool: dict) -> None:
        # `server` stays the real dispatch key (self.upstreams/self.remotes
        # lookup) — the workspace prefix only decorates the PUBLIC name, so
        # routing is unaffected by whether workspace_name is set.
        display_server = f"{self.workspace_name}__{server}" if self.workspace_name else server
        public = public_name(display_server, tool["name"])
        t = dict(tool)
        t["name"] = public
        t["description"] = f"[{server}] {t.get('description', '')}".strip()
        self.agg_tools.append(t)
        self.routes[public] = (server, tool["name"])
        self.route_annotations[public] = dict(tool.get("annotations") or {})

    @property
    def federation_chain(self) -> list[str]:
        """This gateway's own id, prefixed onto the longest chain reported by
        any federated (``type: gateway``) upstream — so a gateway three hops
        downstream sees a 3-long chain and can refuse a 4th if it would
        exceed its ``max_federation_depth``, and any gateway that recognizes
        its own id already in here knows federating back would be a cycle."""
        longest: list[str] = []
        for up in self.upstreams.values():
            if isinstance(up, GatewayUpstream) and up.remote_gateway_id:
                candidate = list(up.remote_chain) or [up.remote_gateway_id]
                if len(candidate) > len(longest):
                    longest = candidate
        return [self.gateway_id] + longest

    # ── Remote (WS-registered) upstreams ────────────────────────────────────

    def register_remote(self, remote: RemoteUpstream) -> None:
        """Assign ``remote`` its public app-name and publish its tools.

        Reconnect-safe: the same token id reconnecting gets back the exact
        public name it already had (no route duplication, no re-numbering).
        Collision-safe: a genuinely different app registering with a base
        name already in use gets uniquely numbered — per Fred's decision,
        BOTH the original and the newcomer end up as "Browser 1"/"Browser 2"
        (not a `{app}_{server}__{tool}` route mangle)."""
        token_id = remote.token_id
        prior = self._remote_by_token.get(token_id)
        if prior is not None:
            self._withdraw_remote(prior)
            remote.app_name = prior.app_name
        else:
            base = f"{remote.workspace_name}::{remote.base_name}"
            group = self._remote_name_groups.setdefault(base, [])
            if not group:
                remote.app_name = remote.base_name
            else:
                if len(group) == 1:
                    self._rename_remote(group[0], f"{remote.base_name} 1")
                remote.app_name = f"{remote.base_name} {len(group) + 1}"
            group.append(token_id)
        self._remote_by_token[token_id] = remote
        self.remotes[remote.route_name] = remote
        for tool in remote.tools:
            self._add_route(remote.route_name, tool)

    def _rename_remote(self, token_id: str, new_name: str) -> None:
        target = self._remote_by_token.get(token_id)
        if target is None:
            return
        was_live = self.remotes.get(target.route_name) is target
        self._withdraw_remote(target)
        target.app_name = new_name
        if was_live:
            # Only republish routes if this remote actually has a live
            # connection right now — a numbering rename triggered by a
            # newcomer must not resurrect routes for a disconnected app.
            self.remotes[target.route_name] = target
            for tool in target.tools:
                self._add_route(target.route_name, tool)

    def _withdraw_remote(self, remote: RemoteUpstream) -> None:
        """Drop a remote's live routes. Does NOT forget its name reservation
        (``_remote_by_token`` / ``_remote_name_groups``) — that's what makes
        a later reconnect land back on the same public name instead of
        colliding with itself. Truth for routing purposes is "is it in
        ``self.remotes``" — a disconnected remote is removed from there
        immediately so no call is ever routed to a dead session."""
        if self.remotes.get(remote.route_name) is remote:
            del self.remotes[remote.route_name]
        self.agg_tools = [t for t in self.agg_tools
                          if self.routes.get(t["name"], ("",))[0] != remote.route_name]
        self.routes = {k: v for k, v in self.routes.items() if v[0] != remote.route_name}
        self.route_annotations = {k: v for k, v in self.route_annotations.items() if k in self.routes}

    def unregister_remote(self, remote: RemoteUpstream) -> None:
        self._withdraw_remote(remote)

    async def handle(self, msg: dict) -> dict | None:
        """Dispatch one JSON-RPC message. Returns None for notifications."""
        method = msg.get("method", "")
        req_id = msg.get("id")

        if method == "initialize":
            requested = (msg.get("params") or {}).get("protocolVersion") or "2024-11-05"
            return {"jsonrpc": "2.0", "id": req_id, "result": {
                "protocolVersion": requested,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "aw-mcp-gateway", "version": "1.0.0"}}}

        if method in ("notifications/initialized", "notifications/cancelled"):
            return None

        if method == "ping":
            return {"jsonrpc": "2.0", "id": req_id, "result": {}}

        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": self.agg_tools}}

        if method == "tools/call":
            params = msg.get("params") or {}
            public = params.get("name", "")
            route = self.routes.get(public)
            if not route:
                metrics.counters.record(metrics.UNROUTED, "tools_call_errors.unknown_tool")
                return {"jsonrpc": "2.0", "id": req_id, "error": {
                    "code": -32602, "message": f"Unknown tool: {public}"}}
            server, tool = route
            parked = self.unavailable.get(server)
            if parked is not None:
                # The route is real — it's still in tools/list — but the
                # upstream it dispatches to just failed to (re)start. Unlike
                # the branch above, this is RETRYABLE: an LLM that reads
                # "Unknown tool" concludes the tool was removed and gives
                # up; this message says try again instead. Keep the existing
                # contract (result + isError, not a JSON-RPC exception) —
                # every caller already handles that shape.
                return {"jsonrpc": "2.0", "id": req_id, "result": {
                    "content": [{"type": "text",
                                 "text": f"upstream '{server}' temporarily unavailable, "
                                         f"reconnecting ({parked['error']}) — retry this call"}],
                    "isError": True}}
            arguments = dict(params.get("arguments", {}) or {})
            handler = self.upstreams.get(server) or self.remotes.get(server)
            if handler is None:
                return {"jsonrpc": "2.0", "id": req_id, "error": {
                    "code": -32602, "message": f"Upstream '{server}' is not connected"}}
            if isinstance(handler, HttpUpstream):
                # Only the HTTP family (HttpUpstream + GatewayUpstream) has
                # the proof-gated retry loop — the stdio path is a different
                # beast (Upstream._ensure_alive already respawns a dead
                # child) and isn't touched here.
                annotations = self.route_annotations.get(public) or {}
                return await handler.call_tool(tool, arguments, req_id,
                                               idempotent_hint=bool(annotations.get("idempotentHint")))
            return await handler.call_tool(tool, arguments, req_id)

        return {"jsonrpc": "2.0", "id": req_id, "error": {
            "code": -32601, "message": f"Unknown method: {method}"}}


class NamedConfigs:
    """The live set of named configs (scoped ``/mcp/<name>`` profiles).

    Holds the specs in memory so ``PUT /admin/configs`` takes effect on the
    very next request — the endpoint is ONE dynamic route that looks a profile
    up by name at call time, not one FastAPI route registered per config at
    boot. That is the difference between "add a config" and "add a config, then
    remember to restart the gateway"; the monolith this was ported from needed
    the restart, and a profile that 404s until someone notices is exactly the
    kind of silent degradation this workspace already has too much of.
    """

    def __init__(self, specs: dict | None = None, agents_base: str = "",
                 upstream_roles: dict | None = None):
        self.specs: dict[str, dict] = dict(specs or {})
        self.agents_base = agents_base
        self.upstream_roles = upstream_roles or policy_upstreams()

    def names(self) -> list[str]:
        return sorted(self.specs)

    def replace(self, specs: dict) -> None:
        self.specs = dict(specs or {})

    def gateway_for(self, gateway: Gateway, name: str) -> ConfigGateway | None:
        spec = self.specs.get(name)
        if spec is None:
            return None
        return ConfigGateway(gateway, spec, name=name,
                             agents_base=self.agents_base,
                             upstream_roles=self.upstream_roles)


def build_app(gateway: Gateway, token: str, named_configs: dict | None = None,
              token_store: TokenStore | None = None, port: int = 9200) -> FastAPI:
    from contextlib import asynccontextmanager

    # Accepts the historical ``{name: [upstreams]}`` shape as well as the full
    # ``{name: {...spec}}`` one — ConfigGateway normalises either.
    configs = NamedConfigs(
        {name: (spec if isinstance(spec, dict) else {"upstreams": list(spec or [])})
         for name, spec in (named_configs or {}).items()},
        agents_base=config.agents_base(),
        upstream_roles=policy_upstreams(config.policy_upstream_overrides()),
    )
    token_store = token_store or FileTokenStore(config.link_tokens_path())

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await gateway.start()
        config.register_self_in_host_mcp_json(port, token)
        yield

    app = FastAPI(title="AW MCP Gateway (standalone)", lifespan=lifespan)

    def _check_auth(authorization: str | None) -> None:
        from fastapi import HTTPException
        expected = f"Bearer {token}"
        if authorization != expected:
            raise HTTPException(status_code=401, detail="missing or invalid bearer token")

    def _check_admin_auth(authorization: str | None, workspace_identity: str | None) -> None:
        if workspace_identity:
            return
        _check_auth(authorization)

    async def _dispatch(handler, request: Request, authorization: str | None) -> Response:
        _check_auth(authorization)
        # Before any upstream call in this request's task — see caller_context.
        await caller_context.capture(request.headers)
        body = await request.json()
        messages = body if isinstance(body, list) else [body]
        responses = []
        for m in messages:
            resp = await handler.handle(m)
            if resp is not None:
                responses.append(resp)
        if not responses:
            return Response(status_code=202)
        out = responses if isinstance(body, list) else responses[0]
        return JSONResponse(out)

    @app.get("/healthz")
    async def healthz():
        federated = [name for name, up in gateway.upstreams.items() if isinstance(up, GatewayUpstream)]
        upstream_names = set(gateway.upstreams) | set(gateway.remotes) | set(gateway.unavailable)
        return {"ok": True,
                "local_upstreams": list(gateway.upstreams),
                "remote_upstreams": list(gateway.remotes),
                "tools": len(gateway.agg_tools),
                "configs": configs.names(),
                "gateway_id": gateway.gateway_id,
                # Which workspace this gateway belongs to. Unauthenticated on
                # purpose, alongside gateway_id: the case a caller most needs
                # to diagnose is the one where its bearer is REJECTED — a
                # client that reached the wrong gateway gets a 401 and no way
                # to tell that from a rotated token. Both fields are
                # self-descriptive, not credentials.
                "workspace_slug": gateway.workspace_name,
                "federation_chain": gateway.federation_chain,
                "federated_gateways": federated,
                # 24h rolling-window resilience counters —
                # resilience:gateway-proof-gated-retry-with-counters. This is
                # the memory the sibling doctor card
                # (resilience:doctor-mcp-needs-24h-memory) reads: retries and
                # retries_exhausted are the companion no retry above is
                # allowed to ship without, tools_call_errors.unknown_tool
                # rising means the route-parking mechanism regressed, and
                # upstream_unavailable_seconds is what would have caught the
                # 2026-08-30 blips a present-tense /healthz check couldn't.
                "resilience": {
                    "window_seconds": metrics.WINDOW_SECONDS,
                    "by_upstream": metrics.counters.snapshot(list(upstream_names)),
                }}

    @app.post("/mcp")
    async def mcp_post(request: Request, authorization: str | None = Header(default=None)):
        return await _dispatch(gateway, request, authorization)

    @app.get("/mcp")
    async def mcp_get():
        return Response(status_code=405)

    @app.websocket("/link")
    async def link_ws(websocket: WebSocket):
        await link_endpoint(websocket, gateway, token_store)

    @app.get("/link-tokens")
    async def list_link_tokens(authorization: str | None = Header(default=None)):
        _check_auth(authorization)
        return {"tokens": [t.public_dict() for t in token_store.list()]}

    @app.get("/admin/config")
    async def get_config(
        authorization: str | None = Header(default=None),
        workspace_identity: str | None = Header(default=None, alias="X-AW-Identity-Sub"),
    ):
        _check_admin_auth(authorization, workspace_identity)
        payload = config.effective_mcp_config(write_final=True)
        # The gateway's own root bearer token — same value that gates /mcp,
        # /link-tokens, etc. via _check_auth. Surfaced here (read-only) so the
        # admin UI can show/copy it without a second privileged endpoint.
        # Gated by the same admin auth as the rest of this route.
        payload["token"] = token
        return payload

    @app.put("/admin/config")
    async def put_config(
        request: Request,
        authorization: str | None = Header(default=None),
        workspace_identity: str | None = Header(default=None, alias="X-AW-Identity-Sub"),
    ):
        _check_admin_auth(authorization, workspace_identity)
        body = await request.json()
        custom = body.get("custom") if isinstance(body, dict) else None
        if custom is None:
            custom = body
        if not isinstance(custom, dict):
            return JSONResponse({"error": "custom config must be an object"}, status_code=400)
        config.save_custom_mcp_config(custom)
        payload = config.effective_mcp_config(write_final=True)
        payload["restart_required"] = True
        payload["token"] = token
        return payload

    @app.post("/reload")
    async def reload_upstreams(
        authorization: str | None = Header(default=None),
        workspace_identity: str | None = Header(default=None, alias="X-AW-Identity-Sub"),
    ):
        """Hot-reload local upstreams from disk — no process restart, no
        "restart_required" left dangling for the caller to act on later.
        Same admin auth as /admin/config (bearer, or the trusted
        X-AW-Identity-Sub header aw-workspace's own internal calls carry).

        The intended caller is aw-workspace right after an app with
        contributes.mcp: true saves its config: that app rewrites its own
        mcp.json to disk FIRST, then aw-workspace calls this directly on
        the container's internal address (no public hairpin through this
        app's own reverse-proxy route)."""
        _check_admin_auth(authorization, workspace_identity)
        return await gateway.reload()

    @app.post("/link-tokens")
    async def mint_link_token(request: Request, authorization: str | None = Header(default=None)):
        _check_auth(authorization)
        body = await request.json()
        full, record = token_store.mint(label=body.get("label", ""), scopes=body.get("scopes"))
        # Full token is returned exactly once — only the hash is persisted.
        return {"token": full, **record.public_dict()}

    @app.post("/link-tokens/{token_id}/revoke")
    async def revoke_link_token(token_id: str, authorization: str | None = Header(default=None)):
        _check_auth(authorization)
        if not token_store.revoke(token_id):
            return JSONResponse({"error": "unknown token id"}, status_code=404)
        return {"ok": True}

    def _upstream_candidates() -> list[dict]:
        """Every upstream a named config could be pointed at, with whether it
        is actually running right now. The admin UI needs both: a config may
        legitimately name an upstream that is momentarily down, and hiding it
        would silently drop it from the config on the next save."""
        specs = config.load_mcp_servers()
        names = set(specs) | set(gateway.upstreams) | set(gateway.remotes)
        out = []
        for name in sorted(names):
            spec = specs.get(name) or {}
            out.append({
                "name": name,
                "type": spec.get("type", "stdio") if spec else "remote",
                "enabled": spec.get("enabled") is not False,
                "running": name in gateway.upstreams or name in gateway.remotes,
                "tools": sum(1 for r in gateway.routes.values() if r[0] == name),
            })
        return out

    def _configs_payload() -> dict:
        return {
            "configs": configs.specs,
            "candidates": _upstream_candidates(),
            "policy_upstreams": {k: sorted(v) for k, v in configs.upstream_roles.items()},
            "agents_base": configs.agents_base,
            "endpoint_template": f"http://127.0.0.1:{port}/mcp/{{name}}",
        }

    @app.get("/admin/configs")
    async def get_named_configs(
        authorization: str | None = Header(default=None),
        workspace_identity: str | None = Header(default=None, alias="X-AW-Identity-Sub"),
    ):
        _check_admin_auth(authorization, workspace_identity)
        return _configs_payload()

    @app.put("/admin/configs")
    async def put_named_configs(
        request: Request,
        authorization: str | None = Header(default=None),
        workspace_identity: str | None = Header(default=None, alias="X-AW-Identity-Sub"),
    ):
        """Replace the whole named-config set and apply it immediately.

        Whole-set (not per-config) because that is what the editor holds and
        it makes deletion expressible without a second verb."""
        _check_admin_auth(authorization, workspace_identity)
        body = await request.json()
        incoming = body.get("configs") if isinstance(body, dict) else None
        if incoming is None:
            incoming = body
        try:
            saved = config.save_named_configs(incoming)
        except ValueError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        configs.replace(saved)
        # Re-derive here too: a config can start naming agents-platform for the
        # first time, and the approval gate needs a base URL for it right away.
        configs.agents_base = config.agents_base()
        log.info("named configs updated: %s", ", ".join(configs.names()) or "—")
        return _configs_payload()

    @app.post("/mcp/{cfg_name}")
    async def mcp_config_post(cfg_name: str, request: Request,
                              authorization: str | None = Header(default=None)):
        _check_auth(authorization)
        handler = configs.gateway_for(gateway, cfg_name)
        if handler is None:
            return JSONResponse({"error": f"No such config: {cfg_name}"}, status_code=404)
        return await _dispatch(handler, request, authorization)

    @app.get("/mcp/{cfg_name}")
    async def mcp_config_get(cfg_name: str):
        return Response(status_code=405)

    for cfg_name in configs.names():
        log.info("config endpoint available: /mcp/%s (%s)", cfg_name,
                 ", ".join(configs.specs[cfg_name].get("upstreams") or []) or "—")

    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    gw_cfg = config.load_gateway_config()

    parser = argparse.ArgumentParser(description="AW MCP Gateway — standalone (Streamable HTTP + /link)")
    parser.add_argument("--host", default=os.environ.get("MCP_GATEWAY_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(
        os.environ.get("MCP_GATEWAY_PORT") or gw_cfg.get("port") or "9200"))
    args = parser.parse_args()

    allow_env = os.environ.get("AW_MCP_GATEWAY_ALLOW", "").strip()
    if allow_env == "*":
        allow = [n for n, s in config.load_mcp_servers().items()
                 if s.get("type", "stdio") == "stdio" and s.get("enabled") is not False]
    elif allow_env:
        allow = [s.strip() for s in allow_env.split(",") if s.strip()]
    elif gw_cfg.get("upstreams"):
        allow = list(gw_cfg["upstreams"])
    else:
        allow = DEFAULT_ALLOW

    tok = config.token()
    gateway = Gateway(allow)
    app = build_app(gateway, tok, config.named_configs(), port=args.port)

    log.info("AW MCP Gateway (standalone) on http://%s:%d/mcp (+ ws /link)", args.host, args.port)
    log.info("local upstream allowlist: %s", ", ".join(allow) or "—")
    log.info("bearer token: %s (source: config/gateway.json)", "set" if tok else "MISSING")
    log.info("gateway_id: %s | max_federation_depth: %d | link tokens: %s",
             gateway.gateway_id, gateway.max_federation_depth, config.link_tokens_path())

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
