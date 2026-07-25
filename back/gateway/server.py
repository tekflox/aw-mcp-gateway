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

from fastapi import FastAPI, Header, Request, WebSocket
from fastapi.responses import JSONResponse, Response

from . import config
from .config_gateway import ConfigGateway
from .remote_upstream import RemoteUpstream, link_endpoint
from .upstream import HttpUpstream, Upstream, public_name

log = logging.getLogger("aw-mcp-gateway")

DEFAULT_ALLOW: list[str] = []  # empty = nothing local unless config/mcp.json + gateway.json say so


class Gateway:
    def __init__(self, allow: list[str]):
        self.allow = allow
        self.upstreams: dict[str, Upstream | HttpUpstream] = {}
        self.remotes: dict[str, RemoteUpstream] = {}  # app_name -> RemoteUpstream
        self.routes: dict[str, tuple[str, str]] = {}  # public name -> (server, tool)
        self.agg_tools: list[dict] = []

    def _load_specs(self) -> dict[str, dict]:
        servers = config.load_mcp_servers()
        out = {}
        for name in self.allow:
            spec = servers.get(name)
            if not spec:
                log.warning("allowlisted upstream '%s' not found in config/mcp.json", name)
                continue
            if spec.get("enabled") is False:
                log.warning("upstream '%s' is disabled in config/mcp.json — skipping", name)
                continue
            kind = spec.get("type", "stdio")
            if kind not in ("stdio", "http"):
                log.warning("upstream '%s' has unsupported type=%s — skipping", name, kind)
                continue
            out[name] = spec
        return out

    async def start(self) -> None:
        specs = self._load_specs()
        for name, spec in specs.items():
            kind = spec.get("type", "stdio")
            up: Upstream | HttpUpstream = (
                HttpUpstream(name, spec) if kind == "http" else Upstream(name, spec)
            )
            try:
                await up.start()
            except Exception:
                log.exception("failed to start upstream %s", name)
                continue
            self.upstreams[name] = up
            for tool in up.tools:
                self._add_route(name, tool)
            log.info("upstream %s (%s) — %d tools", name, kind, len(up.tools))
        log.info("gateway ready: %d local upstreams, %d tools",
                 len(self.upstreams), len(self.agg_tools))

    def _add_route(self, server: str, tool: dict) -> None:
        public = public_name(server, tool["name"])
        t = dict(tool)
        t["name"] = public
        t["description"] = f"[{server}] {t.get('description', '')}".strip()
        self.agg_tools.append(t)
        self.routes[public] = (server, tool["name"])

    # ── Remote (WS-registered) upstreams ────────────────────────────────────

    def register_remote(self, remote: RemoteUpstream) -> None:
        # A re-register from the same app_name replaces the old one — "last
        # register wins" per the module TODO on reconnect semantics.
        old = self.remotes.get(remote.app_name)
        if old is not None:
            self.unregister_remote(old)
        self.remotes[remote.app_name] = remote
        for tool in remote.tools:
            self._add_route(remote.app_name, tool)

    def unregister_remote(self, remote: RemoteUpstream) -> None:
        if self.remotes.get(remote.app_name) is not remote:
            return
        del self.remotes[remote.app_name]
        self.agg_tools = [t for t in self.agg_tools
                          if self.routes.get(t["name"], ("",))[0] != remote.app_name]
        self.routes = {k: v for k, v in self.routes.items() if v[0] != remote.app_name}

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
                return {"jsonrpc": "2.0", "id": req_id, "error": {
                    "code": -32602, "message": f"Unknown tool: {public}"}}
            server, tool = route
            arguments = dict(params.get("arguments", {}) or {})
            handler = self.upstreams.get(server) or self.remotes.get(server)
            if handler is None:
                return {"jsonrpc": "2.0", "id": req_id, "error": {
                    "code": -32602, "message": f"Upstream '{server}' is not connected"}}
            return await handler.call_tool(tool, arguments, req_id)

        return {"jsonrpc": "2.0", "id": req_id, "error": {
            "code": -32601, "message": f"Unknown method: {method}"}}


def build_app(gateway: Gateway, token: str, named_configs: dict[str, list[str]] | None = None) -> FastAPI:
    from contextlib import asynccontextmanager

    named_configs = named_configs or {}

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await gateway.start()
        yield

    app = FastAPI(title="AW MCP Gateway (standalone)", lifespan=lifespan)

    def _check_auth(authorization: str | None) -> None:
        from fastapi import HTTPException
        expected = f"Bearer {token}"
        if authorization != expected:
            raise HTTPException(status_code=401, detail="missing or invalid bearer token")

    async def _dispatch(handler, request: Request, authorization: str | None) -> Response:
        _check_auth(authorization)
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
        return {"ok": True,
                "local_upstreams": list(gateway.upstreams),
                "remote_upstreams": list(gateway.remotes),
                "tools": len(gateway.agg_tools),
                "configs": list(named_configs.keys())}

    @app.post("/mcp")
    async def mcp_post(request: Request, authorization: str | None = Header(default=None)):
        return await _dispatch(gateway, request, authorization)

    @app.get("/mcp")
    async def mcp_get():
        return Response(status_code=405)

    @app.websocket("/link")
    async def link_ws(websocket: WebSocket):
        await link_endpoint(websocket, gateway, token)

    for cfg_name, cfg_upstreams in named_configs.items():
        cgw = ConfigGateway(gateway, cfg_upstreams, name=cfg_name)

        def _make_handler(handler=cgw):
            async def _h(request: Request, authorization: str | None = Header(default=None)):
                return await _dispatch(handler, request, authorization)
            return _h

        app.post(f"/mcp/{cfg_name}")(_make_handler())
        app.get(f"/mcp/{cfg_name}")(lambda: Response(status_code=405))
        log.info("config endpoint registered: /mcp/%s (%s)", cfg_name, ", ".join(cfg_upstreams) or "—")

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
    named_configs = {
        name: list(spec.get("upstreams") or [])
        for name, spec in (gw_cfg.get("configs") or {}).items()
    }
    gateway = Gateway(allow)
    app = build_app(gateway, tok, named_configs)

    log.info("AW MCP Gateway (standalone) on http://%s:%d/mcp (+ ws /link)", args.host, args.port)
    log.info("local upstream allowlist: %s", ", ".join(allow) or "—")
    log.info("bearer token: %s (source: config/gateway.json)", "set" if tok else "MISSING")

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
