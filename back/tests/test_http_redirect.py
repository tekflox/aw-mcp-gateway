"""HttpUpstream must follow redirects during its own handshake — an upstream
that 301s (e.g. a WordPress vhost forcing canonical https) should still be
usable, not dropped with 0 tools. Regression test for commit a14e944
(follow_redirects=True on HttpUpstream/GatewayUpstream's httpx clients).
"""

from __future__ import annotations

import asyncio
import contextlib

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse

from gateway import config as config_module
from gateway.server import Gateway

PING_TOOL = {"name": "ping", "description": "", "inputSchema": {"type": "object"}}


def _redirecting_mcp_app(real_path: str = "/mcp-real") -> FastAPI:
    """A stand-in for an upstream that redirects every request on the
    handshake path to a `real_path` that actually answers MCP.

    Uses 308 (method-preserving), not 301: httpx's redirect handling
    downgrades a POST to GET on a 301/302/303 (RFC 7231 semantics), which
    would drop the JSON-RPC body entirely — a real, separate gap that
    follow_redirects=True does NOT paper over (see the crispal-wordpress-
    production Kanban card: its 301 needed an X-Forwarded-Proto header to
    avoid the redirect altogether, follow_redirects alone wasn't enough).
    This test covers the case follow_redirects=True actually fixes: a
    same-method redirect that previously blew up in raise_for_status()
    before the client ever got a chance to retry it."""
    app = FastAPI()

    @app.post("/mcp")
    async def redirect():
        return RedirectResponse(url=real_path, status_code=308)

    @app.post(real_path)
    async def handle(request: Request):
        body = await request.json()
        method = body.get("method")
        if method == "initialize":
            return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"), "result": {
                "protocolVersion": "2024-11-05", "capabilities": {},
                "serverInfo": {"name": "redirecting-upstream", "version": "1.0.0"}}})
        if method == "tools/list":
            return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"),
                                  "result": {"tools": [PING_TOOL]}})
        return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"), "result": {}})

    return app


@contextlib.asynccontextmanager
async def running_app(app: FastAPI, port: int):
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.02)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


@contextlib.contextmanager
def mcp_servers_override(servers: dict):
    """Same trick as test_federation.py: point config.load_mcp_servers at an
    in-memory dict for the duration of the block."""
    original = config_module.load_mcp_servers
    config_module.load_mcp_servers = lambda: servers
    try:
        yield
    finally:
        config_module.load_mcp_servers = original


async def test_http_upstream_follows_redirect_during_handshake():
    async with running_app(_redirecting_mcp_app(), 19401) as base_url:
        gw = Gateway(["redirecting"])
        with mcp_servers_override({
            "redirecting": {"type": "http", "url": f"{base_url}/mcp"},
        }):
            await gw.start()

        # Without follow_redirects=True, start() raises on the 301 and the
        # upstream never lands in gw.upstreams at all — this is the exact
        # "zero tools" failure mode from degraded:mcp-crispal-wordpress-
        # production-zero-tools.
        assert "redirecting" in gw.upstreams
        assert "redirecting__ping" in gw.routes
