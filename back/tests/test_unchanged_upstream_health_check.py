"""degraded:mcp-gateway-zombie-upstream-dead-container-hostname — the gap
mcp-gateway-http-upstream-zombie-caching first documented (crispal-wordpress-
production, 2026-08-19): ``Gateway.reload()`` only ever (re)starts an upstream
that is ``changed``/``parked_retry``/``added`` — one whose spec never changed
is left running forever, even if its real connection dies underneath it
(container recreated, IP reassigned). Only a full gateway *restart* used to
notice. This covers the active health check added to close that gap for the
HTTP family (stdio already self-heals via ``Upstream._ensure_alive()``).
"""

from __future__ import annotations

import contextlib

import httpx

from gateway import config as config_module
from gateway.server import Gateway
from gateway.upstream import HttpUpstream

HTTP_SPEC = {"type": "http", "enabled": True, "url": "http://svc.example/mcp"}
STDIO_SPEC = {
    "type": "stdio", "enabled": True,
    "command": "python3", "args": ["-m", "gateway.examples.echo_server"],
}


@contextlib.contextmanager
def _servers(servers: dict):
    """Point ``config.load_mcp_servers`` at an in-memory dict for the
    duration of the block — same technique test_park_unavailable_routes.py
    uses, duplicated locally to avoid a cross-file import for one helper."""
    original = config_module.load_mcp_servers
    config_module.load_mcp_servers = lambda: servers
    try:
        yield
    finally:
        config_module.load_mcp_servers = original


async def test_unchanged_http_upstream_recovers_from_a_dead_connection_without_a_spec_change(monkeypatch):
    """The exact gap the incidents exposed: an HTTP upstream whose spec never
    changes is never touched by reload()'s changed/added/parked_retry loop.
    Simulates a cached connection that has gone dead (ConnectError) while a
    brand NEW connection to the same URL would succeed — e.g. a container
    recreated behind a stable hostname. Recovery must happen in a SINGLE
    reload() cycle: no spec change, no manual gateway restart."""
    generation = {"n": 0}
    original_init = HttpUpstream.__init__

    def _init(self, name, spec):
        original_init(self, name, spec)
        generation["n"] += 1
        self._gen = generation["n"]

    async def _post(self, msg):
        method = msg.get("method")
        if method == "initialize":
            return {"jsonrpc": "2.0", "id": msg.get("id"), "result": {"serverInfo": {"name": "svc"}}}
        if method == "tools/list":
            self._tools_list_calls = getattr(self, "_tools_list_calls", 0) + 1
            if self._gen == 1 and self._tools_list_calls > 1:
                # The FIRST tools/list (during gw.start()) succeeds — the
                # connection only goes stale afterward, mirroring a
                # container recreated behind the same hostname: the cached
                # client can't reach it anymore, but a fresh one can.
                raise httpx.ConnectError("stale connection to a recreated container")
            return {"jsonrpc": "2.0", "id": msg.get("id"),
                     "result": {"tools": [{"name": "echo", "description": "d"}]}}
        if method == "tools/call":
            return {"jsonrpc": "2.0", "id": msg.get("id"),
                     "result": {"content": [{"type": "text", "text": "ok"}], "isError": False}}
        raise AssertionError(f"unexpected method {method}")

    monkeypatch.setattr(HttpUpstream, "__init__", _init)
    monkeypatch.setattr(HttpUpstream, "_post", _post)

    gw = Gateway(["svc"])
    with _servers({"svc": HTTP_SPEC}):
        await gw.start()
    assert "svc" in gw.upstreams
    assert generation["n"] == 1

    # Same spec, unchanged — reload() must still notice the connection died.
    with _servers({"svc": HTTP_SPEC}):
        result = await gw.reload()

    assert result["reconnected"] == ["svc"]
    assert result["changed"] == []       # never classified as a spec change
    assert result["unchanged"] == []     # moved out of unchanged, not left there
    assert result["failed"] == []        # the fresh connection succeeded
    assert "svc" in gw.upstreams
    assert "svc" not in gw.unavailable   # recovered directly, never had to park
    assert generation["n"] == 2          # a brand-new HttpUpstream was created

    resp = await gw.handle({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "svc__echo", "arguments": {}},
    })
    assert resp["result"]["isError"] is False


async def test_unchanged_http_upstream_health_check_ignores_ambiguous_failure(monkeypatch):
    """A ReadTimeout on the health-check probe PROVES nothing — the request
    may have reached the real handler and only the response got lost.
    Forcing a reconnect on that evidence would repeat the exact mistake
    call_tool's own proof-gated retry already refuses to make."""
    calls = {"n": 0}

    async def _post(self, msg):
        method = msg.get("method")
        if method == "initialize":
            return {"jsonrpc": "2.0", "id": msg.get("id"), "result": {"serverInfo": {"name": "svc"}}}
        if method == "tools/list":
            calls["n"] += 1
            if calls["n"] == 1:
                return {"jsonrpc": "2.0", "id": msg.get("id"),
                         "result": {"tools": [{"name": "echo", "description": "d"}]}}
            raise httpx.ReadTimeout("slow")
        raise AssertionError(f"unexpected method {method}")

    monkeypatch.setattr(HttpUpstream, "_post", _post)

    gw = Gateway(["svc"])
    with _servers({"svc": HTTP_SPEC}):
        await gw.start()
    original_upstream = gw.upstreams["svc"]

    with _servers({"svc": HTTP_SPEC}):
        result = await gw.reload()

    assert result["reconnected"] == []
    assert result["unchanged"] == ["svc"]
    assert gw.upstreams["svc"] is original_upstream  # never torn down
    assert "svc" not in gw.unavailable


async def test_stdio_unchanged_upstream_is_never_health_checked():
    """stdio already self-heals via Upstream._ensure_alive() on its next
    call — the health check must skip it entirely (isinstance guard), not
    just happen to no-op on it."""
    gw = Gateway(["svc"])
    with _servers({"svc": STDIO_SPEC}):
        await gw.start()
    original_upstream = gw.upstreams["svc"]

    with _servers({"svc": STDIO_SPEC}):
        result = await gw.reload()

    assert result["reconnected"] == []
    assert result["unchanged"] == ["svc"]
    assert gw.upstreams["svc"] is original_upstream
