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

import asyncio
import contextlib

import httpx
import pytest

from gateway import config as config_module
from gateway import metrics
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


@pytest.fixture(autouse=True)
def _reset_counters():
    """metrics.counters is a process-wide singleton (test_proof_gated_retry.py's
    identical fixture) — this file's ambiguous-health-check test asserts an
    exact count, which must not depend on what ran before it."""
    metrics.counters._events.clear()
    yield
    metrics.counters._events.clear()


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

    async def _post(self, msg, *, timeout=None):
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


async def test_unchanged_http_upstream_health_check_ignores_ambiguous_failure(monkeypatch, caplog):
    """A ReadTimeout on the health-check probe PROVES nothing — the request
    may have reached the real handler and only the response got lost.
    Forcing a reconnect on that evidence would repeat the exact mistake
    call_tool's own proof-gated retry already refuses to make.

    But "leave it alone" must not also mean "leave no trace" — a repeatedly
    ambiguous health check on the same upstream needs to show up somewhere a
    doctor/24h-window reader would look: a log line, metrics.counters, and
    reload()'s own return value (all three were silent before this test)."""
    calls = {"n": 0}

    async def _post(self, msg, *, timeout=None):
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

    with _servers({"svc": HTTP_SPEC}), caplog.at_level("WARNING", logger="aw-mcp-gateway"):
        result = await gw.reload()

    assert result["reconnected"] == []
    assert result["unchanged"] == ["svc"]
    assert result["health_check_ambiguous"] == ["svc"]  # was invisible in the return dict
    assert gw.upstreams["svc"] is original_upstream  # never torn down
    assert "svc" not in gw.unavailable

    # metrics.counters — reuses the same tools_call_errors.<class> taxonomy
    # call_tool's own retry gate already records on failure.
    snapshot = metrics.counters.snapshot(["svc"])
    assert snapshot["svc"]["tools_call_errors"]["timeout"] == 1

    # log — a repeatedly-ambiguous upstream must leave a trail, not just a
    # one-shot metric nobody is watching in real time.
    assert any("svc" in r.message and "health check inconclusive" in r.message
               for r in caplog.records)


async def test_unchanged_http_upstream_health_check_does_not_hang_on_a_dead_but_connected_upstream(monkeypatch):
    """The scenario this card exists to catch: an upstream that accepted the
    TCP connection but never answers (a frozen process still listening on
    the port, a proxy holding the connection open). Before HEALTH_CHECK_TIMEOUT
    existed, health_check() reused UPSTREAM_HTTP_TIMEOUT's read=600.0 verbatim
    and would hang for up to 10 minutes here — and since Gateway.reload()
    awaits this SEQUENTIALLY over every `unchanged` upstream, it would have
    blocked every other upstream in the same reload() cycle behind it too.

    The mocked ``_post`` below enforces the SAME timeout ``health_check()``
    hands it (asserted explicitly), so this proves the value actually
    threads through end-to-end rather than merely existing as a constant —
    the real socket-level enforcement is covered separately by
    test_upstream_http_timeout.py's pattern against a live server."""
    monkeypatch.setattr(
        "gateway.upstream.HEALTH_CHECK_TIMEOUT",
        httpx.Timeout(connect=0.1, read=0.2, write=0.1, pool=0.1),
    )
    calls = {"n": 0}
    never_respond = asyncio.Event()  # deliberately never set

    async def _post(self, msg, *, timeout=None):
        method = msg.get("method")
        if method == "initialize":
            return {"jsonrpc": "2.0", "id": msg.get("id"), "result": {"serverInfo": {"name": "svc"}}}
        if method == "tools/list":
            calls["n"] += 1
            if calls["n"] == 1:
                return {"jsonrpc": "2.0", "id": msg.get("id"),
                         "result": {"tools": [{"name": "echo", "description": "d"}]}}
            # A hung zombie never answers — bounded strictly by whatever
            # timeout health_check() actually passed in, not the client's
            # own 600s read default (which this test would time out against
            # if health_check() ever regressed to inheriting it again).
            assert timeout is not None and timeout.read == 0.2
            try:
                await asyncio.wait_for(never_respond.wait(), timeout=timeout.read)
            except asyncio.TimeoutError:
                raise httpx.ReadTimeout("simulated hang past health-check timeout") from None
            raise AssertionError("unreachable — wait_for above must time out first")
        raise AssertionError(f"unexpected method {method}")

    monkeypatch.setattr(HttpUpstream, "_post", _post)

    gw = Gateway(["svc"])
    with _servers({"svc": HTTP_SPEC}):
        await gw.start()

    loop = asyncio.get_event_loop()
    start = loop.time()
    with _servers({"svc": HTTP_SPEC}):
        # The outer 5s bound is a safety net, not the real assertion below —
        # if health_check() ever regressed to the 600s read budget, this
        # would fail the test suite by timing out rather than hanging it.
        result = await asyncio.wait_for(gw.reload(), timeout=5.0)
    elapsed = loop.time() - start

    assert result["health_check_ambiguous"] == ["svc"]
    assert elapsed < 1.0  # bounded by HEALTH_CHECK_TIMEOUT (0.2s), nowhere near 600s

    snapshot = metrics.counters.snapshot(["svc"])
    assert snapshot["svc"]["tools_call_errors"]["timeout"] == 1


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
