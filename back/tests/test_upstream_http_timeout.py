"""resilience: HttpUpstream/GatewayUpstream must not apply the same short
budget to a tools/call's *read* leg as to connect/write/pool.

Root cause (Debugger-confirmed, Kanban card 3e2c4132-9691-816e-bdbd-
d35d15af40d2): ``httpx.AsyncClient(timeout=30.0)`` applies that one scalar
uniformly to connect/read/write/pool, for every ``tools/call`` this gateway
proxies to an HTTP upstream. Several real tools legitimately block past 30s
waiting on a human Telegram approval tap — confirmed live for aw-crispal's
``backup_database`` (up to two Approve/Deny taps, a 300s SSH-key-vault
approval poll, a 900s backup subprocess). The gateway's read timeout fired
first every time; ``call_tool`` then classified the ``httpx.TimeoutException``
as unproven (see ``_classify_call_failure``) and told the caller the result
was UNCERTAIN, while the real handler kept running orphaned server-side —
``docker top`` showed still-running ``rsync``/``download_db_dump.sh``
processes minutes after three separate "failed" gateway calls.

``UPSTREAM_HTTP_TIMEOUT`` fixes this: connect/write/pool stay short (10s/
30s/10s) so a genuinely dead upstream still fails fast, but read gets a
600s budget so a slow-but-eventually-successful approval-gated call
completes instead of getting told it's uncertain.
"""

from __future__ import annotations

import asyncio
import contextlib

import httpx
import pytest
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from gateway.upstream import (
    HEALTH_CHECK_TIMEOUT,
    GatewayUpstream,
    HttpUpstream,
    UPSTREAM_HTTP_TIMEOUT,
)

SLOW_TOOL = {"name": "slow_tool", "description": "", "inputSchema": {"type": "object"}}


def _slow_mcp_app(delay_seconds: float) -> FastAPI:
    """A stand-in for an upstream whose tools/call handler legitimately
    blocks (e.g. on a human approval tap) — only the tools/call leg sleeps,
    so start()'s handshake/tools-list is always fast regardless of delay."""
    app = FastAPI()

    @app.get("/healthz")
    async def healthz():
        return JSONResponse({"gateway_id": "remote", "federation_chain": []})

    @app.post("/mcp")
    async def handle(request: Request):
        body = await request.json()
        method = body.get("method")
        if method == "initialize":
            return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"), "result": {
                "protocolVersion": "2024-11-05", "capabilities": {},
                "serverInfo": {"name": "slow-upstream", "version": "1.0.0"}}})
        if method == "tools/list":
            return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"),
                                  "result": {"tools": [SLOW_TOOL]}})
        if method == "tools/call":
            await asyncio.sleep(delay_seconds)
            return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"), "result": {
                "content": [{"type": "text", "text": "done"}], "isError": False}})
        return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"), "result": {}})

    return app


@contextlib.asynccontextmanager
async def running_app(app: FastAPI, port: int):
    """Same helper as test_http_redirect.py's ``running_app``, duplicated
    locally to avoid a cross-file import for one fixture."""
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.02)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await task


def test_upstream_http_timeout_keeps_connect_write_pool_tight_but_read_generous():
    """The exact shape the incident needs — read gets far more slack than
    the legs that guard against a genuinely dead/unreachable upstream."""
    assert UPSTREAM_HTTP_TIMEOUT.connect == 10.0
    assert UPSTREAM_HTTP_TIMEOUT.write == 30.0
    assert UPSTREAM_HTTP_TIMEOUT.pool == 10.0
    assert UPSTREAM_HTTP_TIMEOUT.read == 600.0


async def test_http_upstream_start_constructs_client_with_the_shared_timeout():
    async with running_app(_slow_mcp_app(0), 19402) as base_url:
        up = HttpUpstream("svc", {"url": f"{base_url}/mcp"})
        await up.start()
        try:
            assert up._client.timeout == UPSTREAM_HTTP_TIMEOUT
        finally:
            await up.stop()


async def test_gateway_upstream_start_constructs_client_with_the_shared_timeout():
    """The federated-gateway client (call_tool is inherited, unmodified)
    needs the identical budget — an approval-gated tool one hop further
    into a federated gateway hits the same 30s-read problem otherwise."""
    async with running_app(_slow_mcp_app(0), 19403) as base_url:
        up = GatewayUpstream("leaf", {"url": f"{base_url}/mcp"}, "own-id", 6)
        await up.start()
        try:
            assert up._client.timeout == UPSTREAM_HTTP_TIMEOUT
        finally:
            await up.stop()


async def test_slow_but_successful_call_within_the_read_budget_does_not_error(monkeypatch):
    """The actual approval-tap scenario, scaled down for test speed: a
    response that blows way past a short connect/write/pool budget must
    still succeed as long as it lands inside READ's generous one."""
    monkeypatch.setattr(
        "gateway.upstream.UPSTREAM_HTTP_TIMEOUT",
        httpx.Timeout(connect=0.2, read=2.0, write=0.2, pool=0.2),
    )
    async with running_app(_slow_mcp_app(1.0), 19404) as base_url:
        up = HttpUpstream("svc", {"url": f"{base_url}/mcp"})
        await up.start()
        try:
            resp = await up.call_tool("slow_tool", {}, req_id=1, idempotent_hint=False)
        finally:
            await up.stop()

    assert resp["result"]["isError"] is False
    assert resp["result"]["content"][0]["text"] == "done"


async def test_a_call_that_outlives_even_the_read_budget_still_times_out(monkeypatch):
    """The budget is generous, not infinite — proves read timeout is still
    enforced (and still comes back distinguishably UNCERTAIN for a
    non-idempotent tool), not accidentally disabled by this fix."""
    monkeypatch.setattr(
        "gateway.upstream.UPSTREAM_HTTP_TIMEOUT",
        httpx.Timeout(connect=0.2, read=0.3, write=0.2, pool=0.2),
    )
    async with running_app(_slow_mcp_app(1.0), 19405) as base_url:
        up = HttpUpstream("svc", {"url": f"{base_url}/mcp"})
        await up.start()
        try:
            resp = await up.call_tool("slow_tool", {}, req_id=1, idempotent_hint=False)
        finally:
            await up.stop()

    assert resp["result"]["isError"] is True
    assert "UNCERTAIN" in resp["result"]["content"][0]["text"]


async def test_dead_connect_is_not_swallowed_by_the_long_read_budget(monkeypatch):
    """A connection that never opens (nothing listening) must fail on
    CONNECT — proves the fix didn't turn 'upstream unreachable' into a 600s
    hang. Backoff sleeps neutered so the retried connect attempts (see
    START_CONNECT_MAX_ATTEMPTS) don't slow this test down."""
    async def _no_sleep(_seconds):
        return None
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

    up = HttpUpstream("svc", {"url": "http://127.0.0.1:1/mcp"})  # nothing listens here
    with pytest.raises(httpx.ConnectError):
        await up.start()


def _zombie_health_check_app(hang_seconds: float) -> FastAPI:
    """A stand-in for the exact upstream this card is about: the connection
    is accepted and the FIRST tools/list (start()'s handshake) answers
    normally, but every tools/list AFTER that — the health-check probe —
    hangs, as a frozen process still listening on the port would."""
    app = FastAPI()
    calls = {"tools_list": 0}

    @app.post("/mcp")
    async def handle(request: Request):
        body = await request.json()
        method = body.get("method")
        if method == "initialize":
            return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"), "result": {
                "protocolVersion": "2024-11-05", "capabilities": {},
                "serverInfo": {"name": "zombie-upstream", "version": "1.0.0"}}})
        if method == "tools/list":
            calls["tools_list"] += 1
            if calls["tools_list"] == 1:
                return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"),
                                      "result": {"tools": [SLOW_TOOL]}})
            await asyncio.sleep(hang_seconds)
            return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"),
                                  "result": {"tools": [SLOW_TOOL]}})
        return JSONResponse({"jsonrpc": "2.0", "id": body.get("id"), "result": {}})

    return app


def test_health_check_timeout_is_short_not_the_600s_read_budget():
    """The whole fix in one assertion: a health-check probe must not share
    UPSTREAM_HTTP_TIMEOUT's read=600.0, which exists for a slow-but-
    eventually-successful tool call, not a liveness check."""
    assert HEALTH_CHECK_TIMEOUT.read < UPSTREAM_HTTP_TIMEOUT.read
    assert HEALTH_CHECK_TIMEOUT.read <= 10.0


async def test_post_omits_timeout_kwarg_by_default_and_forwards_an_explicit_override(monkeypatch):
    """Guards the exact httpx footgun this fix has to avoid: AsyncClient.post
    treats an explicit ``timeout=None`` as "no timeout at all" (infinite),
    not "use the client default" — so _post's default path must OMIT the
    kwarg entirely rather than pass None through."""
    captured: dict = {"calls": []}

    class _FakeResponse:
        headers: dict = {}
        text = '{"jsonrpc": "2.0", "id": "x", "result": {}}'

        @staticmethod
        def raise_for_status():
            pass

        @staticmethod
        def json():
            return {"jsonrpc": "2.0", "id": "x", "result": {}}

    async def _fake_post(self, url, *, json, headers, **kwargs):
        captured["calls"].append(kwargs)
        return _FakeResponse()

    monkeypatch.setattr(httpx.AsyncClient, "post", _fake_post)

    up = HttpUpstream("svc", {"url": "http://example.invalid/mcp"})
    up._client = httpx.AsyncClient(timeout=UPSTREAM_HTTP_TIMEOUT)
    try:
        await up._post({"jsonrpc": "2.0", "id": "x", "method": "tools/list"})
        await up._post({"jsonrpc": "2.0", "id": "x", "method": "tools/list"},
                        timeout=HEALTH_CHECK_TIMEOUT)
    finally:
        await up._client.aclose()

    assert "timeout" not in captured["calls"][0]  # falls back to the client's own default
    assert captured["calls"][1]["timeout"] == HEALTH_CHECK_TIMEOUT  # override forwarded verbatim


async def test_health_check_does_not_inherit_the_600s_read_budget_on_a_real_hang(monkeypatch):
    """End-to-end against a real socket: an upstream that accepts the
    connection and then never answers the probe must fail health_check()
    within HEALTH_CHECK_TIMEOUT, not UPSTREAM_HTTP_TIMEOUT's read=600.0.
    HEALTH_CHECK_TIMEOUT is shrunk only so the test doesn't itself take
    several seconds — the hang on the server side is real, not simulated."""
    monkeypatch.setattr(
        "gateway.upstream.HEALTH_CHECK_TIMEOUT",
        httpx.Timeout(connect=0.2, read=0.3, write=0.2, pool=0.2),
    )
    async with running_app(_zombie_health_check_app(5.0), 19407) as base_url:
        up = HttpUpstream("svc", {"url": f"{base_url}/mcp"})
        await up.start()
        try:
            loop = asyncio.get_event_loop()
            start = loop.time()
            with pytest.raises(httpx.TimeoutException):
                await up.health_check()
            elapsed = loop.time() - start
        finally:
            await up.stop()

    assert elapsed < 2.0  # bounded by HEALTH_CHECK_TIMEOUT (0.3s), nowhere near the 5s hang or 600s read
