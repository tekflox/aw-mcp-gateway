"""Forwarding who the caller is, without forwarding anything else.

An upstream that scopes a grant to a caller (aw-app-secrets does) sees the
gateway, not the agent, unless this works. The risk on the other side is a
gateway that cheerfully relays whatever a caller sets — so the allowlist gets
as much attention here as the happy path.
"""
from __future__ import annotations

import asyncio

from gateway import caller_context


def _capture(headers: dict) -> None:
    """capture() is async (warm-token resolution needs to await Redis) — this
    repo's Redis dep isn't configured in the test env, so it takes the
    no-configured-Redis fast path and never actually suspends. Drive the
    coroutine directly rather than ``asyncio.run()``: that wraps it in a
    Task, and a Task's context is a COPY taken at creation — the
    ``_caller_headers.set()`` inside would land in that copy, not the
    caller's own context, and the assertion below would see nothing.
    Stepping the coroutine by hand runs it in the CURRENT context instead —
    works even when it awaits a fake Redis call, since nothing here ever
    needs a real OS-level suspension (an ``await`` on an already-resolved
    coroutine just yields control back for one ``send`` cycle, not forever)."""
    coro = caller_context.capture(headers)
    try:
        while True:
            coro.send(None)
    except StopIteration:
        pass


def test_the_session_header_is_forwarded():
    _capture({"x-aw-caller-session-id": "sess-42"})

    assert caller_context.current() == {"x-aw-caller-session-id": "sess-42"}


def test_nothing_else_is_forwarded():
    """Passthrough would let a caller set Authorization on somebody else's
    upstream. Only the allowlist travels."""
    _capture({
        "x-aw-caller-session-id": "sess-42",
        "authorization": "Bearer someone-elses-token",
        "cookie": "aw_id_jwt=...",
        "x-api-key": "secret",
    })

    assert list(caller_context.current()) == ["x-aw-caller-session-id"]


def test_absent_headers_are_absent_not_empty():
    """An empty string would still look like an identity to an upstream, and
    every anonymous caller would share it."""
    _capture({"x-aw-caller-session-id": ""})

    assert caller_context.current() == {}


def test_a_long_value_is_bounded():
    _capture({"x-aw-caller-session-id": "x" * 5000})

    assert len(caller_context.current()["x-aw-caller-session-id"]) == 256


def test_concurrent_requests_do_not_see_each_others_caller():
    """The whole reason this is a contextvar. If it leaked, one agent's window
    grant would be reusable by another — the exact bug being fixed upstream."""
    seen = {}

    async def _request(name, delay):
        await caller_context.capture({"x-aw-caller-session-id": name})
        await asyncio.sleep(delay)
        seen[name] = caller_context.current().get("x-aw-caller-session-id")

    async def _both():
        await asyncio.gather(_request("agent-a", 0.02), _request("agent-b", 0.01))

    asyncio.run(_both())

    assert seen == {"agent-a": "agent-a", "agent-b": "agent-b"}


def test_upstream_headers_carry_the_caller(monkeypatch):
    from gateway.upstream import HttpUpstream

    up = HttpUpstream("secrets", {"url": "http://example/mcp"})
    _capture({"x-aw-caller-session-id": "sess-42"})

    assert up._client_headers()["x-aw-caller-session-id"] == "sess-42"


def test_a_configured_header_still_wins_over_a_forwarded_one():
    """An upstream's own Authorization is configuration, not something a caller
    gets to influence."""
    from gateway.upstream import HttpUpstream

    up = HttpUpstream("secrets", {"url": "http://example/mcp",
                                  "headers": {"x-aw-caller-session-id": "configured"}})
    _capture({"x-aw-caller-session-id": "from-the-caller"})

    assert up._client_headers()["x-aw-caller-session-id"] == "configured"


def test_the_agent_identity_is_forwarded_too():
    """Unlike the session, an agent id is the same next week — which is what a
    per-secret allowlist can name."""
    _capture({"x-aw-caller-agent": "agent:nightly-backup"})

    assert caller_context.current() == {"x-aw-caller-agent": "agent:nightly-backup"}


class _FakeRedis:
    def __init__(self, values: dict):
        self._values = values

    async def get(self, key):
        return self._values.get(key)


def test_warm_token_resolves_to_the_current_run_id(monkeypatch):
    """The whole point: a stale per-run header gets corrected by the stable
    warm token, which always points at whichever run is CURRENT."""
    fake = _FakeRedis({"warm_token:tok-1:run_id":
                       '{"run_id": "run-current", "notion_task_id": "", "source_device": ""}'})

    async def fake_get_warm_redis():
        return fake
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    _capture({
        "x-aw-caller-run-id": "run-stale-turn-1",
        "x-aw-warm-token": "tok-1",
    })

    assert caller_context.current()["x-aw-caller-run-id"] == "run-current"


def test_warm_token_falls_back_to_bare_string_value(monkeypatch):
    """set_warm_token_run predates the JSON-blob format for some still-live
    keys (TTL up to 24h) — a bare run_id string must still resolve."""
    fake = _FakeRedis({"warm_token:tok-legacy:run_id": "run-bare"})

    async def fake_get_warm_redis():
        return fake
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    _capture({
        "x-aw-caller-run-id": "run-stale",
        "x-aw-warm-token": "tok-legacy",
    })

    assert caller_context.current()["x-aw-caller-run-id"] == "run-bare"


def test_unmapped_warm_token_falls_back_to_the_raw_header(monkeypatch):
    fake = _FakeRedis({})

    async def fake_get_warm_redis():
        return fake
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    _capture({
        "x-aw-caller-run-id": "run-ephemeral-and-correct",
        "x-aw-warm-token": "tok-unknown",
    })

    assert caller_context.current()["x-aw-caller-run-id"] == "run-ephemeral-and-correct"


def test_redis_down_falls_back_to_the_raw_header_not_an_error(monkeypatch):
    async def fake_get_warm_redis():
        return None
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    _capture({
        "x-aw-caller-run-id": "run-ephemeral-and-correct",
        "x-aw-warm-token": "tok-1",
    })

    assert caller_context.current()["x-aw-caller-run-id"] == "run-ephemeral-and-correct"


def test_no_warm_token_leaves_the_raw_header_untouched(monkeypatch):
    """The common case today (no caller has been updated to send the token
    yet) must be byte-for-byte identical to pre-warm-token behavior."""
    async def fail_if_called():
        raise AssertionError("Redis should never be touched with no warm token header")
    monkeypatch.setattr(caller_context, "_get_warm_redis", fail_if_called)

    _capture({"x-aw-caller-run-id": "run-abc"})

    assert caller_context.current() == {"x-aw-caller-run-id": "run-abc"}
