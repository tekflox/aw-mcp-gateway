"""Forwarding who the caller is, without forwarding anything else.

An upstream that scopes a grant to a caller (aw-app-secrets does) sees the
gateway, not the agent, unless this works. The risk on the other side is a
gateway that cheerfully relays whatever a caller sets — so the allowlist gets
as much attention here as the happy path.
"""
from __future__ import annotations

import asyncio

from gateway import caller_context


def test_the_session_header_is_forwarded():
    caller_context.capture({"x-aw-caller-session-id": "sess-42"})

    assert caller_context.current() == {"x-aw-caller-session-id": "sess-42"}


def test_nothing_else_is_forwarded():
    """Passthrough would let a caller set Authorization on somebody else's
    upstream. Only the allowlist travels."""
    caller_context.capture({
        "x-aw-caller-session-id": "sess-42",
        "authorization": "Bearer someone-elses-token",
        "cookie": "aw_id_jwt=...",
        "x-api-key": "secret",
    })

    assert list(caller_context.current()) == ["x-aw-caller-session-id"]


def test_absent_headers_are_absent_not_empty():
    """An empty string would still look like an identity to an upstream, and
    every anonymous caller would share it."""
    caller_context.capture({"x-aw-caller-session-id": ""})

    assert caller_context.current() == {}


def test_a_long_value_is_bounded():
    caller_context.capture({"x-aw-caller-session-id": "x" * 5000})

    assert len(caller_context.current()["x-aw-caller-session-id"]) == 256


def test_concurrent_requests_do_not_see_each_others_caller():
    """The whole reason this is a contextvar. If it leaked, one agent's window
    grant would be reusable by another — the exact bug being fixed upstream."""
    seen = {}

    async def _request(name, delay):
        caller_context.capture({"x-aw-caller-session-id": name})
        await asyncio.sleep(delay)
        seen[name] = caller_context.current().get("x-aw-caller-session-id")

    async def _both():
        await asyncio.gather(_request("agent-a", 0.02), _request("agent-b", 0.01))

    asyncio.run(_both())

    assert seen == {"agent-a": "agent-a", "agent-b": "agent-b"}


def test_upstream_headers_carry_the_caller(monkeypatch):
    from gateway.upstream import HttpUpstream

    up = HttpUpstream("secrets", {"url": "http://example/mcp"})
    caller_context.capture({"x-aw-caller-session-id": "sess-42"})

    assert up._client_headers()["x-aw-caller-session-id"] == "sess-42"


def test_a_configured_header_still_wins_over_a_forwarded_one():
    """An upstream's own Authorization is configuration, not something a caller
    gets to influence."""
    from gateway.upstream import HttpUpstream

    up = HttpUpstream("secrets", {"url": "http://example/mcp",
                                  "headers": {"x-aw-caller-session-id": "configured"}})
    caller_context.capture({"x-aw-caller-session-id": "from-the-caller"})

    assert up._client_headers()["x-aw-caller-session-id"] == "configured"
