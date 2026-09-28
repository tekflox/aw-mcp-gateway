"""A stdio upstream is one persistent child shared by every caller — unlike
HttpUpstream, it has no per-call HTTP request to carry caller identity on.
agents-platform's own tools (mark_as_planned/mark_flow_done/ask_human/
register_callback) read a ``_gateway_caller_run_id`` key out of their tool
arguments to know which run is calling them; this is the stdio half of
forwarding that, mirroring what HttpUpstream already does via headers.

Gated to the ``agents_platform`` upstream role (see
``config_gateway.policy_upstreams``) — anything else is a third-party stdio
child (``notion``'s ``npx @notionhq/notion-mcp-server``, ``playwright``) that
never asked for this field and, for ``notion`` specifically, 400s on it
because the Notion API does strict schema validation
(degraded:mcp-gateway-notion-caller-id-leak).
"""
from __future__ import annotations

from gateway import caller_context
from gateway.upstream import Upstream

#: Default "agents_platform" role member (see
#: config_gateway.DEFAULT_POLICY_UPSTREAMS) — the only stdio upstream name
#: that should ever receive the injected field in these tests.
AGENTS_PLATFORM_UPSTREAM = "agents-platform-runners"


async def test_call_tool_injects_caller_run_id_into_arguments(monkeypatch):
    up = Upstream(AGENTS_PLATFORM_UPSTREAM, {"command": "true"})

    async def fake_ensure_alive():
        pass
    monkeypatch.setattr(up, "_ensure_alive", fake_ensure_alive)

    written: dict = {}

    async def fake_write(msg):
        written.update(msg)
        fut = up._pending.pop(msg["id"])
        fut.set_result({"jsonrpc": "2.0", "id": msg["id"],
                        "result": {"content": [], "isError": False}})
    monkeypatch.setattr(up, "_write", fake_write)

    await caller_context.capture({"x-aw-caller-run-id": "run-abc"})
    try:
        await up.call_tool("some_tool", {"foo": "bar"}, req_id=1)
    finally:
        await caller_context.capture({})

    sent_args = written["params"]["arguments"]
    assert sent_args["_gateway_caller_run_id"] == "run-abc"
    assert sent_args["foo"] == "bar"


async def test_call_tool_does_not_inject_without_a_caller(monkeypatch):
    up = Upstream(AGENTS_PLATFORM_UPSTREAM, {"command": "true"})

    async def fake_ensure_alive():
        pass
    monkeypatch.setattr(up, "_ensure_alive", fake_ensure_alive)

    written: dict = {}

    async def fake_write(msg):
        written.update(msg)
        fut = up._pending.pop(msg["id"])
        fut.set_result({"jsonrpc": "2.0", "id": msg["id"],
                        "result": {"content": [], "isError": False}})
    monkeypatch.setattr(up, "_write", fake_write)

    await caller_context.capture({})
    await up.call_tool("some_tool", {"foo": "bar"}, req_id=2)

    assert written["params"]["arguments"] == {"foo": "bar"}


async def test_call_tool_does_not_override_an_explicit_caller_run_id(monkeypatch):
    up = Upstream(AGENTS_PLATFORM_UPSTREAM, {"command": "true"})

    async def fake_ensure_alive():
        pass
    monkeypatch.setattr(up, "_ensure_alive", fake_ensure_alive)

    written: dict = {}

    async def fake_write(msg):
        written.update(msg)
        fut = up._pending.pop(msg["id"])
        fut.set_result({"jsonrpc": "2.0", "id": msg["id"],
                        "result": {"content": [], "isError": False}})
    monkeypatch.setattr(up, "_write", fake_write)

    await caller_context.capture({"x-aw-caller-run-id": "run-abc"})
    try:
        await up.call_tool("some_tool", {"_gateway_caller_run_id": "explicit"}, req_id=3)
    finally:
        await caller_context.capture({})

    assert written["params"]["arguments"]["_gateway_caller_run_id"] == "explicit"


async def test_call_tool_does_not_inject_for_a_non_agents_platform_upstream(monkeypatch):
    """Reproduces today's bug (degraded:mcp-gateway-notion-caller-id-leak):
    the ``notion`` upstream is stdio (``npx @notionhq/notion-mcp-server``)
    like ``agents-platform-runners``, but it is a third-party passthrough
    that does strict schema validation and 400s on an unexpected
    ``_gateway_caller_run_id`` field — see the card's repro:
    ``body._gateway_caller_run_id should be not present``. Before the fix
    this injected the field into every ``notion`` tool call unconditionally,
    same as it did for ``agents-platform-runners``; after the fix, only
    upstreams in the ``agents_platform`` role (see
    ``config_gateway.policy_upstreams``) receive it."""
    up = Upstream("notion", {"command": "npx", "args": ["-y", "@notionhq/notion-mcp-server"]})

    async def fake_ensure_alive():
        pass
    monkeypatch.setattr(up, "_ensure_alive", fake_ensure_alive)

    written: dict = {}

    async def fake_write(msg):
        written.update(msg)
        fut = up._pending.pop(msg["id"])
        fut.set_result({"jsonrpc": "2.0", "id": msg["id"],
                        "result": {"content": [], "isError": False}})
    monkeypatch.setattr(up, "_write", fake_write)

    await caller_context.capture({"x-aw-caller-run-id": "0b9b0d2c-415d-4b15-9272-b338e0aff720"})
    try:
        await up.call_tool("API-post-search", {"query": "PhD"}, req_id=4)
    finally:
        await caller_context.capture({})

    assert written["params"]["arguments"] == {"query": "PhD"}
