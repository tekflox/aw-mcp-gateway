"""A stdio upstream is one persistent child shared by every caller — unlike
HttpUpstream, it has no per-call HTTP request to carry caller identity on.
agents-platform's own tools (mark_as_planned/mark_flow_done/ask_human/
register_callback) read a ``_gateway_caller_run_id`` key out of their tool
arguments to know which run is calling them; this is the stdio half of
forwarding that, mirroring what HttpUpstream already does via headers.
"""
from __future__ import annotations

from gateway import caller_context
from gateway.upstream import Upstream


async def test_call_tool_injects_caller_run_id_into_arguments(monkeypatch):
    up = Upstream("dummy", {"command": "true"})

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
    up = Upstream("dummy", {"command": "true"})

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
    up = Upstream("dummy", {"command": "true"})

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
