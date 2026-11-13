"""resilience: ``Upstream.call_tool``'s stdio reply wait must be visible
while it's happening, bounded by a hard backstop well above any legitimate
long wait, and must never leak its ``_pending`` entry.

Root cause (Kanban card 4175bf3b-9510-811f-aef2-c5c10483efe5): the stdio
``Upstream`` keeps exactly ONE long-lived child process per upstream,
multiplexing every caller's tool calls over its stdin/stdout. That child
answers requests strictly sequentially, so one legitimately long-running
call (e.g. a 997s ``run_monitor_sync`` poll) silently starves every other
call queued behind it — with a bare ``resp = await fut``, there was zero
visibility anywhere (no log, no counter, nothing) until the queued calls
either timed out client-side or drained once the blocker finished.

These tests drive the real ``Upstream.call_tool`` over a fake child (same
pattern as ``test_concurrent_caller_id_collision.py``), only shrinking the
watchdog constants via monkeypatch so the slices/backstop are reached in
milliseconds instead of 30s/3900s.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging

import pytest

from gateway import metrics
from gateway.upstream import Upstream


class _FakeProc:
    """Enough of ``asyncio.subprocess.Process`` for ``_reader_loop``."""

    returncode = None

    def __init__(self, stdout: asyncio.StreamReader):
        self.stdout = stdout
        self.pid = 4242


async def _wait_for_pending(pending: dict, count: int) -> None:
    for _ in range(200):
        if len(pending) >= count:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"only {len(pending)} of {count} calls registered: {pending}")


def _reply(wire_id, text: str) -> dict:
    return {"jsonrpc": "2.0", "id": wire_id, "result": {
        "content": [{"type": "text", "text": text}], "isError": False}}


@pytest.fixture
async def stdio_upstream(monkeypatch):
    """A real ``Upstream`` with a real ``_reader_loop`` over a fake child —
    nothing replies until the test feeds the child's stdout, which is what
    lets a call sit "in flight" for as many watchdog slices as the test
    wants before (or instead of) ever resolving."""
    up = Upstream("aw_app_agents_platform_runners", {"command": "true"})

    async def fake_ensure_alive():
        pass
    monkeypatch.setattr(up, "_ensure_alive", fake_ensure_alive)

    stdout = asyncio.StreamReader()
    up.proc = _FakeProc(stdout)

    sent: list[dict] = []

    async def fake_write(msg):
        sent.append(msg)
    monkeypatch.setattr(up, "_write", fake_write)

    reader = asyncio.get_event_loop().create_task(up._reader_loop())
    up._reader_task = reader
    try:
        yield up, sent, stdout
    finally:
        reader.cancel()


def _slow_calls_total(upstream_name: str) -> float:
    return metrics.counters.total(upstream_name, "tools_call_slow")


async def test_slow_call_warns_once_per_watchdog_slice_and_bumps_the_counter(
        stdio_upstream, monkeypatch, caplog):
    """A call that is merely SLOW (not stuck) must log a WARNING with live
    elapsed time at each watchdog slice, but only bump ``tools_call_slow``
    on the FIRST such warning — the counter counts slow CALLS, not elapsed
    30s ticks."""
    up, sent, stdout = stdio_upstream
    monkeypatch.setattr("gateway.upstream.CALL_WATCHDOG_SLICE_SECONDS", 0.05)
    monkeypatch.setattr("gateway.upstream.CALL_HARD_BACKSTOP_SECONDS", 10.0)

    before = _slow_calls_total(up.name)

    with caplog.at_level(logging.INFO, logger="aw-mcp-gateway"):
        task = asyncio.create_task(up.call_tool("slow_tool", {}, req_id=1))
        await _wait_for_pending(up._pending, 1)

        # Let several watchdog slices elapse with no reply yet.
        await asyncio.sleep(0.2)

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING
                    and "still waiting" in r.message]
        assert len(warnings) >= 2, "expected multiple slice warnings while still waiting"
        assert "slow_tool" in warnings[0].message
        assert "aw_app_agents_platform_runners" in warnings[0].message

        # Exactly one slow-call hit recorded, no matter how many slices fired.
        assert _slow_calls_total(up.name) == before + 1

        # Now let the child actually answer.
        wire_id = sent[0]["id"]
        stdout.feed_data((json.dumps(_reply(wire_id, "done")) + "\n").encode())
        resp = await asyncio.wait_for(task, timeout=2)

    assert resp["result"]["isError"] is False
    assert resp["result"]["content"][0]["text"] == "done"
    assert up._pending == {}
    completed = [r for r in caplog.records if r.levelno == logging.INFO
                 and "completed after" in r.message]
    assert completed, "expected a completion INFO log"
    assert "had warned slow" in completed[-1].message


async def test_hard_backstop_returns_retryable_error_and_frees_the_pending_slot(
        stdio_upstream, monkeypatch, caplog):
    """A call that never gets a reply at all must not hang forever — past
    the hard backstop, the gateway gives up, pops its own pending entry so
    it isn't leaked, and tells the caller this is retryable, not a
    permanent failure."""
    up, sent, stdout = stdio_upstream
    monkeypatch.setattr("gateway.upstream.CALL_WATCHDOG_SLICE_SECONDS", 0.02)
    monkeypatch.setattr("gateway.upstream.CALL_HARD_BACKSTOP_SECONDS", 0.08)

    with caplog.at_level(logging.ERROR, logger="aw-mcp-gateway"):
        resp = await asyncio.wait_for(
            up.call_tool("wedged_tool", {}, req_id=9), timeout=5)

    assert resp["result"]["isError"] is True
    text = resp["result"]["content"][0]["text"]
    assert "aw_app_agents_platform_runners" in text
    assert "wedged_tool" in text
    assert "retryable" in text.lower()
    assert "not a permanent failure" in text.lower()

    # The pending slot must be freed — nothing left dangling for a reply
    # that may still arrive later to be silently discarded into.
    assert up._pending == {}

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors and "wedged_tool" in errors[0].message
    assert "hard backstop" in errors[0].message


async def test_cancellation_still_frees_the_pending_slot(stdio_upstream, monkeypatch):
    """If the caller disconnects before the child replies, the coroutine
    awaiting the reply future gets cancelled — the pending entry must be
    popped anyway (the bug this card fixes: it used to be left forever, and
    a reply that eventually did arrive was discarded into a dangling
    entry)."""
    up, sent, stdout = stdio_upstream
    # Slice long enough that the cancellation below lands mid-first-slice,
    # not after a watchdog iteration already completed on its own.
    monkeypatch.setattr("gateway.upstream.CALL_WATCHDOG_SLICE_SECONDS", 5.0)
    monkeypatch.setattr("gateway.upstream.CALL_HARD_BACKSTOP_SECONDS", 10.0)

    task = asyncio.create_task(up.call_tool("any_tool", {}, req_id=3))
    await _wait_for_pending(up._pending, 1)
    assert len(up._pending) == 1

    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2)

    assert up._pending == {}, f"pending entry leaked after cancellation: {up._pending}"

    # A reply that arrives AFTER cancellation must be a harmless no-op, not
    # an error — the id is simply gone from the map.
    stdout.feed_data((json.dumps(_reply("awg-whatever", "late")) + "\n").encode())
    await asyncio.sleep(0)
