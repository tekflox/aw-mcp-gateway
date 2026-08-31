"""Two concurrent callers must never share a slot in an upstream's pending map.

The stdio ``Upstream`` child and the ``/link`` ``RemoteUpstream`` WebSocket are
each ONE connection shared by every session in the workspace, multiplexed by
JSON-RPC id. That id used to be the caller's own, taken verbatim from the
inbound request in ``Gateway.handle()`` — and independent MCP clients number
their requests with their own per-connection counters, so two sessions picking
``id=1`` at the same time was routine. The second registration evicted the
first's future, the child's first reply resolved whichever future currently sat
in that slot, and session B got session A's result with no error raised
anywhere while session A's call hung forever ("job cruzado", 2026-08-31 — it
silently corrupted three production verification calls during the Postgres
cutover before anyone noticed).

These drive the real dispatch paths (``Upstream._reader_loop``,
``RemoteUpstream.resolve``) unmodified; only the transport I/O is stubbed.
"""
from __future__ import annotations

import asyncio
import json

import pytest

from gateway.remote_upstream import RemoteUpstream
from gateway.upstream import Upstream


class _FakeProc:
    """Enough of ``asyncio.subprocess.Process`` for ``_reader_loop``."""

    returncode = None

    def __init__(self, stdout: asyncio.StreamReader):
        self.stdout = stdout
        self.pid = 4242


async def _wait_for_pending(pending: dict, count: int) -> None:
    """Yield until ``count`` calls have registered, rather than guessing how
    many event-loop turns ``call_tool`` needs to get there."""
    for _ in range(200):
        if len(pending) >= count:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"only {len(pending)} of {count} calls registered: {pending}")


def _reply(wire_id, job_id: str) -> dict:
    """What the child echoes back: the id IT was sent, and its own result."""
    return {"jsonrpc": "2.0", "id": wire_id, "result": {
        "content": [{"type": "text", "text": json.dumps({"job_id": job_id, "status": "exited"})}],
        "isError": False}}


def _job_of(resp: dict) -> str:
    return json.loads(resp["result"]["content"][0]["text"])["job_id"]


@pytest.fixture
async def stdio_upstream(monkeypatch):
    """A real ``Upstream`` with a real ``_reader_loop`` over a fake child.

    ``_write`` only records the frame — nothing replies until the test feeds
    the child's stdout, which is what lets BOTH callers be in flight at once
    (the shape that produced the bug).
    """
    up = Upstream("aw_app_remote_host_cli", {"command": "true"})

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


async def test_colliding_caller_ids_do_not_cross_results(stdio_upstream):
    up, sent, stdout = stdio_upstream

    # Two unrelated sessions, each with its own id counter starting at 1.
    task_a = asyncio.create_task(up.call_tool(
        "remote_host_exec_wait", {"job_id": "aaaa1111"}, req_id=1))
    task_b = asyncio.create_task(up.call_tool(
        "remote_host_exec_wait", {"job_id": "bbbb2222"}, req_id=1))
    await _wait_for_pending(up._pending, 2)

    # The whole bug in one assertion: colliding caller ids must still occupy
    # two distinct slots. Before the fix this was 1 — B had evicted A.
    assert len(up._pending) == 2, f"callers collided on one pending key: {up._pending}"
    assert len({m["id"] for m in sent}) == 2, "the child was sent two frames with the same id"

    # The real child reads stdin line by line and answers in that same order,
    # echoing the id it was given.
    for frame in sent:
        job_id = frame["params"]["arguments"]["job_id"]
        stdout.feed_data((json.dumps(_reply(frame["id"], job_id)) + "\n").encode())

    resp_a = await asyncio.wait_for(task_a, timeout=2)
    resp_b = await asyncio.wait_for(task_b, timeout=2)

    assert _job_of(resp_a) == "aaaa1111", "session A got another session's job result"
    assert _job_of(resp_b) == "bbbb2222", "session B got another session's job result"
    # Each caller correlates on the id IT sent, so that id has to come back.
    assert resp_a["id"] == 1 and resp_b["id"] == 1
    assert up._pending == {}


async def test_out_of_order_replies_still_reach_the_right_caller(stdio_upstream):
    """An upstream that answers a slow call last must not shift results by one."""
    up, sent, stdout = stdio_upstream

    task_a = asyncio.create_task(up.call_tool("t", {"job_id": "slow"}, req_id=7))
    task_b = asyncio.create_task(up.call_tool("t", {"job_id": "fast"}, req_id=7))
    await _wait_for_pending(up._pending, 2)

    for frame in reversed(sent):
        job_id = frame["params"]["arguments"]["job_id"]
        stdout.feed_data((json.dumps(_reply(frame["id"], job_id)) + "\n").encode())

    assert _job_of(await asyncio.wait_for(task_a, timeout=2)) == "slow"
    assert _job_of(await asyncio.wait_for(task_b, timeout=2)) == "fast"


class _FakeWebSocket:
    def __init__(self):
        self.sent: list[dict] = []

    async def send_json(self, msg):
        self.sent.append(msg)


async def test_remote_upstream_colliding_caller_ids_do_not_cross_results():
    """Same transport shape, same defect: one WebSocket, every caller on it."""
    ws = _FakeWebSocket()
    remote = RemoteUpstream("some_app", ws, tools=[], token_id="tok")

    task_a = asyncio.create_task(remote.call_tool("t", {"job_id": "aaaa1111"}, req_id=1))
    task_b = asyncio.create_task(remote.call_tool("t", {"job_id": "bbbb2222"}, req_id=1))
    await _wait_for_pending(remote._pending, 2)

    assert len(remote._pending) == 2, f"callers collided on one pending key: {remote._pending}"

    for frame in ws.sent:
        remote.resolve(_reply(frame["id"], frame["params"]["arguments"]["job_id"]))

    resp_a = await asyncio.wait_for(task_a, timeout=2)
    resp_b = await asyncio.wait_for(task_b, timeout=2)

    assert _job_of(resp_a) == "aaaa1111"
    assert _job_of(resp_b) == "bbbb2222"
    assert resp_a["id"] == 1 and resp_b["id"] == 1
    assert remote._pending == {}
