"""2026-09-03 incident: a stdio upstream that never answers its handshake
(there: `npx playwright` on its first launch in a freshly-updated
container) hung Upstream._handshake()'s bare `await self._read_direct()`
forever. Gateway.start() awaits each upstream SEQUENTIALLY, so that one
stuck child blocked every upstream after it from ever starting — the
FastAPI lifespan never finished, and the whole gateway never bound its
port ("connection refused" workspace-wide, for every one of its ~700
tools, for about 6 minutes until it got killed and happened to recover).

Falsification: a hanging upstream, followed in the same start() call by a
healthy one. Before the fix this test never completes (would time out the
whole suite). After the fix: the hanging upstream fails fast and the
healthy one — positioned AFTER it — still starts.
"""

from __future__ import annotations

import contextlib

from gateway import config as config_module
from gateway import upstream as upstream_module
from gateway.server import Gateway

HANGING_SPEC = {
    "type": "stdio", "enabled": True,
    "command": "python3", "args": ["-m", "gateway.examples.hanging_handshake_server"],
}
ECHO_SPEC = {
    "type": "stdio", "enabled": True,
    "command": "python3", "args": ["-m", "gateway.examples.echo_server"],
}


@contextlib.contextmanager
def _servers(servers: dict):
    original = config_module.load_mcp_servers
    config_module.load_mcp_servers = lambda: servers
    try:
        yield
    finally:
        config_module.load_mcp_servers = original


@contextlib.contextmanager
def _short_handshake_timeout(seconds: float):
    original = upstream_module.HANDSHAKE_TIMEOUT_SECONDS
    upstream_module.HANDSHAKE_TIMEOUT_SECONDS = seconds
    try:
        yield
    finally:
        upstream_module.HANDSHAKE_TIMEOUT_SECONDS = original


async def test_hanging_upstream_does_not_block_gateway_start():
    """The real bug: without a timeout this test hangs forever instead of
    failing. dict insertion order is start()'s iteration order, so "hangs"
    starts first and "echo" — the proof that the loop kept going — second."""
    gw = Gateway(["hangs", "echo"])
    with _short_handshake_timeout(0.3), _servers({"hangs": HANGING_SPEC, "echo": ECHO_SPEC}):
        await gw.start()

    assert "hangs" not in gw.upstreams, "a stuck handshake must not count as a healthy start"
    assert "echo" in gw.upstreams and gw.upstreams["echo"].tools, (
        "an upstream started AFTER a hung one must still start — one bad "
        "child must never block the rest of the gateway")


async def test_hanging_upstream_child_process_is_not_leaked():
    """A timed-out handshake must kill the child it spawned, not abandon it
    running with its pipes held open."""
    gw = Gateway(["hangs"])
    with _short_handshake_timeout(0.3), _servers({"hangs": HANGING_SPEC}):
        await gw.start()

    # The upstream object never made it into gw.upstreams (start() only
    # registers a successful one), so reach the spawned process the same
    # way _start_one built it: construct + start it directly and confirm
    # the timeout path terminates the child.
    up = upstream_module.Upstream("hangs", HANGING_SPEC)
    with _short_handshake_timeout(0.3):
        try:
            await up.start()
        except RuntimeError:
            pass
    assert up.proc is None, "stop() must run on handshake timeout, clearing the process handle"
