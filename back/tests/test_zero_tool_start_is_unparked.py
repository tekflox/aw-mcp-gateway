"""resilience:gateway-zero-tool-start-is-unparked-classe-b: an upstream that
(re)starts successfully but publishes ZERO tools — e.g. because its child's
``tools/list`` handler answered with a JSON-RPC error while a dependency
(AP-MT) was down, and ``Upstream._handshake`` flattened that error into an
empty list — must not be treated as a healthy, permanent ``unchanged``
upstream. It should be parked (or at least re-attempted) like any other
failed start, and recover once the dependency comes back.

Falsification test from the card, ported to pytest: simulate "upstream
process is fine, but its tools/list answers with an error" via
``gateway.examples.flaky_tools_list_server`` (FLAKY_MODE=error/healthy) and
confirm the gateway never re-evaluates it on subsequent reload()s.
"""

from __future__ import annotations

import contextlib

from gateway import config as config_module
from gateway.server import Gateway

ERROR_SPEC = {
    "type": "stdio", "enabled": True,
    "command": "python3", "args": ["-m", "gateway.examples.flaky_tools_list_server"],
    "env": {"FLAKY_MODE": "error"},
}
HEALTHY_SPEC = {
    "type": "stdio", "enabled": True,
    "command": "python3", "args": ["-m", "gateway.examples.flaky_tools_list_server"],
    "env": {"FLAKY_MODE": "healthy"},
}


@contextlib.contextmanager
def _servers(servers: dict):
    original = config_module.load_mcp_servers
    config_module.load_mcp_servers = lambda: servers
    try:
        yield
    finally:
        config_module.load_mcp_servers = original


async def test_start_with_zero_tools_is_not_silently_healthy():
    """Boot-time start() against a dependency that's down: up.start() must
    not return success with an empty tool list.

    Nothing was ever live for this name, so — same as any other "added"
    upstream that fails — there's no prior routes to restore/park; it's
    simply never registered. That's enough on its own: the next reload()
    sees the name in neither self.upstreams nor self.unavailable, so it
    lands back in the `added` bucket and gets a fresh start attempt,
    instead of settling into `unchanged` with 0 tools forever."""
    gw = Gateway(["svc"])
    with _servers({"svc": ERROR_SPEC}):
        await gw.start()

    # Before the fix this passed with "svc" in gw.upstreams and 0 tools —
    # a live upstream serving nothing, invisible to doctor and to _park.
    assert "svc" not in gw.upstreams, (
        "upstream started with zero tools and was accepted as healthy")
    assert "svc" not in gw.unavailable  # never had routes — nothing to park

    with _servers({"svc": HEALTHY_SPEC}):
        result = await gw.reload()
    assert "svc" in result["added"], "must retry as a fresh start, not stay unregistered forever"
    assert "svc" in gw.upstreams and gw.upstreams["svc"].tools


async def test_zero_tool_upstream_is_retried_not_stuck_unchanged():
    """The real bug: an upstream that WAS healthy, then restarts (e.g. a
    reload triggered while its dependency is down) and comes back with zero
    tools, must not settle into `unchanged` forever."""
    gw = Gateway(["svc"])
    with _servers({"svc": HEALTHY_SPEC}):
        await gw.start()
    assert "svc__ping" in gw.routes

    # Force a restart while the dependency is down — same technique the
    # card's falsification recipe uses (change the spec to force a
    # (re)start attempt), landing in reload()'s `changed` bucket.
    with _servers({"svc": ERROR_SPEC}):
        result = await gw.reload()

    assert "svc" in gw.unavailable, "zero-tool restart must be parked"
    assert result["failed"] and result["failed"][0]["name"] == "svc"
    # Routes stay published (existing park behavior) — not "Unknown tool".
    assert "svc__ping" in gw.routes

    # Next reload with the SAME (still-erroring) spec: must be retried via
    # the parked_retry path, not fall into `unchanged`.
    with _servers({"svc": ERROR_SPEC}):
        result2 = await gw.reload()
    assert "svc" not in result2["unchanged"], (
        "a permanently-zero-tool upstream must never be classified unchanged")
    assert "svc" in gw.unavailable

    # Dependency recovers: next reload must self-heal.
    with _servers({"svc": HEALTHY_SPEC}):
        result3 = await gw.reload()
    assert "svc" in gw.upstreams
    assert "svc" not in gw.unavailable
    assert gw.upstreams["svc"].tools
