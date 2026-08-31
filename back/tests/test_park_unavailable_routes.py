"""Y.1 (resilience:gateway-park-routes-for-unavailable-upstream): an upstream
that fails to (re)start during Gateway.reload() must not vanish from
tools/list. Its routes get PARKED — kept published, the upstream marked
`unavailable` — instead of dropped, and tools/call answers with a retryable
error instead of "Unknown tool". This is what converts Classe B (a stuck,
call-surviving failure) into Classe A (an in-flight failure retry can fix).
"""

from __future__ import annotations

import contextlib

from gateway import config as config_module
from gateway.server import Gateway

GOOD_SPEC = {
    "type": "stdio", "enabled": True,
    "command": "python3", "args": ["-m", "gateway.examples.echo_server"],
}
BAD_SPEC = {
    "type": "stdio", "enabled": True,
    "command": "python3-does-not-exist-anywhere",
    "args": ["-m", "gateway.examples.echo_server"],
}


@contextlib.contextmanager
def _servers(servers: dict):
    """Point ``config.load_mcp_servers`` at an in-memory dict for the
    duration of the block — same technique test_federation.py's
    mcp_servers_override uses, duplicated locally to avoid a cross-file
    import for one helper."""
    original = config_module.load_mcp_servers
    config_module.load_mcp_servers = lambda: servers
    try:
        yield
    finally:
        config_module.load_mcp_servers = original


async def test_failed_restart_parks_routes_instead_of_dropping_them():
    gw = Gateway(["svc"])
    with _servers({"svc": GOOD_SPEC}):
        await gw.start()
    assert "svc" in gw.upstreams
    assert "svc__echo" in gw.routes

    with _servers({"svc": BAD_SPEC}):
        result = await gw.reload()

    # Not live anymore, but PARKED — not dropped.
    assert "svc" not in gw.upstreams
    assert "svc" in gw.unavailable
    assert result["parked"] == ["svc"]
    assert result["failed"] and result["failed"][0]["name"] == "svc"

    # tools/list still lists it — a session already connected doesn't lose
    # the tool from its namespace mid-run.
    listed = await gw.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = [t["name"] for t in listed["result"]["tools"]]
    assert "svc__echo" in names

    # tools/call gets a RETRYABLE isError result, not "Unknown tool" — an
    # LLM reading "Unknown tool" concludes the tool was removed and gives up.
    resp = await gw.handle({
        "jsonrpc": "2.0", "id": 2, "method": "tools/call",
        "params": {"name": "svc__echo", "arguments": {"text": "hi"}},
    })
    assert "error" not in resp
    assert resp["result"]["isError"] is True
    text = resp["result"]["content"][0]["text"]
    assert "Unknown tool" not in text
    assert "temporarily unavailable" in text
    assert "svc" in text


async def test_parked_upstream_recovers_on_next_reload():
    gw = Gateway(["svc"])
    with _servers({"svc": GOOD_SPEC}):
        await gw.start()
    with _servers({"svc": BAD_SPEC}):
        await gw.reload()
    assert "svc" in gw.unavailable

    with _servers({"svc": GOOD_SPEC}):
        result = await gw.reload()

    assert "svc" in gw.upstreams
    assert "svc" not in gw.unavailable
    assert result["parked"] == []

    resp = await gw.handle({
        "jsonrpc": "2.0", "id": 3, "method": "tools/call",
        "params": {"name": "svc__echo", "arguments": {"text": "back"}},
    })
    assert resp["result"]["isError"] is False
    assert resp["result"]["content"][0]["text"] == "back"


async def test_removed_from_config_drops_immediately_never_parked():
    gw = Gateway(["svc"])
    with _servers({"svc": GOOD_SPEC}):
        await gw.start()
    assert "svc__echo" in gw.routes

    with _servers({}):
        result = await gw.reload()

    assert "svc" not in gw.upstreams
    assert "svc" not in gw.unavailable  # a real removal is never parked
    assert result["parked"] == []
    assert "svc__echo" not in gw.routes

    listed = await gw.handle({"jsonrpc": "2.0", "id": 4, "method": "tools/list"})
    assert not any(t["name"] == "svc__echo" for t in listed["result"]["tools"])


async def test_parked_route_expires_after_ttl():
    gw = Gateway(["svc"])
    with _servers({"svc": GOOD_SPEC}):
        await gw.start()
    with _servers({"svc": BAD_SPEC}):
        await gw.reload()
    assert "svc" in gw.unavailable
    assert "svc__echo" in gw.routes

    # Force the parked entry to look older than PARK_TTL_SECONDS.
    gw.unavailable["svc"]["parked_at"] -= 10_000

    with _servers({"svc": BAD_SPEC}):  # still failing — this is expiry, not recovery
        result = await gw.reload()

    assert "svc" not in gw.unavailable  # gave up parking...
    assert "svc__echo" not in gw.routes  # ...so the phantom route is gone
    assert result["failed"] and result["failed"][0]["name"] == "svc"  # but still retried
