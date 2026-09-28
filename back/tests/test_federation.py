"""Gateway-to-gateway federation (deliverable A): a real leaf gateway (with
the built-in ``example-echo`` stdio upstream) running over real HTTP, and a
parent gateway configured with a ``type: gateway`` upstream pointing at it —
proving a gateway can be an upstream of another gateway end to end, plus the
cycle/depth safety checks that gate it.
"""

from __future__ import annotations

import asyncio
import contextlib

import uvicorn

from gateway import config as config_module
from gateway.server import Gateway, build_app


@contextlib.asynccontextmanager
async def running_gateway(port: int, gateway_id: str, allow=("example-echo",)):
    gw = Gateway(list(allow), gateway_id=gateway_id)
    app = build_app(gw, f"token-{gateway_id}", {})
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.02)
    try:
        yield gw, f"http://127.0.0.1:{port}", f"token-{gateway_id}"
    finally:
        server.should_exit = True
        await task


@contextlib.contextmanager
def mcp_servers_override(servers: dict):
    """Point ``config.load_mcp_servers`` at an in-memory dict for the
    duration of the block, instead of reading ``config/mcp.json`` — lets a
    test wire up a ``type: gateway`` upstream pointing at a URL only known
    at test time (a random free port)."""
    original = config_module.load_mcp_servers
    config_module.load_mcp_servers = lambda: servers
    try:
        yield
    finally:
        config_module.load_mcp_servers = original


async def test_gateway_federates_leaf_echo_server():
    async with running_gateway(19301, "leaf-a") as (leaf_gw, leaf_url, leaf_token):
        await leaf_gw.start()
        assert "example-echo" in leaf_gw.upstreams

        parent_gw = Gateway(["leaf"], gateway_id="parent-a")
        with mcp_servers_override({
            "leaf": {"type": "gateway", "url": f"{leaf_url}/mcp", "token": leaf_token},
        }):
            await parent_gw.start()

        assert "leaf" in parent_gw.upstreams
        assert "leaf__example_echo__echo" in parent_gw.routes

        resp = await parent_gw.handle({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "leaf__example_echo__echo", "arguments": {"text": "federated"}},
        })
        assert resp["result"]["content"][0]["text"] == "federated"
        assert resp["result"]["isError"] is False


async def test_federation_depth_cap_rejects_too_deep_chain():
    async with running_gateway(19302, "leaf-b") as (leaf_gw, leaf_url, leaf_token):
        await leaf_gw.start()

        parent_gw = Gateway(["leaf"], gateway_id="parent-b", max_federation_depth=1)
        with mcp_servers_override({
            "leaf": {"type": "gateway", "url": f"{leaf_url}/mcp", "token": leaf_token},
        }):
            await parent_gw.start()

        # leaf's own chain is depth 1 ([leaf-b]); federating it would make
        # parent's chain depth 2, which exceeds max_federation_depth=1.
        assert "leaf" not in parent_gw.upstreams
        assert parent_gw.federation_chain == ["parent-b"]


async def test_gateway_upstream_allowed_tools_scopes_the_pool():
    """``allowed_tools`` narrows what a ``type: gateway`` upstream publishes
    — the tool still exists on the leaf, but only names on the list get a
    route on the parent."""
    async with running_gateway(19305, "leaf-c") as (leaf_gw, leaf_url, leaf_token):
        await leaf_gw.start()

        # The leaf gateway's own aggregation already prefixes the raw
        # "echo" tool as "example_echo__echo" by the time it reaches
        # tools/list — allowed_tools matches against THAT published name,
        # not the underlying stdio server's bare tool name.
        parent_gw = Gateway(["leaf"], gateway_id="parent-c")
        with mcp_servers_override({
            "leaf": {"type": "gateway", "url": f"{leaf_url}/mcp", "token": leaf_token,
                     "allowed_tools": ["example_echo__echo"]},
        }):
            await parent_gw.start()

        assert "leaf" in parent_gw.upstreams
        assert "leaf__example_echo__echo" in parent_gw.routes
        assert {t["name"] for t in parent_gw.upstreams["leaf"].tools} == {"example_echo__echo"}


async def test_probe_federated_gateway_returns_tools_without_registering_an_upstream():
    """``Gateway.probe_federated_gateway`` (the admin UI's tool picker) must
    report exactly what a real ``type: gateway`` connect would see, but
    never touch ``upstreams``/``routes`` — it's a preview, not a save."""
    async with running_gateway(19307, "leaf-e") as (leaf_gw, leaf_url, leaf_token):
        await leaf_gw.start()

        parent_gw = Gateway([], gateway_id="parent-e")
        result = await parent_gw.probe_federated_gateway(f"{leaf_url}/mcp", leaf_token)

        assert result["gateway_id"] == "leaf-e"
        # The leaf gateway in this suite also auto-loads whatever real apps
        # happen to be scanned on the machine running the tests (see the
        # zero-isolation note on `running_gateway` above) — assert presence,
        # not the full set, the same way the allowed_tools tests above do.
        assert "example_echo__echo" in {t["name"] for t in result["tools"]}
        assert parent_gw.upstreams == {}
        assert parent_gw.routes == {}


async def test_gateway_upstream_allowed_tools_excluding_everything_fails_to_start():
    """An ``allowed_tools`` list that matches nothing on the remote must
    behave exactly like any other zero-tool upstream start (parked/failed,
    not silently "connected with nothing to call") — see
    ``Gateway._start_one``'s zero-tools guard."""
    async with running_gateway(19306, "leaf-d") as (leaf_gw, leaf_url, leaf_token):
        await leaf_gw.start()

        parent_gw = Gateway(["leaf"], gateway_id="parent-d")
        with mcp_servers_override({
            "leaf": {"type": "gateway", "url": f"{leaf_url}/mcp", "token": leaf_token,
                     "allowed_tools": ["does-not-exist-on-the-remote"]},
        }):
            await parent_gw.start()

        assert "leaf" not in parent_gw.upstreams
        assert not any(route.startswith("leaf__") for route in parent_gw.routes)


async def test_federation_cycle_is_rejected():
    """A gateway (B) that already has our own id (A) in its ancestor chain
    must be refused as an upstream — federating it back in would close a
    loop A -> B -> A."""
    async with running_gateway(19303, "gw-a") as (gw_a, url_a, token_a):
        await gw_a.start()

        # gw-b federates gw-a, so gw-b's chain becomes [gw-b, gw-a].
        async with running_gateway(19304, "gw-b", allow=()) as (gw_b, url_b, token_b):
            with mcp_servers_override({
                "upstream-a": {"type": "gateway", "url": f"{url_a}/mcp", "token": token_a},
            }):
                gw_b.allow = ["upstream-a"]
                await gw_b.start()
            assert gw_b.federation_chain == ["gw-b", "gw-a"]

            # Now gw-a tries to add gw-b as ITS upstream — gw-a's own id
            # ("gw-a") is already inside gw-b's chain, so this must fail.
            with mcp_servers_override({
                "upstream-b": {"type": "gateway", "url": f"{url_b}/mcp", "token": token_b},
            }):
                gw_a.allow = ["upstream-b"]
                await gw_a.start()

            assert "upstream-b" not in gw_a.upstreams
