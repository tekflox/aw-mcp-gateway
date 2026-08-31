"""RemoteUpstream + the ``/link`` reverse-registration endpoint.

A connector dials ``/link``, sends a ``register`` message with its
``app_name``, ``workspace_name``, ``token``, and its own ``tools/list`` result,
and the gateway publishes those tools (namespaced
``{workspace_name}__{app_name}__{tool}`` when a workspace name is supplied, or
``{app_name}__{tool}`` for old connectors) and routes
``tools/call`` back down the same live WebSocket, matching a Future to the
JSON-RPC id — same dispatch pattern as ``upstream.Upstream``'s local stdio
reader loop.

Closes the reverse-registration TODOs from the original skeleton:

* **Token**: the ``token`` field is a real ``awlk_<id16>_<secret32>``
  opaque token, verified against a ``TokenStore`` (SHA-256 hash lookup) —
  see ``token_store.py`` for the storage abstraction and the Postgres TODO.
* **Scope**: a token's ``scopes`` (glob allowlist over ``app_name:tool``)
  filters which of the connector's declared tools actually get published —
  an empty result after filtering is a hard reject, not a silent no-op.
* **Collision**: app-name uniqueness is enforced by ``Gateway`` (see
  ``server.py``'s ``register_remote``) — this module just supplies the
  token's stable id as the reconnect-safe identity key.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time

from fastapi import WebSocket, WebSocketDisconnect

from .token_store import TokenStore
from .upstream import next_wire_id

log = logging.getLogger("aw-mcp-gateway")


def _route_segment(value: str) -> str:
    segment = re.sub(r"[^0-9A-Za-z_]+", "_", str(value).strip()).strip("_").lower()
    return segment


class RemoteUpstream:
    """One connector's live WebSocket session, providing the same
    ``call_tool(tool, arguments, req_id) -> dict`` interface as the local
    ``Upstream``/``HttpUpstream`` classes so ``Gateway`` can route to it
    identically regardless of transport."""

    def __init__(self, base_name: str, websocket: WebSocket, tools: list[dict],
                 token_id: str, workspace_name: str = ""):
        self.base_name = base_name
        self.workspace_name = workspace_name
        self.app_name = base_name  # may be renamed by Gateway.register_remote on collision
        self.token_id = token_id
        self.websocket = websocket
        self.tools = tools
        self._pending: dict[str, asyncio.Future] = {}
        self.connected_at = time.time()

    @property
    def route_name(self) -> str:
        """Namespace used in published tool names and routing table keys."""
        if not self.workspace_name:
            return self.app_name
        workspace = _route_segment(self.workspace_name)
        if not workspace:
            return self.app_name
        return f"{workspace}__{self.app_name}"

    async def call_tool(self, tool: str, arguments: dict, req_id) -> dict:
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        # One WebSocket serves every concurrent caller, so — exactly as in the
        # stdio ``Upstream`` — the pending-map key must be unique to this
        # gateway process, not the caller's own colliding JSON-RPC id.
        key = next_wire_id()
        self._pending[key] = fut
        try:
            await self.websocket.send_json({
                "jsonrpc": "2.0", "id": key, "method": "tools/call",
                "params": {"name": tool, "arguments": arguments},
            })
        except Exception as exc:
            self._pending.pop(key, None)
            return {"jsonrpc": "2.0", "id": req_id, "result": {
                "content": [{"type": "text",
                             "text": f"remote upstream '{self.app_name}' send failed: {exc}"}],
                "isError": True}}
        try:
            resp = await asyncio.wait_for(fut, timeout=120)
            # Restore the caller's own id, which correlates the reply on their side.
            resp["id"] = req_id
            return resp
        except asyncio.TimeoutError:
            self._pending.pop(key, None)
            return {"jsonrpc": "2.0", "id": req_id, "result": {
                "content": [{"type": "text",
                             "text": f"remote upstream '{self.app_name}' timed out"}],
                "isError": True}}

    def resolve(self, msg: dict) -> None:
        """Dispatch a tools/call *response* arriving from the connector."""
        key = str(msg.get("id"))
        fut = self._pending.pop(key, None)
        if fut and not fut.done():
            fut.set_result(msg)


async def link_endpoint(websocket: WebSocket, gateway: "Gateway", token_store: TokenStore):  # noqa: F821
    """WS handler for ``/link``. A connector dials in, sends one ``register``
    message, then the connection stays open for ``tools/call``/result
    exchange until it disconnects (at which point its tools are withdrawn)."""
    await websocket.accept()
    token = websocket.query_params.get("token") or websocket.headers.get("x-aw-link-token")
    link_token = token_store.verify(token or "")
    if link_token is None:
        await websocket.close(code=4401, reason="invalid, unknown, or revoked link token")
        return

    remote: RemoteUpstream | None = None
    try:
        raw = await websocket.receive_text()
        msg = json.loads(raw)
        if msg.get("type") != "register":
            await websocket.close(code=4400, reason="expected a 'register' message first")
            return
        app_name = msg.get("app_name")
        workspace_name = msg.get("workspace_name") or ""
        tools = msg.get("tools") or []
        if not app_name:
            await websocket.close(code=4400, reason="register message missing app_name")
            return

        scoped_tools = token_store.filter_scoped_tools(link_token, app_name, tools)
        if tools and not scoped_tools:
            await websocket.close(
                code=4403,
                reason=f"token scope {link_token.scopes} allows none of this app's tools")
            return
        if len(scoped_tools) < len(tools):
            dropped = {t.get("name") for t in tools} - {t.get("name") for t in scoped_tools}
            log.warning("link token %s scope dropped %d/%d tools for app '%s': %s",
                        link_token.id, len(dropped), len(tools), app_name, sorted(dropped))

        remote = RemoteUpstream(app_name, websocket, scoped_tools, link_token.id, workspace_name)
        gateway.register_remote(remote)
        log.info("remote upstream registered: %s (token=%s, %d tools)",
                 remote.route_name, link_token.id, len(scoped_tools))
        await websocket.send_json({
            "type": "registered",
            "app_name": remote.app_name,
            "workspace_name": remote.workspace_name,
            "route_name": remote.route_name,
        })

        while True:
            raw = await websocket.receive_text()
            msg = json.loads(raw)
            # Any message with an "id" that ISN'T a fresh request from us is a
            # tools/call *response* coming back from the connector.
            if "result" in msg or "error" in msg:
                remote.resolve(msg)
            # (Requests originating FROM the connector, e.g. its own
            # notifications, aren't part of this channel's contract yet.)
    except WebSocketDisconnect:
        pass
    finally:
        if remote is not None:
            gateway.unregister_remote(remote)
            log.info("remote upstream disconnected: %s", remote.route_name)
