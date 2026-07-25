"""RemoteUpstream + the ``/link`` reverse-registration endpoint.

STATUS: functional skeleton, not the final design. The full scheme (closed by
the architect 2026-07-25, see the ``project_aw_apps_distribution_mcp_wrapper``
design memory) is:

* Opaque bearer token ``awlk_<id16>_<secret32>``, hashed (SHA-256) and stored
  in the *user's own* Postgres (data plane) — minted from a "Hosts & Apps" UI,
  scoped to an app/host via globs, revocable instantly.
* A unified "host-link" WS transport carrying MCP in-band (this same
  register/tools-call/tools-result shape) alongside other per-host channels
  (agent coordination, byte-streams) — this file only implements the MCP
  channel in isolation.
* Tool names published as ``<app>__<tool>`` with a uniqueness-enforced app
  name (collisions get suffixed, e.g. "Browser 1"/"Browser 2") — not done
  here; this stub just uses whatever ``app_name`` the connector registers
  with the first time.

What IS real and working here: a connector can dial ``/link``, send a
``register`` message with its ``app_name``, ``token``, and its own
``tools/list`` result, and the gateway will publish those tools (namespaced
``{app_name}__{tool}``) and route ``tools/call`` back down the same live
WebSocket, matching a Future to the JSON-RPC id — same dispatch pattern as
``upstream.Upstream``'s local stdio reader loop.

TODO (tracked by the reverse-registration card in the apps-distribution
design): real token minting/hashing/storage, per-app/host scope enforcement,
app-name collision handling, reconnect-safe re-registration semantics beyond
"last register wins".
"""

from __future__ import annotations

import asyncio
import json
import logging
import time

from fastapi import WebSocket, WebSocketDisconnect

log = logging.getLogger("aw-mcp-gateway")


class RemoteUpstream:
    """One connector's live WebSocket session, providing the same
    ``call_tool(tool, arguments, req_id) -> dict`` interface as the local
    ``Upstream``/``HttpUpstream`` classes so ``Gateway`` can route to it
    identically regardless of transport."""

    def __init__(self, app_name: str, websocket: WebSocket, tools: list[dict]):
        self.app_name = app_name
        self.websocket = websocket
        self.tools = tools
        self._pending: dict[str, asyncio.Future] = {}
        self.connected_at = time.time()

    async def call_tool(self, tool: str, arguments: dict, req_id) -> dict:
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        key = str(req_id)
        self._pending[key] = fut
        try:
            await self.websocket.send_json({
                "jsonrpc": "2.0", "id": req_id, "method": "tools/call",
                "params": {"name": tool, "arguments": arguments},
            })
        except Exception as exc:
            self._pending.pop(key, None)
            return {"jsonrpc": "2.0", "id": req_id, "result": {
                "content": [{"type": "text",
                             "text": f"remote upstream '{self.app_name}' send failed: {exc}"}],
                "isError": True}}
        try:
            return await asyncio.wait_for(fut, timeout=120)
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


def _check_link_token(token: str | None, expected: str | None) -> bool:
    """Pre-accept auth check. TODO: replace with the real
    ``awlk_<id16>_<secret32>`` hash lookup — this is a placeholder equality
    check against a single shared token in config/gateway.json."""
    if not expected:
        return False
    return token == expected


async def link_endpoint(websocket: WebSocket, gateway: "Gateway", link_token: str | None):  # noqa: F821
    """WS handler for ``/link``. A connector dials in, sends one ``register``
    message, then the connection stays open for ``tools/call``/result
    exchange until it disconnects (at which point its tools are withdrawn)."""
    await websocket.accept()
    token = websocket.query_params.get("token") or websocket.headers.get("x-aw-link-token")
    if not _check_link_token(token, link_token):
        await websocket.close(code=4401, reason="invalid or missing link token")
        return

    remote: RemoteUpstream | None = None
    try:
        raw = await websocket.receive_text()
        msg = json.loads(raw)
        if msg.get("type") != "register":
            await websocket.close(code=4400, reason="expected a 'register' message first")
            return
        app_name = msg.get("app_name")
        tools = msg.get("tools") or []
        if not app_name:
            await websocket.close(code=4400, reason="register message missing app_name")
            return

        remote = RemoteUpstream(app_name, websocket, tools)
        gateway.register_remote(remote)
        log.info("remote upstream registered: %s (%d tools)", app_name, len(tools))
        await websocket.send_json({"type": "registered", "app_name": app_name})

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
            log.info("remote upstream disconnected: %s", remote.app_name)
