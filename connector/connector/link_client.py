"""Dials the gateway's ``/link`` WebSocket, registers this app's tools, and
forwards any ``tools/call`` the gateway routes back down to the local stdio
MCP (see ``local_mcp.LocalMcp``).

STATUS: skeleton matching the gateway side's current stub (back/gateway/
remote_upstream.py) — a placeholder bearer-token check, not the final
awlk_<id16>_<secret32> scheme. Reconnects with exponential backoff on any
disconnect; re-registers from scratch each time (no session resume yet).
"""

from __future__ import annotations

import asyncio
import json
import logging

import websockets

from .local_mcp import LocalMcp

log = logging.getLogger("aw-mcp-stdio-wrapper")

MAX_BACKOFF = 30.0


async def run(app_name: str, workspace_name: str, gateway_url: str, token: str, local: LocalMcp) -> None:
    """Runs forever: connect, register, serve tool calls, reconnect on drop."""
    backoff = 1.0
    url = f"{gateway_url}?token={token}"
    while True:
        try:
            async with websockets.connect(url) as ws:
                log.info("connected to gateway at %s", gateway_url)
                await ws.send(json.dumps({
                    "type": "register",
                    "app_name": app_name,
                    "workspace_name": workspace_name,
                    "tools": local.tools,
                }))
                ack = json.loads(await ws.recv())
                if ack.get("type") != "registered":
                    log.error("registration rejected: %s", ack)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                log.info("registered as '%s' (%d tools)", app_name, len(local.tools))
                backoff = 1.0  # reset after a clean connect

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("method") != "tools/call":
                        continue
                    params = msg.get("params") or {}
                    result = await local.call_tool(
                        params.get("name", ""), params.get("arguments") or {}, msg.get("id"))
                    await ws.send(json.dumps(result))
        except (websockets.ConnectionClosed, OSError) as exc:
            log.warning("link connection lost (%s) — retrying in %.0fs", exc, backoff)
        except Exception:
            log.exception("unexpected error in link client — retrying in %.0fs", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
