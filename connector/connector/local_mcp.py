"""Spawns and talks to the one local stdio MCP server this connector wraps.

Same reader-loop/Future-dispatch design as back/gateway/upstream.py's
``Upstream`` (this project's own local-child pattern) — duplicated here
rather than imported because connector/ and back/ are independently
deployable (different container, possibly different host/network entirely),
not two modules of one package.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

log = logging.getLogger("aw-mcp-stdio-wrapper")

PROTOCOL = "2024-11-05"


class LocalMcp:
    def __init__(self, command: str, args: list[str], env: dict | None = None):
        self.command = command
        self.args = args
        self.env_extra = env or {}
        self.proc: asyncio.subprocess.Process | None = None
        self._pending: dict[str | int, asyncio.Future] = {}
        self._reader_task: asyncio.Task | None = None
        self.tools: list[dict] = []

    async def start(self) -> None:
        env = dict(os.environ)
        env.update(self.env_extra)
        env.setdefault("PYTHONUNBUFFERED", "1")
        self.proc = await asyncio.create_subprocess_exec(
            self.command, *self.args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            env=env,
            limit=32 * 1024 * 1024,
        )
        await self._handshake()
        self._reader_task = asyncio.get_event_loop().create_task(self._reader_loop())
        log.info("local MCP spawned (pid %s), %d tools", self.proc.pid, len(self.tools))

    async def _handshake(self) -> None:
        await self._write({"jsonrpc": "2.0", "id": "init", "method": "initialize",
                           "params": {"protocolVersion": PROTOCOL, "capabilities": {},
                                      "clientInfo": {"name": "aw-mcp-stdio-wrapper", "version": "1.0.0"}}})
        await self._read_direct()
        await self._write({"jsonrpc": "2.0", "method": "notifications/initialized"})
        await self._write({"jsonrpc": "2.0", "id": "tools", "method": "tools/list"})
        listed = await self._read_direct()
        self.tools = (listed or {}).get("result", {}).get("tools", [])

    async def _reader_loop(self) -> None:
        assert self.proc and self.proc.stdout
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                for fut in list(self._pending.values()):
                    if not fut.done():
                        fut.set_result(None)
                self._pending.clear()
                return
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line.decode())
            except json.JSONDecodeError:
                continue
            fut = self._pending.pop(msg.get("id"), None)
            if fut and not fut.done():
                fut.set_result(msg)

    async def _write(self, msg: dict) -> None:
        assert self.proc and self.proc.stdin
        self.proc.stdin.write((json.dumps(msg) + "\n").encode())
        await self.proc.stdin.drain()

    async def _read_direct(self) -> dict | None:
        assert self.proc and self.proc.stdout
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                return None
            line = line.strip()
            if not line:
                continue
            try:
                return json.loads(line.decode())
            except json.JSONDecodeError:
                continue

    async def call_tool(self, tool: str, arguments: dict, req_id) -> dict:
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending[req_id] = fut
        await self._write({"jsonrpc": "2.0", "id": req_id, "method": "tools/call",
                           "params": {"name": tool, "arguments": arguments}})
        resp = await fut
        if resp is None:
            return {"jsonrpc": "2.0", "id": req_id, "result": {
                "content": [{"type": "text", "text": "local MCP process died"}],
                "isError": True}}
        return resp
