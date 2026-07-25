"""Upstream MCP child-process/HTTP handles.

Ported from ``agentic-workspace``'s ``src/mcp/gateway.py`` (the in-repo
mcp-gateway this standalone app is replacing) — same design, stripped of
AW-internal couplings (warm-container Redis lookup, OTel/SigNoz export,
per-profile KB/presentation scoping). Those are project-specific hooks on
top of this same core, not part of the generic gateway.

Design notes (unchanged from the source):
* One persistent stdio child per upstream, multiplexed via a reader loop that
  dispatches JSON-RPC responses to per-caller ``asyncio.Future``s by id — many
  callers can be in-flight concurrently without blocking each other.
* ``HttpUpstream`` proxies to an upstream that already speaks Streamable HTTP
  (or its SSE-framed variant) instead of spawning a child.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

import httpx

from .config import BASE_DIR

log = logging.getLogger("aw-mcp-gateway")

DEFAULT_PROTOCOL = "2024-11-05"


def public_name(server: str, tool: str) -> str:
    """Namespace a tool by its server: ``aw-tasks`` + ``create_task`` ->
    ``aw_tasks__create_task``. Hyphens -> underscores so the name is a valid
    identifier for the widest set of clients."""
    return f"{server.replace('-', '_')}__{tool}"


class Upstream:
    """One persistent stdio MCP child process with multiplexed concurrent calls."""

    def __init__(self, name: str, spec: dict):
        self.name = name
        self.command: str = spec["command"]
        self.args: list[str] = spec.get("args", [])
        self.env_extra: dict = spec.get("env", {})
        self.cwd: str = spec.get("cwd", BASE_DIR)
        self.proc: asyncio.subprocess.Process | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._pending: dict[str | int, asyncio.Future] = {}
        self._reader_task: asyncio.Task | None = None
        self.tools: list[dict] = []

    async def _spawn(self) -> None:
        cmd = self.command
        if not os.path.isabs(cmd) and ("/" in cmd):
            cmd = os.path.join(BASE_DIR, cmd)
        env = dict(os.environ)
        env.update(self.env_extra)
        env.setdefault("PYTHONUNBUFFERED", "1")
        self.proc = await asyncio.create_subprocess_exec(
            cmd, *self.args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            cwd=self.cwd,
            env=env,
            limit=32 * 1024 * 1024,
        )
        log.info("spawned upstream %s (pid %s)", self.name, self.proc.pid)

    async def _ensure_alive(self) -> None:
        if self.proc is None or self.proc.returncode is not None:
            if self._reader_task and not self._reader_task.done():
                self._reader_task.cancel()
                try:
                    await self._reader_task
                except (asyncio.CancelledError, Exception):
                    pass
            self._reader_task = None
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(RuntimeError(f"upstream '{self.name}' restarted"))
            self._pending.clear()
            await self._spawn()
            await self._handshake()
            self._reader_task = asyncio.get_event_loop().create_task(
                self._reader_loop(), name=f"reader-{self.name}")

    async def _reader_loop(self) -> None:
        assert self.proc and self.proc.stdout
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                for fut in list(self._pending.values()):
                    if not fut.done():
                        fut.set_result(None)
                self._pending.clear()
                self.proc = None
                return
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line.decode())
            except json.JSONDecodeError:
                continue
            msg_id = msg.get("id")
            fut = self._pending.pop(msg_id, None)
            if fut and not fut.done():
                fut.set_result(msg)

    async def _handshake(self) -> None:
        await self._write({"jsonrpc": "2.0", "id": "init", "method": "initialize",
                           "params": {"protocolVersion": DEFAULT_PROTOCOL,
                                      "capabilities": {}, "clientInfo":
                                      {"name": "aw-mcp-gateway", "version": "1.0.0"}}})
        await self._read_direct()
        await self._write({"jsonrpc": "2.0", "method": "notifications/initialized"})
        await self._write({"jsonrpc": "2.0", "id": "tools", "method": "tools/list"})
        listed = await self._read_direct()
        self.tools = (listed or {}).get("result", {}).get("tools", [])

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

    async def start(self) -> None:
        async with self._lifecycle_lock:
            await self._ensure_alive()

    async def call_tool(self, tool: str, arguments: dict, req_id) -> dict:
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()

        async with self._lifecycle_lock:
            await self._ensure_alive()
            self._pending[req_id] = fut
            try:
                await self._write({
                    "jsonrpc": "2.0", "id": req_id, "method": "tools/call",
                    "params": {"name": tool, "arguments": arguments},
                })
            except Exception:
                self._pending.pop(req_id, None)
                raise
        resp = await fut
        if resp is None:
            self.proc = None
            return {"jsonrpc": "2.0", "id": req_id, "result": {
                "content": [{"type": "text",
                             "text": f"upstream '{self.name}' crashed or returned no "
                                     f"response; it will be respawned on the next call"}],
                "isError": True}}
        return resp


class HttpUpstream:
    """An HTTP MCP upstream — proxies JSON-RPC tool calls to a Streamable HTTP endpoint."""

    def __init__(self, name: str, spec: dict):
        self.name = name
        self.url: str = spec["url"]
        self._extra_headers: dict[str, str] = spec.get("headers", {}) or {}
        self.tools: list[dict] = []
        self._client: httpx.AsyncClient | None = None
        self._session_id: str | None = None

    def _client_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            **self._extra_headers,
        }
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        return headers

    async def _post(self, msg: dict) -> dict:
        assert self._client is not None
        resp = await self._client.post(self.url, json=msg, headers=self._client_headers())
        resp.raise_for_status()
        session_id = resp.headers.get("mcp-session-id")
        if session_id:
            self._session_id = session_id
        return self._parse_body(resp)

    @staticmethod
    def _parse_body(resp: "httpx.Response") -> dict:
        content_type = resp.headers.get("content-type", "")
        text = resp.text
        if "text/event-stream" in content_type or text.lstrip().startswith("event:"):
            data_line = None
            for line in text.splitlines():
                if line.startswith("data:"):
                    data_line = line[len("data:"):].strip()
            if data_line is not None:
                return json.loads(data_line)
        return resp.json()

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=30.0)
        init = await self._post({
            "jsonrpc": "2.0", "id": "init", "method": "initialize",
            "params": {"protocolVersion": DEFAULT_PROTOCOL,
                       "capabilities": {}, "clientInfo": {"name": "aw-mcp-gateway", "version": "1.0.0"}}
        })
        log.info("http upstream %s initialized: %s", self.name,
                 init.get("result", {}).get("serverInfo", {}).get("name", "?"))
        listed = await self._post({"jsonrpc": "2.0", "id": "tools", "method": "tools/list"})
        self.tools = listed.get("result", {}).get("tools", [])
        log.info("http upstream %s — %d tools", self.name, len(self.tools))

    async def call_tool(self, tool: str, arguments: dict, req_id) -> dict:
        try:
            resp = await self._post({
                "jsonrpc": "2.0", "id": req_id, "method": "tools/call",
                "params": {"name": tool, "arguments": arguments},
            })
            resp["id"] = req_id
            return resp
        except Exception as exc:
            return {"jsonrpc": "2.0", "id": req_id, "result": {
                "content": [{"type": "text",
                             "text": f"http upstream '{self.name}' error: {exc}"}],
                "isError": True}}
