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

from . import caller_context, metrics

from .config import BASE_DIR

log = logging.getLogger("aw-mcp-gateway")

DEFAULT_PROTOCOL = "2024-11-05"

#: Proof-gated retry (resilience:gateway-proof-gated-retry-with-counters):
#: 3 attempts total (1 original + 2 retries), 1s then 2s backoff between them.
MAX_CALL_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = (1.0, 2.0)


def _classify_call_failure(exc: Exception) -> tuple[str, bool]:
    """(error_class, proven_no_effect) for one failed ``tools/call`` attempt.

    ``proven_no_effect`` is the ONLY argument this gateway accepts for
    retrying a tool that hasn't declared ``idempotentHint`` — the
    generalization of ``runner.py``'s ``RETRYABLE_STATUS`` reasoning (a 404
    never reaches ``start_job``, so nothing was created to duplicate):

    * ``ConnectError``/``ConnectTimeout`` — the connection never opened, so
      the request never left this process.
    * A ``404``/``502`` — a router response that never reached the
      handler on the other side.

    Deliberately EXCLUDED: ``ReadTimeout`` fires *after* the request was
    sent — the write may have landed and only the response got lost, which
    proves nothing about whether the tool ran. Any other exception is
    treated the same way — no proof, no free pass.
    """
    if isinstance(exc, (httpx.ConnectError, httpx.ConnectTimeout)):
        return "upstream_error", True
    if isinstance(exc, httpx.HTTPStatusError):
        return "upstream_error", exc.response.status_code in (404, 502)
    if isinstance(exc, httpx.TimeoutException):
        return "timeout", False
    return "upstream_error", False


def public_name(server: str, tool: str) -> str:
    """Namespace a tool by its server: ``aw-tasks`` + ``create_task`` ->
    ``aw_tasks__create_task``. Hyphens -> underscores so the name is a valid
    identifier for the widest set of clients."""
    return f"{server.replace('-', '_')}__{tool}"


class Upstream:
    """One persistent stdio MCP child process with multiplexed concurrent calls."""

    def __init__(self, name: str, spec: dict):
        self.name = name
        self.spec = spec
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
        if listed and "error" in listed:
            # A JSON-RPC error response (e.g. the child's tools/list handler
            # raised — mcp.server.lowlevel.Server._handle_request turns an
            # uncaught handler exception into exactly this shape) is NOT the
            # same thing as "this upstream legitimately has no tools". Used
            # to fall straight into .get("result", {}).get("tools", [])
            # below and silently become an empty-but-"successful" start —
            # resilience:gateway-zero-tool-start-is-unparked-classe-b.
            raise RuntimeError(f"tools/list returned an error: {listed['error']}")
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

        # Mirror HttpUpstream._client_headers(): this stdio child has no HTTP
        # request of its own to carry caller identity on, so it goes into the
        # JSON-RPC arguments instead — the convention agents-platform's own
        # tools (mark_as_planned/mark_flow_done/ask_human/register_callback,
        # via _caller_run_id()) already expect. Without this, every one of
        # those tools 400s "Could not identify this run" for every caller,
        # because this Upstream is one persistent child shared across all of
        # them (unlike a per-run docker CLI agent, os.environ.AW_RUN_ID here
        # is fixed at spawn and not caller-specific).
        caller_run_id = caller_context.current().get("x-aw-caller-run-id")
        if caller_run_id and "_gateway_caller_run_id" not in arguments:
            arguments = {**arguments, "_gateway_caller_run_id": caller_run_id}

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

    async def stop(self) -> None:
        """Tear down the child process + reader task — used by Gateway.reload()
        when a server is removed/disabled or its spec changed, so the old
        process doesn't leak."""
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass
        self._reader_task = None
        if self.proc is not None and self.proc.returncode is None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                self.proc.kill()
                await self.proc.wait()
        self.proc = None
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(RuntimeError(f"upstream '{self.name}' stopped"))
        self._pending.clear()


class HttpUpstream:
    """An HTTP MCP upstream — proxies JSON-RPC tool calls to a Streamable HTTP endpoint."""

    def __init__(self, name: str, spec: dict):
        self.name = name
        self.spec = spec
        self.url: str = spec["url"]
        self._extra_headers: dict[str, str] = spec.get("headers", {}) or {}
        self.tools: list[dict] = []
        self._client: httpx.AsyncClient | None = None
        self._session_id: str | None = None

    def _client_headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            # Who is on the far side of this gateway. Without it an upstream
            # sees every agent as the same caller — see caller_context.
            **caller_context.current(),
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
        # follow_redirects: an upstream that 301s the handshake (e.g. an
        # Apache/WordPress vhost forcing canonical https) otherwise blows up
        # in _post()'s raise_for_status() — httpx treats an unfollowed
        # redirect as an HTTPStatusError, not a success, so start() would
        # raise and the upstream would be dropped with 0 tools instead of
        # transparently following the hop like a browser would.
        self._client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
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

    async def call_tool(self, tool: str, arguments: dict, req_id, *, idempotent_hint: bool = False) -> dict:
        # Non-recursive federation (resilience:gateway-proof-gated-retry-with-
        # counters, decision 4): a gateway that received THIS call from
        # another gateway must not retry its own onward call — the outer
        # gateway is already retrying the whole round trip. Without this a
        # 2-hop federation turns one 3-attempt retry into 3x3=9 real attempts
        # against the leaf upstream.
        max_attempts = 1 if caller_context.is_federated_inbound() else MAX_CALL_ATTEMPTS
        attempt = 0
        last_exc: Exception | None = None
        last_class = "upstream_error"
        last_proven = False
        while attempt < max_attempts:
            attempt += 1
            try:
                resp = await self._post({
                    "jsonrpc": "2.0", "id": req_id, "method": "tools/call",
                    "params": {"name": tool, "arguments": arguments},
                })
                resp["id"] = req_id
                if attempt > 1:
                    metrics.counters.record(self.name, "retry_succeeded")
                return resp
            except Exception as exc:
                last_exc = exc
                last_class, last_proven = _classify_call_failure(exc)
                if attempt >= max_attempts or not (last_proven or idempotent_hint):
                    break
                metrics.counters.record(self.name, "retries")
                await asyncio.sleep(RETRY_BACKOFF_SECONDS[min(attempt - 1, len(RETRY_BACKOFF_SECONDS) - 1)])

        if attempt > 1:
            metrics.counters.record(self.name, "retries_exhausted")
        metrics.counters.record(self.name, f"tools_call_errors.{last_class}")

        # The taxonomy the card asks for, spelled out in the text an agent
        # actually reads: "didn't reach" (safe to retry) vs "not sure" (must
        # not blindly retry a non-idempotent tool) — collapsing both into one
        # generic error string is exactly what let a re-reading agent
        # duplicate a non-idempotent effect before this card.
        if last_proven:
            reason = "the call never reached the handler (proven — connection or routing failure, safe to retry)"
        elif idempotent_hint:
            reason = "delivery is uncertain, but the tool is idempotent so retrying is safe"
        else:
            reason = "UNCERTAIN whether the call took effect — do not blindly retry a non-idempotent action"
        return {"jsonrpc": "2.0", "id": req_id, "result": {
            "content": [{"type": "text",
                         "text": f"http upstream '{self.name}' error: {reason}: {last_exc}"}],
            "isError": True}}

    async def stop(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


class FederationCycleError(RuntimeError):
    """Raised when connecting a ``GatewayUpstream`` would close a loop back
    to this gateway."""


class FederationDepthExceeded(RuntimeError):
    """Raised when connecting a ``GatewayUpstream`` would exceed the
    configured ``max_federation_depth``."""


def _healthz_url(mcp_url: str) -> str:
    if mcp_url.endswith("/mcp"):
        return mcp_url[: -len("/mcp")] + "/healthz"
    return mcp_url.rstrip("/") + "/healthz"


class GatewayUpstream(HttpUpstream):
    """An upstream that is itself another aw-mcp-gateway instance (or any
    Streamable-HTTP-compatible gateway exposing the same ``/healthz``
    federation fields) — aggregates that gateway's *entire* tool pool into
    this one, one level deeper in the ``{remote}__{tool}`` namespace (its
    tools already carry their own upstream's prefix, so no name is ever
    prefixed twice for the same hop).

    Reuses ``HttpUpstream`` for the actual MCP handshake/dispatch (same
    ``call_tool`` contract as every other upstream kind) and adds two
    federation safety checks, both enforced once at connect time
    (``start()``) against the remote's ``/healthz`` report rather than
    per-request — cheap, and sufficient since the federation graph only
    changes when a gateway (re)starts or its config is reloaded:

    * **cycle detection** — refuse if this gateway's own id already
      appears in the remote's reported ancestor chain (meaning the remote
      is already downstream of us; federating it back in would close a
      loop).
    * **depth cap** — refuse if federating would make the chain longer
      than ``max_federation_depth``.
    """

    def __init__(self, name: str, spec: dict, own_gateway_id: str, max_depth: int):
        original_spec = spec
        if spec.get("token") and "headers" not in spec:
            spec = {**spec, "headers": {"Authorization": f"Bearer {spec['token']}"}}
        super().__init__(name, spec)
        # Keep the ORIGINAL (pre-header-injection) spec for Gateway.reload()'s
        # diffing — otherwise this upstream would never compare equal to its
        # own freshly-reloaded spec (config never has the injected `headers`
        # key) and would be needlessly torn down + reconnected on every
        # reload even when nothing about it actually changed.
        self.spec = original_spec
        self.own_gateway_id = own_gateway_id
        self.max_depth = max_depth
        self.remote_gateway_id: str | None = None
        self.remote_chain: list[str] = []

    def _client_headers(self) -> dict[str, str]:
        # Every request this class sends IS a gateway-to-gateway hop by
        # definition — mark it unconditionally so the far side can make
        # retry non-recursive (see caller_context.is_federated_inbound()).
        headers = super()._client_headers()
        headers["X-Aw-Gateway-Federated"] = "1"
        return headers

    async def start(self) -> None:
        # Same reasoning as HttpUpstream.start(): don't let an unfollowed
        # redirect on /healthz or the handshake sink an otherwise-reachable
        # federated gateway.
        self._client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        resp = await self._client.get(_healthz_url(self.url), headers=self._extra_headers)
        resp.raise_for_status()
        health = resp.json()
        self.remote_gateway_id = health.get("gateway_id")
        self.remote_chain = list(health.get("federation_chain") or [])

        if self.own_gateway_id and self.own_gateway_id in self.remote_chain:
            raise FederationCycleError(
                f"federating '{self.name}' would create a cycle — this gateway's id "
                f"already appears in its ancestor chain {self.remote_chain}")
        if len(self.remote_chain) + 1 > self.max_depth:
            raise FederationDepthExceeded(
                f"federating '{self.name}' would exceed max_federation_depth="
                f"{self.max_depth} (remote chain depth {len(self.remote_chain)})")

        # Handshake + tools/list over the same client/session — identical
        # dispatch to a plain HttpUpstream from here on.
        init = await self._post({
            "jsonrpc": "2.0", "id": "init", "method": "initialize",
            "params": {"protocolVersion": DEFAULT_PROTOCOL,
                       "capabilities": {}, "clientInfo": {"name": "aw-mcp-gateway", "version": "1.0.0"}}
        })
        log.info("gateway upstream %s initialized: %s", self.name,
                 init.get("result", {}).get("serverInfo", {}).get("name", "?"))
        listed = await self._post({"jsonrpc": "2.0", "id": "tools", "method": "tools/list"})
        self.tools = listed.get("result", {}).get("tools", [])
        log.info("gateway upstream %s — %d federated tools (remote id=%s, chain depth=%d)",
                 self.name, len(self.tools), self.remote_gateway_id, len(self.remote_chain))
