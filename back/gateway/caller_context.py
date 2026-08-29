"""The caller's identity, carried from the inbound request to the upstream.

An agent talks to this gateway, and the gateway talks to an app. The app
therefore sees the *gateway* as its caller and has no way to tell which agent
is on the other end — which is fine for most tools and exactly wrong for one:
aw-app-secrets scopes a "approved for 10 minutes" window to a caller, and
without this every agent looked like the same one.

The Agents Platform already writes ``X-Aw-Caller-Run-Id`` into each agent's MCP
config, so the mechanism was there and nothing read it. This forwards a small
allowlist of those headers through to upstreams.

A contextvar rather than a parameter threaded through every layer: the path
from route to upstream crosses the aggregator, the tool dispatcher and the
retry logic, and none of them have any business knowing about caller identity.
Contextvars are per-task in asyncio, so concurrent requests do not see each
other's.

**Allowlist, not passthrough.** Forwarding arbitrary inbound headers would let
a caller set ``Authorization`` on somebody else's upstream. Only these travel:

**Warm-container resolution (2026-08-29).** ``X-Aw-Caller-Run-Id`` is only
correct for an EPHEMERAL runner container — one exec'd fresh per run, whose
mcp.json (and this header) is baked in at that moment. A warm container (6h
TTL, same CLI process fed new turns over a FIFO — see
agents-platform-runners' warm_pool.py) never restarts between turns, so a
header baked in at spawn stays pinned to turn 1's run forever, silently
misattributing every later turn. Confirmed live 2026-08-29: this broke
schedule_wakeup's per-run dedup guard for every turn after the first.

The fix already exists for AP-MT's own native (non-runner) warm path:
``X-Aw-Warm-Token`` is a STABLE per-container token (correct to bake in once,
since it never changes), and AP-MT's ``core/redis_streams.py::set_warm_token_run``
writes ``warm_token:{token}:run_id`` -> ``{run_id, notion_task_id,
source_device}`` to Redis on every dispatch — so the token always resolves to
whichever run is CURRENT. ``src/mcp/gateway.py`` (the AW sandbox's own,
unrelated MCP gateway) already does this resolution for that path
(``_resolve_warm_context``); this module ports the same logic so Runner-
provider agents behind THIS gateway get it too.

Resolution happens here, at capture time, so every downstream consumer —
``StdioUpstream.call_tool``'s ``_gateway_caller_run_id`` injection and
``HttpUpstream``'s forwarded headers, including whatever aw-app-secrets does
with them — gets the corrected value for free, with no other file needing to
change. When ``x-aw-warm-token`` is absent, or Redis is unreachable, or the
token is unmapped, this degrades to the raw ``x-aw-caller-run-id`` header
exactly as before — never worse than today, and safe to deploy standalone
before AP-MT starts sending the new header (see ``_resolve_warm_token``'s
docstring for why the redis db number matters here specifically).
"""
from __future__ import annotations

import json
import logging
import os
from contextvars import ContextVar

log = logging.getLogger("aw-mcp-gateway.caller_context")

#: Inbound headers forwarded to upstreams, lowercase. Keep this short and
#: boring — every addition is something a caller can now assert about itself.
FORWARDED = ("x-aw-caller-session-id", "x-aw-caller-run-id", "x-aw-caller-agent")

#: Not forwarded (aw-app-secrets etc. only ever see x-aw-caller-run-id) — read
#: here only, to resolve the forwarded run-id header when the container that
#: sent it is a warm one.
_WARM_TOKEN_HEADER = "x-aw-warm-token"

_caller_headers: ContextVar[dict] = ContextVar("aw_caller_headers", default={})

_warm_redis = None
_warm_redis_attempted = False


async def _get_warm_redis():
    """Shared async Redis client for warm-token resolution, or None if
    unreachable — every caller degrades to the raw header on None, so a down
    Redis here never breaks tool calls, only un-corrects this one thing.

    ``AW_MCP_GATEWAY_WARM_REDIS_URL`` must point at the SAME Redis db
    ``agents-platform-multitenant``'s ``core/redis_streams.py::get_client()``
    uses (``AP_REDIS_URL`` there — db 1 in this deployment), not
    ``AW_REDIS_URL`` (db 0, a different db on the same instance): the two
    names differ only by which app owns them, and pointing this at the wrong
    db silently means every lookup misses, degrading forever with no error
    (found 2026-08-29 auditing this exact mismatch in the sandbox's own
    ``src/mcp/gateway.py`` reference implementation — worth checking there
    too, out of scope for this module)."""
    global _warm_redis, _warm_redis_attempted
    if _warm_redis is not None:
        return _warm_redis
    if _warm_redis_attempted:
        return None
    _warm_redis_attempted = True
    url = os.environ.get("AW_MCP_GATEWAY_WARM_REDIS_URL") or os.environ.get("AW_SHARED_REDIS_URL")
    if not url:
        log.info("no warm-token Redis configured (AW_MCP_GATEWAY_WARM_REDIS_URL/"
                 "AW_SHARED_REDIS_URL both unset) — X-Aw-Warm-Token will no-op")
        return None
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(url, decode_responses=True,
                              socket_connect_timeout=2, socket_timeout=2)
        await r.ping()
        _warm_redis = r
    except Exception as e:
        log.warning("warm-token Redis unavailable (%s) — X-Aw-Warm-Token will no-op", e)
        _warm_redis = None
    return _warm_redis


async def _resolve_warm_token(token: str) -> str | None:
    """Resolve a warm container's stable token to its CURRENT turn's run_id.

    Mirrors ``src/mcp/gateway.py::_resolve_warm_context`` — same key schema
    (``warm_token:{token}:run_id``), same JSON-blob-with-bare-string-fallback
    decode (``set_warm_token_run`` stores ``{run_id, notion_task_id,
    source_device}``, not a bare run_id). Returns None on any failure —
    unset token, unreachable Redis, expired/never-existed key, or malformed
    value — so the caller can fall back to the raw header rather than error.
    """
    r = await _get_warm_redis()
    if r is None:
        return None
    try:
        raw = await r.get(f"warm_token:{token}:run_id")
        if not raw:
            return None
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and data.get("run_id"):
                return str(data["run_id"])
        except (json.JSONDecodeError, TypeError):
            pass
        return raw  # pre-JSON-blob format: a bare run_id string
    except Exception as e:
        log.warning("warm-token lookup failed token=%s (%s)", token[:12], e)
        return None


async def capture(headers) -> None:
    """Record the forwardable headers of the request being served.

    If ``x-aw-warm-token`` is present and resolves, its run_id REPLACES the
    forwarded ``x-aw-caller-run-id`` value — the warm token is only ever
    sent alongside the (possibly stale) per-run header, never instead of it,
    so a caller not yet updated to send the token keeps working exactly as
    before.
    """
    picked = {}
    for name in FORWARDED:
        value = headers.get(name)
        if value:
            # Bounded: these end up on an outbound request and in an app's logs.
            picked[name] = str(value)[:256]

    warm_token = headers.get(_WARM_TOKEN_HEADER)
    if warm_token:
        resolved_run_id = await _resolve_warm_token(str(warm_token)[:256])
        if resolved_run_id:
            picked["x-aw-caller-run-id"] = resolved_run_id[:256]

    _caller_headers.set(picked)


def current() -> dict:
    return dict(_caller_headers.get())


__all__ = ["capture", "current", "FORWARDED"]
