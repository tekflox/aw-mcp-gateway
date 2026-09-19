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
token is unmapped, this degrades to the raw ``x-aw-caller-run-id`` header —
but as of 2026-09-19 that is no longer "never worse than today" for a warm
Runner-provider container: ``execute.py`` deliberately strips its own
``X-Aw-Caller-Run-Id`` fallback header (a stale header would otherwise
silently beat a fresh Redis value), so for that topology an unresolvable
warm Redis is a total, silent outage, not a graceful degrade. That is why
resolution here now fails LOUDLY (see ``_warn_unresolved`` and
``warm_redis_status``) instead of only logging once at boot.
"""
from __future__ import annotations

import json
import logging
import time
from contextvars import ContextVar

from . import metrics, warm_redis

log = logging.getLogger("aw-mcp-gateway.caller_context")

#: Mirrors ``agents-platform``'s ``core/warm_pool.py::GENERATION_KEY`` and
#: ``agents-platform-runners``'s ``warm_pool.py::GENERATION_KEY`` EXACTLY —
#: same key, same shared Redis db (see ``_get_warm_redis``'s docstring below
#: for why the db number matters). A SET here condemns every warm claude-cli
#: container, native and runner, in one write; each consumer just compares
#: its own label against the current value on its next dispatch. Both of
#: those modules already document "mcp-gateway starting/restarting" as a
#: trigger for this key — this gateway is the missing owner of that event.
GENERATION_KEY = "warm:config_generation"

#: Inbound headers forwarded to upstreams, lowercase. Keep this short and
#: boring — every addition is something a caller can now assert about itself.
FORWARDED = ("x-aw-caller-session-id", "x-aw-caller-run-id", "x-aw-caller-agent")

#: Not forwarded (aw-app-secrets etc. only ever see x-aw-caller-run-id) — read
#: here only, to resolve the forwarded run-id header when the container that
#: sent it is a warm one.
_WARM_TOKEN_HEADER = "x-aw-warm-token"

#: Set by GatewayUpstream on every outbound request — never by an ordinary
#: client — so a gateway can tell "this tools/call arrived from another
#: gateway" apart from "this arrived straight from an agent". Not part of
#: FORWARDED: it must be re-derived fresh at each hop (by whichever upstream
#: object is actually making the next call), never blindly relayed, or a
#: federated flag from hop 1 would leak into a same-request call to a
#: completely unrelated non-federated upstream two lines below it.
_FEDERATION_HEADER = "x-aw-gateway-federated"

_caller_headers: ContextVar[dict] = ContextVar("aw_caller_headers", default={})
_federated_inbound: ContextVar[bool] = ContextVar("aw_federated_inbound", default=False)

_warm_redis = None
_warm_redis_last_attempt = 0.0

#: How long a failed connect attempt sticks before the next call retries —
#: replaces a permanent latch that used to disable warm-token resolution for
#: this process's entire life over a Redis that was merely down for one
#: second at gateway boot.
_WARM_REDIS_RETRY_COOLDOWN_S = 30.0


def _redact(url: str | None) -> str | None:
    """``redis://:password@host:port/db`` -> ``redis://***@host:port/db``.
    Only the credential is secret; host/port/db are exactly what a human
    debugging /healthz needs to see."""
    if not url or "@" not in url:
        return url
    scheme_and_auth, _, rest = url.partition("@")
    scheme = scheme_and_auth.partition("://")[0]
    return f"{scheme}://***@{rest}"


async def _get_warm_redis():
    """Shared async Redis client for warm-token resolution, or None if
    unresolvable/unreachable — every caller degrades to the raw header on
    None, so a down Redis here never breaks tool calls, only un-corrects
    this one thing (see module docstring for why that degrade is no longer
    fully safe for a warm Runner-provider container).

    The URL itself comes from ``warm_redis.resolve()`` — config override,
    then env, then probing the docker bridge gateways — which must land on
    the SAME Redis db ``agents-platform-multitenant``'s
    ``core/redis_streams.py::get_client()`` uses (db 1 in this deployment),
    not ``AW_REDIS_URL`` (a different db on the same instance): pointing this
    at the wrong db silently means every lookup misses, degrading forever
    with no error (found 2026-08-29 auditing this exact mismatch in the
    sandbox's own ``src/mcp/gateway.py`` reference implementation).

    A failed connect attempt is retried after ``_WARM_REDIS_RETRY_COOLDOWN_S``
    rather than latched forever, and also drops ``warm_redis``'s own cached
    resolution — so a probe answer that only just changed (or a Redis that
    only just came up) gets re-discovered on the next attempt instead of
    being pinned to the first outcome for the gateway's entire process life.
    """
    global _warm_redis, _warm_redis_last_attempt
    if _warm_redis is not None:
        return _warm_redis

    now = time.time()
    if now - _warm_redis_last_attempt < _WARM_REDIS_RETRY_COOLDOWN_S:
        return None
    _warm_redis_last_attempt = now

    resolution = warm_redis.resolve()
    if not resolution.url:
        return None
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(resolution.url, decode_responses=True,
                              socket_connect_timeout=2, socket_timeout=2)
        await r.ping()
        _warm_redis = r
    except Exception as e:
        log.warning(
            "warm-token Redis unavailable (%s; url=%s source=%s) — "
            "X-Aw-Warm-Token will no-op, retrying in %ss",
            e, _redact(resolution.url), resolution.source, int(_WARM_REDIS_RETRY_COOLDOWN_S))
        warm_redis.reset_cache()
        _warm_redis = None
    return _warm_redis


async def bump_warm_generation() -> None:
    """Condemn every warm claude-cli container — native and runner — so the
    next dispatch to each one drains it and starts fresh instead of reusing
    a process whose MCP client was built against upstreams that no longer
    exist (see module docstring: those clients are built once at CLI boot
    and never reinitialized). Call this once, from the gateway's own
    startup, after every upstream is up.

    Deliberately reuses ``_get_warm_redis()`` — the SAME client/db as
    warm-token resolution above — rather than a new env var: pointing this
    at the wrong db would degrade to a silent no-op forever, exactly the
    trap ``_get_warm_redis``'s docstring already documents.

    Best-effort and never raises: a failed bump just leaves warm containers
    serving a dead MCP client until the next successful one, no worse than
    before this existed. Idempotent and lazy by design — like the two
    consumers of this key, a bump only marks containers stale; nothing is
    killed synchronously, so repeated calls (five restarts in a row) cost
    no more than one.
    """
    r = await _get_warm_redis()
    if r is None:
        log.warning("bump_warm_generation: no warm Redis available at startup — "
                    "warm containers will NOT be invalidated by this restart")
        return
    try:
        await r.set(GENERATION_KEY, str(time.time()))
        log.info("bump_warm_generation: SET %s — every warm claude-cli container "
                 "is now condemned and will drain+respawn on its next turn", GENERATION_KEY)
    except Exception as e:
        log.warning("bump_warm_generation: Redis write failed (%s) — warm "
                    "containers will NOT be invalidated by this restart", e)


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


#: How long a single "warm token arrived but didn't resolve" WARNING sticks
#: before the next occurrence logs again — a sustained outage should be
#: loud, not one line per tool call.
_UNRESOLVED_WARNING_COOLDOWN_S = 60.0
_last_unresolved_warning = 0.0


def _warn_unresolved() -> None:
    """``_resolve_warm_token`` returning None for a token that WAS present is
    never normal — AP-MT only sends ``X-Aw-Warm-Token`` after
    ``set_warm_token_run`` wrote the mapping. Names the resolved Redis and
    the candidate list tried, because "it's broken" without either is not
    actionable at 3am."""
    global _last_unresolved_warning
    now = time.time()
    if now - _last_unresolved_warning < _UNRESOLVED_WARNING_COOLDOWN_S:
        return
    _last_unresolved_warning = now
    resolution = warm_redis.resolve()
    log.warning(
        "warm-token present but did not resolve to a run_id (redis=%s source=%s, "
        "candidates tried=%s) — X-Aw-Caller-Run-Id falls back to the stale "
        "per-run header for every affected warm session until this is fixed",
        _redact(resolution.url), resolution.source, ", ".join(warm_redis.candidate_hosts()))


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
        metrics.counters.record(metrics.WARM_TOKEN, "seen")
        resolved_run_id = await _resolve_warm_token(str(warm_token)[:256])
        if resolved_run_id:
            picked["x-aw-caller-run-id"] = resolved_run_id[:256]
        else:
            metrics.counters.record(metrics.WARM_TOKEN, "unresolved")
            _warn_unresolved()

    _caller_headers.set(picked)
    _federated_inbound.set(bool(headers.get(_FEDERATION_HEADER)))


def current() -> dict:
    return dict(_caller_headers.get())


#: A wrong-instance/wrong-db warm Redis is silent on a brand-new workspace —
#: zero tokens have arrived yet, which must NOT read as 100% failure. Require
#: a handful of real attempts before treating an all-failed rate as the
#: loud signal rather than early noise.
_MIN_TOKENS_FOR_FAILURE_SIGNAL = 5


def _warm_redis_ok(resolution: warm_redis.Resolution, reachable: bool,
                    tokens_seen: float, tokens_unresolved: float) -> bool:
    if resolution.source == "none":
        return False
    if not reachable:
        return False
    if tokens_seen >= _MIN_TOKENS_FOR_FAILURE_SIGNAL and tokens_unresolved >= tokens_seen:
        return False
    return True


async def warm_redis_status() -> dict:
    """Snapshot for ``/healthz``'s ``warm_redis`` block.

    Calling this attempts a connection exactly like any other warm-token
    lookup would (subject to the same cooldown), so a doctor/monitoring poll
    doubles as the retry trigger rather than needing its own timer — and a
    workspace that never sees a single warm session still gets an honest
    ``reachable``/``source`` reading the first time anything asks.
    """
    r = await _get_warm_redis()
    resolution = warm_redis.resolve()
    tokens_seen = metrics.counters.total(metrics.WARM_TOKEN, "seen")
    tokens_unresolved = metrics.counters.total(metrics.WARM_TOKEN, "unresolved")
    reachable = r is not None
    return {
        "ok": _warm_redis_ok(resolution, reachable, tokens_seen, tokens_unresolved),
        "url": _redact(resolution.url),
        "source": resolution.source,
        "reachable": reachable,
        "tokens_seen_24h": tokens_seen,
        "tokens_unresolved_24h": tokens_unresolved,
    }


def is_federated_inbound() -> bool:
    """True when the request this task is serving arrived from another
    aw-mcp-gateway (a ``GatewayUpstream`` hop), not straight from an agent or
    other end client.

    Used to make retry non-recursive in a federated chain: a gateway that
    itself received this call from an upstream gateway must NOT retry its own
    onward call, because that outer gateway is already retrying the whole
    round trip. Without this, a 2-hop federation multiplies a 3-attempt retry
    into 3x3=9 real attempts against the leaf upstream — see
    ``resilience:gateway-proof-gated-retry-with-counters``.
    """
    return _federated_inbound.get()


__all__ = ["capture", "current", "is_federated_inbound", "FORWARDED", "bump_warm_generation",
           "warm_redis_status"]
