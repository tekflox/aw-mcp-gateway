"""Resolve the Redis this gateway uses for warm-token caller-identity lookup.

Why this module exists (2026-09-19): ``warm_redis_url`` used to be a plain,
hand-entered Setting (``AW_MCP_GATEWAY_WARM_REDIS_URL`` in the container env)
with no fallback beyond ``AW_SHARED_REDIS_URL`` — leave both unset and every
caller-identity-dependent MCP tool (``schedule_wakeup``, ``list_wakeups``,
``ask_human``, ``mark_flow_done``, ``supervise``, async-callback dispatch)
silently misattributes or no-ops for the lifetime of every warm session, with
one INFO line at boot as the only signal. Confirmed live in the crispal
hosted workspace: nobody had set the field, and nothing said so loudly enough
for anyone to notice before a paying customer's bot broke.

``agents-platform-runners`` solved this exact bug for itself in 2026-08-08
(see ``agents_platform_runners_app/shared_redis.py``, publishing to
``run:{run_id}:events`` on the same Redis) by probing instead of requiring a
human to paste a URL. This module ports that same resolution chain rather
than reinventing it — same candidate hosts, same port/db defaults, so the two
callers of the one shared Redis agree on where it is without either needing
to be told.

Resolution order (first hit wins):

1. ``AW_MCP_GATEWAY_WARM_REDIS_URL`` (``${config.warm_redis_url}`` in this
   app's manifest) — an explicit operator override always beats discovery.
2. ``AW_SHARED_REDIS_URL`` — lets a deployment bake the value in without
   touching per-workspace app settings.
3. The first candidate host that answers an authenticated ``PING`` on
   ``AW_SHARED_REDIS_PORT`` (default 6379) — ``AW_SHARED_REDIS_PASSWORD``/
   ``AW_SHARED_REDIS_USER`` (default user ``default``) if set. A host found
   but refusing that credential (or refusing none) is never cached as a
   passwordless URL — this was a bare TCP connect check until 2026-10-12,
   which would "discover" an auth-protected host and hand back a URL that
   fails on every subsequent command. Result is cached process-wide.

Measured 2026-08-08 from inside a workspace agent container (and reconfirmed
2026-09-19 for this gateway's own container): the default-route gateway is
podman's (``10.89.0.1``) and has nothing on :6379, while the shared Redis
answers on the DOCKER bridge gateways (``172.18.0.1``/``172.17.0.1``) —
routable from here but never the default route. Deriving the address from
``/proc/net/route`` alone would therefore produce a confidently wrong URL.

The db index defaults to 1 because that is the db
agents-platform-multitenant's ``core/redis_streams.py::get_client()`` uses
for both ``run:{run_id}:events`` and ``warm_token:{token}:run_id`` — the same
client backs both, so the Redis a Runner must already reach to publish run
events IS the Redis holding warm tokens, by construction. Override with
``AW_SHARED_REDIS_DB`` if that deployment moves.
"""
from __future__ import annotations

import logging
import os
import socket
import struct
from typing import NamedTuple

import redis as _redis_sync

log = logging.getLogger("aw-mcp-gateway.warm_redis")

DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_DB = "1"
PROBE_TIMEOUT_S = 0.5

#: Well-known docker bridge gateways on the podman host. Ordered after the
#: container's own default route (which is usually right in a plain-docker
#: deployment) but they are what actually answers in the nested-podman
#: aw-remote-host topology this app most often ships into.
FALLBACK_GATEWAYS = ("172.18.0.1", "172.17.0.1", "host.docker.internal")


class Resolution(NamedTuple):
    """What ``resolve()`` found, and how — the ``source`` field is what
    ``/healthz`` reports so a human can tell "nobody's set this and nothing
    answered" apart from "it's working, just via a different path than
    expected"."""
    url: str | None
    source: str  # "config" | "env" | "probed" | "auth" | "none"


_cached: Resolution | None = None


def default_gateway_ip() -> str | None:
    """This container's default-route gateway, or None if it can't be read.

    ``/proc/net/route`` columns are Iface, Destination, Gateway, ... with the
    two address columns as little-endian hex. The default route is the row
    whose Destination is ``00000000``.
    """
    try:
        with open("/proc/net/route", encoding="ascii") as fh:
            rows = fh.read().splitlines()
    except OSError:
        return None
    for row in rows[1:]:
        fields = row.split()
        if len(fields) > 2 and fields[1] == "00000000":
            try:
                return socket.inet_ntoa(struct.pack("<L", int(fields[2], 16)))
            except (ValueError, struct.error):
                continue
    return None


def candidate_hosts() -> list[str]:
    """Hosts to probe, most-likely first, de-duplicated."""
    hosts: list[str] = []
    gateway = default_gateway_ip()
    if gateway:
        hosts.append(gateway)
    for host in FALLBACK_GATEWAYS:
        if host not in hosts:
            hosts.append(host)
    return hosts


def _probe(host: str, port: int, user: str | None, password: str | None) -> str:
    """Real RESP ``PING`` against ``host:port``, authenticated if a
    credential is given. Returns ``"ok"`` (answered — authenticated when a
    credential was supplied, unauthenticated otherwise), ``"auth"`` (a
    Redis IS there but refuses us — no credential given and one is
    required, or the one given is wrong), or ``"down"`` (nothing
    answered). A bare TCP connect used to be enough when every Redis on
    this stack ran with no auth at all; once one requires it, a successful
    connect is no longer the same thing as a usable connection."""
    client = _redis_sync.Redis(
        host=host, port=port, username=user, password=password,
        socket_connect_timeout=PROBE_TIMEOUT_S, socket_timeout=PROBE_TIMEOUT_S,
    )
    try:
        client.ping()
        return "ok"
    except _redis_sync.exceptions.AuthenticationError:
        return "auth"
    except _redis_sync.exceptions.ResponseError as e:
        if "NOAUTH" in str(e) or "WRONGPASS" in str(e):
            return "auth"
        return "down"
    except (_redis_sync.exceptions.ConnectionError, OSError):
        return "down"
    finally:
        client.close()


def discover_host(port: int, user: str | None, password: str | None) -> tuple[str | None, str]:
    """First candidate host that answers on ``port``, and how it answered.

    Returns ``(host, "ok")`` when found and usable, ``(host, "auth")`` when
    found but refusing the given (or absent) credential — this already IS
    the shared Redis, just locked, so the remaining candidates are not
    worth trying — or ``(None, "down")`` when nothing answered at all.
    """
    for host in candidate_hosts():
        state = _probe(host, port, user, password)
        if state in ("ok", "auth"):
            return host, state
    return None, "down"


def resolve() -> Resolution:
    """The warm-token Redis URL, with the source it came from.

    ``Resolution(None, "none")`` means no override was set and no candidate
    host answered on the Redis port at all — a real misconfiguration (or a
    topology this probe doesn't know). ``Resolution(None, "auth")`` means a
    candidate host DID answer but refused our credential, or refused
    because we have none — this used to be the silent-degradation trap: a
    bare TCP connect check would "discover" an auth-protected host and
    cache a passwordless URL that then failed on every command. Both cases
    must fail loudly, never degrade forever with no signal.
    """
    global _cached
    if _cached is not None:
        return _cached

    configured = os.environ.get("AW_MCP_GATEWAY_WARM_REDIS_URL")
    if configured:
        _cached = Resolution(configured, "config")
        return _cached

    env_url = os.environ.get("AW_SHARED_REDIS_URL")
    if env_url:
        log.info("warm_redis_url unset in app config — using AW_SHARED_REDIS_URL")
        _cached = Resolution(env_url, "env")
        return _cached

    port = int(os.environ.get("AW_SHARED_REDIS_PORT", DEFAULT_REDIS_PORT))
    user = os.environ.get("AW_SHARED_REDIS_USER", "default")
    password = os.environ.get("AW_SHARED_REDIS_PASSWORD") or None
    host, state = discover_host(port, user, password)

    if state == "auth":
        log.error(
            "found a Redis at %s:%s but it refused us (%s) — set "
            "AW_SHARED_REDIS_PASSWORD (and AW_SHARED_REDIS_USER if not "
            "'default') on this app's Settings; refusing to cache a "
            "passwordless URL that would fail on every command",
            host, port, "wrong credential" if password else "credential missing")
        _cached = Resolution(None, "auth")
        return _cached

    if not host:
        log.error(
            "warm_redis_url is unset, AW_SHARED_REDIS_URL is unset, and nothing "
            "answered on :%s at any of %s — set warm_redis_url on this app's "
            "Settings to override", port, ", ".join(candidate_hosts()))
        _cached = Resolution(None, "none")
        return _cached

    db = os.environ.get("AW_SHARED_REDIS_DB", DEFAULT_REDIS_DB)
    auth = f"{user}:{password}@" if password else ""
    url = f"redis://{auth}{host}:{port}/{db}"
    log.info(
        "warm_redis_url unset in app config — discovered %s by probing :%s "
        "(set warm_redis_url on this app's Settings to override)",
        f"redis://{host}:{port}/{db}" if not password else f"redis://{user}:***@{host}:{port}/{db}",
        port)
    _cached = Resolution(url, "probed")
    return _cached


def reset_cache() -> None:
    """Forget the resolved result — used by tests and by the fixed ~30s
    reconnect cooldown in ``caller_context._get_warm_redis()`` so a Redis
    that only just came up (or a probe answer that only just changed) isn't
    pinned to the first outcome for the gateway's entire process life."""
    global _cached
    _cached = None
