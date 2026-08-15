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
"""
from __future__ import annotations

from contextvars import ContextVar

#: Inbound headers forwarded to upstreams, lowercase. Keep this short and
#: boring — every addition is something a caller can now assert about itself.
FORWARDED = ("x-aw-caller-session-id", "x-aw-caller-run-id", "x-aw-caller-agent")

_caller_headers: ContextVar[dict] = ContextVar("aw_caller_headers", default={})


def capture(headers) -> None:
    """Record the forwardable headers of the request being served."""
    picked = {}
    for name in FORWARDED:
        value = headers.get(name)
        if value:
            # Bounded: these end up on an outbound request and in an app's logs.
            picked[name] = str(value)[:256]
    _caller_headers.set(picked)


def current() -> dict:
    return dict(_caller_headers.get())


__all__ = ["capture", "current", "FORWARDED"]
