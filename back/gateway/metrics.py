"""24h rolling-window resilience counters — see
``resilience:gateway-proof-gated-retry-with-counters``.

The card that added retry to this gateway is explicit about why this module
exists at all: *"NENHUM RETRY ENTRA SEM CONTADOR — um retry não contado é um
retry que esconde outage."* A retry that quietly succeeds on the second or
third attempt makes a core restarting every 5 minutes invisible to everyone
except whoever is staring at a log at the right second. These counters are
what the sibling ``doctor`` card (``resilience:doctor-mcp-needs-24h-memory``)
reads instead.

Rolling 24h window, not a lifetime cumulative counter: a cumulative counter
that never decays goes red after one bad day and stays red forever, which
trains people to ignore it — explicitly called out as a trap in the
architect's design. Every event carries its own timestamp in a deque; reads
prune anything older than the window first, so the count always reflects
"in the last 24h" no matter when you ask.

Kept as plain in-process state (no persistence) — same lifetime as the
gateway process itself, which already loses all state (upstream connections,
parked routes) on restart. Surviving a gateway restart is out of scope here.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque

WINDOW_SECONDS = 24 * 60 * 60

#: tools_call_errors classes — kept short and closed so a doctor/monitoring
#: reader can rely on the exact set instead of discovering new keys at runtime.
ERROR_CLASSES = ("unknown_tool", "upstream_error", "timeout")

# A pseudo upstream name for errors that never had one to attribute to
# (tools/call against a name with no route at all — see Gateway.handle()).
UNROUTED = "_unrouted"


class RollingCounters:
    """Per-(upstream, metric) event log, pruned to a 24h window on read."""

    def __init__(self, window_seconds: float = WINDOW_SECONDS):
        self._window = window_seconds
        self._events: dict[tuple[str, str], deque] = defaultdict(deque)

    def record(self, upstream: str, metric: str, amount: float = 1.0, now: float | None = None) -> None:
        now = time.time() if now is None else now
        dq = self._events[(upstream, metric)]
        dq.append((now, amount))
        self._prune(dq, now)

    @staticmethod
    def _prune(dq: deque, now: float) -> None:
        cutoff = now - WINDOW_SECONDS
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def total(self, upstream: str, metric: str, now: float | None = None) -> float:
        now = time.time() if now is None else now
        dq = self._events.get((upstream, metric))
        if not dq:
            return 0.0
        self._prune(dq, now)
        return sum(amount for _, amount in dq)

    def snapshot(self, upstreams: list[str]) -> dict[str, dict]:
        """One entry per upstream (plus the pseudo ``_unrouted`` one if it has
        events), each with the full metric set — zero-filled so a doctor
        reading this doesn't need to special-case an upstream with no
        history yet."""
        now = time.time()
        names = set(upstreams) | {name for name, _ in self._events if name == UNROUTED}
        out: dict[str, dict] = {}
        for name in sorted(names):
            out[name] = {
                "retries": self.total(name, "retries", now),
                "retry_succeeded": self.total(name, "retry_succeeded", now),
                "retries_exhausted": self.total(name, "retries_exhausted", now),
                "tools_call_errors": {
                    cls: self.total(name, f"tools_call_errors.{cls}", now)
                    for cls in ERROR_CLASSES
                },
                "upstream_unavailable_seconds": self.total(name, "upstream_unavailable_seconds", now),
                "upstream_started_empty": self.total(name, "upstream_started_empty", now),
            }
        return out


#: Process-wide singleton — one gateway process, one set of counters. Mirrors
#: caller_context's module-level state: nothing here is per-request, so a
#: contextvar would be the wrong tool.
counters = RollingCounters()
