"""Stdio MCP server that never answers its handshake — simulates the real
2026-09-03 incident: a freshly-started child (there, `npx playwright` on its
first-ever launch inside a just-updated container) that reads its stdin but
never writes a response, e.g. because it's still downloading/installing
something in the background. Before Upstream._handshake() had a timeout,
this hung Gateway.start()'s sequential loop forever, so the whole gateway
never finished FastAPI startup and never bound its port.

Reads and discards stdin so the parent's write()/drain() doesn't itself
block on a full pipe buffer, then just sleeps — the point is silence, not a
slow-but-eventual reply.
"""

from __future__ import annotations

import sys
import time


def main() -> None:
    for _ in sys.stdin:
        pass  # drain, answer nothing
    time.sleep(3600)


if __name__ == "__main__":
    main()
