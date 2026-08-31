"""Stdio MCP server that simulates the exact failure mode described in
resilience:gateway-zero-tool-start-is-unparked-classe-b: initialize()
succeeds, but tools/list answers with a JSON-RPC ERROR (like the MCP SDK's
own lowlevel server does when a handler — e.g. agents-platform-runners'
``_list_tools``, which calls out to AP-MT with no try/except — raises).

Controlled by the ``FLAKY_MODE`` env var so a test can flip behavior across
a restart without touching the spec (spec equality drives reload()'s
added/changed/unchanged diff):
* ``error``   — tools/list returns {"error": ...}            (the bug)
* ``healthy`` — tools/list returns a real, non-empty tool list
"""

from __future__ import annotations

import json
import os
import sys

PROTOCOL = "2024-11-05"
MODE = os.environ.get("FLAKY_MODE", "error")

TOOLS = [{
    "name": "ping",
    "description": "Ping.",
    "inputSchema": {"type": "object", "properties": {}},
}]


def _write(msg: dict) -> None:
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        method = req.get("method")
        req_id = req.get("id")

        if method == "initialize":
            _write({"jsonrpc": "2.0", "id": req_id, "result": {
                "protocolVersion": PROTOCOL, "capabilities": {"tools": {}},
                "serverInfo": {"name": "example-flaky", "version": "1.0.0"}}})
        elif method == "notifications/initialized":
            continue
        elif method == "tools/list":
            if MODE == "error":
                # Mirrors mcp.server.lowlevel.server._handle_request's own
                # except Exception -> types.ErrorData(code=0, ...) — the
                # shape a handler's uncaught exception actually produces.
                _write({"jsonrpc": "2.0", "id": req_id,
                        "error": {"code": 0, "message": "simulated: AP-MT unreachable"}})
            else:
                _write({"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}})
        elif method == "tools/call":
            _write({"jsonrpc": "2.0", "id": req_id, "result": {
                "content": [{"type": "text", "text": "pong"}], "isError": False}})
        else:
            _write({"jsonrpc": "2.0", "id": req_id,
                    "error": {"code": -32601, "message": f"Unknown method: {method}"}})


if __name__ == "__main__":
    main()
