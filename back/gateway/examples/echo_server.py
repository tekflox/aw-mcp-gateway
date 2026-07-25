"""Minimal stdio MCP server, spawned by default so a fresh checkout has one
real local upstream to prove the gateway boots end to end. Exposes a single
``echo`` tool that returns whatever ``text`` argument it was given.
"""

from __future__ import annotations

import json
import sys

PROTOCOL = "2024-11-05"

TOOLS = [{
    "name": "echo",
    "description": "Echo back the given text.",
    "inputSchema": {
        "type": "object",
        "properties": {"text": {"type": "string"}},
        "required": ["text"],
    },
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
                "serverInfo": {"name": "example-echo", "version": "1.0.0"}}})
        elif method == "notifications/initialized":
            continue
        elif method == "tools/list":
            _write({"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}})
        elif method == "tools/call":
            args = (req.get("params") or {}).get("arguments") or {}
            text = args.get("text", "")
            _write({"jsonrpc": "2.0", "id": req_id, "result": {
                "content": [{"type": "text", "text": text}], "isError": False}})
        else:
            _write({"jsonrpc": "2.0", "id": req_id,
                    "error": {"code": -32601, "message": f"Unknown method: {method}"}})


if __name__ == "__main__":
    main()
