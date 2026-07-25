"""ConfigGateway — a filtered view of a Gateway restricted to a named config's
upstreams (one scoped Streamable HTTP endpoint per config, e.g. ``/mcp/crispal``
next to the unscoped ``/mcp``).

Ported from the in-repo mcp-gateway's ``ConfigGateway``, trimmed to the
generic allowlist behavior only — the source class also carries several
agentic-workspace-specific hooks (per-profile agents-platform run policy +
Telegram approval gate, KB/presentation namespace injection) that are
particular to that project's own upstream MCPs, not to the gateway mechanism
itself. Namespaced/scoped injection for a given upstream's tools is a
reasonable thing to want here too (e.g. scoping ``aw-knowledge-base`` per
tenant) — add it back the same way if/when a standalone upstream needs it:
inject an extra argument keyed off ``route[0] == "<upstream-name>"`` right
before calling ``self._gateway.handle(msg)`` in ``handle()`` below.
"""

from __future__ import annotations

from .upstream import HttpUpstream


class ConfigGateway:
    def __init__(self, gateway: "Gateway", allowed_upstreams: list[str], name: str = ""):  # noqa: F821
        self._gateway = gateway
        self._name = name
        self._allowed = set(allowed_upstreams)

    def _filtered_tools(self) -> list[dict]:
        """Tools for this config, with the ``{upstream}__`` prefix stripped —
        the config's own MCP server name already scopes the tools, so a model
        sees e.g. ``get_site_info`` instead of ``crispal__get_site_info``."""
        tools = []
        for t in self._gateway.agg_tools:
            route = self._gateway.routes.get(t["name"], ("", ""))
            upstream, _real_tool = route[0], route[1]
            if upstream not in self._allowed:
                continue
            tool = dict(t)
            prefix = f"{upstream}__"
            if tool["name"].startswith(prefix):
                tool["name"] = tool["name"][len(prefix):]
            tools.append(tool)
        return tools

    async def handle(self, msg: dict) -> dict | None:
        method = msg.get("method", "")
        req_id = msg.get("id")

        if method in ("initialize", "ping"):
            return await self._gateway.handle(msg)
        if method in ("notifications/initialized", "notifications/cancelled"):
            return None

        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": req_id,
                    "result": {"tools": self._filtered_tools()}}

        if method == "tools/call":
            params = msg.get("params") or {}
            public = params.get("name", "")
            route = self._gateway.routes.get(public)
            if not route:
                for ups in self._allowed:
                    candidate = f"{ups}__{public}"
                    route = self._gateway.routes.get(candidate)
                    if route:
                        params = dict(params)
                        params["name"] = candidate
                        msg = {**msg, "params": params}
                        break
            if not route or route[0] not in self._allowed:
                return {"jsonrpc": "2.0", "id": req_id, "error": {
                    "code": -32602,
                    "message": f"Tool '{public}' is not available in this config"}}
            return await self._gateway.handle(msg)

        return {"jsonrpc": "2.0", "id": req_id, "error": {
            "code": -32601, "message": f"Unknown method: {method}"}}
