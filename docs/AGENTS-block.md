Most tools reach this workspace through **`aw-gateway`** — the
`aw-mcp-gateway` app, which aggregates every installed app's MCP surface
behind one endpoint and prefixes tool names (`aw__kb__…`,
`aw__playwright__…`, `aw__agents_platform_runners__…`). Which tools exist
therefore depends on which apps are installed; `aw-workspace-cli apps` is the
authoritative list.
