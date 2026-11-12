---
name: aw-mcp-gateway
description: A gateway tool looks absent or missing from tools/list, or a federated upstream's tool pool seems smaller than expected — before concluding the tool doesn't exist upstream, check this gateway's own tools_allow allowlist. Use whenever a tool a workspace should have access to isn't showing up through aw-gateway, or when editing a named config's tool/run policy.
---

# aw-mcp-gateway — diagnosing a "missing" tool

A tool missing from this gateway's advertised `tools/list` may be
allowlisted out **locally**, not absent upstream. Don't conclude a
capability was never built just because this gateway isn't advertising it.

## Where the allowlist actually lives

`tools_allow` is a key on **named configs** — the per-scoped-endpoint
profiles reached at `/mcp/<name>`, not a workspace-wide setting and not a
property of `mcp.custom.json`:

- Parsed in `back/gateway/config_gateway.py:125-128` (accepts a string or a
  list of fnmatch globs).
- Enforced at both advertise time and call time by `_tool_allowed`
  (`config_gateway.py:160-166`) — a pattern matches either the stripped
  tool name or the `upstream__tool` qualified form.
- Missing `tools_allow` on a config means unrestricted, for backwards
  compatibility.

The **default** (unscoped) endpoint has its own, separate `upstreams`
allowlist — a different mechanism, coarser-grained (whole upstream, not
per-tool).

`mcp.custom.json` is unrelated to either of these: it only defines custom
**upstreams** (servers to connect to), never a tools_allow or upstreams
restriction on what gets advertised from them.

## Where to look

- `config/gateway.json`'s `configs` key holds the hand-authored named
  configs, including their `tools_allow`.
- That's layered over any `gateway-profiles.json` an installed app
  contributed — the effective merged view is `effective_named_configs()`
  (`config.py:481`): a hand-authored config in `gateway.json` wins over an
  app-contributed profile of the same name.
- `GET /admin/configs` reports the effective profiles as the gateway
  actually sees them — check it before editing anything by hand.

## Diagnosing before you conclude a tool is missing

1. **`search_execution_history` for the tool name.** If it was ever called
   successfully — from any session, any workspace — the tool exists and
   works; this gateway's config is simply not letting it through to you.
2. **Federated upstreams compound this.** A gateway reached through
   federation (another gateway's `/mcp/<name>` endpoint, subject to
   `max_federation_depth`) advertises *that remote's* entire tool pool —
   so a tool "missing through federation" is doubly likely to be a local
   policy artifact on one of the two gateways in the chain, not a real
   capability gap.

## Fix

Edit the named config's (or the default endpoint's) allowlist in
`config/gateway.json` — widen the `tools_allow` glob, or add the upstream
to the `upstreams` list — then restart the gateway app for the change to
take effect.
