"""Config loading for the standalone gateway.

Two files, both JSON, both plain local config (no external services):

* ``config/mcp.json``    — upstream server definitions, same shape as the
  in-repo project's ``.mcp.json`` (``mcpServers: {name: {command, args, env,
  cwd, enabled, type}}``). Source of truth for *what* the gateway can spawn.
* ``config/gateway.json`` — gateway-level settings: bearer ``token``, ``port``,
  the default ``upstreams`` allowlist, and named ``configs`` (scoped
  multi-tenant endpoints, see ``gateway.config_gateway.ConfigGateway``).

Both paths are overridable via env (``AW_MCP_JSON`` / ``AW_GATEWAY_JSON``) so
a deployment can point at mounted config without editing the repo.
"""

from __future__ import annotations

import json
import os
import secrets

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MCP_JSON = os.environ.get("AW_MCP_JSON", os.path.join(BASE_DIR, "config", "mcp.json"))
GATEWAY_JSON = os.environ.get("AW_GATEWAY_JSON", os.path.join(BASE_DIR, "config", "gateway.json"))


def load_mcp_servers() -> dict:
    with open(MCP_JSON) as f:
        return json.load(f).get("mcpServers", {})


def load_gateway_config() -> dict:
    try:
        with open(GATEWAY_JSON) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def token() -> str:
    """Bearer token — sourced from ``config/gateway.json``'s ``token`` field.

    Mints an ephemeral one (and warns) if unset, rather than silently running
    unauthenticated — mirrors the in-repo mcp-gateway's behavior.
    """
    tok = (load_gateway_config().get("token") or "").strip()
    if tok:
        return tok
    tok = secrets.token_urlsafe(32)
    import logging
    logging.getLogger("aw-mcp-gateway").warning(
        "no 'token' in config/gateway.json — using an EPHEMERAL token "
        "for this process only (set one in config/gateway.json): %s", tok)
    return tok
