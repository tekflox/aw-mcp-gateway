"""Config loading for the standalone gateway.

Two files, both JSON, both plain local config (no external services):

* ``config/mcp.json``    — generated upstream server definitions, same shape
  as the in-repo project's ``.mcp.json`` (``mcpServers: {name: {command, args,
  env, cwd, enabled, type}}``). It is rebuilt from installed app ``mcp.json``
  files plus ``config/mcp.custom.json``.
* ``config/mcp.custom.json`` — user-authored overrides/additional MCP servers.
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
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MCP_JSON = os.environ.get("AW_MCP_JSON", os.path.join(BASE_DIR, "config", "mcp.json"))
MCP_CUSTOM_JSON = os.environ.get(
    "AW_MCP_CUSTOM_JSON", os.path.join(BASE_DIR, "config", "mcp.custom.json"))
GATEWAY_JSON = os.environ.get("AW_GATEWAY_JSON", os.path.join(BASE_DIR, "config", "gateway.json"))
LINK_TOKENS_JSON = os.environ.get("AW_LINK_TOKENS_JSON", os.path.join(BASE_DIR, "config", "link_tokens.json"))
APP_SCAN_ROOTS = os.environ.get("AW_APP_SCAN_ROOTS", "/workspace/apps")

DEFAULT_MAX_FEDERATION_DEPTH = 6


def _empty_mcp() -> dict:
    return {"mcpServers": {}}


def _read_json(path: str, default: dict | None = None) -> dict:
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else (default or {})
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(default or {})


def _write_json(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def _mcp_servers(data: dict) -> dict:
    servers = data.get("mcpServers") if isinstance(data, dict) else {}
    return servers if isinstance(servers, dict) else {}


def _scan_roots() -> list[Path]:
    roots = []
    for raw in APP_SCAN_ROOTS.split(os.pathsep):
        raw = raw.strip()
        if raw:
            roots.append(Path(raw))
    return roots


def scan_app_mcp_servers(scan_roots: list[Path] | None = None) -> tuple[dict, dict]:
    """Read ``mcp.json`` from each installed app folder.

    Returns ``(servers, sources)`` where sources maps server name to metadata
    about the app file that provided it. Later app folders override earlier
    folders for the same server name; custom config overrides all scanned
    entries during final composition.
    """
    servers: dict = {}
    sources: dict = {}
    final_path = Path(MCP_JSON).resolve()
    custom_path = Path(MCP_CUSTOM_JSON).resolve()
    for root in scan_roots or _scan_roots():
        if not root.is_dir():
            continue
        for app_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            mcp_path = app_dir / "mcp.json"
            try:
                resolved = mcp_path.resolve()
            except OSError:
                continue
            if resolved in (final_path, custom_path) or not mcp_path.is_file():
                continue
            for name, spec in _mcp_servers(_read_json(str(mcp_path), _empty_mcp())).items():
                servers[name] = spec
                sources[name] = {
                    "source": "scanned",
                    "app": app_dir.name,
                    "path": str(mcp_path),
                }
    return servers, sources


def load_custom_mcp_config() -> dict:
    return {"mcpServers": dict(_mcp_servers(_read_json(MCP_CUSTOM_JSON, _empty_mcp())))}


def save_custom_mcp_config(data: dict) -> dict:
    custom = {"mcpServers": dict(_mcp_servers(data))}
    _write_json(MCP_CUSTOM_JSON, custom)
    return custom


def effective_mcp_config(write_final: bool = True) -> dict:
    scanned, sources = scan_app_mcp_servers()
    custom = load_custom_mcp_config()
    merged = dict(scanned)
    for name, spec in custom["mcpServers"].items():
        merged[name] = spec
        sources[name] = {"source": "custom", "path": MCP_CUSTOM_JSON}
    final = {"mcpServers": merged}
    if write_final:
        _write_json(MCP_JSON, final)
    return {
        "custom": custom,
        "scanned": {"mcpServers": scanned},
        "final": final,
        "sources": sources,
        "paths": {
            "mcp_json": MCP_JSON,
            "mcp_custom_json": MCP_CUSTOM_JSON,
            "scan_roots": [str(p) for p in _scan_roots()],
        },
    }


def load_mcp_servers() -> dict:
    return effective_mcp_config(write_final=True)["final"]["mcpServers"]


def load_gateway_config() -> dict:
    try:
        with open(GATEWAY_JSON) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def link_tokens_path() -> str:
    return LINK_TOKENS_JSON


def max_federation_depth() -> int:
    try:
        return int(load_gateway_config().get("max_federation_depth") or DEFAULT_MAX_FEDERATION_DEPTH)
    except (TypeError, ValueError):
        return DEFAULT_MAX_FEDERATION_DEPTH


def gateway_id() -> str:
    """A stable id for this gateway process, used in the federation
    ancestor-chain cycle check. Configurable (``gateway_id`` in
    gateway.json) so a deployment can pin it across restarts; otherwise a
    fresh one is minted per process — fine for cycle detection since a
    changed id only makes the check more conservative, never less safe."""
    configured = (load_gateway_config().get("gateway_id") or "").strip()
    if configured:
        return configured
    return secrets.token_hex(8)


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
