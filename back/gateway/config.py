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

# Path (inside the container) to the aw-workspace HOST's root .mcp.json — only
# present when installed as an aw-workspace app with the $AW_MCP_JSON volume
# (see aw-app.json, permission 'mcp:register-gateway'). Empty when running
# standalone (bare `python3 -m gateway.server`, tests, or outside aw-workspace
# entirely) — register_self_in_host_mcp_json() no-ops in that case.
HOST_MCP_JSON = os.environ.get("AW_HOST_MCP_JSON", "")

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


def workspace_name() -> str:
    """Slug of the aw-workspace this gateway runs inside — every published
    tool name is namespaced under it (``{workspace}__{server}__{tool}``) so
    tools from different workspaces never collide once aggregated by a
    client, or federated further downstream by another gateway.

    Prefers an explicit ``workspace_name`` in ``config/gateway.json``; falls
    back to the ``AW_WORKSPACE_SLUG`` env var aw-workspace injects into every
    container app it runs (see aw-workspace's ``ContainerRuntime.run``).
    Empty when neither is set — tool names stay unprefixed, same as before
    this existed (a bare local dev checkout has no workspace to name)."""
    configured = (load_gateway_config().get("workspace_name") or "").strip()
    if configured:
        return configured
    return (os.environ.get("AW_WORKSPACE_SLUG") or "").strip()


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


def register_self_in_host_mcp_json(port: int, bearer_token: str) -> None:
    """Best-effort: add/update this gateway's own entry in the aw-workspace
    host's root ``.mcp.json`` so an MCP-JSON-reading client (Claude Code, any
    other agent CLI) picks up the gateway automatically right after install —
    no manual edit needed. Runs once at boot (see server.py's lifespan).

    No-op when ``HOST_MCP_JSON`` is unset — a bare ``python3 -m gateway.server``
    dev run, or the test suite, has no host mount and nothing to write.
    Idempotent: skips the write entirely if the entry already matches, so
    restarts don't churn the file or spam the log.
    """
    if not HOST_MCP_JSON:
        return
    # 127.0.0.1 would resolve to THIS container's own loopback, not to
    # whatever process reads the host .mcp.json (a sibling container on the
    # workspace's shared podman network, in the aw-workspace deployment this
    # was built for). AW_APP_SELF_HOST is aw-workspace's own name for this
    # container (`aw-app-<slug>`, injected by ContainerSupervisor.start()) —
    # exactly what siblings already resolve it by via aardvark-dns. Falls
    # back to 127.0.0.1 for a bare/standalone run with no such env (dev,
    # tests, non-aw-workspace deployments) where that fallback is at least
    # sometimes correct (e.g. host networking).
    host = os.environ.get("AW_APP_SELF_HOST", "127.0.0.1")
    entry = {
        "type": "http",
        "url": f"http://{host}:{port}/mcp",
        "headers": {"Authorization": f"Bearer {bearer_token}"},
    }
    data = _read_json(HOST_MCP_JSON, _empty_mcp())
    servers = _mcp_servers(data)
    if servers.get("aw-gateway") == entry:
        return
    servers["aw-gateway"] = entry
    data["mcpServers"] = servers
    import logging
    log = logging.getLogger("aw-mcp-gateway")
    try:
        _write_json(HOST_MCP_JSON, data)
        log.info("registered self as 'aw-gateway' in host %s", HOST_MCP_JSON)
    except OSError as e:
        log.warning("could not write host mcp.json at %s: %s", HOST_MCP_JSON, e)
