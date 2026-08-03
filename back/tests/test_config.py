from __future__ import annotations

import json

from starlette.testclient import TestClient

from gateway import config
from gateway.server import Gateway, build_app


def _write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def test_effective_mcp_config_scans_apps_and_custom_overrides(tmp_path, monkeypatch):
    apps = tmp_path / "apps"
    final_path = tmp_path / "gateway" / "mcp.json"
    custom_path = tmp_path / "gateway" / "mcp.custom.json"
    _write_json(apps / "app-a" / "mcp.json", {
        "mcpServers": {
            "shared": {"type": "stdio", "command": "from-scan"},
            "scanned-only": {"type": "http", "url": "http://example.test/mcp"},
        }
    })
    _write_json(custom_path, {
        "mcpServers": {
            "shared": {"type": "stdio", "command": "from-custom"},
            "custom-only": {"type": "stdio", "command": "custom"},
        }
    })
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(apps))
    monkeypatch.setattr(config, "MCP_JSON", str(final_path))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(custom_path))

    payload = config.effective_mcp_config(write_final=True)

    assert payload["final"]["mcpServers"]["shared"]["command"] == "from-custom"
    assert payload["final"]["mcpServers"]["scanned-only"]["type"] == "http"
    assert payload["final"]["mcpServers"]["custom-only"]["command"] == "custom"
    assert payload["sources"]["shared"]["source"] == "custom"
    assert json.loads(final_path.read_text()) == payload["final"]


def test_load_specs_auto_trusts_scanned_servers_without_an_allowlist_entry(tmp_path, monkeypatch):
    """Installing an app is enough on its own — Gateway._load_specs() must
    start its contributed (scanned) server even with an empty self.allow.
    Only hand-authored mcp.custom.json entries still need an explicit
    allowlist entry (they aren't reviewed by the app install flow)."""
    apps = tmp_path / "apps"
    custom_path = tmp_path / "gateway" / "mcp.custom.json"
    _write_json(apps / "some-app" / "mcp.json", {
        "mcpServers": {"scanned-app": {"type": "stdio", "command": "from-scan"}}
    })
    _write_json(custom_path, {
        "mcpServers": {"hand-authored": {"type": "stdio", "command": "from-custom"}}
    })
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(apps))
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "gateway" / "mcp.json"))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(custom_path))

    gw = Gateway([])  # empty allowlist — nothing manually approved
    specs = gw._load_specs()

    assert "scanned-app" in specs  # auto-trusted, no allow entry needed
    assert "hand-authored" not in specs  # custom entry still gated

    gw.allow = ["hand-authored"]
    specs = gw._load_specs()
    assert "hand-authored" in specs


def test_admin_config_endpoint_saves_custom_and_rebuilds_final(tmp_path, monkeypatch):
    apps = tmp_path / "apps"
    final_path = tmp_path / "gateway" / "mcp.json"
    custom_path = tmp_path / "gateway" / "mcp.custom.json"
    _write_json(apps / "app-a" / "mcp.json", {
        "mcpServers": {"scanned": {"type": "stdio", "command": "scan"}}
    })
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(apps))
    monkeypatch.setattr(config, "MCP_JSON", str(final_path))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(custom_path))

    app = build_app(Gateway([]), "secret", {})
    with TestClient(app) as client:
        res = client.put(
            "/admin/config",
            headers={"Authorization": "Bearer secret"},
            json={"custom": {"mcpServers": {"custom": {"type": "stdio", "command": "mine"}}}},
        )

    assert res.status_code == 200
    body = res.json()
    assert body["restart_required"] is True
    assert body["final"]["mcpServers"]["scanned"]["command"] == "scan"
    assert body["final"]["mcpServers"]["custom"]["command"] == "mine"
    assert body["token"] == "secret"
    assert json.loads(custom_path.read_text()) == {
        "mcpServers": {"custom": {"command": "mine", "type": "stdio"}}
    }


def test_admin_config_accepts_workspace_identity_header(tmp_path, monkeypatch):
    final_path = tmp_path / "gateway" / "mcp.json"
    custom_path = tmp_path / "gateway" / "mcp.custom.json"
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(tmp_path / "apps"))
    monkeypatch.setattr(config, "MCP_JSON", str(final_path))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(custom_path))

    app = build_app(Gateway([]), "secret", {})
    with TestClient(app) as client:
        res = client.get("/admin/config", headers={"X-AW-Identity-Sub": "user-1"})

    assert res.status_code == 200
    assert res.json()["final"] == {"mcpServers": {}}


def test_register_self_noop_without_host_mcp_json(monkeypatch):
    monkeypatch.setattr(config, "HOST_MCP_JSON", "")
    # Would raise/crash on a bad path if it tried to read or write anything.
    config.register_self_in_host_mcp_json(9200, "tok")


def test_register_self_writes_entry_preserving_other_servers(tmp_path, monkeypatch):
    host_json = tmp_path / ".mcp.json"
    _write_json(host_json, {"mcpServers": {"other-app": {"type": "stdio", "command": "x"}}})
    monkeypatch.setattr(config, "HOST_MCP_JSON", str(host_json))
    monkeypatch.delenv("AW_APP_SELF_HOST", raising=False)

    config.register_self_in_host_mcp_json(9200, "tok-123")

    data = json.loads(host_json.read_text())
    assert data["mcpServers"]["other-app"] == {"type": "stdio", "command": "x"}
    assert data["mcpServers"]["aw-gateway"] == {
        "type": "http",
        "url": "http://127.0.0.1:9200/mcp",
        "headers": {"Authorization": "Bearer tok-123"},
    }


def test_register_self_uses_aw_app_self_host_when_set(tmp_path, monkeypatch):
    """127.0.0.1 only resolves inside THIS container's own netns — on
    aw-workspace, AW_APP_SELF_HOST (injected by ContainerSupervisor.start())
    is the name siblings actually reach this container by, and must be
    preferred over the loopback fallback."""
    host_json = tmp_path / ".mcp.json"
    monkeypatch.setattr(config, "HOST_MCP_JSON", str(host_json))
    monkeypatch.setenv("AW_APP_SELF_HOST", "aw-app-mcp-gateway")

    config.register_self_in_host_mcp_json(9200, "tok-123")

    data = json.loads(host_json.read_text())
    assert data["mcpServers"]["aw-gateway"]["url"] == "http://aw-app-mcp-gateway:9200/mcp"


def test_register_self_creates_missing_host_mcp_json(tmp_path, monkeypatch):
    host_json = tmp_path / "nested" / ".mcp.json"
    monkeypatch.setattr(config, "HOST_MCP_JSON", str(host_json))
    monkeypatch.delenv("AW_APP_SELF_HOST", raising=False)

    config.register_self_in_host_mcp_json(9200, "tok")

    assert json.loads(host_json.read_text())["mcpServers"]["aw-gateway"]["url"] == \
        "http://127.0.0.1:9200/mcp"


def test_register_self_is_idempotent(tmp_path, monkeypatch):
    host_json = tmp_path / ".mcp.json"
    _write_json(host_json, {"mcpServers": {}})
    monkeypatch.setattr(config, "HOST_MCP_JSON", str(host_json))

    config.register_self_in_host_mcp_json(9200, "tok")
    written_at = host_json.stat().st_mtime_ns
    config.register_self_in_host_mcp_json(9200, "tok")

    assert host_json.stat().st_mtime_ns == written_at  # no rewrite — same entry


def test_workspace_name_prefers_gateway_json_over_env(tmp_path, monkeypatch):
    gw_json = tmp_path / "gateway.json"
    _write_json(gw_json, {"workspace_name": "configured-ws"})
    monkeypatch.setattr(config, "GATEWAY_JSON", str(gw_json))
    monkeypatch.setenv("AW_WORKSPACE_SLUG", "env-ws")
    assert config.workspace_name() == "configured-ws"


def test_workspace_name_falls_back_to_env_var(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "GATEWAY_JSON", str(tmp_path / "missing-gateway.json"))
    monkeypatch.setenv("AW_WORKSPACE_SLUG", "env-ws")
    assert config.workspace_name() == "env-ws"


def test_workspace_name_empty_when_neither_set(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "GATEWAY_JSON", str(tmp_path / "missing-gateway.json"))
    monkeypatch.delenv("AW_WORKSPACE_SLUG", raising=False)
    assert config.workspace_name() == ""


async def test_local_upstream_tool_names_are_namespaced_by_workspace(tmp_path, monkeypatch):
    custom_path = tmp_path / "mcp.custom.json"
    _write_json(custom_path, {"mcpServers": {
        "example-echo": {"type": "stdio", "command": "python3",
                          "args": ["-m", "gateway.examples.echo_server"], "enabled": True},
    }})
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(tmp_path / "apps"))
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(custom_path))

    gw = Gateway(["example-echo"], workspace_name="fredericowu")
    await gw.start()

    assert "example-echo" in gw.upstreams  # dispatch key stays unprefixed
    assert "fredericowu__example_echo__echo" in gw.routes
    assert gw.routes["fredericowu__example_echo__echo"] == ("example-echo", "echo")
    names = [t["name"] for t in gw.agg_tools]
    assert "fredericowu__example_echo__echo" in names
    assert "example_echo__echo" not in names

    resp = await gw.handle({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "fredericowu__example_echo__echo", "arguments": {"text": "hi"}},
    })
    assert resp["result"]["content"][0]["text"] == "hi"


async def test_local_upstream_tool_names_unprefixed_without_workspace_name(tmp_path, monkeypatch):
    custom_path = tmp_path / "mcp.custom.json"
    _write_json(custom_path, {"mcpServers": {
        "example-echo": {"type": "stdio", "command": "python3",
                          "args": ["-m", "gateway.examples.echo_server"], "enabled": True},
    }})
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(tmp_path / "apps"))
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(custom_path))

    gw = Gateway(["example-echo"], workspace_name="")
    await gw.start()

    assert "example_echo__echo" in gw.routes


def test_admin_config_get_returns_gateway_bearer_token(tmp_path, monkeypatch):
    final_path = tmp_path / "gateway" / "mcp.json"
    custom_path = tmp_path / "gateway" / "mcp.custom.json"
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(tmp_path / "apps"))
    monkeypatch.setattr(config, "MCP_JSON", str(final_path))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(custom_path))

    app = build_app(Gateway([]), "top-secret-token", {})
    with TestClient(app) as client:
        res = client.get("/admin/config", headers={"Authorization": "Bearer top-secret-token"})

    assert res.status_code == 200
    assert res.json()["token"] == "top-secret-token"


ECHO_SPEC = {
    "type": "stdio", "command": "python3",
    "args": ["-m", "gateway.examples.echo_server"], "enabled": True,
}


def _setup_reload_paths(tmp_path, monkeypatch, apps: dict):
    """Write each {app_name: {server_name: spec}} as that app's mcp.json
    under a fresh scan root, and point config at fresh mcp.json/custom.json
    paths — the standard fixture shape for every reload() test below."""
    apps_root = tmp_path / "apps"
    for app_name, servers in apps.items():
        _write_json(apps_root / app_name / "mcp.json", {"mcpServers": servers})
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(apps_root))
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))


async def test_reload_starts_a_newly_added_server(tmp_path, monkeypatch):
    _setup_reload_paths(tmp_path, monkeypatch, {"app-a": {}})
    gw = Gateway(["echo"])
    await gw.start()
    assert gw.upstreams == {}

    _setup_reload_paths(tmp_path, monkeypatch, {"app-a": {"echo": ECHO_SPEC}})
    result = await gw.reload()

    assert result["added"] == ["echo"]
    assert result["removed"] == [] and result["changed"] == [] and result["failed"] == []
    assert "echo" in gw.upstreams
    assert "echo__echo" in gw.routes


async def test_reload_stops_a_removed_server_and_drops_its_routes(tmp_path, monkeypatch):
    _setup_reload_paths(tmp_path, monkeypatch, {"app-a": {"echo": ECHO_SPEC}})
    gw = Gateway(["echo"])
    await gw.start()
    assert "echo" in gw.upstreams
    old_upstream = gw.upstreams["echo"]

    _setup_reload_paths(tmp_path, monkeypatch, {"app-a": {}})
    result = await gw.reload()

    assert result["removed"] == ["echo"]
    assert "echo" not in gw.upstreams
    assert "echo__echo" not in gw.routes
    assert old_upstream.proc is None  # stopped


async def test_reload_restarts_a_server_whose_spec_changed(tmp_path, monkeypatch):
    _setup_reload_paths(tmp_path, monkeypatch, {"app-a": {"echo": ECHO_SPEC}})
    gw = Gateway(["echo"])
    await gw.start()
    old_upstream = gw.upstreams["echo"]

    changed_spec = {**ECHO_SPEC, "env": {"SOME_FLAG": "1"}}
    _setup_reload_paths(tmp_path, monkeypatch, {"app-a": {"echo": changed_spec}})
    result = await gw.reload()

    assert result["changed"] == ["echo"]
    assert gw.upstreams["echo"] is not old_upstream  # torn down + rebuilt
    assert "echo__echo" in gw.routes


async def test_reload_leaves_an_unchanged_server_running_untouched(tmp_path, monkeypatch):
    _setup_reload_paths(tmp_path, monkeypatch, {"app-a": {"echo": ECHO_SPEC}})
    gw = Gateway(["echo"])
    await gw.start()
    old_upstream = gw.upstreams["echo"]

    # Same spec, re-scanned from disk (a fresh dict, same content).
    _setup_reload_paths(tmp_path, monkeypatch, {"app-a": {"echo": dict(ECHO_SPEC)}})
    result = await gw.reload()

    assert result["unchanged"] == ["echo"]
    assert result["added"] == [] and result["removed"] == [] and result["changed"] == []
    assert gw.upstreams["echo"] is old_upstream  # same process, not restarted


def test_reload_endpoint_requires_admin_auth(tmp_path, monkeypatch):
    _setup_reload_paths(tmp_path, monkeypatch, {"app-a": {}})
    app = build_app(Gateway([]), "top-secret-token", {})
    with TestClient(app) as client:
        unauth = client.post("/reload")
        assert unauth.status_code == 401

        via_bearer = client.post("/reload", headers={"Authorization": "Bearer top-secret-token"})
        assert via_bearer.status_code == 200
        assert via_bearer.json()["upstreams"] == []

        via_identity = client.post("/reload", headers={"X-AW-Identity-Sub": "system"})
        assert via_identity.status_code == 200
