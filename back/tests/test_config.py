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
