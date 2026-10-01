"""Opt-in ``require_token: false`` (Kanban card 3ec5bf3b-9510-81eb-a188-cc06837aa793):
a gateway reachable only over a trusted private network (e.g. a Kali-sandboxed
leaf with no published port) can skip the bearer check on ``/mcp`` — every
other route stays gated, and the combination with ``public: true`` must
refuse to start rather than silently serve a tokenless public endpoint.
"""

from __future__ import annotations

import json

import pytest
from starlette.testclient import TestClient

from gateway import config
from gateway.server import Gateway, build_app, main


def _write_gateway_json(monkeypatch, tmp_path, data):
    path = tmp_path / "gateway.json"
    path.write_text(json.dumps(data))
    monkeypatch.setattr(config, "GATEWAY_JSON", str(path))


def test_default_still_requires_a_bearer_token(tmp_path):
    app = build_app(Gateway([]), "secret", {})
    with TestClient(app) as client:
        res = client.post("/mcp", json={"jsonrpc": "2.0", "id": 1,
                                         "method": "tools/list", "params": {}})
    assert res.status_code == 401


def test_require_token_false_skips_auth_on_mcp(tmp_path):
    app = build_app(Gateway([]), "secret", {}, require_token=False)
    with TestClient(app) as client:
        res = client.post("/mcp", json={"jsonrpc": "2.0", "id": 1,
                                         "method": "tools/list", "params": {}})
    assert res.status_code == 200


def test_require_token_false_does_not_relax_admin_routes(tmp_path):
    app = build_app(Gateway([]), "secret", {}, require_token=False)
    with TestClient(app) as client:
        res = client.get("/admin/config")
    assert res.status_code == 401


def test_config_require_token_reads_gateway_json(tmp_path, monkeypatch):
    _write_gateway_json(monkeypatch, tmp_path, {"require_token": False})
    assert config.require_token() is False


def test_config_require_token_defaults_true(tmp_path, monkeypatch):
    _write_gateway_json(monkeypatch, tmp_path, {})
    assert config.require_token() is True


def test_config_public_exposure_defaults_false(tmp_path, monkeypatch):
    _write_gateway_json(monkeypatch, tmp_path, {})
    assert config.public_exposure_configured() is False


def test_main_refuses_to_boot_tokenless_and_public(tmp_path, monkeypatch):
    _write_gateway_json(monkeypatch, tmp_path, {"require_token": False, "public": True})
    monkeypatch.setattr("sys.argv", ["gateway"])
    with pytest.raises(SystemExit):
        main()


def test_main_boots_tokenless_when_not_public(tmp_path, monkeypatch, caplog):
    _write_gateway_json(monkeypatch, tmp_path, {"require_token": False})
    monkeypatch.setattr("sys.argv", ["gateway", "--port", "0"])

    class _FakeUvicorn:
        @staticmethod
        def run(app, host, port, log_level):
            pass

    monkeypatch.setitem(__import__("sys").modules, "uvicorn", _FakeUvicorn)
    with caplog.at_level("WARNING"):
        main()
    assert any("require_token=false" in r.message for r in caplog.records)
