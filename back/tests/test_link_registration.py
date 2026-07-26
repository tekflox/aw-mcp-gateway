"""Reverse-registration TODOs (deliverable B): real token verification,
scope enforcement, and app-name collision handling on ``/link``.
"""

from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from gateway.server import Gateway, build_app
from gateway.token_store import FileTokenStore


@pytest.fixture
def gateway_app(tmp_path):
    store = FileTokenStore(str(tmp_path / "link_tokens.json"))
    gw = Gateway([])
    app = build_app(gw, "citoken", {}, token_store=store)
    return gw, store, app


ECHO_TOOL = {"name": "echo", "description": "", "inputSchema": {"type": "object"}}
SHELL_TOOL = {"name": "shell", "description": "", "inputSchema": {"type": "object"}}


def test_register_with_unknown_token_is_rejected(gateway_app):
    _gw, _store, app = gateway_app
    with TestClient(app) as client:
        with pytest.raises(Exception):
            with client.websocket_connect("/link?token=awlk_deadbeefdeadbeef_notreal") as ws:
                ws.receive_text()


def test_register_publishes_tools_and_routes(gateway_app):
    gw, store, app = gateway_app
    full, _record = store.mint(label="full")
    with TestClient(app) as client:
        with client.websocket_connect(f"/link?token={full}") as ws:
            ws.send_json({"type": "register", "app_name": "browser1", "tools": [ECHO_TOOL]})
            ack = ws.receive_json()
            assert ack == {"type": "registered", "app_name": "browser1"}
            assert "browser1__echo" in gw.routes
            assert "browser1" in gw.remotes
        # disconnected: routes withdrawn, no dead-session routing left behind
        assert "browser1" not in gw.remotes
        assert "browser1__echo" not in gw.routes


def test_scope_filters_disallowed_tools(gateway_app):
    gw, store, app = gateway_app
    full, _record = store.mint(label="echo-only", scopes=["*:echo"])
    with TestClient(app) as client:
        with client.websocket_connect(f"/link?token={full}") as ws:
            ws.send_json({"type": "register", "app_name": "toolbox",
                          "tools": [ECHO_TOOL, SHELL_TOOL]})
            ack = ws.receive_json()
            assert ack == {"type": "registered", "app_name": "toolbox"}
            assert "toolbox__echo" in gw.routes
            assert "toolbox__shell" not in gw.routes


def test_scope_rejects_registration_when_nothing_matches(gateway_app):
    _gw, store, app = gateway_app
    full, _record = store.mint(label="shell-only", scopes=["*:shell"])
    with TestClient(app) as client:
        with pytest.raises(Exception):
            with client.websocket_connect(f"/link?token={full}") as ws:
                ws.send_json({"type": "register", "app_name": "toolbox", "tools": [ECHO_TOOL]})
                ws.receive_json()


def test_revoked_token_is_rejected(gateway_app):
    _gw, store, app = gateway_app
    full, record = store.mint(label="temp")
    store.revoke(record.id)
    with TestClient(app) as client:
        with pytest.raises(Exception):
            with client.websocket_connect(f"/link?token={full}") as ws:
                ws.receive_text()


def test_same_token_reconnect_keeps_same_public_name(gateway_app):
    gw, store, app = gateway_app
    full, _record = store.mint(label="stable-host")
    with TestClient(app) as client:
        with client.websocket_connect(f"/link?token={full}") as ws:
            ws.send_json({"type": "register", "app_name": "Browser", "tools": [ECHO_TOOL]})
            ws.receive_json()
        assert "Browser" not in gw.remotes  # withdrawn on disconnect

        with client.websocket_connect(f"/link?token={full}") as ws2:
            ws2.send_json({"type": "register", "app_name": "Browser", "tools": [ECHO_TOOL]})
            ack = ws2.receive_json()
            assert ack == {"type": "registered", "app_name": "Browser"}
            assert "Browser__echo" in gw.routes
            assert "Browser 1" not in gw.remotes  # no bogus renumbering on a plain reconnect


def test_app_name_collision_from_different_tokens_gets_numbered(gateway_app):
    gw, store, app = gateway_app
    full_1, _r1 = store.mint(label="host-1")
    full_2, _r2 = store.mint(label="host-2")
    with TestClient(app) as client:
        with client.websocket_connect(f"/link?token={full_1}") as ws1:
            ws1.send_json({"type": "register", "app_name": "Browser", "tools": [ECHO_TOOL]})
            ack1 = ws1.receive_json()
            assert ack1 == {"type": "registered", "app_name": "Browser"}

            with client.websocket_connect(f"/link?token={full_2}") as ws2:
                ws2.send_json({"type": "register", "app_name": "Browser", "tools": [ECHO_TOOL]})
                ack2 = ws2.receive_json()
                # Fred's decision: BOTH get numbered, not `{app}_{server}__{tool}`.
                assert ack2 == {"type": "registered", "app_name": "Browser 2"}
                assert "Browser 1" in gw.remotes
                assert "Browser 2" in gw.remotes
                assert "Browser 1__echo" in gw.routes
                assert "Browser 2__echo" in gw.routes
                assert "Browser" not in gw.remotes
