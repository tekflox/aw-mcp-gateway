"""The generic external-upstream CRUD (Architect design, Kanban card
3e95bf3b-9510-813c-af4a-f475eada9ec3): GET/PUT/DELETE /admin/external-upstreams,
credential refs resolved only at use time, and the allow-list sync a raw
``PUT /admin/config`` save can't do (server.py:891-903 — gateway.allow is
captured once at boot, so a hand-authored custom entry never actually starts
until the allowlist is updated too).
"""

from __future__ import annotations

import json

from starlette.testclient import TestClient

from gateway import config
from gateway.server import Gateway, build_app
from gateway.upstream import HttpUpstream

ECHO_SPEC = {
    "type": "stdio", "command": "python3",
    "args": ["-m", "gateway.examples.echo_server"],
}


def _write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


def _isolate(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(tmp_path / "apps"))
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))
    monkeypatch.setattr(config, "UPSTREAM_SECRETS_JSON", str(tmp_path / "upstream_secrets.json"))
    config._secrets_cache["key"] = None
    config._secrets_cache["data"] = {}


def _app(tmp_path, monkeypatch, allow=None):
    _isolate(tmp_path, monkeypatch)
    return build_app(Gateway(list(allow or [])), "secret", {})


ADMIN = {"Authorization": "Bearer secret"}


# ── GET ──────────────────────────────────────────────────────────────────

def test_get_returns_empty_list_when_none_registered(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.get("/admin/external-upstreams", headers=ADMIN)
    assert res.status_code == 200
    assert res.json() == {"upstreams": []}


def test_get_requires_admin_auth(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.get("/admin/external-upstreams")
    assert res.status_code == 401


def test_get_only_lists_custom_entries_not_scanned_app_upstreams(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _write_json(tmp_path / "apps" / "some-app" / "mcp.json", {
        "mcpServers": {"scanned-one": {"type": "stdio", "command": "from-scan"}}
    })
    app = build_app(Gateway([]), "secret", {})
    with TestClient(app) as client:
        res = client.get("/admin/external-upstreams", headers=ADMIN)
    assert res.json() == {"upstreams": []}


# ── PUT validation ───────────────────────────────────────────────────────

def test_put_requires_admin_auth(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/x", json={"spec": {"type": "http", "url": "http://x"}})
    assert res.status_code == 401


def test_put_rejects_unsupported_type(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/x", headers=ADMIN,
                          json={"spec": {"type": "carrier-pigeon", "url": "http://x"}})
    assert res.status_code == 400


def test_put_rejects_gateway_without_url(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/x", headers=ADMIN,
                          json={"spec": {"type": "gateway"}})
    assert res.status_code == 400


def test_put_rejects_gateway_allowed_tools_that_is_not_a_string_list(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/x", headers=ADMIN,
                          json={"spec": {"type": "gateway", "url": "http://x/mcp",
                                         "allowed_tools": "not-a-list"}})
    assert res.status_code == 400


def test_put_rejects_stdio_without_command(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/x", headers=ADMIN, json={"spec": {"type": "stdio"}})
    assert res.status_code == 400


def test_put_rejects_http_without_url(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/x", headers=ADMIN, json={"spec": {"type": "http"}})
    assert res.status_code == 400


def test_put_rejects_name_colliding_with_a_scanned_app_upstream(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    _write_json(tmp_path / "apps" / "some-app" / "mcp.json", {
        "mcpServers": {"scanned-one": {"type": "stdio", "command": "from-scan"}}
    })
    app = build_app(Gateway([]), "secret", {})
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/scanned-one", headers=ADMIN,
                          json={"spec": {"type": "http", "url": "http://x"}})
    assert res.status_code == 409


# ── PUT happy path: CRUD + allow-list sync + reload ────────────────────

def test_put_creates_and_starts_a_stdio_upstream_end_to_end(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/echo", headers=ADMIN, json={"spec": ECHO_SPEC})

        assert res.status_code == 200
        body = res.json()
        assert body["status"] == "running"
        assert body["tools"] == 1
        assert body["spec"] == ECHO_SPEC

        # Visible on GET too, and allow-listed both on disk and in memory —
        # the exact trap a raw PUT /admin/config falls into (server.py:891-903).
        listed = client.get("/admin/external-upstreams", headers=ADMIN).json()["upstreams"]
        assert [u["name"] for u in listed] == ["echo"]
        assert json.loads((tmp_path / "gateway.json").read_text())["upstreams"] == ["echo"]
        # mcp.custom.json holds the entry too, unresolved (nothing to resolve here).
        assert json.loads((tmp_path / "mcp.custom.json").read_text())["mcpServers"]["echo"] == ECHO_SPEC


def test_put_edit_replaces_the_spec_of_an_existing_entry(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        client.put("/admin/external-upstreams/echo", headers=ADMIN, json={"spec": ECHO_SPEC})

        changed = {**ECHO_SPEC, "env": {"SOME_FLAG": "1"}}
        res = client.put("/admin/external-upstreams/echo", headers=ADMIN, json={"spec": changed})

        assert res.status_code == 200
        assert res.json()["spec"]["env"] == {"SOME_FLAG": "1"}
        assert json.loads((tmp_path / "mcp.custom.json").read_text())["mcpServers"]["echo"]["env"] == \
            {"SOME_FLAG": "1"}


def test_put_warns_when_allow_env_override_is_set(tmp_path, monkeypatch):
    monkeypatch.setenv("AW_MCP_GATEWAY_ALLOW", "*")
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/echo", headers=ADMIN, json={"spec": ECHO_SPEC})
    assert res.status_code == 200
    assert "AW_MCP_GATEWAY_ALLOW" in res.json()["warning"]


def test_put_with_unknown_secret_ref_starts_parked_with_a_clean_error(tmp_path, monkeypatch):
    """No secret stored for the name the spec references — must surface as a
    clear, retryable-looking parked error in the response, not a 500."""
    app = _app(tmp_path, monkeypatch)
    spec = {**ECHO_SPEC, "env": {"TOKEN": "${secret:never-stored}"}}
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/echo", headers=ADMIN, json={"spec": spec})

    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "error"
    assert "never-stored" in body["error"]


# ── Secrets: stored write-only, spec/mcp.json stay in reference form ──────

def test_put_with_secret_stores_it_and_spec_keeps_the_reference_form(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    spec = {**ECHO_SPEC, "env": {"TOKEN": "${secret:echo-TOKEN}"}}
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/echo", headers=ADMIN,
                          json={"spec": spec, "secrets": {"echo-TOKEN": "super-secret-value"}})

    assert res.status_code == 200
    body = res.json()
    assert body["spec"]["env"]["TOKEN"] == "${secret:echo-TOKEN}"
    assert config.resolve_secret_refs({"T": "${secret:echo-TOKEN}"}) == {"T": "super-secret-value"}


def test_secret_value_never_appears_in_any_get_response_or_written_config_files(tmp_path, monkeypatch):
    """Grep-style assertion the card asks for explicitly: the raw secret
    value must not leak into GET /admin/external-upstreams, GET
    /admin/config, mcp.json, or mcp.custom.json — only its ${secret:...}
    reference is allowed to appear anywhere but upstream_secrets.json."""
    app = _app(tmp_path, monkeypatch)
    secret_value = "sekrit-do-not-leak-9f8e7d"
    spec = {**ECHO_SPEC, "env": {"TOKEN": "${secret:echo-TOKEN}"}}
    with TestClient(app) as client:
        put_res = client.put("/admin/external-upstreams/echo", headers=ADMIN,
                              json={"spec": spec, "secrets": {"echo-TOKEN": secret_value}})
        get_list = client.get("/admin/external-upstreams", headers=ADMIN)
        get_config = client.get("/admin/config", headers=ADMIN)

    assert secret_value not in put_res.text
    assert secret_value not in get_list.text
    assert secret_value not in get_config.text
    assert secret_value not in (tmp_path / "mcp.json").read_text()
    assert secret_value not in (tmp_path / "mcp.custom.json").read_text()
    # It DOES live in the write-only side store — that's the point of it.
    assert secret_value in (tmp_path / "upstream_secrets.json").read_text()


# ── PUT: gateway-type upstreams (generic federation, allowed_tools) ───────
#
# A real end-to-end "does a live peer's tools actually get pulled and
# scoped by allowed_tools" is already covered by test_federation.py (which
# runs a real second gateway over real HTTP). The tests below stay
# synchronous and point at an unreachable URL on purpose — mixing a live
# async uvicorn peer with starlette's sync TestClient (its own blocking
# portal/event loop) inside one async test deadlocked (a caller_context
# resource ending up bound across two different event loops); exercising
# the endpoint's own wiring/validation doesn't need a real peer to do that.

def test_put_accepts_gateway_type_and_attempts_a_real_start(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.put("/admin/external-upstreams/leaf", headers=ADMIN, json={"spec": {
            "type": "gateway", "url": "http://127.0.0.1:1/mcp", "token": "x",
            "allowed_tools": ["some_tool"],
        }})

    # Validation passed (kind="gateway" is accepted) and it genuinely tried
    # to connect — port 1 refusing the connection proves this isn't a
    # config-shape rejection, it's a real (failed) start attempt.
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "error"
    assert body["spec"]["allowed_tools"] == ["some_tool"]


def test_put_gateway_upstream_secret_token_never_leaks_in_any_response(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    secret_value = "sekrit-peer-token-9f8e7d"
    with TestClient(app) as client:
        put_res = client.put("/admin/external-upstreams/leaf", headers=ADMIN, json={
            "spec": {"type": "gateway", "url": "http://127.0.0.1:1/mcp",
                     "token": "${secret:leaf-token}", "allowed_tools": []},
            "secrets": {"leaf-token": secret_value},
        })
        get_res = client.get("/admin/external-upstreams", headers=ADMIN)
    assert secret_value not in put_res.text
    assert secret_value not in get_res.text
    assert config.resolve_secret_refs({"t": "${secret:leaf-token}"}) == {"t": secret_value}


# ── Probe: preview a candidate gateway's tools before saving anything ─────

def test_probe_gateway_requires_admin_auth(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.post("/admin/external-upstreams/probe-gateway", json={"url": "http://x/mcp"})
    assert res.status_code == 401


def test_probe_gateway_requires_url(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.post("/admin/external-upstreams/probe-gateway", headers=ADMIN, json={})
    assert res.status_code == 400


def test_probe_gateway_reports_a_clean_error_for_an_unreachable_url(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.post("/admin/external-upstreams/probe-gateway", headers=ADMIN,
                           json={"url": "http://127.0.0.1:1/mcp", "token": "x"})
    assert res.status_code == 502


def test_probe_gateway_does_not_persist_anything(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        client.post("/admin/external-upstreams/probe-gateway", headers=ADMIN,
                     json={"url": "http://127.0.0.1:1/mcp", "token": "x"})
        listed = client.get("/admin/external-upstreams", headers=ADMIN).json()["upstreams"]
    assert listed == []


# ── DELETE ───────────────────────────────────────────────────────────────

def test_delete_unknown_name_returns_404(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        res = client.delete("/admin/external-upstreams/nope", headers=ADMIN)
    assert res.status_code == 404


def test_delete_requires_admin_auth(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        client.put("/admin/external-upstreams/echo", headers=ADMIN, json={"spec": ECHO_SPEC})
        res = client.delete("/admin/external-upstreams/echo")
    assert res.status_code == 401


def test_delete_removes_entry_stops_it_and_cleans_up_the_allowlist(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    with TestClient(app) as client:
        client.put("/admin/external-upstreams/echo", headers=ADMIN, json={"spec": ECHO_SPEC})

        res = client.delete("/admin/external-upstreams/echo", headers=ADMIN)

        assert res.status_code == 200
        listed = client.get("/admin/external-upstreams", headers=ADMIN).json()["upstreams"]
        assert listed == []
        assert "echo" not in json.loads((tmp_path / "gateway.json").read_text()).get("upstreams", [])
        assert "echo" not in json.loads((tmp_path / "mcp.custom.json").read_text())["mcpServers"]


def test_delete_removes_a_secret_only_this_entry_referenced(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    spec = {**ECHO_SPEC, "env": {"TOKEN": "${secret:echo-TOKEN}"}}
    with TestClient(app) as client:
        client.put("/admin/external-upstreams/echo", headers=ADMIN,
                    json={"spec": spec, "secrets": {"echo-TOKEN": "v"}})
        assert config.load_upstream_secret_names() == {"echo-TOKEN"}

        client.delete("/admin/external-upstreams/echo", headers=ADMIN)

    assert config.load_upstream_secret_names() == set()


def test_delete_keeps_a_secret_still_referenced_by_another_entry(tmp_path, monkeypatch):
    app = _app(tmp_path, monkeypatch)
    spec_a = {**ECHO_SPEC, "env": {"TOKEN": "${secret:shared}"}}
    spec_b = {"type": "http", "url": "http://example.test/mcp", "headers": {"Authorization": "${secret:shared}"}}
    with TestClient(app) as client:
        client.put("/admin/external-upstreams/echo-a", headers=ADMIN,
                    json={"spec": spec_a, "secrets": {"shared": "v"}})
        client.put("/admin/external-upstreams/http-b", headers=ADMIN, json={"spec": spec_b})

        client.delete("/admin/external-upstreams/echo-a", headers=ADMIN)

    assert config.load_upstream_secret_names() == {"shared"}


# ── Ref resolution at use time (Upstream.start / HttpUpstream headers) ───

async def test_stdio_spawn_resolves_env_refs_without_mutating_spec(tmp_path, monkeypatch):
    """Upstream._spawn() must inject the RESOLVED value into the actual
    child's environment while leaving self.spec/self.env_extra in
    reference form — the invariant Gateway.reload()'s spec diff depends on
    (server.py:219's ``up.spec != new_specs[n]``)."""
    from gateway.upstream import Upstream

    _isolate(tmp_path, monkeypatch)
    config.save_upstream_secret("tok", "resolved-value")

    spec = {"type": "stdio", "command": "python3",
            "args": ["-m", "gateway.examples.echo_server"],
            "env": {"TOKEN": "${secret:tok}"}}
    up = Upstream("echo", spec)
    try:
        await up.start()
        assert up.tools  # real handshake succeeded
    finally:
        await up.stop()

    assert up.spec is spec
    assert up.env_extra == {"TOKEN": "${secret:tok}"}  # never resolved in place


def test_http_client_headers_resolves_refs_per_call_without_mutating_spec(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    config.save_upstream_secret("google-token", "abc123")

    spec = {"type": "http", "url": "http://example.test/mcp",
            "headers": {"Authorization": "Bearer ${secret:google-token}"}}
    up = HttpUpstream("google-workspace", spec)

    headers = up._client_headers()

    assert headers["Authorization"] == "Bearer abc123"
    assert up.spec is spec
    assert up._extra_headers == {"Authorization": "Bearer ${secret:google-token}"}

    # Rotate the secret — the very next call must see the new value, with
    # no restart and no re-construction of the HttpUpstream.
    config.save_upstream_secret("google-token", "rotated")
    assert up._client_headers()["Authorization"] == "Bearer rotated"


async def test_start_one_reports_a_clean_error_for_an_unknown_secret_ref_stdio(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    gw = Gateway([])
    spec = {"type": "stdio", "command": "python3",
            "args": ["-m", "gateway.examples.echo_server"],
            "env": {"TOKEN": "${secret:never-stored}"}}

    error = await gw._start_one("echo", spec)

    assert error is not None
    assert "never-stored" in error
    assert "echo" not in gw.upstreams


async def test_start_one_reports_a_clean_error_for_an_unknown_secret_ref_http(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    gw = Gateway([])
    spec = {"type": "http", "url": "http://example.test/mcp",
            "headers": {"Authorization": "${secret:never-stored}"}}

    error = await gw._start_one("http-up", spec)

    assert error is not None
    assert "never-stored" in error
    assert "http-up" not in gw.upstreams
