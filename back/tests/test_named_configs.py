"""Named configs (scoped ``/mcp/<name>`` profiles) — allowlist, run policy,
approval gate, namespace injection, and the admin CRUD that edits them."""

from __future__ import annotations

import json

import pytest
from starlette.testclient import TestClient

from gateway import config
from gateway import config_gateway as cgw_mod
from gateway.config_gateway import ConfigGateway, policy_upstreams
from gateway.server import Gateway, build_app

TOKEN = "test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def _write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


async def _no_sleep(_seconds):
    """Collapse the approval gate's 2s poll interval so a test that exercises
    the real ``_await_approval`` loop doesn't take five minutes to fail."""
    return None


class _FakeUpstream:
    """Records what actually reached the upstream, so a test can assert on the
    arguments the gateway injected rather than only on the reply."""

    def __init__(self, name: str, tools: list[str]):
        self.name = name
        self.spec: dict = {}
        self.tools = [{"name": t, "description": t, "inputSchema": {"type": "object"}}
                      for t in tools]
        self.calls: list[tuple[str, dict]] = []

    async def call_tool(self, tool: str, arguments: dict, req_id) -> dict:
        self.calls.append((tool, arguments))
        return {"jsonrpc": "2.0", "id": req_id, "result": {"content": [], "isError": False}}


def _gateway(upstreams: dict[str, list[str]], workspace_name: str = "") -> Gateway:
    gw = Gateway([], gateway_id="test-gw", workspace_name=workspace_name)
    for name, tools in upstreams.items():
        up = _FakeUpstream(name, tools)
        gw.upstreams[name] = up
        for tool in up.tools:
            gw._add_route(name, tool)
    return gw


def _cgw(gw: Gateway, spec: dict, name: str = "profile") -> ConfigGateway:
    return ConfigGateway(gw, spec, name=name, agents_base="http://ap.test",
                         upstream_roles=policy_upstreams())


async def _call(cgw: ConfigGateway, tool: str, arguments: dict | None = None) -> dict:
    return await cgw.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                             "params": {"name": tool, "arguments": arguments or {}}})


# ── Upstream allowlist ──────────────────────────────────────────────────────


async def test_tools_list_is_restricted_and_prefix_stripped():
    gw = _gateway({"aw-crispal": ["get_site_info"], "kb": ["search_knowledge_base"]})
    cgw = _cgw(gw, {"upstreams": ["aw-crispal"]})

    reply = await cgw.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})

    assert [t["name"] for t in reply["result"]["tools"]] == ["get_site_info"]


async def test_prefix_stripping_survives_a_workspace_namespace():
    # Regression: the published name is `{workspace}__{server}__{tool}` with
    # hyphens normalised, so stripping the raw hyphenated upstream name alone
    # leaves the whole prefix on every tool of every named config.
    gw = _gateway({"aw-crispal": ["get_site_info"]}, workspace_name="aw")
    cgw = _cgw(gw, {"upstreams": ["aw-crispal"]})

    reply = await cgw.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert [t["name"] for t in reply["result"]["tools"]] == ["get_site_info"]

    # ...and the stripped name still routes back to the upstream.
    await _call(cgw, "get_site_info")
    assert gw.upstreams["aw-crispal"].calls == [("get_site_info", {})]


async def test_tool_outside_the_config_is_rejected():
    gw = _gateway({"aw-crispal": ["get_site_info"], "kb": ["search_knowledge_base"]})
    cgw = _cgw(gw, {"upstreams": ["aw-crispal"]})

    reply = await _call(cgw, "search_knowledge_base", {"query": "x"})

    assert "not available in this config" in reply["error"]["message"]
    assert gw.upstreams["kb"].calls == []


async def test_tool_acl_filters_list_and_rejects_direct_calls():
    gw = _gateway({"agents-platform-runners": [
        "agent_crispal_haiku", "agent_crispal_sonnet", "list_agents"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "tools_allow": ["agent_crispal_sonnet"]})

    listed = await cgw.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert [t["name"] for t in listed["result"]["tools"]] == ["agent_crispal_sonnet"]

    denied = await _call(cgw, "agent_crispal_haiku")
    assert "not available in this config" in denied["error"]["message"]
    assert gw.upstreams["agents-platform-runners"].calls == []

    assert "error" not in await _call(cgw, "agent_crispal_sonnet")


async def test_tool_acl_supports_upstream_qualified_globs():
    gw = _gateway({"agents-platform-runners": ["agent_crispal_sonnet"],
                   "other": ["agent_crispal_sonnet"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners", "other"],
                    "tools_allow": ["agents-platform-runners__agent_crispal_*"]})

    listed = await cgw.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert [t["name"] for t in listed["result"]["tools"]] == ["agent_crispal_sonnet"]


# ── Run policy ──────────────────────────────────────────────────────────────


async def test_run_policy_blocks_a_disallowed_agent_slug():
    gw = _gateway({"agents-platform-runners": ["run_agent_async", "list_agents"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "run_agents_allow": ["crispal*"]})

    denied = await _call(cgw, "run_agent_async", {"slug": "coder-opus"})
    assert "not runnable by this profile" in denied["error"]["message"]

    allowed = await _call(cgw, "run_agent_async", {"slug": "crispal-sonnet"})
    assert "error" not in allowed
    assert gw.upstreams["agents-platform-runners"].calls == [
        ("run_agent_async", {"slug": "crispal-sonnet"})]


async def test_run_policy_leaves_read_tools_alone():
    gw = _gateway({"agents-platform-runners": ["list_agents"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "run_agents_allow": ["crispal*"]})

    reply = await _call(cgw, "list_agents")

    assert "error" not in reply


async def test_run_policy_matches_the_dynamic_per_resource_runner():
    # `agent_crispal_codex` (underscores) must match the pattern `crispal*`
    # written against the canonical hyphenated slug.
    gw = _gateway({"agents-platform-runners": ["agent_crispal_codex", "agent_coder_opus"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "run_agents_allow": ["crispal*"]})

    listed = await cgw.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert [t["name"] for t in listed["result"]["tools"]] == ["agent_crispal_codex"]

    assert "error" not in await _call(cgw, "agent_crispal_codex")
    assert "error" in await _call(cgw, "agent_coder_opus")


async def test_run_policy_rejects_a_parallel_batch_on_one_bad_slug():
    gw = _gateway({"agents-platform-runners": ["run_agents_parallel"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "run_agents_allow": ["crispal*"]})

    reply = await _call(cgw, "run_agents_parallel",
                        {"agents": [{"slug": "crispal-sonnet"}, {"slug": "coder-opus"}]})

    assert "coder-opus" in reply["error"]["message"]
    assert gw.upstreams["agents-platform-runners"].calls == []


# ── Approval gate ───────────────────────────────────────────────────────────


async def test_approval_gate_blocks_until_approved(monkeypatch):
    gw = _gateway({"agents-platform-runners": ["run_agent_async"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "run_agents_approval": ["*"]})
    seen: list[str] = []

    async def _fake(resource, reason):
        seen.append(resource)
        return True

    monkeypatch.setattr(cgw, "_await_approval", _fake)
    reply = await _call(cgw, "run_agent_async", {"slug": "coder-opus"})

    assert "error" not in reply
    assert seen == ["agent 'coder-opus'"]


async def test_approval_denial_fails_closed(monkeypatch):
    gw = _gateway({"agents-platform-runners": ["run_agent_async"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "run_agents_approval": ["*"]})

    async def _fake(resource, reason):
        return False

    monkeypatch.setattr(cgw, "_await_approval", _fake)
    reply = await _call(cgw, "run_agent_async", {"slug": "coder-opus"})

    assert "was not approved" in reply["error"]["message"]
    assert gw.upstreams["agents-platform-runners"].calls == []


async def test_always_allow_exempts_a_slug_from_a_catch_all_approval(monkeypatch):
    # The aw-crispal profile's exact shape: approval `*`, always-allow `crispal*`.
    gw = _gateway({"agents-platform-runners": ["run_agent_async"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "run_agents_approval": ["*"],
                    "run_agents_always_allow": ["crispal*"]})
    asked: list[str] = []

    async def _fake(resource, reason):
        asked.append(resource)
        return True

    monkeypatch.setattr(cgw, "_await_approval", _fake)

    assert "error" not in await _call(cgw, "run_agent_async", {"slug": "crispal-sonnet"})
    assert asked == []  # never even asked

    assert "error" not in await _call(cgw, "run_agent_async", {"slug": "coder-opus"})
    assert asked == ["agent 'coder-opus'"]


@pytest.mark.parametrize("missing", ["AW_BACKEND_URL",
                                     "AW_WORKSPACE_SLUG",
                                     "AW_WORKSPACE_HOST_TOKEN"])
async def test_approval_gate_fails_closed_without_a_backend_credential(monkeypatch, missing):
    """Each of the three is load-bearing on its own.

    A gate that cannot authenticate must refuse the run, not fall back to an
    unauthenticated POST — that fallback is exactly what sent every gated run
    to one tenant's sysadmin regardless of which workspace it came from.

    Asserted as "no request was attempted", not merely "the run was refused":
    with a missing slug or token the URL is still dial-able and the refusal
    would come from the connection failing, which is indistinguishable from
    the gate having never noticed.
    """
    for name, value in (("AW_BACKEND_URL", "http://awb.test"),
                        ("AW_WORKSPACE_SLUG", "crispal"),
                        ("AW_WORKSPACE_HOST_TOKEN", "awlk_x_y")):
        monkeypatch.setenv(name, value)
    monkeypatch.delenv(missing)

    attempted: list[str] = []

    class _RecordingClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, url, json=None, headers=None):
            attempted.append(url)
            raise AssertionError(f"the gate dialled {url} with no credential")
        async def get(self, url, headers=None):
            attempted.append(url)
            raise AssertionError(f"the gate dialled {url} with no credential")

    monkeypatch.setattr(cgw_mod.httpx, "AsyncClient", _RecordingClient)

    gw = _gateway({"agents-platform-runners": ["run_agent_async"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "run_agents_approval": ["*"]})

    reply = await _call(cgw, "run_agent_async", {"slug": "coder-opus"})

    assert "was not approved" in reply["error"]["message"]
    assert attempted == []
    assert gw.upstreams["agents-platform-runners"].calls == []


async def test_approval_request_goes_to_this_workspaces_own_backend_front_door(monkeypatch):
    """The whole point of the re-point: WHICH workspace is asking is in the
    URL and proved by the host token, instead of being absent (and therefore
    defaulted to the platform's bootstrap tenant) as it was when this posted
    at agents-platform's ``/api/telegram/approval/request``.
    """
    monkeypatch.setenv("AW_BACKEND_URL", "http://awb.test/")
    monkeypatch.setenv("AW_WORKSPACE_SLUG", "crispal")
    monkeypatch.setenv("AW_WORKSPACE_HOST_TOKEN", "awlk_abc_def")

    posts: list[tuple[str, dict, dict]] = []
    gets: list[tuple[str, dict]] = []

    class _FakeResponse:
        def __init__(self, payload): self._payload, self.status_code = payload, 200
        def json(self): return self._payload

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False

        async def post(self, url, json=None, headers=None):
            posts.append((url, json or {}, headers or {}))
            return _FakeResponse({"request_id": "req-1"})

        async def get(self, url, headers=None):
            gets.append((url, headers or {}))
            return _FakeResponse({"status": "approved"})

    monkeypatch.setattr(cgw_mod.httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(cgw_mod.asyncio, "sleep", _no_sleep)

    gw = _gateway({"agents-platform-runners": ["run_agent_async"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "run_agents_approval": ["*"]})

    reply = await _call(cgw, "run_agent_async", {"slug": "coder-opus"})
    assert "error" not in reply

    url, body, headers = posts[0]
    assert url == "http://awb.test/api/workspaces/crispal/approval/request"
    assert headers["Authorization"] == "Bearer awlk_abc_def"
    assert body["request_type"] == "agent_run"
    assert body["secret_name"] == "agent 'coder-opus'"
    # The poll is scoped and authenticated the same way — an unauthenticated
    # GET would 401 forever and the gate would time out on an approved run.
    assert gets[0] == ("http://awb.test/api/workspaces/crispal/approval/status/req-1",
                       {"Authorization": "Bearer awlk_abc_def"})


@pytest.mark.parametrize("status", ["denied", "expired", "not_found", "error"])
async def test_a_terminal_poll_status_refuses_immediately(monkeypatch, status):
    """``not_found``/``error`` are aw-backend answers the old AP route never
    gave. Polling them for the full five minutes would hold an agent's call
    open for a verdict that is never coming."""
    monkeypatch.setenv("AW_BACKEND_URL", "http://awb.test")
    monkeypatch.setenv("AW_WORKSPACE_SLUG", "crispal")
    monkeypatch.setenv("AW_WORKSPACE_HOST_TOKEN", "awlk_abc_def")

    polls: list[str] = []

    class _FakeResponse:
        def __init__(self, payload): self._payload, self.status_code = payload, 200
        def json(self): return self._payload

    class _FakeClient:
        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def post(self, url, json=None, headers=None):
            return _FakeResponse({"request_id": "req-1"})
        async def get(self, url, headers=None):
            polls.append(url)
            return _FakeResponse({"status": status})

    monkeypatch.setattr(cgw_mod.httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(cgw_mod.asyncio, "sleep", _no_sleep)

    gw = _gateway({"agents-platform-runners": ["run_agent_async"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform-runners"],
                    "run_agents_approval": ["*"]})

    reply = await _call(cgw, "run_agent_async", {"slug": "coder-opus"})

    assert "was not approved" in reply["error"]["message"]
    assert len(polls) == 1


# ── Namespace injection ─────────────────────────────────────────────────────


async def test_kb_index_is_forced_onto_scoped_tools():
    gw = _gateway({"kb": ["search_knowledge_base", "search_skills"]})
    cgw = _cgw(gw, {"upstreams": ["kb"], "kb_index": "crispal"})

    await _call(cgw, "search_knowledge_base", {"query": "x", "_gateway_kb_index": "other"})
    await _call(cgw, "search_skills", {"query": "x"})

    scoped, unscoped = gw.upstreams["kb"].calls
    # Caller-supplied value is overridden, not merged.
    assert scoped[1]["_gateway_kb_index"] == "crispal"
    # A tool outside the scoped set is left untouched.
    assert "_gateway_kb_index" not in unscoped[1]


async def test_presentation_namespace_is_forced_onto_scoped_tools():
    gw = _gateway({"aw-presentation": ["create_presentation"]})
    cgw = _cgw(gw, {"upstreams": ["aw-presentation"], "presentation_namespace": "crispal"})

    await _call(cgw, "create_presentation", {"title": "x"})

    _, args = gw.upstreams["aw-presentation"].calls[0]
    assert args["_gateway_presentation_namespace"] == "crispal"


async def test_policy_roles_accept_the_monolith_upstream_names():
    # A gateway.json copied over from agentic-workspace names the upstream
    # `agents-platform`; the policy has to bind to it without an edit.
    gw = _gateway({"agents-platform": ["run_agent_async"]})
    cgw = _cgw(gw, {"upstreams": ["agents-platform"], "run_agents_allow": ["crispal*"]})

    assert "error" in await _call(cgw, "run_agent_async", {"slug": "coder-opus"})


# ── Persistence + admin API ─────────────────────────────────────────────────


def test_save_named_configs_normalizes_and_preserves_the_token(tmp_path, monkeypatch):
    path = tmp_path / "gateway.json"
    path.write_text(json.dumps({"token": "keep-me", "gateway_id": "abc"}))
    monkeypatch.setattr(config, "GATEWAY_JSON", str(path))

    config.save_named_configs({
        "crispal": {"upstreams": ["aw-crispal", "  ", ""],
                    "tools_allow": "get_*",
                    "run_agents_allow": "crispal*",
                    "kb_index": ["crispal"],
                    "bogus_key": ["dropped"]},
    })

    saved = json.loads(path.read_text())
    assert saved["token"] == "keep-me" and saved["gateway_id"] == "abc"
    assert saved["configs"]["crispal"] == {
        "upstreams": ["aw-crispal"],
        "tools_allow": ["get_*"],
        "run_agents_allow": ["crispal*"],
        "kb_index": "crispal",
    }


def test_save_named_configs_rejects_an_unroutable_name(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "GATEWAY_JSON", str(tmp_path / "gateway.json"))
    with pytest.raises(ValueError):
        config.save_named_configs({"bad/name": {"upstreams": []}})


def test_admin_configs_roundtrip_applies_without_a_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "GATEWAY_JSON", str(tmp_path / "gateway.json"))
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(tmp_path / "apps"))
    monkeypatch.setattr(config, "HOST_MCP_JSON", "")

    gw = _gateway({"aw-crispal": ["get_site_info"]})
    client = TestClient(build_app(gw, TOKEN, {}, port=9200))

    # Not there yet.
    assert client.post("/mcp/crispal", json={"jsonrpc": "2.0", "id": 1,
                                             "method": "tools/list"},
                       headers=AUTH).status_code == 404

    res = client.put("/admin/configs", headers=AUTH,
                     json={"configs": {"crispal": {"upstreams": ["aw-crispal"]}}})
    assert res.status_code == 200
    assert res.json()["configs"]["crispal"]["upstreams"] == ["aw-crispal"]

    # Live on the very next request — no restart.
    listed = client.post("/mcp/crispal", headers=AUTH,
                         json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert [t["name"] for t in listed.json()["result"]["tools"]] == ["get_site_info"]
    assert client.get("/healthz").json()["configs"] == ["crispal"]

    # And deleting it takes the endpoint away again.
    client.put("/admin/configs", headers=AUTH, json={"configs": {}})
    assert client.post("/mcp/crispal", headers=AUTH,
                       json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
                       ).status_code == 404


def test_scanned_profile_is_reachable_at_mcp_name(tmp_path, monkeypatch):
    apps = tmp_path / "apps"
    _write_json(apps / "app-a" / "gateway-profiles.json", {
        "profiles": {"crispal": {"upstreams": ["aw-crispal"]}}
    })
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(apps))
    monkeypatch.setattr(config, "GATEWAY_JSON", str(tmp_path / "missing-gateway.json"))

    gw = _gateway({"aw-crispal": ["get_site_info"]})
    client = TestClient(build_app(gw, TOKEN, config.effective_named_configs(), port=9200))

    listed = client.post("/mcp/crispal", headers=AUTH,
                         json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert [t["name"] for t in listed.json()["result"]["tools"]] == ["get_site_info"]


def test_reload_applies_a_newly_scanned_profile_without_a_restart(tmp_path, monkeypatch):
    """PLAN.md risk 4: POST /reload must re-derive the scanned half of the
    named-config set too, not just upstreams — a profile an app just wrote
    to disk must go live on the next /reload, exactly like an upstream does.
    Deliberately does NOT rebuild the app/client (which would be equivalent
    to a restart and would pass even if the /reload handler never touched
    NamedConfigs at all) — the profile file is written AFTER the app is
    already built and serving, then only /reload is called.
    """
    apps = tmp_path / "apps"
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(apps))
    monkeypatch.setattr(config, "GATEWAY_JSON", str(tmp_path / "missing-gateway.json"))
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))
    monkeypatch.setattr(config, "HOST_MCP_JSON", "")

    gw = _gateway({"aw-crispal": ["get_site_info"]})

    async def _noop_reload():
        # Gateway.reload() reconciles local upstreams against config/mcp.json,
        # which this test never populates (the fake upstream above is wired
        # in directly) — stubbed out so the assertion stays isolated to the
        # NamedConfigs re-derivation this test actually targets.
        return {"added": [], "removed": [], "changed": [], "unchanged": [],
                "failed": [], "parked": [], "upstreams": [], "tools": 0}
    monkeypatch.setattr(gw, "reload", _noop_reload)

    client = TestClient(build_app(gw, TOKEN, {}, port=9200))  # boots with NO profiles

    assert client.post("/mcp/crispal", headers=AUTH,
                       json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
                       ).status_code == 404

    # The profile file appears on disk only now — after the app already booted.
    _write_json(apps / "app-a" / "gateway-profiles.json", {
        "profiles": {"crispal": {"upstreams": ["aw-crispal"]}}
    })

    res = client.post("/reload", headers=AUTH)
    assert res.status_code == 200

    listed = client.post("/mcp/crispal", headers=AUTH,
                         json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert [t["name"] for t in listed.json()["result"]["tools"]] == ["get_site_info"]
    assert client.get("/healthz").json()["configs"] == ["crispal"]


def test_admin_configs_requires_auth(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "GATEWAY_JSON", str(tmp_path / "gateway.json"))
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(tmp_path / "apps"))

    client = TestClient(build_app(_gateway({}), TOKEN, {}, port=9200))

    assert client.get("/admin/configs").status_code == 401
    # aw-workspace's own identity header is accepted in place of the bearer.
    assert client.get("/admin/configs", headers={"X-AW-Identity-Sub": "1"}).status_code == 200


def test_admin_configs_rejects_a_bad_name(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "GATEWAY_JSON", str(tmp_path / "gateway.json"))
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(tmp_path / "apps"))

    client = TestClient(build_app(_gateway({}), TOKEN, {}, port=9200))
    res = client.put("/admin/configs", headers=AUTH,
                     json={"configs": {"../etc": {"upstreams": []}}})

    assert res.status_code == 400


def test_agents_base_is_derived_from_the_upstreams_own_env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "GATEWAY_JSON", str(tmp_path / "gateway.json"))
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.delenv("AGENTS_BASE", raising=False)
    (tmp_path / "mcp.json").write_text(json.dumps({"mcpServers": {
        "agents-platform-runners": {"command": "python3",
                                     "env": {"AGENTS_BASE": "http://172.18.0.1:10014"}},
    }}))

    assert config.agents_base() == "http://172.18.0.1:10014"
