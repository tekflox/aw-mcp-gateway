"""Named configs (scoped ``/mcp/<name>`` profiles) — allowlist, run policy,
approval gate, namespace injection, and the admin CRUD that edits them."""

from __future__ import annotations

import json

import pytest
from starlette.testclient import TestClient

from gateway import config
from gateway.config_gateway import ConfigGateway, policy_upstreams
from gateway.server import Gateway, build_app

TOKEN = "test-token"
AUTH = {"Authorization": f"Bearer {TOKEN}"}


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


async def test_approval_gate_fails_closed_without_a_platform_url():
    gw = _gateway({"agents-platform-runners": ["run_agent_async"]})
    cgw = ConfigGateway(gw, {"upstreams": ["agents-platform-runners"],
                             "run_agents_approval": ["*"]},
                        name="p", agents_base="", upstream_roles=policy_upstreams())

    reply = await _call(cgw, "run_agent_async", {"slug": "coder-opus"})

    assert "was not approved" in reply["error"]["message"]


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
