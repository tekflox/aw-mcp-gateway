"""mcp-gateway:reload-diff-ignores-app-version — an app update that changes an
upstream's TOOL LIST without changing its mcp.json SPEC used to be bucketed
``unchanged`` by ``Gateway.reload()``, left running, and never re-dialled: the
gateway kept serving the tool list it cached when it first dialled. A tool
could ship, deploy green, pass CI, and be invisible to every live session at
once (2026-08-30, agents-platform-runners 0.96.0 -> 0.99.0 adding
``list_warm_containers``, stdio; again 2026-09-29, knowledgeable 0.2.0 -> 0.3.0
adding ``search_graph``, http).

Two mechanisms, because neither covers both upstream families:

* **version in the compared spec** (``config.scan_app_mcp_servers`` injects
  ``x_app_version``) — the cause-level trigger, and the ONLY one that reaches
  a stdio upstream, whose already-spawned child answers ``tools/list`` out of
  the pre-update module it still holds in memory.
* **tool-surface divergence on the existing health-check probe** — the
  self-healing convergence loop for HTTP upstreams, whose own container IS
  recreated by the update, so the endpoint reports the new list while the
  gateway serves its cached one. Costs zero extra round trips: the probe
  already ran and used to discard its result.

Plus the observable both families need: ``/healthz`` reports which app version
each upstream was actually DIALED at, which core's ``doctor`` compares against
``apps/<slug>/aw-app.json`` on disk. Without it the failure is
indistinguishable from the session-cache lesson
(``verify-new-gateway-tools-in-same-session``), which has already misdirected
a diagnosis once.
"""

from __future__ import annotations

import contextlib
import json
from pathlib import Path

import httpx
import pytest
from starlette.testclient import TestClient

from gateway import config as config_module
from gateway import metrics
from gateway.server import Gateway, build_app
from gateway.upstream import HttpUpstream

HTTP_SPEC = {"type": "http", "enabled": True, "url": "http://svc.example/mcp"}

TOOL_ECHO = {"name": "echo", "description": "d", "inputSchema": {"type": "object"}}
TOOL_NEW = {"name": "brand_new", "description": "shipped by the update",
            "inputSchema": {"type": "object"}}


@contextlib.contextmanager
def _servers(servers: dict):
    """Point ``config.load_mcp_servers`` at an in-memory dict for the duration
    of the block — same helper as test_unchanged_upstream_health_check.py."""
    original = config_module.load_mcp_servers
    config_module.load_mcp_servers = lambda: servers
    try:
        yield
    finally:
        config_module.load_mcp_servers = original


@pytest.fixture(autouse=True)
def _reset_counters():
    """metrics.counters is a process-wide singleton — the divergence test below
    asserts an exact count, which must not depend on what ran before it."""
    metrics.counters._events.clear()
    yield
    metrics.counters._events.clear()


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


# ── (a) the owning app's version is part of upstream identity ───────────────


def test_scan_injects_the_owning_apps_version_into_the_spec(tmp_path, monkeypatch):
    apps = tmp_path / "apps"
    _write_json(apps / "demo" / "aw-app.json", {"id": "demo", "version": "0.99.0"})
    _write_json(apps / "demo" / "mcp.json",
                {"mcpServers": {"svc": {"type": "http", "url": "http://x.test/mcp"}}})
    monkeypatch.setattr(config_module, "APP_SCAN_ROOTS", str(apps))
    monkeypatch.setattr(config_module, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config_module, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))

    servers, _sources = config_module.scan_app_mcp_servers()

    assert servers["svc"]["x_app_version"] == "0.99.0"
    # And it survives the trip through the generated config/mcp.json, which is
    # the file _load_specs() actually reads.
    assert config_module.load_mcp_servers()["svc"]["x_app_version"] == "0.99.0"


def test_scan_omits_the_version_when_the_manifest_is_missing_or_unusable(tmp_path, monkeypatch):
    """No manifest, unreadable manifest, or no version => the key is absent, so
    the spec is byte-identical to what it was before versions existed. A
    placeholder value would be worse than nothing: it can later CHANGE into a
    real version and trigger a re-dial that means nothing."""
    apps = tmp_path / "apps"
    _write_json(apps / "no-manifest" / "mcp.json",
                {"mcpServers": {"a": {"type": "http", "url": "http://a.test/mcp"}}})
    (apps / "broken").mkdir(parents=True, exist_ok=True)
    (apps / "broken" / "aw-app.json").write_text("{not json")
    _write_json(apps / "broken" / "mcp.json",
                {"mcpServers": {"b": {"type": "http", "url": "http://b.test/mcp"}}})
    _write_json(apps / "blank" / "aw-app.json", {"id": "blank", "version": "   "})
    _write_json(apps / "blank" / "mcp.json",
                {"mcpServers": {"c": {"type": "http", "url": "http://c.test/mcp"}}})
    monkeypatch.setattr(config_module, "APP_SCAN_ROOTS", str(apps))
    monkeypatch.setattr(config_module, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config_module, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))

    servers, _sources = config_module.scan_app_mcp_servers()

    assert servers["a"] == {"type": "http", "url": "http://a.test/mcp"}
    assert "x_app_version" not in servers["b"]
    assert "x_app_version" not in servers["c"]


def test_two_consecutive_scans_produce_equal_specs(tmp_path, monkeypatch):
    """The inverted-bug regression test. ``Gateway._load_specs()`` reads the
    app scan TWICE — once through ``load_mcp_servers()`` and once directly —
    so a version injected on only one of those paths would make
    ``up.spec != new_specs[n]`` true on every reload: a permanent ``changed``
    bucket and a re-dial storm on the awaited install critical path, which is
    strictly worse than the staleness this fix is for."""
    apps = tmp_path / "apps"
    _write_json(apps / "demo" / "aw-app.json", {"id": "demo", "version": "1.2.3"})
    _write_json(apps / "demo" / "mcp.json",
                {"mcpServers": {"svc": {"type": "http", "url": "http://x.test/mcp"}}})
    monkeypatch.setattr(config_module, "APP_SCAN_ROOTS", str(apps))
    monkeypatch.setattr(config_module, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config_module, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))

    first, _ = config_module.scan_app_mcp_servers()
    second, _ = config_module.scan_app_mcp_servers()
    assert first == second

    # The real shape of the hazard: both of _load_specs()'s own reads must
    # agree with each other, not merely with themselves.
    gw = Gateway([])
    assert gw._load_specs() == gw._load_specs()
    assert config_module.load_mcp_servers()["svc"] == first["svc"]


def _stdio_app(app_root: Path, version: str, tools: list[dict]) -> None:
    """An installed app whose stdio MCP child reads its tool list from a file
    in its own package dir ONCE at import time — the load-bearing property of
    a real stdio upstream: the gateway spawned that child, so it keeps serving
    the tool list the pre-update code had in memory no matter what the update
    wrote to the read-only app mount. Its spec (command + absolute script
    path) never changes, so only the app's version can move it.
    """
    app_root.mkdir(parents=True, exist_ok=True)
    (app_root / "tools.json").write_text(json.dumps(tools))
    (app_root / "server.py").write_text(
        "import json, os, sys\n"
        "TOOLS = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tools.json')))\n"
        "def w(m):\n"
        "    sys.stdout.write(json.dumps(m) + '\\n'); sys.stdout.flush()\n"
        "for line in sys.stdin:\n"
        "    line = line.strip()\n"
        "    if not line:\n"
        "        continue\n"
        "    req = json.loads(line)\n"
        "    m, i = req.get('method'), req.get('id')\n"
        "    if m == 'initialize':\n"
        "        w({'jsonrpc': '2.0', 'id': i, 'result': {'protocolVersion': '2024-11-05',\n"
        "           'capabilities': {'tools': {}}, 'serverInfo': {'name': 'demo'}}})\n"
        "    elif m == 'tools/list':\n"
        "        w({'jsonrpc': '2.0', 'id': i, 'result': {'tools': TOOLS}})\n"
        "    elif m == 'tools/call':\n"
        "        w({'jsonrpc': '2.0', 'id': i, 'result': {'content': [{'type': 'text',\n"
        "           'text': 'ok'}], 'isError': False}})\n"
    )
    _write_json(app_root / "aw-app.json", {"id": "demo", "version": version})
    _write_json(app_root / "mcp.json", {"mcpServers": {"svc": {
        "type": "stdio", "enabled": True,
        "command": "python3", "args": [str(app_root / "server.py")],
    }}})


async def test_a_version_only_bump_redials_a_stdio_upstream(tmp_path, monkeypatch):
    """The 2026-08-30 incident, end to end, on the family that ONLY leg (a)
    can reach: an app update rewrites the child's code and bumps its version,
    the mcp.json spec is byte-identical, and the new tool has to become
    callable in a single reload() with no gateway restart."""
    apps = tmp_path / "apps"
    _stdio_app(apps / "demo", "0.96.0", [TOOL_ECHO])
    monkeypatch.setattr(config_module, "APP_SCAN_ROOTS", str(apps))
    monkeypatch.setattr(config_module, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config_module, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))

    gw = Gateway([])
    await gw.start()
    try:
        assert [t["name"] for t in gw.upstreams["svc"].tools] == ["echo"]
        spec_before = dict(gw.upstreams["svc"].spec)

        # A reload with NOTHING changed must not re-dial — the re-dial-storm
        # guard, asserted on the live object and not just on the scan.
        assert (await gw.reload())["unchanged"] == ["svc"]

        # The app update: new code on disk, new version, same spec.
        (apps / "demo" / "tools.json").write_text(json.dumps([TOOL_ECHO, TOOL_NEW]))
        _write_json(apps / "demo" / "aw-app.json", {"id": "demo", "version": "0.99.0"})

        result = await gw.reload()

        assert result["changed"] == ["svc"]
        assert result["failed"] == []
        assert spec_before["args"] == gw.upstreams["svc"].spec["args"]  # spec itself never moved
        assert spec_before["x_app_version"] == "0.96.0"
        assert gw.upstreams["svc"].spec["x_app_version"] == "0.99.0"
        assert sorted(t["name"] for t in gw.upstreams["svc"].tools) == ["brand_new", "echo"]

        # Callable, not merely listed.
        resp = await gw.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                "params": {"name": "svc__brand_new", "arguments": {}}})
        assert resp["result"]["isError"] is False
    finally:
        for up in list(gw.upstreams.values()):
            await up.stop()


# ── (b) tool-surface divergence on the existing HTTP probe ──────────────────


def _diverging_post(served_tools: dict):
    """A mocked ``_post`` whose ``tools/list`` answers from ``served_tools``,
    so a test can change what the upstream reports mid-flight — the HTTP
    analogue of an app's own container being recreated by an update."""
    async def _post(self, msg, *, timeout=None):
        method = msg.get("method")
        if method == "initialize":
            return {"jsonrpc": "2.0", "id": msg.get("id"), "result": {"serverInfo": {"name": "svc"}}}
        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": msg.get("id"),
                    "result": {"tools": list(served_tools["tools"])}}
        if method == "tools/call":
            return {"jsonrpc": "2.0", "id": msg.get("id"),
                    "result": {"content": [{"type": "text", "text": "ok"}], "isError": False}}
        raise AssertionError(f"unexpected method {method}")
    return _post


async def test_diverged_http_tool_surface_is_redialled_on_an_unchanged_spec(monkeypatch):
    """The 2026-09-29 incident: the app's own container was recreated, so its
    endpoint reports the new tool while the gateway serves the list it cached.
    The probe reload() already pays for now answers this, so recovery must
    happen in a SINGLE reload cycle with no spec change and no restart."""
    served = {"tools": [TOOL_ECHO]}
    generation = {"n": 0}
    original_init = HttpUpstream.__init__

    def _init(self, name, spec):
        original_init(self, name, spec)
        generation["n"] += 1

    monkeypatch.setattr(HttpUpstream, "__init__", _init)
    monkeypatch.setattr(HttpUpstream, "_post", _diverging_post(served))

    gw = Gateway(["svc"])
    with _servers({"svc": HTTP_SPEC}):
        await gw.start()
    assert [t["name"] for t in gw.upstreams["svc"].tools] == ["echo"]

    served["tools"] = [TOOL_ECHO, TOOL_NEW]
    with _servers({"svc": HTTP_SPEC}):
        result = await gw.reload()

    assert result["diverged"] == ["svc"]
    assert result["changed"] == []        # never classified as a spec change
    assert result["reconnected"] == []    # nor as a dead connection
    assert result["unchanged"] == []      # moved out of unchanged, not left there
    assert result["failed"] == []
    assert generation["n"] == 2           # a brand-new HttpUpstream was dialled
    assert sorted(t["name"] for t in gw.upstreams["svc"].tools) == ["brand_new", "echo"]

    # Every self-heal leaves a trace in the 24h window — no retry/re-dial in
    # this module ships without its counter.
    assert metrics.counters.snapshot(["svc"])["svc"]["tools_diverged"] == 1

    resp = await gw.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                            "params": {"name": "svc__brand_new", "arguments": {}}})
    assert resp["result"]["isError"] is False


async def test_an_identical_tool_surface_is_left_alone(monkeypatch):
    """The other half of the same guard: the probe runs on every unchanged
    HTTP upstream, so a false positive here would re-dial all 23 of them on
    every 60s rescan. Reordering must not count as divergence either."""
    served = {"tools": [TOOL_ECHO, TOOL_NEW]}
    generation = {"n": 0}
    original_init = HttpUpstream.__init__

    def _init(self, name, spec):
        original_init(self, name, spec)
        generation["n"] += 1

    monkeypatch.setattr(HttpUpstream, "__init__", _init)
    monkeypatch.setattr(HttpUpstream, "_post", _diverging_post(served))

    gw = Gateway(["svc"])
    with _servers({"svc": HTTP_SPEC}):
        await gw.start()
    original_upstream = gw.upstreams["svc"]

    served["tools"] = [TOOL_NEW, TOOL_ECHO]  # same set, different order
    with _servers({"svc": HTTP_SPEC}):
        result = await gw.reload()

    assert result["diverged"] == []
    assert result["unchanged"] == ["svc"]
    assert gw.upstreams["svc"] is original_upstream
    assert generation["n"] == 1
    assert metrics.counters.snapshot(["svc"])["svc"].get("tools_diverged", 0) == 0


async def test_a_description_only_change_counts_as_divergence(monkeypatch):
    """Full tool dicts, not names: ``_add_route`` folds ``description`` into
    the published tool and an ``inputSchema`` change is behaviorally visible
    to every caller, so a name-only comparison would keep serving a stale
    signature and call it unchanged."""
    served = {"tools": [TOOL_ECHO]}
    monkeypatch.setattr(HttpUpstream, "_post", _diverging_post(served))

    gw = Gateway(["svc"])
    with _servers({"svc": HTTP_SPEC}):
        await gw.start()

    served["tools"] = [{**TOOL_ECHO, "description": "rewritten by the update"}]
    with _servers({"svc": HTTP_SPEC}):
        result = await gw.reload()

    assert result["diverged"] == ["svc"]
    assert gw.upstreams["svc"].tools[0]["description"] == "rewritten by the update"


async def test_a_diverged_upstream_that_fails_to_redial_parks_with_its_routes(monkeypatch):
    """Risk 1, and the reason ``diverged`` has to join the ``prior_tools``
    condition and not just the re-dial loop's range: if the fresh dial fails
    — including the zero-tool case, which ``_start_one`` treats as a failure —
    the name must be parked with its old routes still published (tools/call
    gets a retryable error), never left routeless answering "Unknown tool".
    Self-healing must not be able to end worse than the staleness it was
    fixing."""
    served = {"tools": [TOOL_ECHO]}
    calls = {"n": 0}

    async def _post(self, msg, *, timeout=None):
        method = msg.get("method")
        if method == "initialize":
            calls["n"] += 1
            if calls["n"] > 1:
                # The re-dial's own handshake fails — the app's container is
                # mid-recreation, which is exactly when a divergence shows up.
                raise httpx.ConnectError("upstream down mid-update")
            return {"jsonrpc": "2.0", "id": msg.get("id"), "result": {"serverInfo": {"name": "svc"}}}
        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": msg.get("id"),
                    "result": {"tools": list(served["tools"])}}
        raise AssertionError(f"unexpected method {method}")

    monkeypatch.setattr(HttpUpstream, "_post", _post)

    gw = Gateway(["svc"])
    with _servers({"svc": HTTP_SPEC}):
        await gw.start()

    served["tools"] = [TOOL_ECHO, TOOL_NEW]
    with _servers({"svc": HTTP_SPEC}):
        result = await gw.reload()

    assert result["diverged"] == ["svc"]
    assert [f["name"] for f in result["failed"]] == ["svc"]
    assert result["parked"] == ["svc"]
    assert "svc" in gw.unavailable
    # Routes restored from the pre-re-dial list, so tools/list still lists it.
    assert "svc__echo" in gw.routes
    resp = await gw.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                            "params": {"name": "svc__echo", "arguments": {}}})
    assert "Unknown tool" not in json.dumps(resp)


async def test_stdio_is_never_divergence_probed():
    """A stdio child is a process THIS gateway spawned: it answers tools/list
    out of the module it already imported, so the probe would read the OLD
    list and see no divergence however new the code on disk is. Skipping it is
    correct, and leg (a) is what covers that family."""
    stdio_spec = {"type": "stdio", "enabled": True,
                  "command": "python3", "args": ["-m", "gateway.examples.echo_server"]}
    gw = Gateway(["svc"])
    with _servers({"svc": stdio_spec}):
        await gw.start()
    try:
        with _servers({"svc": stdio_spec}):
            result = await gw.reload()
        assert result["diverged"] == []
        assert result["unchanged"] == ["svc"]
    finally:
        for up in list(gw.upstreams.values()):
            await up.stop()


# ── the doctor observable on /healthz ───────────────────────────────────────


def test_healthz_reports_the_app_version_each_upstream_was_dialed_at(tmp_path, monkeypatch):
    """What core's ``doctor`` compares against ``apps/<slug>/aw-app.json``.
    It must be the version the RUNNING upstream was dialled at, not whatever
    the scan sees now — the gap between the two IS the finding."""
    monkeypatch.setattr(config_module, "GATEWAY_JSON", str(tmp_path / "gateway.json"))
    monkeypatch.setattr(config_module, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config_module, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))
    monkeypatch.setattr(config_module, "APP_SCAN_ROOTS", str(tmp_path / "apps"))
    monkeypatch.setattr(config_module, "HOST_MCP_JSON", "")

    gw = Gateway([])
    gw.upstreams["svc"] = HttpUpstream("svc", {**HTTP_SPEC, "x_app_version": "0.96.0"})
    gw.upstreams["hand-authored"] = HttpUpstream("hand-authored", dict(HTTP_SPEC))
    gw.upstream_apps = {"svc": "demo"}

    payload = TestClient(build_app(gw, "citoken", {})).get("/healthz").json()

    assert payload["upstream_app_versions"] == {"svc": {"app": "demo", "version": "0.96.0"}}
    # A custom (mcp.custom.json) upstream has no owning app and is absent
    # rather than reported with a null app — nothing on disk to compare it to.
    assert "hand-authored" not in payload["upstream_app_versions"]


def test_healthz_reports_a_null_version_when_the_manifest_was_unreadable():
    """Degrade to "unknown", never to a wrong answer: an app whose aw-app.json
    could not be read at scan time has no dialled version, and doctor must be
    able to tell that apart from a version that MISMATCHES."""
    gw = Gateway([])
    gw.upstreams["svc"] = HttpUpstream("svc", dict(HTTP_SPEC))
    gw.upstream_apps = {"svc": "demo"}

    payload = TestClient(build_app(gw, "citoken", {})).get("/healthz").json()

    assert payload["upstream_app_versions"] == {"svc": {"app": "demo", "version": None}}


def test_load_specs_records_the_owning_app_of_each_upstream(tmp_path, monkeypatch):
    """``upstream_apps`` is what /healthz pairs the version with — it comes
    from the scan's own ``sources``, so it can never drift from the upstream
    names the gateway actually loaded."""
    apps = tmp_path / "apps"
    _write_json(apps / "demo" / "aw-app.json", {"id": "demo", "version": "2.0.0"})
    _write_json(apps / "demo" / "mcp.json",
                {"mcpServers": {"svc": {"type": "http", "url": "http://x.test/mcp"}}})
    monkeypatch.setattr(config_module, "APP_SCAN_ROOTS", str(apps))
    monkeypatch.setattr(config_module, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config_module, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))

    gw = Gateway([])
    gw._load_specs()

    assert gw.upstream_apps == {"svc": "demo"}
