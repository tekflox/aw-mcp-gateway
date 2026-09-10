"""resilience:mcp-gateway-session-auto-reconnect — a warm claude-cli
container's MCP client is built once at CLI boot and never reinitialized
(see agents-platform's and agents-platform-runners' own warm_pool.py
docstrings). When THIS gateway restarts, every upstream a warm container's
client is talking to gets new connections underneath it, and the container
has no way to notice — it keeps calling a dead client forever.

The cure already exists on the consumer side: `bump_generation()`, keyed on
`warm:config_generation` in the shared Redis, condemns every warm container
in one write. Both consumers' own docstrings already list "mcp-gateway
starting/restarting" as a trigger for that key — nothing on this side of the
trigger ever fired it. This confirms the gateway's own startup now does,
right after every upstream is up, reusing the same Redis client
`caller_context._get_warm_redis()` already gives warm-token resolution.
"""
from __future__ import annotations

from starlette.testclient import TestClient

from gateway import caller_context, config
from gateway.server import Gateway, build_app

TOKEN = "test-token"


class _FakeRedis:
    def __init__(self):
        self.sets: list[tuple[str, str]] = []

    async def set(self, key, value):
        self.sets.append((key, value))


def _app(tmp_path, monkeypatch):
    # Empty, isolated config so gateway.start() has nothing real to spawn —
    # this test is about the bump, not about upstream startup.
    monkeypatch.setattr(config, "MCP_JSON", str(tmp_path / "mcp.json"))
    monkeypatch.setattr(config, "MCP_CUSTOM_JSON", str(tmp_path / "mcp.custom.json"))
    monkeypatch.setattr(config, "APP_SCAN_ROOTS", str(tmp_path / "apps"))
    monkeypatch.setattr(config, "HOST_MCP_JSON", "")
    return build_app(Gateway([]), TOKEN, {})


def test_lifespan_bumps_the_warm_generation_key_on_startup(tmp_path, monkeypatch):
    fake = _FakeRedis()

    async def fake_get_warm_redis():
        return fake
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    with TestClient(_app(tmp_path, monkeypatch)):
        pass

    assert len(fake.sets) == 1, "gateway startup must bump the generation exactly once"
    key, value = fake.sets[0]
    # Literal string, not just the constant, so a typo divergence from
    # AP-MT's/runners' own GENERATION_KEY = "warm:config_generation" fails
    # loudly here instead of silently never invalidating anything.
    assert key == "warm:config_generation"
    assert key == caller_context.GENERATION_KEY
    float(value)  # a bare timestamp — same shape the other two writers use


def test_lifespan_survives_warm_redis_being_unreachable(tmp_path, monkeypatch):
    """A dead warm-Redis must not block gateway startup — best-effort means
    best-effort, not a new way for the gateway to fail to boot."""
    async def fake_get_warm_redis():
        return None
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    with TestClient(_app(tmp_path, monkeypatch)) as client:
        assert client.get("/healthz").status_code == 200


def test_lifespan_survives_warm_redis_write_failing(tmp_path, monkeypatch):
    class _BoomRedis:
        async def set(self, key, value):
            raise ConnectionError("redis unreachable")

    async def fake_get_warm_redis():
        return _BoomRedis()
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    with TestClient(_app(tmp_path, monkeypatch)) as client:
        assert client.get("/healthz").status_code == 200
