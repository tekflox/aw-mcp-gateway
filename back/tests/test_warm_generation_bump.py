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

import pytest
from starlette.testclient import TestClient

from gateway import caller_context, config, metrics, warm_redis
from gateway.server import Gateway, build_app

TOKEN = "test-token"


@pytest.fixture(autouse=True)
def _reset_warm_token_counters():
    """metrics.counters is a process-wide singleton (see
    test_proof_gated_retry.py's identical fixture) — this file's new
    healthz test asserts exact tokens_seen_24h/tokens_unresolved_24h
    values, which must not depend on what ran before it."""
    metrics.counters._events.clear()
    yield
    metrics.counters._events.clear()


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


def test_healthz_reports_the_warm_redis_block(tmp_path, monkeypatch):
    """The actual point of this card: an unconfigured/unreachable warm Redis
    used to be a total, silent outage — this is the doctor-visible signal
    that replaces the one INFO line at boot."""
    async def fake_get_warm_redis():
        return None
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)
    monkeypatch.setattr(warm_redis, "resolve",
                         lambda: warm_redis.Resolution("redis://:secret@example:6379/1", "probed"))

    with TestClient(_app(tmp_path, monkeypatch)) as client:
        body = client.get("/healthz").json()

    assert body["warm_redis"] == {
        "ok": False,  # resolved but unreachable
        "url": "redis://***@example:6379/1",
        "source": "probed",
        "reachable": False,
        "tokens_seen_24h": 0,
        "tokens_unresolved_24h": 0,
    }
