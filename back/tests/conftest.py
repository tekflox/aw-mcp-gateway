from __future__ import annotations

import pytest

from gateway import config, warm_redis


@pytest.fixture(autouse=True)
def _no_real_warm_redis_probing(monkeypatch):
    """``warm_redis.resolve()`` falls back to probing real docker-bridge
    gateways (172.18.0.1 etc.) when no override env var is set — never
    appropriate in a test run: slow, needs real sockets, and its answer
    depends on whatever happens to be reachable from wherever tests run.
    Every ``/healthz`` call now goes through this (via
    ``caller_context.warm_redis_status()``), not just the tests that mean to
    exercise it.

    Tests that care about a specific resolution (test_warm_redis.py's own
    probe tests, test_caller_context.py's warm-redis tests) override this
    per-test with their own ``monkeypatch.setattr`` afterward, same
    override-after-fixture pattern as ``_isolate_gateway_json`` below.
    """
    for var in ("AW_MCP_GATEWAY_WARM_REDIS_URL", "AW_SHARED_REDIS_URL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(warm_redis, "_accepts_tcp", lambda host, port: False)
    warm_redis.reset_cache()
    yield
    warm_redis.reset_cache()


@pytest.fixture(autouse=True)
def _isolate_gateway_json(tmp_path, monkeypatch):
    """Point ``GATEWAY_JSON`` at a throwaway file for every test.

    ``config.gateway_id()`` and ``config.token()`` both mint-and-persist on
    first use, and ``Gateway.__init__`` calls ``gateway_id()`` whenever no id
    is passed in — so any test that builds a bare ``Gateway([])`` writes a
    real id into the repo's tracked ``back/config/gateway.json``. Caught the
    moment gateway_id() gained its persist (2026-08-25): a green test run
    left the checked-in config modified.

    Tests that care about the file still monkeypatch ``GATEWAY_JSON``
    themselves; their setattr runs after this fixture and wins.
    """
    monkeypatch.setattr(config, "GATEWAY_JSON", str(tmp_path / "gateway.json"))
