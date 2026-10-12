"""warm_redis.resolve() — the discovery chain that keeps a freshly created
workspace's caller-identity resolution working without a hand-pasted Redis
URL. A straight port of agents-platform-runners' shared_redis.resolve(),
which already solved this exact bug class for itself in 2026-08-08 — see
that module's test_shared_redis.py, which this file mirrors test-for-test."""
from __future__ import annotations

import pytest

from gateway import warm_redis


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    for var in ("AW_MCP_GATEWAY_WARM_REDIS_URL", "AW_SHARED_REDIS_URL",
                "AW_SHARED_REDIS_DB", "AW_SHARED_REDIS_PORT",
                "AW_SHARED_REDIS_PASSWORD", "AW_SHARED_REDIS_USER"):
        monkeypatch.delenv(var, raising=False)
    warm_redis.reset_cache()
    yield
    warm_redis.reset_cache()


def _reachable(*hosts):
    """Stub _probe so only ``hosts`` answer "ok"; everything else is "down"."""
    return lambda host, port, user, password: "ok" if host in hosts else "down"


def _locked(*hosts):
    """Stub _probe so ``hosts`` answer "auth" (found, but refuses us)."""
    return lambda host, port, user, password: "auth" if host in hosts else "down"


def test_explicit_config_env_wins_over_everything(monkeypatch):
    monkeypatch.setenv("AW_MCP_GATEWAY_WARM_REDIS_URL", "redis://:pw@explicit:6379/3")
    monkeypatch.setenv("AW_SHARED_REDIS_URL", "redis://from-env:6379/9")
    monkeypatch.setattr(warm_redis, "_probe", _reachable("172.18.0.1"))
    assert warm_redis.resolve() == warm_redis.Resolution("redis://:pw@explicit:6379/3", "config")


def test_shared_env_used_when_config_blank(monkeypatch):
    monkeypatch.setenv("AW_SHARED_REDIS_URL", "redis://from-env:6379/9")
    monkeypatch.setattr(warm_redis, "_probe", _reachable("172.18.0.1"))
    assert warm_redis.resolve() == warm_redis.Resolution("redis://from-env:6379/9", "env")


def test_prefers_default_route_when_it_answers(monkeypatch):
    monkeypatch.setattr(warm_redis, "default_gateway_ip", lambda: "10.0.0.1")
    monkeypatch.setattr(warm_redis, "_probe", _reachable("10.0.0.1", "172.18.0.1"))
    assert warm_redis.resolve() == warm_redis.Resolution("redis://10.0.0.1:6379/1", "probed")


def test_skips_default_route_that_has_no_redis(monkeypatch):
    """The measured aw-remote-host topology: podman's gateway is the default
    route but the shared Redis lives on the docker bridge gateway. This is
    the exact chain the Architect verified live for THIS gateway's own
    container on 2026-09-19 (10.89.0.1 default route, 172.18.0.1:6379 the
    real answer)."""
    monkeypatch.setattr(warm_redis, "default_gateway_ip", lambda: "10.89.0.1")
    monkeypatch.setattr(warm_redis, "_probe", _reachable("172.18.0.1"))
    assert warm_redis.resolve() == warm_redis.Resolution("redis://172.18.0.1:6379/1", "probed")


def test_db_and_port_overridable(monkeypatch):
    monkeypatch.setenv("AW_SHARED_REDIS_DB", "4")
    monkeypatch.setenv("AW_SHARED_REDIS_PORT", "6380")
    monkeypatch.setattr(warm_redis, "default_gateway_ip", lambda: None)
    monkeypatch.setattr(warm_redis, "_probe", _reachable("172.17.0.1"))
    assert warm_redis.resolve() == warm_redis.Resolution("redis://172.17.0.1:6380/4", "probed")


def test_none_when_nothing_answers(monkeypatch):
    """Callers must keep failing loudly rather than inventing an address."""
    monkeypatch.setattr(warm_redis, "default_gateway_ip", lambda: "10.89.0.1")
    monkeypatch.setattr(warm_redis, "_probe", _reachable())
    assert warm_redis.resolve() == warm_redis.Resolution(None, "none")


def test_auth_required_and_no_credential_configured_fails_loudly(monkeypatch):
    """The whole point of the fix: a host that IS there but demands a
    credential we don't have must never be cached as a passwordless URL —
    collapsing this into "nothing answered" would recreate the exact
    silent-degradation incident (memory:
    mcp-gateway-warm-redis-url-override-pointed-at-wrong-instance) this
    probe exists to prevent."""
    monkeypatch.setattr(warm_redis, "default_gateway_ip", lambda: "10.89.0.1")
    monkeypatch.setattr(warm_redis, "_probe", _locked("172.18.0.1"))
    assert warm_redis.resolve() == warm_redis.Resolution(None, "auth")


def test_credential_configured_is_injected_into_the_discovered_url(monkeypatch):
    monkeypatch.setenv("AW_SHARED_REDIS_PASSWORD", "s3cret")
    monkeypatch.setattr(warm_redis, "default_gateway_ip", lambda: "10.89.0.1")
    monkeypatch.setattr(warm_redis, "_probe", _reachable("172.18.0.1"))
    assert warm_redis.resolve() == warm_redis.Resolution(
        "redis://default:s3cret@172.18.0.1:6379/1", "probed")


def test_credential_user_overridable(monkeypatch):
    monkeypatch.setenv("AW_SHARED_REDIS_PASSWORD", "s3cret")
    monkeypatch.setenv("AW_SHARED_REDIS_USER", "svc-runs")
    monkeypatch.setattr(warm_redis, "default_gateway_ip", lambda: "10.89.0.1")
    monkeypatch.setattr(warm_redis, "_probe", _reachable("172.18.0.1"))
    assert warm_redis.resolve() == warm_redis.Resolution(
        "redis://svc-runs:s3cret@172.18.0.1:6379/1", "probed")


def test_probe_result_is_cached(monkeypatch):
    calls = []

    def _probe(host, port, user, password):
        calls.append(host)
        return "ok" if host == "172.18.0.1" else "down"

    monkeypatch.setattr(warm_redis, "default_gateway_ip", lambda: "10.89.0.1")
    monkeypatch.setattr(warm_redis, "_probe", _probe)
    assert warm_redis.resolve().url == "redis://172.18.0.1:6379/1"
    before = len(calls)
    assert warm_redis.resolve().url == "redis://172.18.0.1:6379/1"
    assert len(calls) == before, "second resolve() must not re-probe"


def test_reset_cache_forces_a_fresh_probe(monkeypatch):
    """The ~30s reconnect cooldown in caller_context._get_warm_redis() calls
    this after a failed connect — otherwise a Redis that only just came up,
    or a probe answer that only just changed, stays pinned to the first
    outcome for the gateway's entire process life."""
    monkeypatch.setattr(warm_redis, "default_gateway_ip", lambda: "10.89.0.1")
    monkeypatch.setattr(warm_redis, "_probe", _reachable())
    assert warm_redis.resolve() == warm_redis.Resolution(None, "none")

    monkeypatch.setattr(warm_redis, "_probe", _reachable("172.18.0.1"))
    warm_redis.reset_cache()
    assert warm_redis.resolve() == warm_redis.Resolution("redis://172.18.0.1:6379/1", "probed")


def test_candidate_hosts_dedupes_default_route(monkeypatch):
    monkeypatch.setattr(warm_redis, "default_gateway_ip", lambda: "172.18.0.1")
    hosts = warm_redis.candidate_hosts()
    assert hosts[0] == "172.18.0.1"
    assert hosts.count("172.18.0.1") == 1


def test_default_gateway_ip_parses_proc_net_route(monkeypatch, tmp_path):
    route = tmp_path / "route"
    # Real /proc/net/route shape: the default route is the 00000000 destination
    # row; addresses are little-endian hex. 010012AC == 172.18.0.1.
    route.write_text(
        "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\n"
        "eth0\t000012AC\t00000000\t0001\t0\t0\t0\t0000FFFF\n"
        "eth0\t00000000\t010012AC\t0003\t0\t0\t0\t00000000\n"
    )
    real_open = open
    monkeypatch.setattr(
        "builtins.open",
        lambda p, *a, **k: real_open(route, *a, **k) if p == "/proc/net/route"
        else real_open(p, *a, **k),
    )
    assert warm_redis.default_gateway_ip() == "172.18.0.1"


def test_default_gateway_ip_none_when_unreadable(monkeypatch):
    def _boom(p, *a, **k):
        raise OSError("no /proc here")
    monkeypatch.setattr("builtins.open", _boom)
    assert warm_redis.default_gateway_ip() is None
