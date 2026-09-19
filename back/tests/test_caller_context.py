"""Forwarding who the caller is, without forwarding anything else.

An upstream that scopes a grant to a caller (aw-app-secrets does) sees the
gateway, not the agent, unless this works. The risk on the other side is a
gateway that cheerfully relays whatever a caller sets — so the allowlist gets
as much attention here as the happy path.
"""
from __future__ import annotations

import asyncio

import pytest

from gateway import caller_context, metrics, warm_redis


@pytest.fixture(autouse=True)
def _reset_warm_redis_state(monkeypatch):
    """Most tests in this file monkeypatch ``_get_warm_redis`` wholesale and
    never touch this state, but the ones below exercise the real
    cooldown/resolution/counter plumbing — process-wide module state (the
    connect cache, the retry timer, the rate-limited warning, the rolling
    counters) must not leak between tests either way."""
    monkeypatch.setattr(caller_context, "_warm_redis", None)
    monkeypatch.setattr(caller_context, "_warm_redis_last_attempt", 0.0)
    monkeypatch.setattr(caller_context, "_last_unresolved_warning", 0.0)
    warm_redis.reset_cache()
    metrics.counters._events.clear()
    yield
    metrics.counters._events.clear()


def _capture(headers: dict) -> None:
    """capture() is async (warm-token resolution needs to await Redis) — this
    repo's Redis dep isn't configured in the test env, so it takes the
    no-configured-Redis fast path and never actually suspends. Drive the
    coroutine directly rather than ``asyncio.run()``: that wraps it in a
    Task, and a Task's context is a COPY taken at creation — the
    ``_caller_headers.set()`` inside would land in that copy, not the
    caller's own context, and the assertion below would see nothing.
    Stepping the coroutine by hand runs it in the CURRENT context instead —
    works even when it awaits a fake Redis call, since nothing here ever
    needs a real OS-level suspension (an ``await`` on an already-resolved
    coroutine just yields control back for one ``send`` cycle, not forever)."""
    coro = caller_context.capture(headers)
    try:
        while True:
            coro.send(None)
    except StopIteration:
        pass


def test_the_session_header_is_forwarded():
    _capture({"x-aw-caller-session-id": "sess-42"})

    assert caller_context.current() == {"x-aw-caller-session-id": "sess-42"}


def test_nothing_else_is_forwarded():
    """Passthrough would let a caller set Authorization on somebody else's
    upstream. Only the allowlist travels."""
    _capture({
        "x-aw-caller-session-id": "sess-42",
        "authorization": "Bearer someone-elses-token",
        "cookie": "aw_id_jwt=...",
        "x-api-key": "secret",
    })

    assert list(caller_context.current()) == ["x-aw-caller-session-id"]


def test_absent_headers_are_absent_not_empty():
    """An empty string would still look like an identity to an upstream, and
    every anonymous caller would share it."""
    _capture({"x-aw-caller-session-id": ""})

    assert caller_context.current() == {}


def test_a_long_value_is_bounded():
    _capture({"x-aw-caller-session-id": "x" * 5000})

    assert len(caller_context.current()["x-aw-caller-session-id"]) == 256


def test_concurrent_requests_do_not_see_each_others_caller():
    """The whole reason this is a contextvar. If it leaked, one agent's window
    grant would be reusable by another — the exact bug being fixed upstream."""
    seen = {}

    async def _request(name, delay):
        await caller_context.capture({"x-aw-caller-session-id": name})
        await asyncio.sleep(delay)
        seen[name] = caller_context.current().get("x-aw-caller-session-id")

    async def _both():
        await asyncio.gather(_request("agent-a", 0.02), _request("agent-b", 0.01))

    asyncio.run(_both())

    assert seen == {"agent-a": "agent-a", "agent-b": "agent-b"}


def test_upstream_headers_carry_the_caller(monkeypatch):
    from gateway.upstream import HttpUpstream

    up = HttpUpstream("secrets", {"url": "http://example/mcp"})
    _capture({"x-aw-caller-session-id": "sess-42"})

    assert up._client_headers()["x-aw-caller-session-id"] == "sess-42"


def test_a_configured_header_still_wins_over_a_forwarded_one():
    """An upstream's own Authorization is configuration, not something a caller
    gets to influence."""
    from gateway.upstream import HttpUpstream

    up = HttpUpstream("secrets", {"url": "http://example/mcp",
                                  "headers": {"x-aw-caller-session-id": "configured"}})
    _capture({"x-aw-caller-session-id": "from-the-caller"})

    assert up._client_headers()["x-aw-caller-session-id"] == "configured"


def test_the_agent_identity_is_forwarded_too():
    """Unlike the session, an agent id is the same next week — which is what a
    per-secret allowlist can name."""
    _capture({"x-aw-caller-agent": "agent:nightly-backup"})

    assert caller_context.current() == {"x-aw-caller-agent": "agent:nightly-backup"}


class _FakeRedis:
    def __init__(self, values: dict):
        self._values = values
        self.sets: list[tuple[str, str]] = []

    async def get(self, key):
        return self._values.get(key)

    async def set(self, key, value):
        self.sets.append((key, value))


def test_warm_token_resolves_to_the_current_run_id(monkeypatch):
    """The whole point: a stale per-run header gets corrected by the stable
    warm token, which always points at whichever run is CURRENT."""
    fake = _FakeRedis({"warm_token:tok-1:run_id":
                       '{"run_id": "run-current", "notion_task_id": "", "source_device": ""}'})

    async def fake_get_warm_redis():
        return fake
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    _capture({
        "x-aw-caller-run-id": "run-stale-turn-1",
        "x-aw-warm-token": "tok-1",
    })

    assert caller_context.current()["x-aw-caller-run-id"] == "run-current"


def test_warm_token_falls_back_to_bare_string_value(monkeypatch):
    """set_warm_token_run predates the JSON-blob format for some still-live
    keys (TTL up to 24h) — a bare run_id string must still resolve."""
    fake = _FakeRedis({"warm_token:tok-legacy:run_id": "run-bare"})

    async def fake_get_warm_redis():
        return fake
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    _capture({
        "x-aw-caller-run-id": "run-stale",
        "x-aw-warm-token": "tok-legacy",
    })

    assert caller_context.current()["x-aw-caller-run-id"] == "run-bare"


def test_unmapped_warm_token_falls_back_to_the_raw_header(monkeypatch):
    fake = _FakeRedis({})

    async def fake_get_warm_redis():
        return fake
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    _capture({
        "x-aw-caller-run-id": "run-ephemeral-and-correct",
        "x-aw-warm-token": "tok-unknown",
    })

    assert caller_context.current()["x-aw-caller-run-id"] == "run-ephemeral-and-correct"


def test_redis_down_falls_back_to_the_raw_header_not_an_error(monkeypatch):
    async def fake_get_warm_redis():
        return None
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)

    _capture({
        "x-aw-caller-run-id": "run-ephemeral-and-correct",
        "x-aw-warm-token": "tok-1",
    })

    assert caller_context.current()["x-aw-caller-run-id"] == "run-ephemeral-and-correct"


def test_no_warm_token_leaves_the_raw_header_untouched(monkeypatch):
    """The common case today (no caller has been updated to send the token
    yet) must be byte-for-byte identical to pre-warm-token behavior."""
    async def fail_if_called():
        raise AssertionError("Redis should never be touched with no warm token header")
    monkeypatch.setattr(caller_context, "_get_warm_redis", fail_if_called)

    _capture({"x-aw-caller-run-id": "run-abc"})

    assert caller_context.current() == {"x-aw-caller-run-id": "run-abc"}


def test_bump_warm_generation_sets_the_shared_key(monkeypatch):
    """Must be the literal string AP-MT's and runners' own warm_pool.py use
    for GENERATION_KEY — a typo here means this gateway's restarts silently
    stop invalidating anything, forever, with no error."""
    fake = _FakeRedis({})
    monkeypatch.setattr(caller_context, "_get_warm_redis", lambda: _async(fake))

    asyncio.run(caller_context.bump_warm_generation())

    assert fake.sets == [("warm:config_generation", fake.sets[0][1])]
    assert fake.sets[0][0] == caller_context.GENERATION_KEY
    float(fake.sets[0][1])  # a bare timestamp, not a JSON blob


def test_bump_warm_generation_is_a_noop_without_redis(monkeypatch):
    """Best-effort: no warm Redis configured must not raise."""
    monkeypatch.setattr(caller_context, "_get_warm_redis", lambda: _async(None))

    asyncio.run(caller_context.bump_warm_generation())  # must not raise


def test_bump_warm_generation_survives_a_write_failure(monkeypatch):
    class _BoomRedis:
        async def set(self, key, value):
            raise ConnectionError("redis unreachable")

    monkeypatch.setattr(caller_context, "_get_warm_redis", lambda: _async(_BoomRedis()))

    asyncio.run(caller_context.bump_warm_generation())  # must not raise


async def _async(value):
    return value


# --- warm_redis.py integration: deriving by probing, failing loudly -------

def test_get_warm_redis_retries_after_cooldown_not_immediately(monkeypatch):
    """The bug being fixed: a permanent latch used to disable warm-token
    resolution for the gateway process's entire life the moment one connect
    attempt failed — even if the Redis was only down for a single second at
    boot. This must retry after the cooldown instead of never again."""
    monkeypatch.setattr(warm_redis, "resolve",
                         lambda: warm_redis.Resolution("redis://example:6379/1", "probed"))

    attempts = {"n": 0}

    class _BoomClient:
        async def ping(self):
            attempts["n"] += 1
            raise ConnectionError("refused")

    monkeypatch.setattr("redis.asyncio.from_url", lambda *a, **k: _BoomClient())

    now = {"t": 1000.0}
    monkeypatch.setattr(caller_context.time, "time", lambda: now["t"])

    assert asyncio.run(caller_context._get_warm_redis()) is None
    assert attempts["n"] == 1, "first call must attempt to connect"

    assert asyncio.run(caller_context._get_warm_redis()) is None
    assert attempts["n"] == 1, "within the cooldown window, must NOT re-attempt"

    now["t"] += caller_context._WARM_REDIS_RETRY_COOLDOWN_S + 0.1
    assert asyncio.run(caller_context._get_warm_redis()) is None
    assert attempts["n"] == 2, "past the cooldown, must retry"


def test_get_warm_redis_resets_the_probe_cache_on_failed_connect(monkeypatch):
    """A failed connect must also drop warm_redis's own cached resolution —
    otherwise a probe answer that only just changed (or a Redis that only
    just came up on a different candidate host) stays pinned to the first
    outcome for the gateway's entire process life, same bug class as the
    latch above just one layer down."""
    monkeypatch.setattr(warm_redis, "resolve",
                         lambda: warm_redis.Resolution("redis://example:6379/1", "probed"))

    class _BoomClient:
        async def ping(self):
            raise ConnectionError("refused")

    monkeypatch.setattr("redis.asyncio.from_url", lambda *a, **k: _BoomClient())

    reset_calls = {"n": 0}
    real_reset = warm_redis.reset_cache

    def _spy_reset():
        reset_calls["n"] += 1
        real_reset()
    monkeypatch.setattr(warm_redis, "reset_cache", _spy_reset)

    asyncio.run(caller_context._get_warm_redis())

    assert reset_calls["n"] == 1


def test_get_warm_redis_connects_successfully_and_caches_the_client(monkeypatch):
    monkeypatch.setattr(warm_redis, "resolve",
                         lambda: warm_redis.Resolution("redis://example:6379/1", "probed"))

    connects = {"n": 0}

    class _OkClient:
        async def ping(self):
            return True

    def _from_url(*a, **k):
        connects["n"] += 1
        return _OkClient()
    monkeypatch.setattr("redis.asyncio.from_url", _from_url)

    r1 = asyncio.run(caller_context._get_warm_redis())
    r2 = asyncio.run(caller_context._get_warm_redis())

    assert r1 is r2
    assert connects["n"] == 1, "a connected client is cached, not re-established every call"


def test_redact_hides_only_the_credential():
    assert caller_context._redact("redis://:supersecret@172.18.0.1:6379/1") == \
        "redis://***@172.18.0.1:6379/1"
    assert caller_context._redact("redis://172.18.0.1:6379/1") == "redis://172.18.0.1:6379/1"
    assert caller_context._redact(None) is None


def test_warm_redis_status_none_when_nothing_resolves(monkeypatch):
    monkeypatch.setattr(warm_redis, "resolve", lambda: warm_redis.Resolution(None, "none"))
    monkeypatch.setattr(caller_context, "_get_warm_redis", lambda: _async(None))

    status = asyncio.run(caller_context.warm_redis_status())

    assert status["ok"] is False
    assert status["source"] == "none"
    assert status["reachable"] is False


def test_warm_redis_status_not_ok_when_resolved_but_unreachable(monkeypatch):
    """Case (b): a URL WAS resolved (config, env or probe) but the connect
    itself failed — must be loud even before any token has ever arrived."""
    monkeypatch.setattr(warm_redis, "resolve",
                         lambda: warm_redis.Resolution("redis://example:6379/1", "probed"))
    monkeypatch.setattr(caller_context, "_get_warm_redis", lambda: _async(None))

    status = asyncio.run(caller_context.warm_redis_status())

    assert status["ok"] is False
    assert status["reachable"] is False


def test_warm_redis_status_ok_when_reachable_with_zero_tokens_seen(monkeypatch):
    """A brand-new workspace with no warm sessions yet must read as healthy,
    not as a 100%-failure outage — the Architect's design calls this out by
    name as the trap a naive rate check would fall into."""
    monkeypatch.setattr(warm_redis, "resolve",
                         lambda: warm_redis.Resolution("redis://example:6379/1", "probed"))
    monkeypatch.setattr(caller_context, "_get_warm_redis", lambda: _async(_FakeRedis({})))

    status = asyncio.run(caller_context.warm_redis_status())

    assert status["ok"] is True
    assert status["tokens_seen_24h"] == 0


def test_wrong_db_resolves_but_tokens_never_match_is_loud(monkeypatch):
    """The wrong-instance/wrong-db case named in the Architect's design:
    the probe (or an override) finds *A* Redis and the connection succeeds,
    but it is not the one AP-MT writes warm_token:* keys to — every lookup
    misses. Locally unreproducible any other way, since this workspace's own
    AW_REDIS_URL happens to share db 1 with the real warm-token Redis."""
    fake = _FakeRedis({})  # reachable, just holds none of the expected keys

    async def fake_get_warm_redis():
        return fake
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)
    monkeypatch.setattr(warm_redis, "resolve",
                         lambda: warm_redis.Resolution("redis://wrong-db:6379/1", "probed"))

    for i in range(caller_context._MIN_TOKENS_FOR_FAILURE_SIGNAL):
        _capture({"x-aw-caller-run-id": "run-stale", "x-aw-warm-token": f"tok-{i}"})

    status = asyncio.run(caller_context.warm_redis_status())

    assert status["tokens_seen_24h"] == caller_context._MIN_TOKENS_FOR_FAILURE_SIGNAL
    assert status["tokens_unresolved_24h"] == caller_context._MIN_TOKENS_FOR_FAILURE_SIGNAL
    assert status["ok"] is False


def test_a_handful_of_early_misses_is_not_yet_the_loud_signal(monkeypatch):
    """Below the minimum sample size, an all-miss rate is still noise, not
    proof of a wrong-db outage — avoids flapping ok=False on the very first
    unlucky token before real volume arrives."""
    fake = _FakeRedis({})

    async def fake_get_warm_redis():
        return fake
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)
    monkeypatch.setattr(warm_redis, "resolve",
                         lambda: warm_redis.Resolution("redis://wrong-db:6379/1", "probed"))

    assert caller_context._MIN_TOKENS_FOR_FAILURE_SIGNAL > 1
    _capture({"x-aw-caller-run-id": "run-stale", "x-aw-warm-token": "tok-only-one"})

    status = asyncio.run(caller_context.warm_redis_status())

    assert status["ok"] is True


def test_unresolved_warning_is_rate_limited(monkeypatch, caplog):
    """A sustained outage should log once a minute, not once per tool call —
    the whole reason this is rate-limited rather than a WARNING per capture()."""
    fake = _FakeRedis({})

    async def fake_get_warm_redis():
        return fake
    monkeypatch.setattr(caller_context, "_get_warm_redis", fake_get_warm_redis)
    monkeypatch.setattr(warm_redis, "resolve",
                         lambda: warm_redis.Resolution("redis://wrong-db:6379/1", "probed"))

    now = {"t": 1000.0}
    monkeypatch.setattr(caller_context.time, "time", lambda: now["t"])

    with caplog.at_level("WARNING", logger="aw-mcp-gateway.caller_context"):
        _capture({"x-aw-caller-run-id": "run-a", "x-aw-warm-token": "tok-1"})
        _capture({"x-aw-caller-run-id": "run-b", "x-aw-warm-token": "tok-2"})

    unresolved_warnings = [r for r in caplog.records if "did not resolve" in r.message]
    assert len(unresolved_warnings) == 1, "second call within the cooldown must not log again"

    now["t"] += caller_context._UNRESOLVED_WARNING_COOLDOWN_S + 0.1
    with caplog.at_level("WARNING", logger="aw-mcp-gateway.caller_context"):
        _capture({"x-aw-caller-run-id": "run-c", "x-aw-warm-token": "tok-3"})

    unresolved_warnings = [r for r in caplog.records if "did not resolve" in r.message]
    assert len(unresolved_warnings) == 2, "past the cooldown, the next miss logs again"
