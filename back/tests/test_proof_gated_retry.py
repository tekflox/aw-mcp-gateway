"""resilience:gateway-proof-gated-retry-with-counters — card 2 of the MCP
resilience chain (card 1: gateway-park-routes-for-unavailable-upstream).

Covers the whole rule in one place: retry only fires when the caller can
PROVE the failed attempt had no effect, or when the tool declares
``annotations.idempotentHint``; an ambiguous failure (``ReadTimeout``) on a
non-idempotent tool must come back distinguishably "uncertain", never
silently retried; a call that arrived from another gateway must never
retry again (non-recursive federation); and none of the above may ship
without its counter, exposed via ``/healthz``.
"""

from __future__ import annotations

import httpx
import pytest

from gateway import caller_context, metrics
from gateway.server import Gateway
from gateway.upstream import HttpUpstream


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://upstream.example/mcp")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(f"{status}", request=request, response=response)


def _scripted_post(behaviors):
    """Fake ``HttpUpstream._post`` — behaviors[i] is either an Exception to
    raise on the i-th call, or a dict to return. The last entry repeats for
    any call beyond the list's length."""
    calls = {"n": 0}

    async def _post(msg):
        i = min(calls["n"], len(behaviors) - 1)
        calls["n"] += 1
        behavior = behaviors[i]
        if isinstance(behavior, BaseException):
            raise behavior
        return {"jsonrpc": "2.0", "id": msg.get("id"), "result": behavior}

    return _post, calls


@pytest.fixture(autouse=True)
def _reset_counters():
    """metrics.counters is a process-wide singleton — start every test with
    a clean slate so one test's retries can't inflate another's assertions."""
    metrics.counters._events.clear()
    yield
    metrics.counters._events.clear()


def _up(name: str = "svc") -> HttpUpstream:
    return HttpUpstream(name, {"url": "http://upstream.example/mcp"})


# ── Proof gate ───────────────────────────────────────────────────────────

async def test_connect_error_is_retried_and_succeeds_without_idempotent_hint():
    """ConnectError proves the request never left this process — retryable
    even for a tool with no idempotentHint at all."""
    up = _up()
    up._post, calls = _scripted_post([
        httpx.ConnectError("boom"),
        {"content": [{"type": "text", "text": "ok"}], "isError": False},
    ])

    resp = await up.call_tool("thing", {}, req_id=1, idempotent_hint=False)

    assert calls["n"] == 2
    assert resp["result"]["isError"] is False
    assert resp["result"]["content"][0]["text"] == "ok"
    assert metrics.counters.total("svc", "retries") == 1
    assert metrics.counters.total("svc", "retry_succeeded") == 1


async def test_404_is_retried_even_for_a_non_idempotent_tool():
    """A 404 means the router never dispatched to the handler — same proof
    as a ConnectError, per the runner.py precedent this generalizes."""
    up = _up()
    up._post, calls = _scripted_post([
        _http_status_error(404),
        {"content": [{"type": "text", "text": "ok"}], "isError": False},
    ])

    resp = await up.call_tool("thing", {}, req_id=1, idempotent_hint=False)

    assert calls["n"] == 2
    assert resp["result"]["isError"] is False


async def test_read_timeout_on_non_idempotent_tool_is_not_retried():
    """A ReadTimeout fires AFTER the request was sent — the write may have
    landed. Retrying blind here is exactly what the card forbids."""
    up = _up()
    up._post, calls = _scripted_post([httpx.ReadTimeout("timed out")])

    resp = await up.call_tool("thing", {}, req_id=1, idempotent_hint=False)

    assert calls["n"] == 1  # never retried
    assert resp["result"]["isError"] is True
    text = resp["result"]["content"][0]["text"]
    assert "UNCERTAIN" in text
    assert metrics.counters.total("svc", "retries") == 0
    assert metrics.counters.total("svc", "tools_call_errors.timeout") == 1


async def test_read_timeout_on_idempotent_tool_is_retried():
    """The second source of proof: the TOOL says repeating is safe."""
    up = _up()
    up._post, calls = _scripted_post([
        httpx.ReadTimeout("timed out"),
        {"content": [{"type": "text", "text": "ok"}], "isError": False},
    ])

    resp = await up.call_tool("thing", {}, req_id=1, idempotent_hint=True)

    assert calls["n"] == 2
    assert resp["result"]["isError"] is False
    assert metrics.counters.total("svc", "retry_succeeded") == 1


async def test_idempotent_hint_default_is_false_fail_closed():
    """Omitting idempotent_hint must behave exactly like passing False —
    the card's DEFAULT: idempotentHint ausente => false."""
    up = _up()
    up._post, calls = _scripted_post([httpx.ReadTimeout("timed out")])

    resp = await up.call_tool("thing", {}, req_id=1)  # no idempotent_hint kwarg

    assert calls["n"] == 1
    assert "UNCERTAIN" in resp["result"]["content"][0]["text"]


async def test_generic_exception_is_not_retried_and_reports_uncertain():
    """Anything not explicitly recognized as proof gets no free pass either
    — fail closed, not fail open."""
    up = _up()
    up._post, calls = _scripted_post([RuntimeError("something else broke")])

    resp = await up.call_tool("thing", {}, req_id=1, idempotent_hint=False)

    assert calls["n"] == 1
    assert "UNCERTAIN" in resp["result"]["content"][0]["text"]


async def test_retries_exhausted_after_repeated_proven_failures():
    up = _up()
    up._post, calls = _scripted_post([httpx.ConnectError("still down")])  # always fails

    resp = await up.call_tool("thing", {}, req_id=1, idempotent_hint=False)

    assert calls["n"] == 3  # MAX_CALL_ATTEMPTS
    assert resp["result"]["isError"] is True
    assert "safe to retry" in resp["result"]["content"][0]["text"]
    assert metrics.counters.total("svc", "retries") == 2
    assert metrics.counters.total("svc", "retries_exhausted") == 1
    assert metrics.counters.total("svc", "tools_call_errors.upstream_error") == 1


# ── Non-recursive federation ─────────────────────────────────────────────

async def test_federated_inbound_call_never_retries_even_with_proof():
    """A gateway that received this call from ANOTHER gateway must not
    retry — the outer gateway already retries the whole round trip.
    Without this a 2-hop chain turns 3 attempts into 3x3=9."""
    await caller_context.capture({"x-aw-gateway-federated": "1"})
    try:
        up = _up()
        up._post, calls = _scripted_post([
            httpx.ConnectError("boom"),  # provably retryable...
            {"content": [{"type": "text", "text": "ok"}], "isError": False},
        ])

        resp = await up.call_tool("thing", {}, req_id=1, idempotent_hint=True)  # ...and idempotent too

        assert calls["n"] == 1  # ...but still only tried once
        assert resp["result"]["isError"] is True
        assert metrics.counters.total("svc", "retries") == 0
    finally:
        await caller_context.capture({})  # don't leak into the next test's task


async def test_gateway_upstream_marks_outbound_calls_as_federated():
    from gateway.upstream import GatewayUpstream

    up = GatewayUpstream("leaf", {"url": "http://leaf.example/mcp"}, "own-id", 6)
    assert up._client_headers()["X-Aw-Gateway-Federated"] == "1"


# ── Route annotations wiring (Gateway._add_route -> route_annotations) ───

def test_add_route_stores_idempotent_hint_without_touching_routes_tuple():
    gw = Gateway([])
    gw._add_route("svc", {"name": "thing", "description": "d",
                          "annotations": {"idempotentHint": True}})

    assert gw.routes["svc__thing"] == ("svc", "thing")  # tuple shape unchanged
    assert gw.route_annotations["svc__thing"] == {"idempotentHint": True}


def test_add_route_with_no_annotations_defaults_to_empty():
    gw = Gateway([])
    gw._add_route("svc", {"name": "thing", "description": "d"})

    assert gw.route_annotations["svc__thing"] == {}


def test_drop_local_routes_also_drops_its_annotations():
    gw = Gateway([])
    gw._add_route("svc", {"name": "thing", "annotations": {"idempotentHint": True}})
    gw._drop_local_routes("svc")

    assert "svc__thing" not in gw.routes
    assert "svc__thing" not in gw.route_annotations


# ── End-to-end through Gateway.handle() ──────────────────────────────────

async def test_handle_passes_idempotent_hint_through_to_call_tool():
    gw = Gateway([])
    up = _up("svc")
    up._post, calls = _scripted_post([
        httpx.ReadTimeout("ambiguous"),
        {"content": [{"type": "text", "text": "ok"}], "isError": False},
    ])
    gw.upstreams["svc"] = up
    gw._add_route("svc", {"name": "thing", "annotations": {"idempotentHint": True}})

    resp = await gw.handle({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "svc__thing", "arguments": {}},
    })

    assert calls["n"] == 2  # retried BECAUSE the route's annotation said so
    assert resp["result"]["isError"] is False


async def test_handle_without_idempotent_hint_does_not_retry_ambiguous_failure():
    gw = Gateway([])
    up = _up("svc")
    up._post, calls = _scripted_post([httpx.ReadTimeout("ambiguous")])
    gw.upstreams["svc"] = up
    gw._add_route("svc", {"name": "thing"})  # no annotations at all

    resp = await gw.handle({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "svc__thing", "arguments": {}},
    })

    assert calls["n"] == 1
    assert "UNCERTAIN" in resp["result"]["content"][0]["text"]


async def test_unknown_tool_is_counted():
    gw = Gateway([])

    await gw.handle({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "nope__nothing", "arguments": {}},
    })

    assert metrics.counters.total(metrics.UNROUTED, "tools_call_errors.unknown_tool") == 1


# ── /healthz surfacing ────────────────────────────────────────────────────

def test_healthz_snapshot_includes_zero_filled_metrics_for_a_quiet_upstream():
    snap = metrics.counters.snapshot(["svc"])

    assert snap["svc"]["retries"] == 0
    assert snap["svc"]["retries_exhausted"] == 0
    assert snap["svc"]["tools_call_errors"] == {
        "unknown_tool": 0, "upstream_error": 0, "timeout": 0}
    assert snap["svc"]["upstream_unavailable_seconds"] == 0


def test_park_recovery_records_upstream_unavailable_seconds(monkeypatch):
    import time as time_module

    gw = Gateway([])
    fake_now = {"t": 1000.0}
    monkeypatch.setattr(time_module, "monotonic", lambda: fake_now["t"])

    gw._park("svc", "boom", tools=[])
    fake_now["t"] += 42.0
    gw._unpark("svc")

    assert metrics.counters.total("svc", "upstream_unavailable_seconds") == 42.0
