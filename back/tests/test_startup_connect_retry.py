"""resilience:mcp-gateway-windows-pilot-dns-race.

A freshly (re)started gateway container can dial a declared upstream before
aardvark-dns has propagated a just-(re)created container's hostname record
to it — the very first ``initialize`` POST raises ``httpx.ConnectError``
("Name or service not known"), even though the same hostname resolves fine
moments later from every other container on the network. Before this fix
``HttpUpstream.start()`` had no retry around that first connection, so
``_start_one`` logged "failed to start upstream" and gave up for good —
windows-pilot's 21 tools never appeared until the next full gateway restart
happened to win the race.

The fix retries ONLY a connection-level failure (DNS/connect refused) with
backoff; a genuine auth/4xx response must still fail immediately, not be
masked as a startup race.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from gateway.upstream import (
    START_CONNECT_MAX_ATTEMPTS,
    HttpUpstream,
)


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "http://upstream.example/mcp")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(f"{status}", request=request, response=response)


def _scripted_post(behaviors):
    """Same fake as test_proof_gated_retry.py's ``_scripted_post`` — behaviors[i]
    is either an Exception to raise on the i-th call, or a dict to return."""
    calls = {"n": 0}

    async def _post(msg):
        i = min(calls["n"], len(behaviors) - 1)
        calls["n"] += 1
        behavior = behaviors[i]
        if isinstance(behavior, BaseException):
            raise behavior
        return {"jsonrpc": "2.0", "id": msg.get("id"), "result": behavior}

    return _post, calls


def _up(name: str = "aw-windows-pilot") -> HttpUpstream:
    return HttpUpstream(name, {"url": "http://workspace-host:9030/api/apps/windows-pilot/mcp"})


async def _no_sleep(_seconds):
    return None


async def test_transient_dns_failure_at_startup_is_retried_and_recovers(monkeypatch):
    """The exact 2026-09-04 windows-pilot reproduction: ConnectError on the
    first dial, resolved by the second — start() must succeed, not sink the
    upstream for good."""
    up = _up()
    up._post, calls = _scripted_post([
        httpx.ConnectError("[Errno -2] Name or service not known"),
        {"serverInfo": {"name": "windows-pilot"}},
        {"tools": [{"name": "t1"}]},
    ])
    sleeps = []

    async def _record_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _record_sleep)

    await up.start()

    assert calls["n"] == 3  # 1 failed init + 1 succeeded init + tools/list
    assert up.tools == [{"name": "t1"}]
    assert sleeps == [1.0]  # backed off once, per START_CONNECT_RETRY_BACKOFF_SECONDS[0]


async def test_startup_connect_retry_is_exhausted_after_max_attempts(monkeypatch):
    """A sustained outage (not a race) must still fail — this is a bounded
    retry, not infinite patience. The gateway's own reload()/parked-retry
    loop is what eventually recovers a sustained failure, not this."""
    up = _up()
    up._post, calls = _scripted_post([httpx.ConnectError("still down")])  # always fails
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

    with pytest.raises(httpx.ConnectError):
        await up.start()

    assert calls["n"] == START_CONNECT_MAX_ATTEMPTS


async def test_connect_timeout_is_also_retried(monkeypatch):
    up = _up()
    up._post, calls = _scripted_post([
        httpx.ConnectTimeout("timed out connecting"),
        {"serverInfo": {"name": "windows-pilot"}},
        {"tools": []},
    ])
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

    await up.start()

    assert calls["n"] == 3


async def test_auth_failure_is_not_retried_and_surfaces_immediately(monkeypatch):
    """A 4xx is a real failure, not a startup race — must not be masked by
    the connect retry, and must not cost the caller the retry budget/delay."""
    up = _up()
    up._post, calls = _scripted_post([_http_status_error(401)])
    slept = []

    async def _record_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", _record_sleep)

    with pytest.raises(httpx.HTTPStatusError):
        await up.start()

    assert calls["n"] == 1  # never retried
    assert slept == []


async def test_no_retry_needed_is_unaffected():
    """The common case — first dial just works — must not change shape."""
    up = _up()
    up._post, calls = _scripted_post([
        {"serverInfo": {"name": "windows-pilot"}},
        {"tools": [{"name": "t1"}]},
    ])

    await up.start()

    assert calls["n"] == 2
    assert up.tools == [{"name": "t1"}]
