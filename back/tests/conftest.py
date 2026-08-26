from __future__ import annotations

import pytest

from gateway import config


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
