"""Connector config — one JSON file, contract:

{
  "app_name": "my-app",              // unique name this app's tools show up as
  "workspace_name": "my-workspace",  // optional; defaults to AW_WORKSPACE_NAME
                                      // or WORKSPACE_NAME. When set, gateway
                                      // publishes "<workspace>__<app_name>__<tool>".
  "gateway_url": "wss://gateway.example.com/link",
  "token": "awlk_xxxx",              // opaque link token (see the top-level
                                      // README's "connector" section for the
                                      // current placeholder-vs-final-design
                                      // status of this token)
  "mcp": {
    "command": "python3",
    "args": ["-m", "my_app.mcp_server"],
    "env": {}
  }
}

Path defaults to ./app.json, overridable via APP_CONFIG env var.
"""

from __future__ import annotations

import json
import os

CONFIG_PATH = os.environ.get("APP_CONFIG", "app.json")


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)
    for required in ("app_name", "gateway_url", "token", "mcp"):
        if required not in cfg:
            raise ValueError(f"{CONFIG_PATH}: missing required field '{required}'")
    cfg["workspace_name"] = (
        cfg.get("workspace_name")
        or os.environ.get("AW_WORKSPACE_NAME")
        or os.environ.get("WORKSPACE_NAME")
        or ""
    )
    return cfg
