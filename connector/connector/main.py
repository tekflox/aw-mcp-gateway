from __future__ import annotations

import asyncio
import logging

from .config import load_config
from .link_client import run as run_link
from .local_mcp import LocalMcp


async def _main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = load_config()
    mcp_spec = cfg["mcp"]
    local = LocalMcp(mcp_spec["command"], mcp_spec.get("args", []), mcp_spec.get("env"))
    await local.start()
    await run_link(cfg["app_name"], cfg.get("workspace_name", ""), cfg["gateway_url"], cfg["token"], local)


def main() -> None:
    asyncio.run(_main())


if __name__ == "__main__":
    main()
