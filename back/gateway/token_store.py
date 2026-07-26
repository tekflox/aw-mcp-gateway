"""Link-token minting/hashing/verification for ``/link`` reverse-registration.

Closes the token TODO on top of the ``remote_upstream.py`` skeleton: an
opaque ``awlk_<id16>_<secret32>`` token, SHA-256 hashed at rest, checked
against a per-token glob scope before a connector's tools get published.

Storage is abstracted behind ``TokenStore`` per the architect's design
(hash lives in the *user's own* Postgres, data plane — see the
``project_aw_apps_distribution_mcp_wrapper`` KB memory). No Postgres wiring
exists in this repo yet, so the only implementation today is
``FileTokenStore`` (plain JSON file). Swap in a ``PgTokenStore`` behind the
same interface once that lands — nothing above this module should need to
change.

TODO(PG): implement ``PgTokenStore`` against the data-plane Postgres and
select it in ``config.py``/``server.py`` once that connection exists.
"""

from __future__ import annotations

import fnmatch
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field

log = logging.getLogger("aw-mcp-gateway")

TOKEN_PREFIX = "awlk"
DEFAULT_SCOPES = ["*:*"]  # "{app_name_glob}:{tool_name_glob}" — "*:*" = unrestricted


def mint_token_value() -> tuple[str, str, str]:
    """Returns (full_token, id16, hash). The full token is shown to the
    caller exactly once — only the hash is ever persisted."""
    id16 = secrets.token_hex(8)
    secret32 = secrets.token_hex(16)
    full = f"{TOKEN_PREFIX}_{id16}_{secret32}"
    return full, id16, hash_token(full)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def parse_token_id(token: str) -> str | None:
    """Pull the id16 lookup key out of an ``awlk_<id16>_<secret32>`` token
    without needing the store — used to find the candidate record before
    doing the real (constant-time) hash comparison."""
    parts = token.split("_")
    if len(parts) != 3 or parts[0] != TOKEN_PREFIX:
        return None
    return parts[1]


@dataclass
class LinkToken:
    id: str
    hash: str
    label: str = ""
    scopes: list[str] = field(default_factory=lambda: list(DEFAULT_SCOPES))
    created_at: float = 0.0
    revoked: bool = False

    def public_dict(self) -> dict:
        return {
            "id": self.id,
            "label": self.label,
            "scopes": self.scopes,
            "created_at": self.created_at,
            "revoked": self.revoked,
        }

    def allows(self, app_name: str, tool_name: str) -> bool:
        for scope in self.scopes:
            app_glob, _, tool_glob = scope.partition(":")
            tool_glob = tool_glob or "*"
            if fnmatch.fnmatch(app_name, app_glob) and fnmatch.fnmatch(tool_name, tool_glob):
                return True
        return False


class TokenStore:
    """Abstract interface — every ``/link`` auth/scope decision goes through
    this, never a direct file/DB read, so a future ``PgTokenStore`` is a
    drop-in swap."""

    def verify(self, token: str) -> LinkToken | None:
        raise NotImplementedError

    def filter_scoped_tools(self, link_token: LinkToken, app_name: str, tools: list[dict]) -> list[dict]:
        return [t for t in tools if link_token.allows(app_name, t.get("name", ""))]

    def mint(self, label: str = "", scopes: list[str] | None = None) -> tuple[str, LinkToken]:
        raise NotImplementedError

    def revoke(self, token_id: str) -> bool:
        raise NotImplementedError

    def list(self) -> list[LinkToken]:
        raise NotImplementedError


class FileTokenStore(TokenStore):
    """Plain-JSON-file implementation — fine for a single-host dev/BYOD
    deployment, not a substitute for the eventual Postgres-backed store."""

    def __init__(self, path: str):
        self.path = path
        self._tokens: dict[str, LinkToken] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path) as f:
                raw = json.load(f)
        except (json.JSONDecodeError, OSError):
            log.warning("link token store at %s is unreadable — starting empty", self.path)
            return
        for entry in raw.get("tokens", []):
            tok = LinkToken(**entry)
            self._tokens[tok.id] = tok

    def _save(self) -> None:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"tokens": [vars(t) for t in self._tokens.values()]}, f, indent=2)
        os.replace(tmp, self.path)

    def verify(self, token: str) -> LinkToken | None:
        token_id = parse_token_id(token or "")
        if not token_id:
            return None
        record = self._tokens.get(token_id)
        if record is None or record.revoked:
            return None
        if not hmac.compare_digest(record.hash, hash_token(token)):
            return None
        return record

    def mint(self, label: str = "", scopes: list[str] | None = None) -> tuple[str, LinkToken]:
        full, id16, digest = mint_token_value()
        record = LinkToken(id=id16, hash=digest, label=label,
                           scopes=list(scopes) if scopes else list(DEFAULT_SCOPES),
                           created_at=time.time())
        self._tokens[record.id] = record
        self._save()
        return full, record

    def revoke(self, token_id: str) -> bool:
        record = self._tokens.get(token_id)
        if record is None:
            return False
        record.revoked = True
        self._save()
        return True

    def list(self) -> list[LinkToken]:
        return sorted(self._tokens.values(), key=lambda t: t.created_at)
