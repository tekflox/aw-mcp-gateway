"""ConfigGateway — a filtered view of a Gateway restricted to a named config's
upstreams (one scoped Streamable HTTP endpoint per config, e.g. ``/mcp/crispal``
next to the unscoped ``/mcp``).

A named config ("profile") is more than an upstream allowlist. Ported in full
from the in-repo mcp-gateway's ``ConfigGateway``, it also carries three
per-profile policies, each keyed off *which upstream* a call routes to:

* **Run policy** (agents-platform upstream) — glob allowlists for which agents
  / workflows this profile may *run*. Read/CRUD tools are never gated; only the
  run entrypoints. Empty allowlist = unrestricted.
* **Approval gate** (agents-platform upstream) — globs for runs that need
  human-in-the-loop Telegram approval before dispatch, with an "always allow"
  exemption list so a profile can say "everything needs approval EXCEPT these".
  Fail-closed on denial/timeout/error.
* **Namespace injection** (knowledge-base / presentation upstreams) — forces a
  profile's scope onto tool arguments the caller cannot see or unset, so one
  shared upstream can be sliced per profile without a dedicated MCP server per
  slice.

Which upstream each policy applies to is *not* hardcoded to the monolith's
names: every workspace names its apps differently (``agents-platform`` there,
``agents-platform-runners`` here), so each policy matches against a set of
accepted names, overridable per deployment via ``config/gateway.json``'s
``policy_upstreams`` block.
"""

from __future__ import annotations

import asyncio
import fnmatch
import logging
import os

import httpx

log = logging.getLogger("aw-mcp-gateway")

#: Upstream names each policy applies to, by role. A deployment overrides these
#: in ``config/gateway.json`` under ``policy_upstreams``; the defaults cover
#: both the decoupled aw-workspace app names and the monolith names they were
#: ported from, so a config file copied across from agentic-workspace keeps
#: working without an edit.
DEFAULT_POLICY_UPSTREAMS: dict[str, list[str]] = {
    "agents_platform": ["agents-platform-runners", "agents-platform"],
    "knowledge_base": ["kb", "aw-knowledge-base"],
    "presentation": ["aw-presentation", "presentations"],
}

#: Seconds between polls of an outstanding approval, and how many to make —
#: 150 × 2s ≈ 300s, matching the Agents Platform approval expiry.
_APPROVAL_POLL_INTERVAL_S = 2
_APPROVAL_POLL_ATTEMPTS = 150


def policy_upstreams(overrides: dict | None = None) -> dict[str, set[str]]:
    """Resolve the role → accepted-upstream-names map, merging ``overrides``
    (a ``gateway.json`` ``policy_upstreams`` block) over the defaults. A role
    set to a bare string is accepted as a one-element list."""
    resolved = {role: set(names) for role, names in DEFAULT_POLICY_UPSTREAMS.items()}
    for role, names in (overrides or {}).items():
        if role not in resolved:
            continue
        if isinstance(names, str):
            names = [names]
        if isinstance(names, list):
            resolved[role] = {str(n) for n in names if n}
    return resolved


class ConfigGateway:
    # Tools on the knowledge-base upstream that accept a gateway-injected scope.
    _KB_SCOPED_TOOLS = {"search_knowledge_base", "update_knowledge_base",
                        "delete_knowledge_base"}
    # Same pattern for presentations.
    _PRESENTATION_SCOPED_TOOLS = {
        "create_presentation", "update_presentation", "delete_presentation",
        "list_presentations", "export_presentation_to_image", "share_presentation",
        "show_image",
    }

    # ── Run entrypoints on the agents-platform upstream ──────────────────────
    # Static entrypoints carry the slug in the arguments.
    _AP_AGENT_RUN_TOOLS = {"run_agent_async"}          # args["slug"]
    _AP_WORKFLOW_RUN_TOOLS = {"run_workflow_async"}    # args["slug"]
    _AP_PARALLEL_RUN_TOOLS = {"run_agents_parallel"}   # args["agents"][*]["slug"]
    # Dynamic per-resource runners are named ``agent_<slug>`` / ``workflow_<slug>``
    # (slug hyphens normalised to underscores). No static AP tool starts with
    # these prefixes, so a prefix test cleanly identifies a run of a named
    # resource with the slug encoded in the tool name.
    _AP_AGENT_RUN_PREFIX = "agent_"
    _AP_WORKFLOW_RUN_PREFIX = "workflow_"

    def __init__(self, gateway: "Gateway", spec: dict | list[str] | None = None,  # noqa: F821
                 name: str = "", *, agents_base: str = "",
                 upstream_roles: dict[str, set[str]] | None = None):
        # ``spec`` accepts the full named-config object OR the bare upstream
        # list this class used to take, so older callers keep working.
        if isinstance(spec, list):
            spec = {"upstreams": spec}
        spec = dict(spec or {})

        self._gateway = gateway
        self._name = name
        self._spec = spec
        self._allowed = set(spec.get("upstreams") or [])
        self._roles = upstream_roles or policy_upstreams()
        self._agents_base = agents_base

        # Optional profile-wide tool ACL. Patterns may target the stripped
        # tool name (``agent_crispal_sonnet``) or qualify it with the raw
        # upstream name (``agents-platform-runners__agent_crispal_sonnet``).
        # Missing means unrestricted for backwards compatibility; when set it
        # is enforced both while advertising tools and again at call time.
        raw_tools_allow = spec.get("tools_allow")
        if isinstance(raw_tools_allow, str):
            raw_tools_allow = [raw_tools_allow]
        self._tools_allow = [str(p) for p in (raw_tools_allow or []) if p]

        # A profile-level knowledge-base scope (str or list[str], None =
        # unrestricted) — enforced server-side regardless of what the caller
        # passes, so one shared index can be sliced per profile.
        self._kb_index = spec.get("kb_index") or None
        # Same idea for presentations: a single namespace string, or None.
        self._presentation_namespace = spec.get("presentation_namespace") or None

        def _globs(key: str) -> list[str]:
            raw = spec.get(key)
            if isinstance(raw, str):
                raw = [raw]
            return [str(p) for p in (raw or []) if p]

        # Run policy: which agents / workflows this profile may run at all.
        self._run_agents_allow = _globs("run_agents_allow")
        self._run_workflows_allow = _globs("run_workflows_allow")
        # Approval policy: which runs need Telegram approval first. Checked
        # AFTER the allowlist — a run must be allowed AND (if matched) approved.
        self._run_agents_approval = _globs("run_agents_approval")
        self._run_workflows_approval = _globs("run_workflows_approval")
        # Approval exemption: runs that go straight through even when they
        # match the approval globs (approval=["*"] + always_allow=["crispal*"]).
        self._run_agents_always_allow = _globs("run_agents_always_allow")
        self._run_workflows_always_allow = _globs("run_workflows_always_allow")

    # ── Role tests ───────────────────────────────────────────────────────────

    def _is_role(self, upstream: str, role: str) -> bool:
        return upstream in self._roles.get(role, set())

    def _tool_allowed(self, upstream: str, tool: str) -> bool:
        if not self._tools_allow:
            return True
        qualified = f"{upstream}__{tool}"
        return any(fnmatch.fnmatchcase(tool, pattern)
                   or fnmatch.fnmatchcase(qualified, pattern)
                   for pattern in self._tools_allow)

    # ── Run-policy helpers ───────────────────────────────────────────────────

    @staticmethod
    def _slug_matches(slug: str, patterns: list[str]) -> bool:
        """Case-insensitive glob match. Empty patterns = unrestricted (True).

        Hyphens and underscores are treated as equivalent because the dynamic
        per-resource runner tool names normalise the slug's hyphens to
        underscores (``agent_crispal_codex``) while the canonical slug and the
        ``run_*_async`` arguments keep hyphens (``crispal-codex``). Normalising
        both sides lets an exact pattern like ``crispal-codex`` match either."""
        if not patterns:
            return True

        def _norm(v: str) -> str:
            return (v or "").lower().replace("_", "-")

        s = _norm(slug)
        return any(fnmatch.fnmatchcase(s, _norm(p)) for p in patterns)

    def _list_hidden(self, tool: str) -> bool:
        """True if a *named* per-resource runner (``agent_<slug>`` /
        ``workflow_<slug>``) should be hidden from tools/list for this profile.
        Generic ``run_*_async`` tools are never hidden — they're enforced on the
        slug argument at call time, so hiding them here (with no args to judge)
        would wrongly drop them."""
        if not (self._run_agents_allow or self._run_workflows_allow):
            return False
        if tool.startswith(self._AP_AGENT_RUN_PREFIX):
            return not self._slug_matches(tool[len(self._AP_AGENT_RUN_PREFIX):],
                                          self._run_agents_allow)
        if tool.startswith(self._AP_WORKFLOW_RUN_PREFIX):
            return not self._slug_matches(tool[len(self._AP_WORKFLOW_RUN_PREFIX):],
                                          self._run_workflows_allow)
        return False

    def _run_policy_denied(self, tool: str, arguments: dict) -> str | None:
        """Return a rejection message if this profile may not run ``tool``,
        else None. Only run entrypoints are gated; every read/CRUD tool on the
        same upstream passes through untouched."""
        if not (self._run_agents_allow or self._run_workflows_allow):
            return None  # no policy configured → nothing to enforce

        if tool.startswith(self._AP_AGENT_RUN_PREFIX):
            slug = tool[len(self._AP_AGENT_RUN_PREFIX):]
            if not self._slug_matches(slug, self._run_agents_allow):
                return f"Agent '{slug}' is not runnable by this profile"
            return None
        if tool.startswith(self._AP_WORKFLOW_RUN_PREFIX):
            slug = tool[len(self._AP_WORKFLOW_RUN_PREFIX):]
            if not self._slug_matches(slug, self._run_workflows_allow):
                return f"Workflow '{slug}' is not runnable by this profile"
            return None

        if tool in self._AP_AGENT_RUN_TOOLS:
            slug = str((arguments or {}).get("slug") or "")
            if not self._slug_matches(slug, self._run_agents_allow):
                return f"Agent '{slug}' is not runnable by this profile"
            return None
        if tool in self._AP_WORKFLOW_RUN_TOOLS:
            slug = str((arguments or {}).get("slug") or "")
            if not self._slug_matches(slug, self._run_workflows_allow):
                return f"Workflow '{slug}' is not runnable by this profile"
            return None
        if tool in self._AP_PARALLEL_RUN_TOOLS:
            for entry in (arguments or {}).get("agents") or []:
                slug = str((entry or {}).get("slug") or "")
                if not self._slug_matches(slug, self._run_agents_allow):
                    return f"Agent '{slug}' is not runnable by this profile"
            return None
        return None

    # ── Approval gate ────────────────────────────────────────────────────────

    def _run_slugs(self, tool: str, arguments: dict) -> list[tuple[str, str]]:
        """The ``(kind, slug)`` pairs a run tool would launch, where kind is
        ``agent`` or ``workflow``. Non-run tools return ``[]``."""
        if tool.startswith(self._AP_AGENT_RUN_PREFIX):
            return [("agent", tool[len(self._AP_AGENT_RUN_PREFIX):])]
        if tool.startswith(self._AP_WORKFLOW_RUN_PREFIX):
            return [("workflow", tool[len(self._AP_WORKFLOW_RUN_PREFIX):])]
        if tool in self._AP_AGENT_RUN_TOOLS:
            return [("agent", str((arguments or {}).get("slug") or ""))]
        if tool in self._AP_WORKFLOW_RUN_TOOLS:
            return [("workflow", str((arguments or {}).get("slug") or ""))]
        if tool in self._AP_PARALLEL_RUN_TOOLS:
            return [("agent", str((e or {}).get("slug") or ""))
                    for e in (arguments or {}).get("agents") or []]
        return []

    def _approval_needed(self, tool: str, arguments: dict) -> str | None:
        """A label of the resource(s) that require approval, or None. A resource
        needs approval if it matches the approval globs AND is not exempted by
        the always-allow globs. Empty approval list for a kind = that kind never
        needs approval."""
        if not (self._run_agents_approval or self._run_workflows_approval):
            return None
        hits: list[str] = []
        for kind, slug in self._run_slugs(tool, arguments):
            if kind == "agent":
                pats, exempt = self._run_agents_approval, self._run_agents_always_allow
            else:
                pats, exempt = self._run_workflows_approval, self._run_workflows_always_allow
            is_exempt = bool(exempt) and self._slug_matches(slug, exempt)
            if pats and self._slug_matches(slug, pats) and not is_exempt:
                hits.append(f"{kind} '{slug}'")
        return ", ".join(hits) if hits else None

    def _approval_base(self) -> str:
        """Base URL of the Agents Platform that owns the Telegram approval
        flow. Prefers the explicit ``agents_base`` this instance was built
        with (derived by the server from the agents-platform upstream's own
        ``env.AGENTS_BASE`` — so it is whatever that upstream already talks
        to, with nothing extra to configure), then ``$AGENTS_BASE``."""
        return (self._agents_base or os.environ.get("AGENTS_BASE") or "").rstrip("/")

    async def _await_approval(self, resource: str, reason: str) -> bool:
        """POST an ``agent_run`` approval request to the Agents Platform and
        block until it is approved. Fail-closed: any error, denial, timeout —
        or no reachable platform at all — returns False."""
        base = self._approval_base()
        if not base:
            log.error("approval gate: no agents-platform base URL — refusing %s", resource)
            return False
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                r = await client.post(
                    f"{base}/api/telegram/approval/request",
                    json={"secret_name": resource, "reason": reason,
                          "request_type": "agent_run"},
                )
                if r.status_code != 200:
                    log.warning("approval gate: request failed %s: %s",
                                r.status_code, r.text[:200])
                    return False
                rid = (r.json() or {}).get("request_id")
                if not rid:
                    return False
                log.info("approval gate: request_id=%s resource=%s profile=%s",
                         rid, resource, self._name)
                for _ in range(_APPROVAL_POLL_ATTEMPTS):
                    await asyncio.sleep(_APPROVAL_POLL_INTERVAL_S)
                    s = await client.get(f"{base}/api/telegram/approval/status/{rid}")
                    if s.status_code != 200:
                        continue
                    status = (s.json() or {}).get("status")
                    if status == "approved":
                        return True
                    if status in ("denied", "expired"):
                        return False
                return False
        except Exception:
            log.exception("approval gate: error for %s", resource)
            return False

    # ── MCP surface ──────────────────────────────────────────────────────────

    def _filtered_tools(self) -> list[dict]:
        """Tools for this config, with the ``{upstream}__`` prefix stripped —
        the config's own MCP server name already scopes the tools, so a model
        sees e.g. ``get_site_info`` instead of ``crispal__get_site_info``."""
        tools = []
        for t in self._gateway.agg_tools:
            route = self._gateway.routes.get(t["name"], ("", ""))
            upstream, real_tool = route[0], route[1]
            if upstream not in self._allowed:
                continue
            if not self._tool_allowed(upstream, real_tool):
                continue
            # Hide run-tools this profile isn't allowed to run, so tools/list
            # only advertises the runnable subset.
            if self._is_role(upstream, "agents_platform") and self._list_hidden(real_tool):
                continue
            tool = dict(t)
            # ``public_name`` builds the aggregated name from the DISPLAY server
            # (workspace prefix + upstream, hyphens normalised to underscores),
            # so strip against that same form rather than the raw upstream key —
            # comparing against the hyphenated name never matches for the
            # (nearly universal) hyphenated server names, and the redundant
            # prefix this method exists to strip leaks through silently.
            display = (f"{self._gateway.workspace_name}__{upstream}"
                       if getattr(self._gateway, "workspace_name", "") else upstream)
            prefix = f"{display.replace('-', '_')}__"
            if tool["name"].startswith(prefix):
                tool["name"] = tool["name"][len(prefix):]
            tools.append(tool)
        return tools

    def _resolve_route(self, public: str) -> tuple[str | None, dict | None, str | None]:
        """Map a (possibly prefix-stripped) public tool name back to a route.

        Returns ``(rewritten_name, route, error)`` — ``rewritten_name`` is the
        aggregated name the parent Gateway knows, or None when unresolvable."""
        route = self._gateway.routes.get(public)
        if route:
            return public, route, None
        for ups in self._allowed:
            display = (f"{self._gateway.workspace_name}__{ups}"
                       if getattr(self._gateway, "workspace_name", "") else ups)
            candidate = f"{display.replace('-', '_')}__{public}"
            route = self._gateway.routes.get(candidate)
            if route:
                return candidate, route, None
        return None, None, f"Tool '{public}' is not available in this config"

    async def handle(self, msg: dict) -> dict | None:
        method = msg.get("method", "")
        req_id = msg.get("id")

        if method in ("initialize", "ping"):
            return await self._gateway.handle(msg)
        if method in ("notifications/initialized", "notifications/cancelled"):
            return None

        if method == "tools/list":
            return {"jsonrpc": "2.0", "id": req_id,
                    "result": {"tools": self._filtered_tools()}}

        if method == "tools/call":
            return await self._handle_call(msg, req_id)

        return {"jsonrpc": "2.0", "id": req_id, "error": {
            "code": -32601, "message": f"Unknown method: {method}"}}

    async def _handle_call(self, msg: dict, req_id) -> dict:
        def _error(message: str) -> dict:
            return {"jsonrpc": "2.0", "id": req_id,
                    "error": {"code": -32602, "message": message}}

        params = dict(msg.get("params") or {})
        public = params.get("name", "")
        resolved, route, error = self._resolve_route(public)
        if error or not route or route[0] not in self._allowed:
            return _error(error or f"Tool '{public}' is not available in this config")
        params["name"] = resolved
        upstream, tool = route
        if not self._tool_allowed(upstream, tool):
            return _error(f"Tool '{public}' is not available in this config")
        args = dict(params.get("arguments") or {})

        if self._is_role(upstream, "agents_platform"):
            denied = self._run_policy_denied(tool, args)
            if denied:
                return _error(denied)
            # Human-in-the-loop gate: block the call until the run is approved
            # on Telegram. Fail-closed on denial/timeout/error.
            need = self._approval_needed(tool, args)
            if need:
                profile = self._name or "unknown"
                log.info("approval gate: profile=%s awaiting approval for %s", profile, need)
                approved = await self._await_approval(
                    need, f"Profile '{profile}' wants to run {need}")
                if not approved:
                    return _error(f"Run of {need} was not approved")

        # Namespace injection — forced server-side, overriding anything the
        # caller passed: the argument is not in the tool's public schema, so a
        # model has no way to set (or unset) it itself.
        if self._kb_index and self._is_role(upstream, "knowledge_base") \
                and tool in self._KB_SCOPED_TOOLS:
            args["_gateway_kb_index"] = self._kb_index
        if self._presentation_namespace and self._is_role(upstream, "presentation") \
                and tool in self._PRESENTATION_SCOPED_TOOLS:
            args["_gateway_presentation_namespace"] = self._presentation_namespace

        params["arguments"] = args
        return await self._gateway.handle({**msg, "params": params})
