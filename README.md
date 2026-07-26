# aw-mcp-gateway

A **standalone, public** app that exposes stdio MCP servers over Streamable
HTTP — one gateway, reachable by any HTTP-capable client (models, other
apps), fronting a pool of local and remote MCP upstreams.

This is the standalone twin of
[`src/mcp/gateway.py`](https://github.com/tekflox/agentic-workspace) in the
main Agentic Workspace (AW) repo. That in-repo gateway keeps running
unchanged for now — this repo is a scaffold that ports its logic out into a
publishable, independently-deployable app, as the first step toward apps that
run on a user's own machine (BYOD) needing a single place to reach MCP tools
without every app spawning its own stdio children. See the design notes in
[Architecture](#architecture) below.

## Why public

Same transparency principle as
[`tekflox/aw-remote-host`](https://github.com/tekflox/aw-remote-host): this
runs on a user's own machine, so the code doing it is open for anyone to
read before they run it.

## Repository layout

```
back/         the gateway server — spawns/proxies MCP upstreams (incl. other
              gateways), serves Streamable HTTP (:9200) + a /link WebSocket
front/        management UI (React + Vite) — upstream status, hosts & apps
connector/    aw-mcp-stdio-wrapper — runs a stdio MCP elsewhere and dials
              INTO back/'s /link, so that app's tools show up on the gateway
              without opening any inbound port on the app's own machine
```

## Architecture

```
                    ┌─────────────┐
  local stdio MCP ──┤             │
  (child process)   │   back/     │◄── HTTP ── any client (model, app, curl)
                    │  gateway    │      POST /mcp  {jsonrpc tools/call ...}
  another gateway ──┤             │
  (type: gateway)   └──────┬──────┘
       HTTP /mcp   ▲       │
  remote app  ──────┘      └── front/ (React) fetches /healthz, mints/lists
  (via connector/)                /link tokens via the Hosts & Apps view
       WS /link
       (dial OUT,
        no inbound port)
```

* **`back/`** owns the upstream pool. Three kinds:
  * **Local** — a stdio MCP child it spawns itself (`Upstream` in
    `back/gateway/upstream.py`), or an already-HTTP upstream it proxies to
    (`HttpUpstream`). Direct port of the in-repo gateway's
    `Upstream`/`HttpUpstream`/`Gateway`/`build_app` — same reader-loop/
    Future-dispatch design, same allowlist + named-config (`ConfigGateway`)
    scoping.
  * **Gateway** (federation) — `GatewayUpstream` (`back/gateway/upstream.py`,
    extends `HttpUpstream`) makes *another* aw-mcp-gateway an upstream of
    this one: its entire tool pool gets aggregated in, one namespace level
    deeper (`{that-gateway}__{its-upstream}__{tool}`, never double-prefixed
    within one hop). Configured via `type: "gateway"` in `back/config/mcp.json`
    (`url` + `token`). Connecting checks the remote's `/healthz` — which
    reports `gateway_id` and its own `federation_chain` — and refuses to
    federate if this gateway's id is already in that chain (would close a
    loop) or if the resulting chain would exceed `max_federation_depth`
    (`back/config/gateway.json`). Both checks run once at connect time.
  * **Remote** (reverse registration) — an app running `connector/`
    elsewhere, registered over the `/link` WebSocket (`RemoteUpstream` in
    `back/gateway/remote_upstream.py`). A connector dials in, registers its
    tools, and `tools/call` routes back down the live socket. Auth is a real
    `awlk_<id16>_<secret32>` token, SHA-256-hashed at rest and verified via
    `TokenStore` (`back/gateway/token_store.py`) — `FileTokenStore` is the
    only implementation today (plain JSON at `back/config/link_tokens.json`,
    gitignored); swap in a Postgres-backed store behind the same interface
    once the data-plane Postgres exists (see the
    `project_aw_apps_distribution_mcp_wrapper` design memory). Each token
    carries a glob `scopes` allowlist (`"{app_glob}:{tool_glob}"`,
    default `["*:*"]`) — tools outside scope are silently dropped from what
    gets published, and a registration with *nothing* left in scope is
    rejected outright. App-name collisions are numbered, not route-mangled:
    a second connector registering the same base name as a live one gets
    both renamed to `"Browser 1"`/`"Browser 2"` (never
    `{app}_{server}__{tool}`). Reconnects are keyed by the token's stable id,
    not the app name, so the same host redialing lands back on its existing
    public name with no duplicate routes; a disconnect withdraws that
    remote's routes immediately (truth = live connection) without forgetting
    its name reservation.
* **`connector/`** is `aw-mcp-stdio-wrapper`: it spawns exactly one local
  stdio MCP (`connector/connector/local_mcp.py`, same spawn/dispatch pattern
  as `back/`'s `Upstream`, duplicated rather than shared since the two are
  independently deployable) and dials `back/`'s `/link` endpoint
  (`connector/connector/link_client.py`), with reconnect + exponential
  backoff. Config is one `app.json` (see `app.example.json`) — app name,
  gateway URL, token, and the local MCP's command/args/env.
* **`front/`** is a React + Vite (matches the stack used across AW's other
  management UIs) shell with three views: Status (health probe), Upstreams
  (local, remote, and federated gateways, read-only), and Hosts & Apps
  (mint/list/revoke real link tokens against `back/`'s new `/link-tokens`
  REST endpoints — needs the gateway's own bearer token pasted in, since
  there's no separate admin session yet).

## Running back/ locally

```sh
cd back
pip install -r requirements.txt
python3 -m gateway.server --port 9200
```

Ships with one real (if trivial) local upstream enabled by default —
`example-echo` (`back/gateway/examples/echo_server.py`) — so a fresh
checkout has something to talk to immediately:

```sh
curl -s http://127.0.0.1:9200/healthz
curl -s -X POST http://127.0.0.1:9200/mcp \
  -H "Authorization: Bearer $(python3 -c 'import json;print(json.load(open("config/gateway.json"))["token"])')" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"example_echo__echo","arguments":{"text":"hi"}}}'
```

Set a real `token` in `back/config/gateway.json` before exposing this off
loopback — an ephemeral one is minted (and logged) if left empty.

## Running front/ locally

```sh
cd front
npm install
npm run dev     # proxies /api -> http://127.0.0.1:9200 (back/), see vite.config.ts
```

## Running connector/ locally

```sh
cd connector
pip install -r requirements.txt
cp app.example.json app.json   # edit app_name / gateway_url / token / mcp command
python3 -m connector.main
```

## Testing

* **back/** — `pip install -r requirements-dev.txt && pytest -q` in `back/`
  runs the real test suite (`back/tests/`):
  * `test_federation.py` — boots a real leaf gateway (`example-echo`) over
    real HTTP with `uvicorn.Server`, points a parent `Gateway` at it via a
    `type: gateway` upstream, and asserts `tools/call` round-trips through
    both hops; separately proves `max_federation_depth` and the ancestor-
    chain cycle check each reject a bad federation without touching the
    other's passing case.
  * `test_link_registration.py` — drives `/link` through Starlette's
    `TestClient` (real WebSocket handshake, no mocks): unknown/revoked
    tokens are rejected, scoped tokens publish only the tools their globs
    allow (and a token matching nothing is rejected outright), a reconnect
    on the same token keeps its public name, and two different tokens
    colliding on the same base app name both get numbered.
  * Also still runs the original standalone smoke test (`python3 -c "..."`
    in CI) that boots the gateway and answers one real `tools/call` — kept
    as a fast pre-flight before the full suite.
* **front/** — `npm run build` (tsc + vite build) passes clean.
* **connector/** — end-to-end verified against a live `back/` instance: the
  connector dialed `/link`, registered `example-echo` as app `test-app`, and
  a `curl POST /mcp` call to `test_app__echo` round-tripped through the
  WebSocket and back with the correct result.

## What's real vs. stub

| Part | Real | Stub / TODO |
|---|---|---|
| `back/` local upstreams (stdio + HTTP) | Full port of the in-repo gateway's `Upstream`/`HttpUpstream`/`Gateway`/`ConfigGateway` | Per-profile run-policy/approval-gate/KB-scoping hooks from the in-repo version were intentionally dropped — those are agentic-workspace-specific, not part of the generic gateway |
| `back/` gateway↔gateway federation (`type: gateway`) | `GatewayUpstream` aggregates a remote gateway's whole tool pool, with cycle detection + depth cap enforced against the remote's `/healthz`, verified end to end against a real second `uvicorn` instance | Cycle/depth checks are point-in-time (at connect), not re-validated if the federation graph changes later without a restart/reconnect |
| `back/` `/link` remote registration | Real `awlk_...` token (SHA-256 hash, `TokenStore`), glob scope enforcement, numbered app-name collisions, reconnect-safe by token identity — all verified end to end | `TokenStore` only has a `FileTokenStore` impl (plain JSON) — swap in the Postgres-backed one once the data-plane DB exists; a disconnected app's name slot is held forever (not freed for reuse by an unrelated app) |
| `connector/` | Spawns a local stdio MCP and dials/registers/serves calls over `/link`, with reconnect+backoff, verified end to end | One MCP per connector instance (no multi-MCP fan-in) |
| `front/` | Builds; Status shows live health; Upstreams lists local/remote/federated with connected status; Hosts & Apps mints/lists/revokes real link tokens (paste the gateway bearer token in) | No separate admin session/login — the bearer token is typed in ad hoc each time |

## Do not touch the in-repo gateway

This scaffold **ports** `src/mcp/gateway.py`'s logic out of the main
`agentic-workspace` repo — it does not replace it. The in-repo gateway
keeps running as-is; migrating real traffic over to this standalone app is a
separate, later step.

## CI

`.github/workflows/build.yml` is manual (`workflow_dispatch`) only — no
auto-build on push, to conserve CI minutes during early development.

## License

MIT — see [LICENSE](LICENSE).
