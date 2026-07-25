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
back/         the gateway server — spawns/proxies MCP upstreams, serves
              Streamable HTTP (:9200) + a /link WebSocket for remote apps
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
  remote app   ─────┤             │
  (via connector/)  └──────┬──────┘
       WS /link            │
       (dial OUT,          └── front/ (React) fetches /healthz, will grow
        no inbound port)       into the "Hosts & Apps" token-mint UI
```

* **`back/`** owns the upstream pool. Two kinds today:
  * **Local** — a stdio MCP child it spawns itself (`Upstream` in
    `back/gateway/upstream.py`), or an already-HTTP upstream it proxies to
    (`HttpUpstream`). This part is a direct, working port of the in-repo
    gateway's `Upstream`/`HttpUpstream`/`Gateway`/`build_app` — same
    reader-loop/Future-dispatch design, same allowlist + named-config
    (`ConfigGateway`) scoping.
  * **Remote** — an app running `connector/` elsewhere, registered over the
    `/link` WebSocket (`RemoteUpstream` in `back/gateway/remote_upstream.py`).
    This is a **functional skeleton, not the final design** — see the status
    note in that file's docstring. What's real: a connector can dial in,
    register its tools, and have `tools/call` routed back down the live
    socket, verified end to end (see [Testing](#testing)). What's still a
    placeholder: the link token is a single shared secret compared by string
    equality, not the `awlk_<id16>_<secret32>` scheme (hashed, stored in the
    user's own Postgres, scoped per app/host, instantly revocable) the
    architect's design closes on. That scheme, plus the "Hosts & Apps" token
    UI (`front/src/views/Hosts.tsx`), plus app-name collision handling, are
    tracked as follow-up cards.
* **`connector/`** is `aw-mcp-stdio-wrapper`: it spawns exactly one local
  stdio MCP (`connector/connector/local_mcp.py`, same spawn/dispatch pattern
  as `back/`'s `Upstream`, duplicated rather than shared since the two are
  independently deployable) and dials `back/`'s `/link` endpoint
  (`connector/connector/link_client.py`), with reconnect + exponential
  backoff. Config is one `app.json` (see `app.example.json`) — app name,
  gateway URL, token, and the local MCP's command/args/env.
* **`front/`** is a React + Vite (matches the stack used across AW's other
  management UIs) shell with three placeholder views: Status (health probe),
  Upstreams (local vs. remote, read-only), and Hosts & Apps (where token
  minting will live once `back/` has a real token store).

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

Each part has a real smoke test:

* **back/** — boots, spawns `example-echo`, and answers a real `tools/call`
  over `/mcp` (no mocks — verified with `python3 -c "..."` against the actual
  `Gateway` class).
* **front/** — `npm run build` (tsc + vite build) passes clean.
* **connector/** — end-to-end verified against a live `back/` instance: the
  connector dialed `/link`, registered `example-echo` as app `test-app`, and
  a `curl POST /mcp` call to `test_app__echo` round-tripped through the
  WebSocket and back with the correct result.

## What's real vs. stub

| Part | Real | Stub / TODO |
|---|---|---|
| `back/` local upstreams (stdio + HTTP) | Full port of the in-repo gateway's `Upstream`/`HttpUpstream`/`Gateway`/`ConfigGateway` | Per-profile run-policy/approval-gate/KB-scoping hooks from the in-repo version were intentionally dropped — those are agentic-workspace-specific, not part of the generic gateway |
| `back/` `/link` remote registration | Register → publish tools → route `tools/call` back over the socket, verified end to end | Token is a placeholder shared-secret check, not the real `awlk_...` hash-in-user's-Postgres scheme; no app-name collision handling; "last register wins" on reconnect |
| `connector/` | Spawns a local stdio MCP and dials/registers/serves calls over `/link`, with reconnect+backoff, verified end to end | Same token caveat as above; one MCP per connector instance (no multi-MCP fan-in) |
| `front/` | Builds, Status view shows live health from `back/` | Upstreams/Hosts views are read-only placeholders; no token minting yet |

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
