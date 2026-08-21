---
repo: architecture
path: docs/architecture/aw-mcp-gateway.md
source: generated
edited: false
checksum: sha256:6d098ce3322bb702c0ea4494ccfdbe238baa5bb5a0b882bf500ef8ed3e06534d
---
# aw-mcp-gateway

- **repo**: aw-mcp-gateway
- **layer**: infrastructure
- **technologies**: typescript
- **health** (derived): planned

A standalone, public app that exposes stdio MCP servers over Streamable HTTP — one gateway, reachable by any HTTP-capable client (models, other apps), fronting a pool of local and remote MCP upstreams.

## Connections
_none_

## MCP tools
_none exposed_

## Requirements
### O gateway repassa quem chamou, e só isso — três headers, curtos, e nunca vazados entre requisições
- Given um upstream que escopa uma permissão pelo chamador (o aw-app-secrets faz) enxergaria o gateway em vez do agente, e o gateway atende muitas requisições concorrentes no mesmo processo
- When os headers da requisição são capturados e repassados (repos/aw-mcp-gateway/back/gateway/caller_context.py::capture:33, allowlist em ::FORWARDED:28, leitura em ::current:44)
- Then só x-aw-caller-session-id, x-aw-caller-run-id e x-aw-caller-agent atravessam — todo o resto do que o chamador mandou é descartado —, cada valor é cortado em 256 caracteres, header ausente fica ausente em vez de virar string vazia, um header configurado no upstream ganha do repassado, e como o estado vive num ContextVar duas requisições simultâneas nunca leem o chamador uma da outra. A allowlist é o ponto: um repasse genérico deixa qualquer chamador afirmar o que quiser sobre si mesmo para um upstream que confia nesses campos para autorizar. Coberto por back/tests/test_caller_context.py — teste não registrado no catálogo (ver relatório)
- intended_status: `not_implemented` · derived health: `not_implemented`
- tests: _none linked_

### Upstream stdio carrega a identidade do chamador nos argumentos, já que não tem header onde pendurá-la
- Given um upstream stdio é UM processo filho persistente compartilhado por todos os chamadores — ao contrário do HTTP, não existe requisição por chamada onde a identidade caiba
- When uma tool é invocada nesse upstream (repos/aw-mcp-gateway/back/gateway/upstream.py::Upstream.call_tool:151, injeção em :165-166)
- Then o run do chamador entra como _gateway_caller_run_id nos argumentos quando existe, NÃO entra quando não há chamador, e um valor explícito já presente nunca é sobrescrito. É assim que mark_as_planned, mark_flow_done, ask_human e register_callback descobrem qual run está falando; sem isso elas voltavam 400 por não saber a quem atribuir a chamada, e o agente lia como "a tool não existe/está quebrada" em vez de "o gateway não me identificou". Coberto por back/tests/test_stdio_caller_id.py — teste não registrado no catálogo (ver relatório)
- intended_status: `not_implemented` · derived health: `not_implemented`
- tests: _none linked_

### Reconectar mantém o nome público; colidir numera os DOIS lados
- Given um app se registra de volta no gateway por token e publica suas tools sob um nome público, e tanto reconexão quanto dois apps diferentes com o mesmo nome base acontecem na prática
- When o remoto é registrado (repos/aw-mcp-gateway/back/gateway/server.py::Gateway.register_remote:214, renomeação do incumbente em :236 via ::_rename_remote:243)
- Then o mesmo token id reconectando recebe de volta exatamente o nome que já tinha — sem rota duplicada e sem renumerar —, e um app genuinamente diferente com o nome base ocupado faz os DOIS virarem "Browser 1"/"Browser 2", não só o recém-chegado. Registro com token desconhecido ou revogado é recusado, e um scope que não casa com nenhuma tool recusa o registro em vez de publicar um upstream vazio. Renumerar na reconexão faria toda tool do app trocar de nome a cada queda de conexão, e os prompts de agente que citam o nome antigo passariam a apontar para lugar nenhum. Coberto por back/tests/test_link_registration.py — teste não registrado no catálogo (ver relatório)
- intended_status: `not_implemented` · derived health: `not_implemented`
- tests: _none linked_
