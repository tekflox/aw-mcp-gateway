import { useEffect, useState } from "react";
import { getHealth, type HealthResponse } from "../api";

export default function Upstreams() {
  const [health, setHealth] = useState<HealthResponse | null>(null);

  useEffect(() => {
    getHealth().then(setHealth).catch(() => setHealth(null));
  }, []);

  return (
    <div>
      <h1>Upstreams</h1>
      <div className="card">
        <h3>Local (stdio, spawned by back/)</h3>
        <p>Defined in <code>back/config/mcp.json</code>, enabled via <code>back/config/gateway.json</code>'s <code>upstreams</code> list.</p>
        <ul>
          {(health?.local_upstreams ?? []).map((name) => (
            <li key={name}>{name}</li>
          ))}
          {!health?.local_upstreams?.length && <li>none running</li>}
        </ul>
      </div>
      <div className="card">
        <h3>Remote (dialed in via /link)</h3>
        <p>
          Apps running the <code>connector/</code> wrapper elsewhere register here,
          token-verified and scope-checked — see <code>back/gateway/remote_upstream.py</code>.
        </p>
        <ul>
          {(health?.remote_upstreams ?? []).map((name) => (
            <li key={name}>{name} — connected</li>
          ))}
          {!health?.remote_upstreams?.length && <li>none connected</li>}
        </ul>
      </div>
      <div className="card">
        <h3>Federated gateways ({"type: gateway"} upstreams)</h3>
        <p>
          Other aw-mcp-gateway instances whose whole tool pool is aggregated into
          this one — configured in <code>back/config/mcp.json</code>.
        </p>
        <ul>
          {(health?.federated_gateways ?? []).map((name) => (
            <li key={name}>{name} — connected</li>
          ))}
          {!health?.federated_gateways?.length && <li>none federated</li>}
        </ul>
        <p>
          This gateway: <code>{health?.gateway_id ?? "…"}</code>
          {" — federation chain: "}
          <code>{(health?.federation_chain ?? []).join(" → ") || "…"}</code>
        </p>
      </div>
    </div>
  );
}
