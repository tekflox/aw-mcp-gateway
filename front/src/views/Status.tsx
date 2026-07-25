import { useEffect, useState } from "react";
import { getHealth, type HealthResponse } from "../api";

export default function Status() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    getHealth().then(setHealth).catch((e) => setError(String(e)));
  }, []);

  return (
    <div>
      <h1>Status</h1>
      <div className="card">
        {error && <p>Could not reach the gateway: {error}</p>}
        {!error && !health && <p>Loading…</p>}
        {health && (
          <ul>
            <li>Local upstreams: {health.local_upstreams.join(", ") || "—"}</li>
            <li>Remote upstreams (via /link): {health.remote_upstreams.join(", ") || "—"}</li>
            <li>Tools published: {health.tools}</li>
            <li>Named configs: {health.configs.join(", ") || "—"}</li>
          </ul>
        )}
      </div>
    </div>
  );
}
