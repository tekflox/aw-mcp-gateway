import { useEffect, useState } from "react";
import {
  getHealth, type HealthResponse,
  listExternalUpstreams, putExternalUpstream, deleteExternalUpstream,
  type ExternalUpstreamStatus, type ExternalUpstreamSpec,
} from "../api";

const SECRET_REF_RE = /^\$\{secret:(.+)\}$/;

interface KvRow {
  id: number;
  key: string;
  value: string;
  secret: boolean;
  existingRef: string | null; // the "${secret:name}" value this row started as, when editing
  replacing: boolean; // user asked to replace an existing secret's value
}

let rowSeq = 0;
function newRow(partial: Partial<KvRow> = {}): KvRow {
  return { id: ++rowSeq, key: "", value: "", secret: false, existingRef: null, replacing: false, ...partial };
}

function specKv(spec: ExternalUpstreamSpec): Record<string, string> {
  return spec.type === "http" ? spec.headers ?? {} : spec.env ?? {};
}

function rowsFromSpec(spec: ExternalUpstreamSpec): KvRow[] {
  return Object.entries(specKv(spec)).map(([key, value]) => {
    const m = SECRET_REF_RE.exec(value);
    return m ? newRow({ key, secret: true, existingRef: value }) : newRow({ key, value });
  });
}

// Managed CRUD for hand-registered external MCP upstreams — the generic
// mechanism behind "connect the gateway to an external MCP server" (first
// case: Google Workspace MCP, registered through this form with zero
// Google-specific code here). Distinct from the read-only sections below,
// which only ever reflect what's already running.
function ExternalUpstreams() {
  const [external, setExternal] = useState<ExternalUpstreamStatus[] | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [formOpen, setFormOpen] = useState(false);
  const [editingName, setEditingName] = useState<string | null>(null); // null = adding new
  const [name, setName] = useState("");
  const [type, setType] = useState<"stdio" | "http">("stdio");
  const [command, setCommand] = useState("");
  const [args, setArgs] = useState("");
  const [url, setUrl] = useState("");
  const [rows, setRows] = useState<KvRow[]>([]);
  const [saving, setSaving] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);

  useEffect(() => {
    refreshExternal();
  }, []);

  async function refreshExternal() {
    try {
      setExternal(await listExternalUpstreams());
      setLoadError(null);
    } catch (e) {
      setLoadError(String(e));
    }
  }

  function resetForm() {
    setEditingName(null);
    setName("");
    setType("stdio");
    setCommand("");
    setArgs("");
    setUrl("");
    setRows([]);
    setFormError(null);
  }

  function openAddForm() {
    resetForm();
    setFormOpen(true);
  }

  function openEditForm(entry: ExternalUpstreamStatus) {
    setEditingName(entry.name);
    setName(entry.name);
    setType(entry.spec.type);
    setCommand(entry.spec.command ?? "");
    setArgs((entry.spec.args ?? []).join(" "));
    setUrl(entry.spec.url ?? "");
    setRows(rowsFromSpec(entry.spec));
    setFormError(null);
    setFormOpen(true);
  }

  function addRow() {
    setRows((r) => [...r, newRow()]);
  }

  function updateRow(id: number, patch: Partial<KvRow>) {
    setRows((r) => r.map((row) => (row.id === id ? { ...row, ...patch } : row)));
  }

  function removeRow(id: number) {
    setRows((r) => r.filter((row) => row.id !== id));
  }

  async function save() {
    setFormError(null);
    const cleanName = name.trim();
    if (!cleanName) {
      setFormError("name is required");
      return;
    }
    const spec: ExternalUpstreamSpec = { type };
    if (type === "stdio") {
      if (!command.trim()) {
        setFormError("command is required for a stdio upstream");
        return;
      }
      spec.command = command.trim();
      const argList = args.split(/\s+/).map((a) => a.trim()).filter(Boolean);
      if (argList.length) spec.args = argList;
    } else {
      if (!url.trim()) {
        setFormError("url is required for an http upstream");
        return;
      }
      spec.url = url.trim();
    }

    const kv: Record<string, string> = {};
    const secrets: Record<string, string> = {};
    for (const row of rows) {
      const key = row.key.trim();
      if (!key) continue;
      if (!row.secret) {
        kv[key] = row.value;
        continue;
      }
      if (row.existingRef && !row.replacing) {
        kv[key] = row.existingRef; // untouched — no new secret write
        continue;
      }
      if (!row.value) {
        setFormError(`enter a value for secret field "${key}", or uncheck "secret"`);
        return;
      }
      const secretName = `${cleanName}-${key}`.replace(/[^a-zA-Z0-9_-]/g, "-");
      secrets[secretName] = row.value;
      kv[key] = `\${secret:${secretName}}`;
    }
    if (Object.keys(kv).length) {
      if (type === "http") spec.headers = kv;
      else spec.env = kv;
    }

    setSaving(true);
    try {
      await putExternalUpstream(cleanName, spec, Object.keys(secrets).length ? secrets : undefined);
      await refreshExternal();
      setFormOpen(false);
      resetForm();
    } catch (e) {
      setFormError(String(e));
    } finally {
      setSaving(false);
    }
  }

  async function remove(entryName: string) {
    if (!confirm(`Remove external upstream "${entryName}"? This also deletes any secret it alone referenced.`)) {
      return;
    }
    try {
      await deleteExternalUpstream(entryName);
      await refreshExternal();
      if (editingName === entryName) {
        setFormOpen(false);
        resetForm();
      }
    } catch (e) {
      setLoadError(String(e));
    }
  }

  const kvLabel = type === "http" ? "headers" : "env";
  const addRowLabel = type === "http" ? "header" : "env var";

  return (
    <div className="card">
      <h3>External upstreams (managed)</h3>
      <p>
        Registered here rather than by hand-editing <code>mcp.custom.json</code> — a credential is
        stored write-only and referenced from the spec as <code>{"${secret:<name>}"}</code>, never
        written to disk (or shown here) in the clear.
      </p>
      {loadError && <p style={{ color: "red" }}>{loadError}</p>}
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ textAlign: "left" }}>
            <th>Name</th>
            <th>Type</th>
            <th>Target</th>
            <th>Status</th>
            <th>Tools</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {(external ?? []).map((entry) => (
            <tr key={entry.name}>
              <td>{entry.name}</td>
              <td>{entry.spec.type}</td>
              <td>
                <code>{entry.spec.type === "http" ? entry.spec.url : entry.spec.command}</code>
              </td>
              <td>
                {entry.status}
                {entry.error && <span title={entry.error}> ⚠️</span>}
              </td>
              <td>{entry.tools}</td>
              <td>
                <button onClick={() => openEditForm(entry)}>Edit</button>{" "}
                <button onClick={() => remove(entry.name)}>Remove</button>
              </td>
            </tr>
          ))}
          {external && !external.length && (
            <tr>
              <td colSpan={6}>none registered</td>
            </tr>
          )}
        </tbody>
      </table>

      {!formOpen && <button onClick={openAddForm}>Add external upstream</button>}

      {formOpen && (
        <div style={{ marginTop: 16, borderTop: "1px solid #262a33", paddingTop: 16 }}>
          <h4>{editingName ? `Edit "${editingName}"` : "Add external upstream"}</h4>
          <div>
            <label>
              Name{" "}
              <input value={name} onChange={(e) => setName(e.target.value)} disabled={!!editingName} />
            </label>
          </div>
          <div>
            <label>
              Type{" "}
              <select
                value={type}
                onChange={(e) => setType(e.target.value as "stdio" | "http")}
                disabled={!!editingName}
              >
                <option value="stdio">stdio</option>
                <option value="http">http</option>
              </select>
            </label>
          </div>
          {type === "stdio" ? (
            <>
              <div>
                <label>
                  Command{" "}
                  <input value={command} onChange={(e) => setCommand(e.target.value)} placeholder="uvx" />
                </label>
              </div>
              <div>
                <label>
                  Args (space-separated){" "}
                  <input
                    value={args}
                    onChange={(e) => setArgs(e.target.value)}
                    placeholder="workspace-mcp --single-user"
                    style={{ width: 320 }}
                  />
                </label>
              </div>
            </>
          ) : (
            <div>
              <label>
                URL{" "}
                <input
                  value={url}
                  onChange={(e) => setUrl(e.target.value)}
                  placeholder="https://example.test/mcp"
                  style={{ width: 320 }}
                />
              </label>
            </div>
          )}

          <h5>{kvLabel}</h5>
          {rows.map((row) => (
            <div key={row.id} style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 4 }}>
              <input
                placeholder="key"
                value={row.key}
                onChange={(e) => updateRow(row.id, { key: e.target.value })}
                style={{ width: 200 }}
              />
              {row.secret && row.existingRef && !row.replacing ? (
                <>
                  <code>{row.existingRef}</code>
                  <button onClick={() => updateRow(row.id, { replacing: true, value: "" })}>
                    Replace value
                  </button>
                </>
              ) : (
                <input
                  placeholder="value"
                  type={row.secret ? "password" : "text"}
                  value={row.value}
                  onChange={(e) => updateRow(row.id, { value: e.target.value })}
                  style={{ width: 260 }}
                />
              )}
              <label>
                <input
                  type="checkbox"
                  checked={row.secret}
                  onChange={(e) =>
                    updateRow(row.id, { secret: e.target.checked, replacing: e.target.checked && row.replacing })
                  }
                />{" "}
                secret
              </label>
              <button onClick={() => removeRow(row.id)}>Remove</button>
            </div>
          ))}
          <button onClick={addRow}>Add {addRowLabel}</button>

          {formError && <p style={{ color: "red" }}>{formError}</p>}
          <div style={{ marginTop: 8 }}>
            <button onClick={save} disabled={saving}>
              {saving ? "Saving…" : "Save"}
            </button>{" "}
            <button
              onClick={() => {
                setFormOpen(false);
                resetForm();
              }}
            >
              Cancel
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

export default function Upstreams() {
  const [health, setHealth] = useState<HealthResponse | null>(null);

  useEffect(() => {
    getHealth().then(setHealth).catch(() => setHealth(null));
  }, []);

  return (
    <div>
      <h1>Upstreams</h1>
      <ExternalUpstreams />
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
