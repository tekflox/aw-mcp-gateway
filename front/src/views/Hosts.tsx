import { useState } from "react";
import { listLinkTokens, mintLinkToken, revokeLinkToken, type LinkTokenSummary } from "../api";

// "Hosts & Apps" — mint/list/revoke real awlk_<id16>_<secret32> link tokens
// (back/gateway/token_store.py) that a connector/ instance uses to dial
// /link. Minting/listing/revoking need the gateway's own bearer token (the
// same one that guards /mcp) — there's no separate admin session yet, so
// this view just asks for it inline rather than blocking on that.
export default function Hosts() {
  const [bearerToken, setBearerToken] = useState("");
  const [tokens, setTokens] = useState<LinkTokenSummary[] | null>(null);
  const [minted, setMinted] = useState<string | null>(null);
  const [label, setLabel] = useState("");
  const [scopes, setScopes] = useState("*:*");
  const [error, setError] = useState<string | null>(null);

  async function refresh() {
    setError(null);
    try {
      setTokens(await listLinkTokens(bearerToken));
    } catch (e) {
      setError(String(e));
    }
  }

  async function mint() {
    setError(null);
    try {
      const scopeList = scopes.split(",").map((s) => s.trim()).filter(Boolean);
      const result = await mintLinkToken(bearerToken, label, scopeList.length ? scopeList : undefined);
      setMinted(result.token);
      await refresh();
    } catch (e) {
      setError(String(e));
    }
  }

  async function revoke(id: string) {
    setError(null);
    try {
      await revokeLinkToken(bearerToken, id);
      await refresh();
    } catch (e) {
      setError(String(e));
    }
  }

  return (
    <div>
      <h1>Hosts &amp; Apps</h1>
      <div className="card">
        <p>
          Connected hosts/apps (via <code>connector/</code>, dialing <code>/link</code>)
          each need a link token minted here. A token is shown once in
          plaintext — only its SHA-256 hash is stored (see{" "}
          <code>back/gateway/token_store.py</code>).
        </p>
        <input
          placeholder="gateway bearer token (same as /mcp auth)"
          type="password"
          value={bearerToken}
          onChange={(e) => setBearerToken(e.target.value)}
        />
        <button onClick={refresh} disabled={!bearerToken}>
          List tokens
        </button>
      </div>

      <div className="card">
        <h3>Mint a link token</h3>
        <input placeholder="label (e.g. laptop-browser)" value={label} onChange={(e) => setLabel(e.target.value)} />
        <input
          placeholder="scopes, comma-separated (app_glob:tool_glob) — default *:*"
          value={scopes}
          onChange={(e) => setScopes(e.target.value)}
        />
        <button onClick={mint} disabled={!bearerToken}>
          Mint
        </button>
        {minted && (
          <p>
            New token (copy now, shown once): <code>{minted}</code>
          </p>
        )}
      </div>

      {error && <p style={{ color: "red" }}>{error}</p>}

      {tokens && (
        <div className="card">
          <h3>Existing tokens</h3>
          <ul>
            {tokens.map((t) => (
              <li key={t.id}>
                {t.label || "(no label)"} — id <code>{t.id}</code> — scopes{" "}
                <code>{t.scopes.join(", ")}</code> — {t.revoked ? "revoked" : "active"}
                {!t.revoked && <button onClick={() => revoke(t.id)}>Revoke</button>}
              </li>
            ))}
            {!tokens.length && <li>none minted yet</li>}
          </ul>
        </div>
      )}
    </div>
  );
}
