import { useState } from "react";

// Placeholder "Hosts & Apps" view — the design (project_aw_apps_distribution_
// mcp_wrapper memory) calls for minting an opaque awlk_<id16>_<secret32>
// token here, shown once in plaintext, scoped to an app/host via globs, with
// instant revocation. None of that backend exists yet (no token store, no
// /link auth beyond the placeholder equality check in remote_upstream.py) —
// this view is UI-only scaffolding so the real flow has a home to land in.
export default function Hosts() {
  const [minted, setMinted] = useState<string | null>(null);

  function mintPlaceholderToken() {
    setMinted("TODO: real minting is not implemented — back/ has no token store yet");
  }

  return (
    <div>
      <h1>Hosts &amp; Apps</h1>
      <div className="card">
        <p>
          Connected hosts/apps (via <code>connector/</code>, dialing <code>/link</code>)
          will be listed here once reverse registration is fully built.
        </p>
        <button onClick={mintPlaceholderToken}>Mint a link token (stub)</button>
        {minted && <p>{minted}</p>}
      </div>
    </div>
  );
}
