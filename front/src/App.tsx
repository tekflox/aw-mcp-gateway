import { useState } from "react";
import Status from "./views/Status";
import Upstreams from "./views/Upstreams";
import Hosts from "./views/Hosts";

type View = "status" | "upstreams" | "hosts";

export default function App() {
  const [view, setView] = useState<View>("status");

  return (
    <div className="layout">
      <nav>
        <a className={view === "status" ? "active" : ""} onClick={() => setView("status")}>
          Status
        </a>
        <a className={view === "upstreams" ? "active" : ""} onClick={() => setView("upstreams")}>
          Upstreams
        </a>
        <a className={view === "hosts" ? "active" : ""} onClick={() => setView("hosts")}>
          Hosts &amp; Apps
        </a>
      </nav>
      <main>
        {view === "status" && <Status />}
        {view === "upstreams" && <Upstreams />}
        {view === "hosts" && <Hosts />}
      </main>
    </div>
  );
}
