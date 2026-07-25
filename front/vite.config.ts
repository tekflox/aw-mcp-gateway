import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Why Vite+React: matches the stack already used across this project's other
// management UIs (AW dashboard, agents-platform frontend) — same tooling,
// same mental model for whoever maintains this next. No SSR/routing needs
// here (a handful of internal views behind one gateway), so Next would add
// weight without buying anything.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": {
        target: process.env.BACK_URL || "http://127.0.0.1:9200",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
    },
  },
});
