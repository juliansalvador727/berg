import { defineConfig } from "vite";

export default defineConfig({
  worker: { format: "es" },
  optimizeDeps: {
    // duckdb-wasm ships its own workers; letting Vite pre-bundle them breaks the ESM build.
    exclude: ["@duckdb/duckdb-wasm"],
  },
});
