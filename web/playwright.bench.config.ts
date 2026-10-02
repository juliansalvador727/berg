import { defineConfig, devices } from "@playwright/test";

/** Worker latency benchmarks against the live bucket. Local only; never run in CI. */
export default defineConfig({
  testDir: "./tests/perf",
  testMatch: "**/*.bench.ts",
  outputDir: "./test-results/perf",
  workers: 1,
  reporter: [["list"]],
  use: {
    baseURL: "http://localhost:5173",
    ...devices["Desktop Chrome"],
  },
  webServer: {
    command: "npm run dev:e2e",
    url: "http://localhost:5173",
    reuseExistingServer: true,
    timeout: 30_000,
  },
  projects: [{ name: "bench", use: { browserName: "chromium" } }],
});
