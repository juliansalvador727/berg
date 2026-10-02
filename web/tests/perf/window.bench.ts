/**
 * Worker query latency against the live bucket. Not part of CI: run with `npm run bench`.
 *
 * Starts legs.worker.ts directly on a blank page on the dev origin, so the numbers are the
 * worker's round-trip alone, with no map, rendering or main-thread work in them.
 */
import { test, type BrowserContext, type Page } from "@playwright/test";

const DAY = Date.parse("2026-05-13T00:00:00Z") / 1000;
const JUMP_DAY = Date.parse("2026-05-20T00:00:00Z") / 1000;
const LOOKAHEAD = 3000;
const SCRUBS = 40;
const REFILLS = 15;

const COUNTRY_SETS: Array<{ label: string; ids: string[] }> = [
  { label: "ch", ids: ["ch"] },
  { label: "ch+de+at+it", ids: ["ch", "de", "at", "it"] },
  { label: "all 8", ids: ["ch", "at", "be", "de", "fi", "gb", "it", "nl"] },
];

interface Answer {
  ms: number;
  legs: number;
  datasets: unknown;
}

interface Sample {
  ms: number;
  requests: number;
  legs: number;
}

/** mulberry32: the same scrub positions on every run. */
function seeded(seed: number): () => number {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function stats(samples: Sample[]): string {
  const ms = samples.map((s) => s.ms).sort((a, b) => a - b);
  const at = (q: number) => ms[Math.min(ms.length - 1, Math.floor(q * ms.length))]!;
  const requests = samples.reduce((sum, s) => sum + s.requests, 0);
  return [
    `p50 ${at(0.5).toFixed(0)}`,
    `p95 ${at(0.95).toFixed(0)}`,
    `min ${ms[0]!.toFixed(0)}`,
    `max ${ms[ms.length - 1]!.toFixed(0)} ms`,
    `parquet req ${requests} (${(requests / samples.length).toFixed(2)}/query)`,
    `legs ~${Math.round(samples.reduce((sum, s) => sum + s.legs, 0) / samples.length)}`,
  ].join(" · ");
}

async function openWorker(context: BrowserContext): Promise<Page> {
  const page = await context.newPage();
  await page.route("**/__bench", (route) =>
    route.fulfill({ contentType: "text/html", body: "<!doctype html><title>bench</title>" }),
  );
  await page.goto("/__bench");
  await page.evaluate(() => {
    const worker = new Worker("/src/worker/legs.worker.ts", { type: "module" });
    let next = 1;
    (window as unknown as { ask: unknown }).ask = (payload: Record<string, unknown>) =>
      new Promise((resolve, reject) => {
        const requestId = next++;
        const started = performance.now();
        const onMessage = (event: MessageEvent) => {
          if (event.data.requestId !== requestId) return;
          worker.removeEventListener("message", onMessage);
          const ms = performance.now() - started;
          if (event.data.kind === "error") reject(new Error(event.data.message));
          // Only the count crosses back to Node: serializing 100K legs per query would dominate.
          else resolve({ ms, legs: event.data.columns?.t_dep.length ?? 0, datasets: event.data.datasets });
        };
        worker.addEventListener("message", onMessage);
        worker.postMessage({ ...payload, requestId });
      });
  });
  return page;
}

test("window query latency", async ({ browser }) => {
  test.setTimeout(15 * 60_000);
  const lines: string[] = [];

  for (const set of COUNTRY_SETS) {
    // A fresh context per set: empty HTTP cache, so the cold numbers really are cold.
    const context = await browser.newContext();
    const parquet: Array<{ method: string; status: number; url: string }> = [];
    context.on("requestfinished", async (request) => {
      if (!request.url().endsWith(".parquet")) return;
      const response = await request.response();
      parquet.push({ method: request.method(), status: response?.status() ?? 0, url: request.url() });
    });
    const page = await openWorker(context);
    const ask = async (payload: Record<string, unknown>) =>
      page.evaluate(
        (p) =>
          (window as unknown as { ask: (p: unknown) => Promise<Answer> }).ask(p),
        payload,
      );

    const ready = await ask({ kind: "init" });
    console.log(`[${set.label}] ready in ${ready.ms.toFixed(0)} ms`);
    const indices: number[] = (ready.datasets as Array<{ id: string; index: number }>)
      .filter((info: { id: string }) => set.ids.includes(info.id))
      .map((info: { index: number }) => info.index);

    const measure = async (simTime: number): Promise<Sample> => {
      const before = parquet.length;
      const { ms, legs } = await ask({ kind: "window", simTime, lookahead: LOOKAHEAD, datasets: indices });
      await page.waitForTimeout(50); // let trailing requestfinished events land
      return { ms, requests: parquet.length - before, legs };
    };

    const cold = await measure(DAY + 8 * 3600);
    const coldRequests = parquet.slice();
    console.log(`[${set.label}] cold window ${cold.ms.toFixed(0)} ms, ${cold.requests} parquet req`);

    // Scrubs stay clear of midnight so every one is a warm query on the loaded day.
    const random = seeded(20260513);
    const scrubs: Sample[] = [];
    for (let i = 0; i < SCRUBS; i++) {
      scrubs.push(await measure(DAY + 2 * 3600 + random() * (20 * 3600 - LOOKAHEAD)));
    }

    console.log(`[${set.label}] scrubs ${stats(scrubs)}`);
    const refills: Sample[] = [];
    let t = DAY + 12 * 3600;
    for (let i = 0; i < REFILLS; i++, t += LOOKAHEAD * 0.9) refills.push(await measure(t));

    const jump = await measure(JUMP_DAY + 8 * 3600);

    const methods = new Map<string, number>();
    for (const r of coldRequests) methods.set(`${r.method} ${r.status}`, (methods.get(`${r.method} ${r.status}`) ?? 0) + 1);
    lines.push(
      `[${set.label}] init ${ready.ms.toFixed(0)} ms`,
      `  cold window   ${cold.ms.toFixed(0)} ms · ${cold.requests} parquet req (${[...methods].map(([k, n]) => `${n}× ${k}`).join(", ")}) · ${cold.legs} legs`,
      `  warm scrubs   ${stats(scrubs)}`,
      `  refills       ${stats(refills)}`,
      `  cold jump     ${jump.ms.toFixed(0)} ms · ${jump.requests} parquet req`,
    );
    await context.close();
  }

  console.log(`\n${lines.join("\n")}\n`);
});
