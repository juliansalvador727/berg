# web

Vite + TypeScript + MapLibre GL + deck.gl + DuckDB WASM. Deployed on Cloudflare Pages. No
framework — add one only if the UI grows to need it.

## Run

```sh
npm install
npm run dev
```

Point it at data with `VITE_DATA_BASE_URL` (defaults to the production R2 origin):

```sh
VITE_DATA_BASE_URL=http://localhost:8787 npm run dev
```

## Threading

- **Worker** (`src/worker/`): DuckDB WASM, day-file prefetch, all SQL. Never the main thread — a
  cold range request is hundreds of milliseconds and would drop frames.
- **Main** (`src/main.ts`): MapLibre, deck.gl, clock, controls.

## The two ideas that matter

**The clock advances on wall-clock delta, never frame count** (`src/clock.ts`). This is what makes
8x smooth and speed changes not lurch.

**Every leg in flight at T departed in `[T - max_leg_duration, T]`.** The pipeline guarantees it by
splitting long legs, so a cold scrub touches about two row groups instead of scanning the day.
`max_leg_duration` comes from `manifest.json` — never hardcode it.

Scrub precision is free: a position is a lerp along a polyline at `simTime`, so h:mm:ss resolution
costs nothing regardless of how coarse the files are.
