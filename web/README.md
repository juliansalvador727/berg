# web

Vite + TypeScript + MapLibre GL + deck.gl + DuckDB WASM. Deployed on Cloudflare Pages at
`https://berg-rail-observer.pages.dev`. The oversized DuckDB WASM modules live in R2; the
version contract is `duckdb-runtime.json`. No framework — add one only if the UI grows to need it.

## Run

```sh
npm install
npm run dev -- --host localhost --port 5173
```

Open <http://localhost:5173>. The production R2 CORS policy permits that exact origin. Do not use
`127.0.0.1` or another port unless you first intentionally add that origin to the bucket policy.

Point it at data with `VITE_DATA_BASE_URL` (defaults to the production R2 origin):

```sh
VITE_DATA_BASE_URL=http://localhost:8787 npm run dev
```

For a production-build smoke test:

```sh
npm run build
npm run preview -- --host localhost --port 5173
```

Deploy the already-built `dist/` directory using the free Pages domain configured in
`wrangler.jsonc`:

```sh
npx wrangler pages deploy --branch main
```

The checked-in default and the repository's `.env.local` both point at the published R2 dataset.

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

## Current product gaps

The train-observer foundation is implemented: automatic 600× playback, Ctrl/Cmd+K speed and
date/train search, journey spectating, a startup progress bar, service-family filters, clickable
observed station boards, a dark city-labelled vector map, hillshade, and an observed-route track
overlay. The public product is not finished:

- complete the human browser smoke checklist in `../current_state.md`;
- create/license generic service-family GLBs and render them at close zoom;
- build complete semantic rail PMTiles with tunnel/bridge attributes;
- self-host the production basemap and terrain rather than relying on hosted map sources;
- add About, source-license, and data-attribution UI;
- make archive gaps explicit in the command-palette date experience;
- measure cold-load and playback/query performance before investing in a custom GPU layer.

See [`../docs/product-roadmap.md`](../docs/product-roadmap.md) for the asset and data contracts.
