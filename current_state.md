# Current state

Updated 2026-07-22 after the completed historical rebuild, Pages deployment, and Playwright suite.
This file is the authoritative project status and TODO. `docs/data-notes.md` remains the
authority for measured source-data behavior. The gitignored `plan.md` is a legacy planning
scratchpad and may contain obsolete estimates.

## Executive summary

The historical data milestone is complete. The usable 2018-01 through 2026-06 archive was
rebuilt with the station-validity, clock-basis, partial-month, and absurd-duration fixes. All
daily leg and journey files, static assets, and the manifest were synchronized to R2. The final
sync uploaded 5,195 objects, retained 990 already-current objects, published the manifest last,
and deleted 30 obsolete managed objects.

There is no known data-integrity blocker. “Complete” does not mean the source archive is
perfect: its genuine holes are represented explicitly, quarantined rows remain excluded, and
1,347 routes use a flagged straight-line fallback when the Switzerland OSM extract cannot
provide a usable rail path.

## European datasets (2026-09-23)

Switzerland is joined by **Finland**, the first country in `europe.md`'s rollout order, as a
separate dataset under `datasets/fi/` with its own id spaces, manifest and `routes.bin`.
`catalog.json` at the bucket root lists both; Switzerland is listed with `"path": ""` and not
one Swiss object was rebuilt, moved or renumbered. Clients without the catalog still read the
Swiss root manifest.

| Finland | Value |
|---|---:|
| Source | Fintraffic / Digitraffic, CC BY 4.0 |
| Coverage | 2023-01-01 → 2025-12-31, 1,096 UTC days, 0 missing |
| Published legs | 14,374,199 (2.3% scheduled fallback) |
| Routes / fallbacks | 1,808 / 14 |
| Published size | 137.8 MB of its 150 MB allocation |
| Bucket after upload | ~3.91 GB of the 9.0 GB ceiling |

Seven strike days are published thin and flagged as `source_cancelled_days`. Details:
[`docs/data-notes-fi.md`](docs/data-notes-fi.md). Rebuild: `scripts/fetch_fi.py`,
`scripts/build_fi.py`, the geometry job with `--fit-bbox`, then `scripts/sync_dataset.py fi`.

The **Netherlands** follows as `datasets/nl/`, built from the Rijden de Treinen archive. Its
times are scheduled plus the last reported delay in whole minutes. They are not observations,
so the dataset is labelled `delay_only` and the UI shows it as "scheduled + reported delay".

| Netherlands | Value |
|---|---:|
| Source | Rijden de Treinen, CC BY 4.0 |
| Coverage | 2023-01-01 → 2026-08-30, 1,338 UTC days, 0 missing |
| Published legs | 59,901,681 (Dutch station pairs only) |
| Routes / fallbacks | 2,294 / 0 |
| Leg + journey bytes | 349.4 MB of its 500 MB allocation |

Duplicate service records, diversions and mid-route cancellations each needed a rule. The
details are in [`docs/data-notes-nl.md`](docs/data-notes-nl.md). To rebuild, run
`scripts/build_nl.py --fetch`, then the geometry job with `--fit-bbox`, then
`scripts/sync_dataset.py nl`.

**Belgium** is `datasets/be/`, built from Infrabel's monthly raw punctuality files. Planned
and actual times are to the second from Infrabel's train detection, so it is `observed` like
Finland. The source lists only trains that ran and only Infrabel's own network.

| Belgium | Value |
|---|---:|
| Source | Infrabel, CC0 |
| Coverage | 2023-01-01 → 2026-08-30, 1,338 UTC days, 0 missing |
| Published legs | 46,017,097 |
| Routes / fallbacks | 3,859 / 0 |
| Leg + journey bytes | 401.0 MB of its 450 MB allocation (zstd level 19) |

The source's columns move twice in 2025, so files are read by column name. S lines are renamed
at the 2025-12-14 timetable, and two closed halts are missing from today's station list. The
details are in [`docs/data-notes-be.md`](docs/data-notes-be.md). To rebuild, run
`scripts/build_be.py --fetch`, then the geometry job with `--fit-bbox` on the Overpass export
(see `geometry/README.md`), then `scripts/sync_dataset.py be`.

The frontend loads the catalog, fetches a country's static layer only when it enters the
viewport, queries only visible countries' day files, keys everything by dataset, shows the
clock in the focused country's timezone, labels missing coverage explicitly, and shows each
dataset's attribution and licence. Typing a country name in Ctrl/Cmd+K flies there and jumps to
its nearest covered day.

**Germany** is `datasets/de/`. It is built from DB's Timetables API (IRIS) as crawled every six
hours by piebro/deutsche-bahn-data, CC BY 4.0. Bahn-Vorhersage, the source `europe.md` names, is
available only through a Mobilithek account and access request, which is still to be done. This
archive covers every station only from 2025-11-02, so Germany covers a later window than the
other countries. Times are the last scheduled or changed time the crawler saw, so the dataset is
labelled `final_prediction`.

| Germany | Value |
|---|---:|
| Source | DB Timetables + StaDa APIs via piebro/deutsche-bahn-data, CC BY 4.0 |
| Coverage | 2025-11-03 → 2026-08-30, 301 UTC days, 0 missing, 203 crawl-gap hours flagged |
| Published legs | 124,204,786 |
| Routes / fallbacks | 19,586 / 67 |
| Leg + journey bytes | 652.5 MB of its 2.95 GB allocation (zstd level 19) |
| Bucket after upload | ~5.18 GB of the 9.0 GB ceiling |

The crawler missed some hours: six-hour blocks through July 2026, and single midnight hours in
April-June. These are published as `source_gap_hours`, and the UI labels them as a source gap
rather than showing an empty map. The source revision is pinned and checked by SHA-256, because
the maintainer rewrites history. The details are in
[`docs/data-notes-de.md`](docs/data-notes-de.md). To rebuild, run `scripts/build_de.py --fetch`,
then the geometry job on the rail extract of the Geofabrik PBF (see `geometry/README.md`), then
`scripts/sync_dataset.py de`.

The viewer now treats a neighbouring country as in view only when the current day falls inside
its coverage, or when it is the country under the map center. Germany's box covers Basel, and
without this rule the default Swiss view on 2018-01-01 fetched German geometry and showed a "no
data" notice.

**Cross-border layer** (`links/`, europe.md phase 4). This is a derived layer above the
datasets, and none of them is edited.

- **Station crosswalk:** 102 groups of the same physical station across datasets, each with its
  match method, distance and confidence.
- **Drawing each movement once:** where the Swiss archive and the German dataset both hold a leg
  in the German border belt (4,138 legs on 2026-03-10), only the copy with stronger time evidence
  is drawn. Swiss observations win over German final predictions.
- **Journey links:** 82,513 overlaps, 5,376 handovers and 111,517 bridged crossings over 1,278
  overlap days. A bridge crosses a gap neither dataset covers, such as ICEs between Freiburg and
  Basel Bad Bf, Emmerich and Zevenaar, Aachen and Liège, or Rotterdam and Antwerp.
  - It has interpolated times, is labelled as such, and runs on its own routed geometry (52
    routes, 0 fallbacks).
  - Spectating follows a train into the next country.
  - Bridge station pairs are accepted only on their record over the whole build, because a
    shared train number repeats daily by coincidence. The rules and the per-pair evidence are in
    [`docs/cross-border.md`](docs/cross-border.md) and `links/manifest.json`.
- **Rebuilt 2026-09-25** over the extended window: 101,190 overlaps, 6,434 handovers and
  177,904 bridged crossings over 1,338 days, 65 bridge routes, 0 fallbacks.
- **Size and rebuild:** the bucket is ~5.46 GB (15,824 objects) after the 2026-09-25 extension. To rebuild after any dataset
  changes, run `scripts/build_links.py`, then `scripts/sync_links.py`.

The viewer now opens on the first UTC day every dataset has published, currently
**2025-11-03**, when Germany's archive starts. It is computed from the manifests, so it moves
if coverage changes. The Swiss 2018-01-01 default applies only when the datasets share no day.

**Austria** is `datasets/at`, integrated 2026-09-25. ÖBB-Infrastruktur's train-run file (EU
Delegated Regulation 2024/490 data, CC BY 3.0 AT) gives each run's planned and actual time at
the first and last operating point ÖBB recorded. These are yards, junctions and track groups,
not passenger termini. ÖBB-Personenverkehr's GTFS timetable (CC BY 4.0) gives the stops. Runs
and trips are joined by train number and day. Each stop's delay is interpolated between the
two observations, so the dataset is labelled `delay_interpolated`, shown as "scheduled +
interpolated delay".

| Austria | Value |
|---|---:|
| Sources | ÖBB-Infrastruktur train runs + ÖBB-Personenverkehr GTFS |
| Coverage | 2025-12-15 → 2026-09-05, 265 UTC days, 0 missing |
| Published legs | 15,983,394 (Austrian station pairs only) |
| Routes / fallbacks | 2,875 / 0 |
| Leg + journey bytes | 130.9 MB of its 200 MB allocation (zstd level 19) |

- The timetable starts at the 2025-12-14 change, so Austria starts 2025-12-15 and the viewer's
  shared default day moved from 2025-11-03 to 2025-12-15.
- The GTFS was published once and never updated, so the share of runs paired with a trip falls
  from about 91% in winter to about 79% in August.
- OSM tags the Wien S-Bahn Stammstrecke and Feldkirch – Buchs as under construction, so the
  geometry input adds those ways.
- The details are in [`docs/data-notes-at.md`](docs/data-notes-at.md).
- To rebuild, run `scripts/build_at.py --fetch`, then the geometry job on
  `austria-rail.osm.pbf` (see `geometry/README.md`), then `scripts/sync_dataset.py at`.

**Forward extension (2026-09-25).** A German backfill before 2025-11 is not an option, so the
shared window grows forward instead. The Netherlands and Belgium now run to 2026-08-30, and
Switzerland gained 2026-07 and 2026-08 (see the snapshot below). Both European rebuilds re-ran
over their existing databases: the append-only registries kept every id, and every 2023-2025
day file came out byte-identical, so only the 242 new days per country were uploaded. Finland
and Belgium's caps rose to 0.20 and 0.45 GB, taken from Germany's unused allocation (europe.md).

Four datasets now share 2025-11-03 → 2026-08-30, and with Austria, five share 2025-12-15 →
2026-08-30. **Finland is still pending**: Digitraffic's
rail API was down all evening on 2026-09-25 (timeouts, then HTTP 503; its status page listed
the rail endpoints as down). Its config stays at 2025-12-31 until the extension lands. To do it,
set `coverage_end="2026-08-30"` in `europe/config.py`, run `scripts/fetch_fi.py 2026-01-02
2026-08-31`, then `scripts/build_fi.py`, the geometry job with `--fit-bbox`, and
`scripts/sync_dataset.py fi`.

## Published snapshot

| Item | Current value |
|---|---:|
| Manifest schema | 3 |
| UTC day entries | 3,152 |
| Manifest range | 2017-12-31 through 2026-09-01 |
| Explicit missing UTC days | 15 |
| Published legs | 431,289,031 |
| Local leg files | 3,152 |
| Local journey files | 3,152 |
| Registered routes | 11,580 |
| Geometry records | 11,580 |
| Local publish mirror | about 3.7 GB plus manifest |

The apparent 2017-12-31/2026-09-01 edges come from converting Europe/Zurich service times to
UTC. The source service-day coverage is 2018-01 through 2026-08 (2026-07 and 2026-08 were
added on 2026-09-25 from the v2 archive, the only series published from 2026-07). The 29 missing source days
collapse to 15 fully absent UTC files because neighboring service days can contribute rows
across UTC midnight.

Public application: `https://berg-rail-observer.pages.dev`.

Public data base: `https://pub-40f06e4404c049578963083898f4ab57.r2.dev`. Credentials are in the
repository-root `.env` and must never be committed.

## Validation completed

- The public manifest was fetched after upload and reports schema 3, 3,090 days, the expected
  range, 15 missing days, and 422,103,769 legs.
- Local and R2 inventories have 3,090 leg files and 3,090 journey files.
- Every one of the 11,502 registered route IDs has a record in `routes.bin`; `dim_route`
  materialized successfully after geometry regeneration.
- Route-ID, duration, type, and flag invariant queries reported zero violations.
- Maximum daily journey cardinality is 16,203, safely below the uint16 limit of 65,535.
- Pipeline test suite: 100 passed. Geometry test suite: 10 passed. Ruff: clean.
- Playwright suite: 3 Chromium scenarios passed against the published R2 archive, covering load,
  playback controls, filters, date/train search, route spectating, direct map clicks, station
  boards, and unspectating. CI runs it after the production frontend build.
- The frontend production build passes and contains no asset over 1.8 MB; the two 34–39 MB
  DuckDB WASM modules are pinned at version 1.32.0 and served from R2 with immutable caching.
- R2 CORS permits `https://berg-rail-observer.pages.dev` and `http://localhost:5173`; production
  range requests return `206 Partial Content`. The first
  local smoke attempt used `http://127.0.0.1:4173`, which correctly failed because origins are
  exact. The app now serves successfully at the allowed localhost origin; the final interactive
  map/control inspection still needs a human pass.

## Geometry snapshot

The 2026-07-20 build loaded 402,921 rail nodes and 409,536 edges, then routed all station pairs
in 110 seconds.

| Metric | Value |
|---|---:|
| Routes | 11,502 |
| Stations referenced | 2,148 |
| Stations snapped | 1,894 |
| Snap distance p50 / p95 | 9.3 m / 36.3 m |
| Straight-line fallbacks | 1,347 |
| Fallback: unsnappable | 948 |
| Fallback: unreachable | 384 |
| Fallback: absurd detour | 15 |
| Routed distance ratio p50 / p99 / max | 1.115 / 2.543 / 6.379 |
| Encoded points / bytes | 512,412 / 2,153,214 |

Fallbacks are deliberate and flagged. They mostly represent foreign/cross-border stations not
covered by the Switzerland PBF, disconnected rail components, or a path rejected by the detour
guard. They are a visual-quality limitation, not a missing-geometry condition.

## Publication behavior

`pipeline/scripts/sync_publish.py` now:

- runs a preflight that requires leg/journey parity and complete route geometry;
- compares local files with R2 so interrupted uploads resume instead of starting over;
- uploads data and static assets before `manifest.json`;
- publishes the manifest last, then deletes obsolete managed objects only with `--delete`;
- retries transient R2 connection failures with backoff.

The completed upload encountered transient R2 connection closures and recovered on retry. A
second invocation is safe: already-current objects are skipped.

## Known data limitations

The final quarantine contains 4,599,076 candidate legs that were deliberately not published:

| Reason | Rows | Share |
|---|---:|---:|
| `negative_duration` | 2,224,176 | 48.36% |
| `zero_duration` | 1,407,200 | 30.60% |
| `unmatched_station` | 933,967 | 20.31% |
| `missing_time` | 33,664 | 0.73% |
| `absurd_duration` | 69 | <0.01% |

These are visible reason-coded exclusions, not silent drops. Two follow-up decisions remain:

- Investigate the 33,664 `missing_time` rows by era/operator/schema.
- Decide whether short zero-duration hops should remain excluded or receive an explicit minimum
  duration. Do not change this without documenting the semantic tradeoff and rebuilding.

The source archive itself has 29 known missing or stub service days. These cannot be recovered
by pipeline code and are represented rather than fabricated.

## Next work

Do these in order unless product priorities change:

- [x] Replace the timeline-scrubber prototype with the train-observer foundation: automatic 600×
  playback, Ctrl/Cmd+K speed/date/train search, spectating, loading progress, service filters,
  clickable station boards, a dark city-labelled map, hillshade, and observed-network tracks.
- [ ] Complete the browser smoke test at `http://localhost:5173`: exercise Ctrl/Cmd+K date and
  train search, spectate an IC/S-Bahn journey, click several stations, toggle every service
  family, cross UTC midnight, and inspect the console and network range requests.
- [x] Deploy the frontend to `https://berg-rail-observer.pages.dev`; verify production CORS,
  `206 Partial Content` requests, and immutable WASM cache headers.
- [x] Add an automated Chromium smoke suite for the primary observer flows and enforce it in CI.
- [ ] Perform a cold load from a clean desktop and mobile browser profile and inspect the console.
- [ ] Run `.github/workflows/monthly-ingest.yml` once via manual dispatch for a known month and
  verify its build, upload, manifest-last behavior, and rerun idempotency before trusting cron.
- [ ] Create or license six low-poly GLB service-family models and render them with route-tangent
  orientation at close zoom. Exact rolling-stock models require a new vehicle/formation source;
  the current data identifies service class only.
- [ ] Build semantic OSM rail PMTiles for the complete track network, retaining tunnel, bridge,
  usage, service, gauge, and station/platform attributes. `routes.bin` covers observed paths but
  cannot distinguish tunnels or represent every physical track.
- [ ] Self-host the production dark basemap and DEM, tune zoom-dependent track/station/city
  detail, and add an About/data-attribution panel before public launch.
- [ ] Publish an archive search index and richer journey/call sidecars for global train search
  and timetable-grade historical station boards.
- [ ] Improve date/gap UX so missing days are visibly skipped rather than appearing frozen.
- [ ] Measure cold-load and playback/query latency on desktop and mobile. Build the custom GPU path or
  change file grouping only if measurements show the current scatter/range-request path misses
  the target.
- [ ] Decide whether aggregates are a product requirement; `aggregates_json` is still an
  intentional `NotImplementedError` stub.
- [ ] Audit `missing_time` and decide the zero-duration policy described above.

The detailed UI, GLB, rail-tile, terrain, search, and station-board contracts are in
[`docs/product-roadmap.md`](docs/product-roadmap.md).

## Local cleanup still available

The published artifacts are safe remotely, but local build scratch is large. The DuckDB staging
table contains roughly 472 million rows, `berg.duckdb` is about 22 GB, and an old spill directory
is about 18 GB. Cleanup is optional and destructive; inspect the paths and retain the database
if further data-quality analysis is planned. Do not remove `data/publish/` until the remote
post-upload checks and frontend smoke test are complete.

Suggested cleanup after those checks:

```sh
cd /home/julian/berg/pipeline
./.venv/bin/python - <<'PY'
import duckdb
con = duckdb.connect('../data/berg.duckdb')
con.execute('DELETE FROM stg_istdaten')
con.execute('CHECKPOINT')
con.close()
PY
```

Then inspect `data/berg.duckdb.tmp` and the extracted raw month directories before deleting them
manually. They are not needed to serve the published site, but they can be useful for audits.

## Architecture and invariants

- No application backend in the serving path.
- Daily Parquet and static files are immutable data objects; `manifest.json` is the publication
  pointer and is always written last.
- A healthy published day must meet the minimum leg floor; file presence alone is not success.
- Each leg uses one time basis at both endpoints. Scheduled fallback is explicit in its flags.
- Durations above 24 hours are quarantined before the split rule can amplify them.
- Station validity segments change only when station attributes change, not when a weekly GTFS
  snapshot temporarily omits a station.
- Every published `route_id` must have geometry; straight-line fallbacks are encoded and flagged.

## Ideas beyond launch

- Cancellation layer (`FAELLT_AUS_TF` is staged and counted, then excluded from movement facts).
- Rolling-stock/working-chain exploration using `umlauf_id`.
- Train-number search supporting both `85:...` and `sjyid...` trip-ID eras.

Commits must not include credentials or generated data. Redact long tokens from pasted output.
