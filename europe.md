# European Train Watcher plan

## Goal

Turn the Swiss Train Watcher into a source-aware European railway viewer without rebuilding or
renumbering the existing Swiss archive. The product should present one navigable European network
while remaining honest about the difference between observed movements, final predictions, and
scheduled movements.

The implementation must remain inside the Cloudflare R2 free tier. As of 2026-09-17, that means
Standard storage only, at most 10 GB-month of storage, 1 million Class A operations per month, and
10 million Class B operations per month. R2 egress is free. The current limits are documented at
<https://developers.cloudflare.com/r2/pricing/>.

This is a hard design constraint, not a target to exceed temporarily.

## Product definition

The European product has three explicitly labelled data layers:

1. **Observed history**: scheduled and measured/final stop events suitable for historical replay.
2. **Realtime-derived history**: the last prediction observed for an event, retained with its weaker
   time semantics.
3. **Scheduled network**: timetable interpolation where no historical actuals exist.

The UI must never render these layers as if they have equal evidential quality. Observed history is
the default. Realtime-derived and scheduled movements require visible labels and separate filters.

“United Europe” means:

- one station and route search experience;
- multiple country datasets queried together for a selected date;
- consistent playback and station-board interactions;
- canonical links between equivalent stations and, where defensible, cross-border journeys;
- preserved source provenance, licenses, coverage, and timing semantics.

It does not mean forcing every source into one global ID space or claiming complete historical
coverage where open data does not support it.

## Date policy

### Published historical core

Use **2023-01-01 through 2025-12-31** as the initial common European comparison window.

This is shorter than the maximum technically available window of 2021-10-01 through 2025-12-31,
but it is the best starting point under the 10 GB R2 storage ceiling. It gives three complete years
for five useful national networks:

- Switzerland;
- Germany;
- Belgium;
- the Netherlands;
- Finland.

The exact start date is a storage gate, not an irreversible product decision. After representative
months have been encoded, move the start back toward 2021-10-01 only if total projected R2 usage
remains below the hard budget. If the three-year projection is too large, move the shared start
forward until it fits; never delete or rewrite the existing Swiss history to make room.

### Country-specific history

The common window is a comparison preset, not a destructive archive boundary. Switzerland retains
its existing 2018 onward coverage. Each dataset advertises its own coverage intervals and gaps, and
country-only mode may expose dates outside the common window when they are already published.

### Prospective European epoch

Use **2027-01-01 onward** as the clean prospective collection epoch for countries that expose live
data but no useful historical archive. Begin collectors before that date so feed revisions, outage
behaviour, and identifier stability can be measured.

Because R2 cannot hold an indefinitely growing full-Europe archive for free, prospective countries
start with a rolling retention window. Retention is expanded only when measured compact output fits
the storage budget.

## Dataset inventory and rollout order

### Tier 1: national historical actuals

| Dataset | Initial published window | Available source depth | Semantics | License/notes |
| --- | --- | --- | --- | --- |
| Switzerland | Existing 2018 onward; common view 2023-2025 | 2018 onward | Observed stop times with scheduled fallback flags | Preserve current schema-v3 artifacts unchanged |
| Finland | 2023-2025 | Verified at least from 2016 | Scheduled and actual times in one official response | Fintraffic CC BY 4.0: <https://www.digitraffic.fi/en/railway-traffic/> |
| Netherlands | 2023-2025 | 2019 onward | Scheduled time plus final delay/cancellation per stop | Rijden de Treinen CC BY 4.0: <https://www.rijdendetreinen.nl/en/open-data/train-archive> |
| Belgium | 2023-2025 | Official monthly files back to at least 2014 | Planned and actual arrivals/departures in seconds | Infrabel source: <https://opendata.infrabel.be/explore/dataset/stiptheid-gegevens-maandelijksebestanden/export/?flg=en-gb> |
| Germany | 2023-2025, subject to storage gate | September 2021 onward | Final prediction/actual per train event; minute source precision | Bahn-Vorhersage ODbL, Mobilithek access: <https://bahnvorhersage.de/open-data/parsed-train-delays/> |

Rollout order is Finland, Netherlands, Belgium, then Germany. The first three are comparatively
small and validate the multi-dataset architecture before the German storage commitment.

### Tier 2: partial or short historical coverage

| Dataset | Coverage decision | Constraint |
| --- | --- | --- |
| Sweden | Pilot selected rail operators; do not claim national completeness | KoDa archives operator-level GTFS and GTFS-RT from 2020, but realtime rail coverage is incomplete: <https://www.trafiklab.se/api/our-apis/koda/> |
| Great Britain | Evaluate a short import only after Tier 1 | National Rail HSP is a rolling one-year service requiring registration; bulk access and licensing need validation: <https://www.nationalrail.co.uk/developers/> |

These datasets do not extend the default “five complete national networks” claim until completeness
has been measured and documented.

### Tier 3: prospective collection

| Dataset | Feed | Initial policy |
| --- | --- | --- |
| France | SNCF GTFS plus GTFS-RT/SIRI | Collect deltas, never full snapshots; separate TGV/IC/TER scope from IDFM and regional feeds |
| Norway | Entur national NeTEx/GTFS plus SIRI/GTFS-RT | Train-only filtering; rolling retention |
| Denmark | Rejseplanen GTFS plus SIRI-ET | No historical realtime is published; rolling retention |
| Belgium current | Belgian Mobility/SNCB GTFS-RT | Continue beyond official monthly history where useful |
| Sweden current | Trafiklab and Trafikverket | Per-operator quality and coverage flags |
| Great Britain current | Darwin/TRUST | Only after access, license, and event-volume validation |
| Austria | ÖBB/NAP research adapter | Do not depend on undocumented HAFAS interfaces for the public archive |

France documentation: <https://ressources.data.sncf.com/explore/dataset/horaires-sncf/>.
Norway documentation: <https://developer.entur.org/pages-real-time-intro/>.
Denmark documentation: <https://labs.rejseplanen.dk/hc/en-us/articles/24750139021341-Oversigt-over-udstillede-data>.

Italy, Spain, Poland, Czechia, and additional countries remain discovery tasks. Static/scheduled
coverage may be added before historical actuals, but it must be presented as a separate layer.

## Preserve Switzerland without rebuilding it

The current Swiss R2 objects remain at their current keys and retain schema version 3. Do not:

- regenerate 422 million Swiss legs;
- renumber Swiss routes, train types, stations, or daily journeys;
- move Swiss objects to a new prefix merely for symmetry;
- add country/provider columns to every Swiss Parquet row;
- replace the Swiss `routes.bin`.

Add a catalog above the existing dataset instead:

```json
{
  "catalog_schema_version": 1,
  "datasets": {
    "ch": {
      "base_url": "<existing DATA_BASE_URL>",
      "leg_schema_version": 3,
      "country": "CH",
      "timezone": "Europe/Zurich",
      "time_semantics": "observed",
      "scope": "national-passenger",
      "coverage": [{"start": "2018-01-01", "end": null}]
    },
    "fi": {
      "base_url": "<DATA_BASE_URL>/datasets/fi",
      "leg_schema_version": 3,
      "country": "FI",
      "timezone": "Europe/Helsinki",
      "time_semantics": "observed",
      "scope": "national-passenger",
      "coverage": [{"start": "2023-01-01", "end": "2025-12-31"}]
    }
  }
}
```

The frontend reads the catalog, then loads one or more ordinary dataset manifests. Old clients can
continue reading the existing Swiss root manifest.

## Identity model

### Local wire identities

Compact IDs remain local to a dataset:

- route identity: `(dataset_id, route_id)`;
- journey identity: `(dataset_id, UTC departure day, journey_id)`;
- train type identity: `(dataset_id, type_id)`;
- local station identity: `(dataset_id, station_id)`.

This preserves the current `uint16` daily journey ID and low-16-bit route ID. It also prevents the
combined European route count from exhausting the Swiss wire contract.

The frontend injects `dataset_id` as a constant when it reads a country’s Parquet file; no country
column is needed in each fact row.

### Canonical European stations

Create a separate crosswalk:

```text
(dataset_id, source_station_id) -> local station_id -> eu_station_id
```

`eu_station_id` represents a physical passenger station or station complex. Matching evidence may
include coordinates, normalized names, UIC/RICS codes, parent-stop relationships, and manually
reviewed aliases. Never use a bare numeric UIC/EVA/BPUIC value as a global primary key.

Every crosswalk record stores:

- source namespace and source ID;
- canonical ID;
- match method;
- confidence;
- validity interval;
- review status;
- source coordinates and normalized name.

Low-confidence matches remain separate stations until reviewed.

### Cross-border journeys

Add an optional, derived journey-link table:

```text
(dataset_id, departure_day, journey_id) -> european_journey_id
```

Candidate links use operator RICS code, commercial train number, service date, station sequence,
and scheduled times. Do not merge on train number alone.

For rendering before journey linking is complete:

1. keep every source record internally;
2. canonicalize station endpoints;
3. detect overlapping legs by canonical pair, operator/train number, and time tolerance;
4. apply a documented source-priority rule to the rendered duplicate;
5. retain provenance so the decision can be audited.

## Dataset manifest contract

Each country manifest extends the existing fields with dataset-level metadata:

- `dataset_id`;
- country and optional region;
- provider and source URLs;
- license, attribution text, and redistribution notes;
- coverage intervals and known missing days;
- national, regional, or operator scope;
- `time_semantics`: `observed`, `final_prediction`, `delay_only`, or `scheduled`;
- timestamp precision;
- source and display timezones;
- source punctuality threshold;
- station namespace;
- leg, journey-sidecar, station, and geometry schema versions;
- cancellation and added-journey capabilities;
- generation timestamp and source revision/checksum.

Keep a normalized European punctuality threshold for comparison, but also retain the source’s
official threshold. Statistics must state which threshold they use.

## Adapter contract

Country adapters produce a common normalized staging table before the existing leg builder:

```text
dataset_id
service_day
source_trip_id
operator_id
commercial_train_number
train_category
line
source_station_id
stop_sequence
scheduled_arrival_utc
scheduled_departure_utc
observed_arrival_utc
observed_departure_utc
time_semantics
cancelled
commercial_stop
source_revision
```

An adapter is responsible for:

- timezone and DST conversion;
- distinguishing observations from estimates;
- added, cancelled, and partially cancelled journeys;
- stable stop ordering;
- filtering non-passenger and non-commercial operational points;
- source-specific identifiers and revisions;
- gaps and quality metrics;
- idempotent replay.

The normalized table is internal. Published daily leg files retain the compact contract wherever
possible. Source-specific details belong in manifests, static dictionaries, or journey sidecars.

## Geometry

Keep one `routes.bin` per dataset. The format already carries its own bounding box, so it does not
need a Europe-wide quantization box. A single continental box would materially reduce coordinate
precision.

Replace pipeline constants such as `CH_BBOX`, `SOURCE_TZ`, minimum daily legs, and punctuality
threshold with per-adapter configuration. This affects future builds only; existing Swiss geometry
and facts remain valid.

Canonical station links join the country geometry layers visually. A later cross-border geometry
overlay may fill border gaps, but must not require renumbering local route IDs.

## R2 free-tier budget

### Hard limits

- Use **Standard** storage only; the free tier does not apply to Infrequent Access.
- Never exceed **9.0 GB-month projected steady-state usage**. The remaining 1 GB protects against
  GB-month averaging, temporary duplicate objects, manifests, and estimation error.
- Stop publication automatically if a pre-upload inventory would exceed 9.0 GB.
- Target fewer than 100,000 Class A operations/month.
- Target fewer than 7.5 million Class B operations/month.
- Do not add R2 SQL or R2 Data Catalog; browser DuckDB reads ordinary objects directly.
- The allowance is account-wide, not a reason to create one bucket per country.

### Initial storage envelope

| Allocation | Hard planning cap |
| --- | ---: |
| Existing Switzerland | 3.70 GB |
| Germany, 2023-2025 | 3.10 GB |
| Netherlands, 2023-2025 | 0.50 GB |
| Belgium, 2023-2025 | 0.35 GB |
| Finland, 2023-2025 | 0.15 GB |
| Catalog, stations, route pairs, train types, and geometry | 0.30 GB |
| Safety reserve | 0.90 GB |
| **Total** | **9.00 GB** |

These are publication gates, not promises that the source will fit. Measure at least one weekday,
one weekend day, and one disruption-heavy month before approving a country backfill.

If a dataset exceeds its allocation, apply these remedies in order:

1. inspect schema and compression regressions;
2. remove unused source columns and dictionary-encode repeated strings in sidecars;
3. tune row-group size and Zstandard compression using measured browser latency;
4. remove duplicate/non-commercial events;
5. narrow that country’s published date window;
6. defer the country.

Never solve a budget failure by deleting Swiss history or silently degrading time semantics.

### Raw-data policy

R2 stores only browser-ready artifacts:

- daily leg Parquet;
- daily journey sidecars;
- compact static dictionaries;
- geometry;
- manifests and the catalog.

Raw ZIP, CSV, XML, NeTEx, SIRI, GTFS, and GTFS-RT snapshots remain in ephemeral CI storage or local
staging and are deleted after validated compaction. Preserve source URLs, checksums, revisions, and
retrieval timestamps in manifests so a build remains auditable.

For realtime feeds, store changes or finalized events locally during collection. Do not upload every
poll to R2. France’s current full GTFS-RT snapshot cadence would otherwise consume hundreds of GB per
year before compaction.

### Read-operation policy

- Load only countries intersecting the viewport or explicitly selected by the user.
- Do not fetch every country for every clock tick.
- Use the manifest instead of `HEAD` requests to discover object existence and size.
- Give immutable day files and versioned static assets long-lived cache headers.
- Serve R2 through a cacheable custom domain so repeated public reads are edge-cache hits.
- Cache catalog and manifest responses in the browser/service worker.
- Fetch each daily object once per session and reuse the registered DuckDB relation.
- Benchmark how many R2 GET/range operations DuckDB performs per day file.
- Add a request-budget test that fails if a normal playback session exceeds 40 origin reads.

At 40 origin reads per uncached session, the 7.5 million monthly target permits approximately
187,500 fully uncached sessions. Edge and browser caching should make normal usage substantially
cheaper.

### Write-operation policy

- Upload only after a complete country-month passes validation.
- Do not rewrite immutable day objects when only a manifest changes.
- Avoid full-bucket `ListObjects` scans in routine CI; carry forward inventory from manifests.
- Produce an inventory report containing object count, bytes by dataset/type/year, and projected
  GB-month before every publish.

## Frontend changes

1. Add `catalog.json` loading with a fallback that wraps the existing Swiss manifest as dataset
   `ch`.
2. Replace the single global data base URL with a registry of dataset clients.
3. Load and unload country geometry independently.
4. Union selected daily Parquet relations in DuckDB while injecting `dataset_id`.
5. Use compound dataset-local keys everywhere in application state.
6. Add coverage-aware date selection. Missing coverage is visually distinct from “no trains.”
7. Add country, provider, and evidence-layer filters.
8. Show source attribution and timing semantics in journey/station details.
9. Add a “common European window” preset for 2023-2025.
10. Keep the existing Swiss-only experience and URLs working.

## Quality and reconciliation rules

Every country-month must report:

- source events and journeys;
- published legs and journeys;
- scheduled versus observed/final-prediction counts;
- cancelled and partially cancelled journeys;
- unmatched static/realtime trips where applicable;
- unknown stations;
- duplicate candidates;
- missing hours/days;
- invalid or implausible durations;
- station-crosswalk confidence distribution;
- unique route-pair growth;
- compressed bytes per leg and per journey;
- projected full-window R2 size.

Country-specific minimum-volume checks replace the Swiss-only `MIN_LEGS_PER_DAY`. Missing data must
never be interpreted as a quiet operating day.

## Licensing and provenance

Do not erase dataset boundaries in storage. France and the German Bahn-Vorhersage archive use ODbL;
other sources use CC BY, CC0, open government licenses, or provider-specific terms. Every dataset
must retain attribution and transformation notices.

Before public release:

- record exact license text and version;
- verify commercial and redistribution rights;
- record share-alike obligations for derived databases;
- expose source attribution in the UI;
- keep independently downloadable derived datasets separate where licenses may conflict;
- obtain legal advice before claiming that the combined catalog is exempt from database
  share-alike obligations.

## Delivery phases

### Phase 0: measurements and contracts

- Add storage and operation budget checks.
- Benchmark representative Finnish, Dutch, Belgian, and German days/months.
- Freeze catalog and adapter contracts.
- Record checksums for the current Swiss manifest and static objects.
- Define acceptance fixtures for DST changes and cross-midnight services.

Exit criterion: projected Tier 1 total is at most 9.0 GB and no Swiss object needs regeneration.

### Phase 1: multi-dataset shell

- Publish `catalog.json` with only the existing Swiss dataset.
- Refactor the frontend to use dataset-local clients and compound keys.
- Prove that Swiss playback, search, boards, and geometry are unchanged.
- Retain fallback compatibility with the current root manifest.

Exit criterion: existing Swiss files load through both the old and new entry paths with identical
visible results.

### Phase 2: small historical adapters

- Implement Finland.
- Implement the Netherlands.
- Implement Belgium from official Infrabel files, using RIDE as a validation reference rather than
  the canonical dependency.
- Build the station crosswalk and coverage UI.

Exit criterion: the same date can render all four countries, with provenance and evidence semantics
visible and total R2 usage within their allocations.

### Phase 3: Germany

- Obtain and inventory Bahn-Vorhersage annual packages.
- Filter final passenger events and commercial stops.
- Validate completeness against published source gaps.
- Measure 2023-2025 before uploading any full year.
- Publish progressively, stopping at the 3.10 GB German cap.

Exit criterion: five-country common playback works and total projected R2 usage remains at most
9.0 GB.

### Phase 4: European product polish

- Deduplicate overlapping cross-border legs.
- Link high-confidence cross-border journeys.
- Add common-window and country-specific date modes.
- Add evidence-layer legend and normalized/source punctuality thresholds.
- Add pan-European search and station boards.
- Publish coverage and quality documentation.

### Phase 5: prospective collectors

- Run short pilots for France, Norway, Denmark, Sweden, Great Britain, and Austria.
- Measure daily compact size and identifier stability.
- Keep raw polling data outside R2.
- Allocate rolling retention from the safety reserve only after Tier 1 is stable.
- Prefer adding a small, honestly labelled country layer over pretending partial data is complete.

## Acceptance criteria

The European milestone is complete when:

- no existing Swiss fact, journey, or geometry object was rebuilt or renumbered;
- Switzerland still works through its original manifest and through the new catalog;
- five national datasets can be queried for a shared date in 2023-2025;
- all application keys are dataset-scoped;
- station search can resolve canonical stations without losing source identities;
- cross-border duplicates have deterministic, documented handling;
- observed, final-prediction, and scheduled movements are distinguishable in the UI;
- license and attribution information is visible;
- missing coverage is not rendered as zero service;
- the published inventory is at most 9.0 GB;
- projected Class A and Class B operations remain below their internal targets;
- warm-query and rendering regressions stay within agreed performance budgets;
- automated tests cover multi-country loading, DST, cross-midnight journeys, gaps, and legacy Swiss
  compatibility.

## Explicit non-goals for the first release

- Complete historical actuals for every European country.
- A single global numeric route, journey, or source-station ID.
- Perfect linking of every international train across providers.
- Uploading raw source archives or realtime snapshots to R2.
- Rebuilding Switzerland solely to make the storage layout symmetrical.
- Treating scheduled interpolation as observed train movement.
- Exceeding the R2 free tier to preserve an arbitrary common start date.

## Resume-quality outcome

After the five-country milestone is measured and shipped, the defensible project claim is that Berg
unifies heterogeneous European railway datasets while retaining hundreds of millions of compact
movement records, browser-side analytical queries, and cross-border visualization. Final résumé
figures must be regenerated from the published inventory rather than extrapolated from the Swiss
archive.
