# Cross-border layer: one station, one movement, one train

Every dataset stops at its own edge, and the edges disagree. This layer stitches them together
without rebuilding or editing any dataset. It is built by `pipeline/scripts/build_links.py`
(`berg_pipeline/europe/links.py`) from the local publish mirrors, published by
`pipeline/scripts/sync_links.py` under `links/`, and listed in `catalog.json` as
`"links": {"path": "links"}`. Clients that don't read it still draw every dataset alone,
exactly as before.

## Why it exists

These were measured on 2026-03-10, when Switzerland and Germany both have data.

- **Switzerland also draws the German border belt.** It keeps every leg inside its box, which
  reaches 47.9°N, so it carries the Lörrach S-Bahn, the Basel–Waldshut Rhine line, and Singen,
  Radolfzell and Konstanz. That is 5,128 German-to-German legs a day. 81% of them (4,138) are
  also in the German dataset: the same station pair, departing within 3 minutes. Both datasets
  drew them.
- **Long-distance trains leave a hole between Freiburg and Basel.** The German archive has no
  Basel Bad Bf, so a southbound ICE ends at Freiburg. The Swiss dataset loses Freiburg at its box
  edge, so the same train starts at Basel Bad Bf. Nothing covered the 60 km in between.
- The Netherlands clips every leg with a foreign end, and Belgium's source stops at its last
  Belgian point. The same kind of hole therefore appears at Emmerich/Zevenaar,
  Gronau/Glanerbrug, Aachen/Liège, Breda/Noorderkempen and Rotterdam/Antwerp.

## Files

| Key | What it is |
|---|---|
| `links/crosswalk.json` | Groups of `(dataset, station)` that are one physical station |
| `links/days/YYYY/MM/DD.json` | Journey links for one UTC day, with bridge legs |
| `links/static/routes.bin` | Track geometry for every bridge station pair |
| `links/manifest.json` | Days, rules, per-bridge-pair evidence and counts |

## Station crosswalk

Every station at either end of a published route is compared with those of every neighbouring
dataset (boxes that touch). A pair matches under one of three rules:

- `distance+name` (high confidence): within 400 m, and the normalized names are similar. Similar
  means one token set contains the other, or their Jaccard overlap is at least 0.5, after
  dropping accents, punctuation and words like "Bf", "Hbf" or "station".
- `same-name` (medium confidence): within 1 km with identical normalized names.
- `same-point` (medium confidence): within 60 m, whatever the names.

Each station takes its best match per other dataset, and matches are then joined transitively.
Every group records its members, method, largest distance and confidence, plus
`review_status: "unreviewed"`. Station IDs are never compared across datasets: only 8 of the
105 German stations in the Swiss archive share an ID with the German dataset.

## Drawing a movement once

The client groups the legs in its window by crosswalked station pair and route fraction. Two
legs are the same movement when they come from different datasets and depart within 180 s of
each other. Only the copy from the dataset with the stronger time evidence is drawn:
`observed`, then `final_prediction`, then `delay_only`, then `scheduled`, with ties going to
the catalog's earlier dataset. In the Basel belt the Swiss observation is therefore drawn and
the German final prediction is hidden. The hidden leg stays in its dataset: station boards
still list it, and it is drawn when its own journey is spectated.

## Journey links

A link says that journey B continues journey A, where A and B come from two neighbouring
datasets on the same UTC day. The two must have the same public train number, and train
numbers are never enough on their own. The link also needs one of these:

- `handover`: A's last station is B's first station (through the crosswalk), within 10
  minutes.
- `overlap`: A and B call at a crosswalked station within 10 minutes of each other, and B runs
  on past A's end. This is the border belt: a German RE shows up in both datasets.
- `bridge`: A ends and B starts in their own countries, on opposite sides of a border.
  - The two ends are at most 100 km apart, 1-90 minutes apart, at a straight-line speed of 4-70
    m/s.
  - Neither end is a station the other dataset also serves. Switzerland serves Waldshut, so a
    German train ending there that really ran on to Basel would be in the Swiss journey.

Each journey continues into at most one journey and is continued by at most one, across all
datasets at once. The tightest match wins: a shared station first, then the shortest gap. An
Aachen–Heerlen–Liège train therefore links Germany → Netherlands → Belgium, and never skips
the Dutch section.

### Bridge judgement

Timetables repeat, so a coincidence repeats too. Leuven → Weert (BE L/IC and NL trains that
share a number) showed up on 38 of 59 days, and IC 3626 Gent → Roosendaal on every weekday. A
bridge station pair is therefore judged on its record over the whole build. It is accepted only
if all of these hold:

- It appears on at least 3 days.
- It runs both ways, or averages at least 2 trains a day.
- At least 5 distinct train numbers use it, or 8 if it runs one way only.
- Both ends are within 60 km of a station the other dataset serves.
- At least 60% of its crossings take within 30% (at least 5 minutes) of the pair's median
  duration.

A crossing on an accepted pair is still dropped when its duration is off the pair's median by
more than 35% (at least 10 minutes). One example is IC 2872 → L 2872, which took 78 minutes on
the 40-minute Rotterdam → Antwerp pair. The manifest lists every candidate pair with its
evidence and verdict. Over 2025-11-03 → 2025-12-31, the accepted pairs were the real crossings:

- Freiburg ↔ Basel Bad Bf and Gronau ↔ Glanerbrug.
- Herzogenrath ↔ Eygelshoven Markt/Heerlen and Emmerich ↔ Zevenaar.
- Bad Bentheim ↔ Oldenzaal/Hengelo and Oberhausen ↔ Arnhem.
- Aachen ↔ Liège, Breda ↔ Noorderkempen, Rotterdam ↔ Antwerp and Visé ↔ Eijsden/Maastricht.

The rejected pairs were one number repeating (Leuven → Weert, Mol → 's-Hertogenbosch, Den Haag
↔ Antwerp IC 3134, Duisburg ↔ Zutphen RB/ST 31273).

### Bridge legs

A bridge leg departs at A's last arrival and arrives at B's first departure. It carries A's
train type and last delay, and it is attached to A's journey, so spectating A carries straight
on. The times are **interpolated between two sources**. The layer's `bridge_semantics` is
`interpolated`, and the spectating panel says so ("the border section is interpolated between
the two sources"). Geometry comes from the ordinary geometry job, routed over the merged rail
extracts of the countries involved. Route IDs are offset past every dataset's uint16 route
space (`BRIDGE_ROUTE_BASE`), so they can never collide with a dataset's own routes.

## Client behaviour

- The layer loads once, when there is more than one dataset. Each window refill fetches that
  window's day files and hides duplicates, adds bridge legs whose first journey is drawn, and
  marks linked journey ends.
- A linked end does not fade out. The continuation stands at full strength where the previous
  train stopped, so one train is not drawn as two half-trains.
- Spectating follows a link at its `at` time and moves to the next country's journey. The panel
  says "Continues in Switzerland as …" and "Continued from Germany (…)".

## Limits

- A link needs both halves on the same UTC day file. A train crossing midnight between its two
  datasets is not linked. Neither is a bridge that would depart on the next UTC day, because
  journey IDs are local to one day file.
- The bridge rules are measured, not proven. A daily coincidence that uses 5+ numbers in both
  directions with consistent timing would pass. The manifest's per-pair evidence is there so
  pairs can be audited and reviewed.
- Only neighbouring datasets are compared. Finland has no neighbour in the catalog.
