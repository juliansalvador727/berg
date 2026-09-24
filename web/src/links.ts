/**
 * The cross-border layer (berg_pipeline/europe/links.py is the authority on these files).
 *
 * Datasets stop at their own edges and some overlap: Switzerland also carries the German lines
 * along its border, and an ICE from Frankfurt ends at Freiburg in one dataset and starts at
 * Basel Bad Bf in the other. This layer never edits a dataset. It only decides:
 *
 *  - which of two copies of one movement to draw (the crosswalk + an evidence priority), and
 *  - how a train continues across a border: a link between two journeys, and where they do
 *    not meet, a bridge leg with interpolated times on its own routed geometry.
 */

import { DATA_BASE_URL } from "./config";
import { Routes, fetchRoutes } from "./routes";
import type { DatasetInfo, Leg, TimeSemantics } from "./types";

/** Bridge route ids are offset past every uint16 dataset route id, so the two never meet. */
export const BRIDGE_ROUTE_BASE = 0x10000;
/** Client-only leg flag: this leg is an interpolated bridge, not a published movement. */
export const FLAG_BRIDGE = 1 << 7;

export interface LinksManifest {
  links_schema_version: number;
  days: Record<string, number>;
  crosswalk: string;
  routes: string;
  dedup_window_s: number;
  evidence_rank: Partial<Record<TimeSemantics, number>>;
  bridge_semantics: string;
}

interface CrosswalkFile {
  groups: Array<{ id: number; members: Array<{ dataset: string; station: number }> }>;
}

export interface Bridge {
  route: number;
  from_station: number;
  to_station: number;
  t_dep: number;
  dur: number;
  type: number;
  delay: number;
}

export interface Link {
  kind: "handover" | "overlap" | "bridge";
  from: [string, number];
  to: [string, number];
  from_trip: string;
  to_trip: string;
  train: string;
  /** When spectating switches from the first journey to the second. */
  at: number;
  bridge?: Bridge;
}

interface DayLinks {
  day: string;
  links: Link[];
}

/** journey_id is unique within one dataset's departure-day file; see render/trains.ts. */
export const journeyKeyOf = (dataset: number, day: number, journeyId: number): number =>
  (dataset * 100_000 + day) * 65_536 + journeyId;

const dayNumber = (day: string): number => Math.floor(Date.parse(`${day}T00:00:00Z`) / 86_400_000);

export class CrossBorder {
  private canonical = new Map<string, number>();
  private dayCache = new Map<string, Promise<DayLinks | null>>();
  private indexById: Map<string, number>;

  private constructor(
    readonly manifest: LinksManifest,
    crosswalk: CrosswalkFile,
    readonly routes: Routes,
    private readonly base: string,
    private readonly datasets: DatasetInfo[],
  ) {
    for (const group of crosswalk.groups) {
      for (const member of group.members) this.canonical.set(`${member.dataset}:${member.station}`, group.id);
    }
    this.indexById = new Map(datasets.map((info) => [info.id, info.index]));
  }

  static async load(path: string, datasets: DatasetInfo[]): Promise<CrossBorder> {
    const base = `${DATA_BASE_URL}/${path}`;
    const manifest = (await (await fetch(`${base}/manifest.json`)).json()) as LinksManifest;
    const [crosswalk, routes] = await Promise.all([
      fetch(`${base}/${manifest.crosswalk}`).then((r) => r.json() as Promise<CrosswalkFile>),
      fetchRoutes(`${base}/${manifest.routes}`),
    ]);
    return new CrossBorder(manifest, crosswalk, routes, base, datasets);
  }

  /** The physical station a dataset's station belongs to, when another dataset has it too. */
  stationGroup(dataset: number, station: number): number | undefined {
    return this.canonical.get(`${this.datasets[dataset]!.id}:${station}`);
  }

  /** Lower is stronger evidence; ties go to the catalog's earlier dataset. */
  rank(dataset: number): number {
    const semantics = this.datasets[dataset]!.time_semantics;
    return (this.manifest.evidence_rank[semantics] ?? 9) * 1_000 + dataset;
  }

  async day(day: string): Promise<DayLinks | null> {
    if (!(day in this.manifest.days)) return null;
    let load = this.dayCache.get(day);
    if (!load) {
      const [y, m, d] = day.split("-");
      load = fetch(`${this.base}/days/${y}/${m}/${d}.json`)
        .then((r) => (r.ok ? (r.json() as Promise<DayLinks>) : null))
        .catch(() => null);
      this.dayCache.set(day, load);
    }
    return load;
  }

  index(datasetId: string): number | undefined {
    return this.indexById.get(datasetId);
  }

  /** Bridge legs of these days departing in [from, to], attached to their first journey. */
  bridgeLegs(days: DayLinks[], from: number, to: number, usable: (dataset: number) => boolean): Leg[] {
    const out: Leg[] = [];
    for (const { links } of days) {
      for (const link of links) {
        const bridge = link.bridge;
        if (!bridge || bridge.t_dep < from || bridge.t_dep > to) continue;
        const dataset = this.index(link.from[0]);
        if (dataset === undefined || !usable(dataset)) continue;
        out.push({
          dataset,
          route_id: BRIDGE_ROUTE_BASE + bridge.route,
          journey_id: link.from[1],
          route_start: 0,
          route_end: 1,
          t_dep: bridge.t_dep,
          dur: bridge.dur,
          type: bridge.type,
          delay: bridge.delay,
          flags: FLAG_BRIDGE,
        });
      }
    }
    return out;
  }

  /** Journey keys whose end continues in another dataset, and whose start continues one. */
  linkedEnds(days: DayLinks[], drawn: (a: number, b: number) => boolean): { ends: Set<number>; starts: Set<number> } {
    const ends = new Set<number>();
    const starts = new Set<number>();
    for (const { day, links } of days) {
      const n = dayNumber(day);
      for (const link of links) {
        const a = this.index(link.from[0]);
        const b = this.index(link.to[0]);
        // Only when both halves are on the map; otherwise the train ends and fades as before.
        if (a === undefined || b === undefined || !drawn(a, b)) continue;
        ends.add(journeyKeyOf(a, n, link.from[1]));
        starts.add(journeyKeyOf(b, n, link.to[1]));
      }
    }
    return { ends, starts };
  }
}

/**
 * One movement drawn once. Legs whose both ends are crosswalked stations are grouped by
 * (station group, station group, route fraction); within a group a leg is hidden when a
 * dataset with stronger evidence has a leg departing within `windowS` of it.
 */
export function dedupe(
  legs: Leg[],
  endpoints: (leg: Leg) => [number, number] | undefined,
  rank: (dataset: number) => number,
  windowS: number,
): { kept: Leg[]; hidden: Leg[] } {
  const groups = new Map<string, Leg[]>();
  for (const leg of legs) {
    const ends = endpoints(leg);
    if (!ends) continue;
    const key = `${ends[0]}>${ends[1]}>${leg.route_start.toFixed(3)}`;
    const group = groups.get(key);
    if (group) group.push(leg);
    else groups.set(key, [leg]);
  }
  const hidden = new Set<Leg>();
  for (const group of groups.values()) {
    if (group.length < 2 || group.every((leg) => leg.dataset === group[0]!.dataset)) continue;
    group.sort((a, b) => rank(a.dataset) - rank(b.dataset) || a.t_dep - b.t_dep);
    const shown: Leg[] = [];
    for (const leg of group) {
      const duplicate = shown.some(
        (other) =>
          other.dataset !== leg.dataset &&
          rank(other.dataset) < rank(leg.dataset) &&
          Math.abs(other.t_dep - leg.t_dep) <= windowS,
      );
      if (duplicate) hidden.add(leg);
      else shown.push(leg);
    }
  }
  if (hidden.size === 0) return { kept: legs, hidden: [] };
  return { kept: legs.filter((leg) => !hidden.has(leg)), hidden: [...hidden] };
}
