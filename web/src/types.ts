/** The 8-byte wire schema. Keep it 8 bytes — 800M legs is 6.4 GB and the budget is 10. */
export interface Leg {
  route_id: number; // decoded base FK into routes.bin
  route_start: number; // normalized progress on the route, normally 0
  route_end: number; // normalized progress on the route, normally 1
  t_dep: number; // uint32, epoch seconds UTC
  dur: number; // uint16, seconds
  type: number; // uint8, train category enum
  delay: number; // int16, departure delay
  flags: number; // uint8, see FLAG_*
}

export const FLAG_SCHEDULED_FALLBACK = 1 << 0;
export const FLAG_SYNTHETIC_SPLIT = 1 << 1;
export const FLAG_ROUTE_FRACTION = 1 << 2;

/** One published day. `legs` is the row count; `bytes` the file size. */
export interface ManifestDay {
  bytes: number;
  legs: number;
}

/**
 * Written by the pipeline (berg_pipeline/publish.py — the authority on these names), fetched
 * once at startup. The client hardcodes none of this.
 */
export interface Manifest {
  schema_version: number;
  max_leg_duration_s: number; // bounds every scrub query
  generated_at: string; // ISO 8601 UTC
  start: string | null; // YYYY-MM-DD; null when nothing is published yet
  end: string | null;
  days: Record<string, ManifestDay>; // keyed YYYY-MM-DD — absent key == no data that day
  missing_days: string[]; // archive holes inside [start, end]; the scrub bar must skip these
}
