/** The compact leg wire schema decoded from a daily Parquet file. */
export interface Leg {
  // Index into the loaded dataset list. Not on the wire: the worker injects it as a constant
  // per country file, so route/journey/type ids stay dataset-local and every key that uses
  // them must include it.
  dataset: number;
  route_id: number; // decoded base FK into routes.bin
  journey_id: number; // uint16, unique within this departure-day file
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
export const LEG_SCHEMA_VERSION = 3;
export const JOURNEY_ID_UNAVAILABLE = 0xffff;

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
  // UTC days the source itself records as mostly cancelled (strikes): published, but thin.
  source_cancelled_days?: string[];
}

/** How strong a dataset's times are as evidence. */
export type TimeSemantics = "observed" | "final_prediction" | "delay_only" | "scheduled";

/**
 * One entry of catalog.json (berg_pipeline/europe/catalog.py is the authority). `path` is
 * relative to the data base URL; "" is the Swiss archive at the bucket root.
 */
export interface CatalogEntry {
  path: string;
  name: string;
  country: string;
  timezone: string;
  bbox: [number, number, number, number];
  leg_schema_version: number;
  time_semantics: TimeSemantics;
  scope: string;
  provider: string;
  license: string;
  license_url: string;
  attribution: string;
  punctuality_threshold_s: number;
  coverage: Array<{ start: string; end: string | null }>;
}

export interface Catalog {
  catalog_schema_version: number;
  datasets: Record<string, CatalogEntry>;
}

/** A catalog entry resolved by the worker, with its id and position in the dataset list. */
export interface DatasetInfo extends CatalogEntry {
  id: string;
  index: number;
}

export const CATALOG_SCHEMA_VERSION = 1;
