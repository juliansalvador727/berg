/** The 8-byte wire schema. Keep it 8 bytes — 800M legs is 6.4 GB and the budget is 10. */
export interface Leg {
  route_id: number; // uint32, FK into routes.bin
  t_dep: number; // uint32, epoch seconds UTC
  dur: number; // uint16, seconds
  type: number; // uint8, train category enum
  delay: number; // int16, departure delay
  flags: number; // uint8, see FLAG_*
}

export const FLAG_SCHEDULED_FALLBACK = 1 << 0;
export const FLAG_SYNTHETIC_SPLIT = 1 << 1;

/** Written by the pipeline, fetched once at startup. The client hardcodes none of this. */
export interface Manifest {
  schema_version: number;
  first_day: string; // YYYY-MM-DD
  last_day: string;
  max_leg_duration: number; // seconds — bounds every scrub query
}
