/**
 * Main thread: map, clock, controls. No DuckDB here — that lives in the worker.
 *
 * Target is roughly 500 lines for the whole app. Add a framework only if the UI actually grows
 * to need one.
 */

import { Clock } from "./clock";
import { INITIAL_VIEW } from "./config";

async function main(): Promise<void> {
  const clock = new Clock(Date.UTC(2024, 0, 15, 8, 0, 0) / 1000);
  void clock;
  void INITIAL_VIEW;

  // M0: straight-line geometry, dots on the map, one day of data. Proves the whole loop.
  // M4: MapLibre + Protomaps basemap, deck.gl overlay, worker handshake, scrub bar,
  //     datetime input, speed buttons, prefetch driven by clock.speed.
  throw new Error("M0: not built yet");
}

main().catch((err) => {
  console.error(err);
});
