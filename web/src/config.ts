/** Where the data lives. Everything is CDN-fetched; there is no server in the serving path. */
export const DATA_BASE_URL = import.meta.env.VITE_DATA_BASE_URL ?? "https://data.berg.ch";

export const MANIFEST_URL = `${DATA_BASE_URL}/manifest.json`;
export const ROUTES_URL = `${DATA_BASE_URL}/static/routes.bin`;
export const STATIONS_URL = `${DATA_BASE_URL}/static/stations.json`;
export const TRAIN_TYPES_URL = `${DATA_BASE_URL}/static/train_types.json`;
export const ROUTE_PAIRS_URL = `${DATA_BASE_URL}/static/route_pairs.json`;
export const PMTILES_URL = `${DATA_BASE_URL}/tiles/switzerland.pmtiles`;
export const MAP_STYLE_URL = "https://tiles.openfreemap.org/styles/dark";
export const TERRAIN_TILEJSON_URL = "https://demotiles.maplibre.org/terrain-tiles/tiles.json";

export const dayFileUrl = (day: string): string => {
  const [y, m, d] = day.split("-");
  return `${DATA_BASE_URL}/legs/${y}/${m}/${d}.parquet`;
};

export const journeyFileUrl = (day: string): string => {
  const [y, m, d] = day.split("-");
  return `${DATA_BASE_URL}/journeys/${y}/${m}/${d}.parquet`;
};

/** Initial view: all of Switzerland. */
export const INITIAL_VIEW = { longitude: 8.23, latitude: 46.8, zoom: 7.2 };

/** How many seconds of simulated time to keep buffered ahead of simTime. */
export const BUFFER_SECONDS = 300;
