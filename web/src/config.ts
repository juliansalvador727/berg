/** Where the data lives. Everything is CDN-fetched; there is no server in the serving path. */
export const DATA_BASE_URL =
  import.meta.env.VITE_DATA_BASE_URL ??
  "https://pub-40f06e4404c049578963083898f4ab57.r2.dev";

/** Lists every dataset. Absent (404) means a pre-catalog bucket: Switzerland alone. */
export const CATALOG_URL = `${DATA_BASE_URL}/catalog.json`;

/** A file inside one dataset. Switzerland's path is "", so its keys stay at the bucket root. */
export const datasetUrl = (path: string, file: string): string =>
  `${DATA_BASE_URL}/${path ? `${path}/` : ""}${file}`;

export const PMTILES_URL = `${DATA_BASE_URL}/tiles/switzerland.pmtiles`;
export const MAP_STYLE_URL = "https://tiles.openfreemap.org/styles/dark";
export const TERRAIN_TILE_URL =
  "https://elevation-tiles-prod.s3.amazonaws.com/terrarium/{z}/{x}/{y}.png";

export const dayFileUrl = (path: string, day: string): string => {
  const [y, m, d] = day.split("-");
  return datasetUrl(path, `legs/${y}/${m}/${d}.parquet`);
};

export const journeyFileUrl = (path: string, day: string): string => {
  const [y, m, d] = day.split("-");
  return datasetUrl(path, `journeys/${y}/${m}/${d}.parquet`);
};

/** Initial view: all of Switzerland. */
export const INITIAL_VIEW = { longitude: 8.23, latitude: 46.8, zoom: 7.2 };

/** How many seconds of simulated time to keep buffered ahead of simTime. */
export const BUFFER_SECONDS = 300;
