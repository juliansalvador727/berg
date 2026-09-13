import { expect, test, type Page } from "@playwright/test";

interface ScreenPoint {
  x: number;
  y: number;
}

interface StationPoint extends ScreenPoint {
  id: number;
  name: string;
}

interface TrainPoint extends ScreenPoint {
  journeyId: number;
}

async function activate(page: Page, selector: string): Promise<void> {
  await page.locator(selector).evaluate((element: HTMLElement) => element.click());
}

async function openObserver(page: Page): Promise<void> {
  await page.goto("/");
  // The topbar is a zero-height positioning wrapper, so assert one of its rendered controls.
  await expect(page.getByRole("button", { name: /Find a train/ })).toBeVisible({
    timeout: 120_000,
  });
  // Headless Chromium uses software WebGL in CI. Pause the default 600× clock immediately so
  // background window refills do not starve Playwright while the controls are being exercised.
  await activate(page, "#playback-button");
  await expect(page.locator("#playback-label")).toHaveText("Resume playback");
  await expect(page.locator("#topbar")).not.toHaveClass(/\bhidden\b/);
  await expect(page.locator("#loading")).toHaveClass(/done/);
  await expect(page.locator("#error")).toHaveClass(/\bhidden\b/);
  await expect(page.locator("#time")).toHaveAttribute("datetime", /^2018-01-01T/);
}

async function spectateFromSearch(page: Page): Promise<void> {
  await activate(page, "#command-button");
  const input = page.locator("#command-input");
  await input.fill("IC");
  const result = page.locator("#command-results .command-item").first();
  await expect(result).toBeVisible({ timeout: 30_000 });
  await result.evaluate((element: HTMLElement) => element.click());
  await expect(page.locator("#details .eyebrow")).toContainText("Spectating train", {
    timeout: 30_000,
  });
  await expect(page.locator("#speed-value")).toHaveText("1×");
}

async function trainPoints(page: Page): Promise<TrainPoint[]> {
  return page.evaluate(() => window.__BERG_E2E__?.trainPoints() ?? []);
}

async function stationPoints(page: Page): Promise<StationPoint[]> {
  return page.evaluate(() => window.__BERG_E2E__?.stationPoints() ?? []);
}

test("loads the archive and controls historical playback", async ({ page }) => {
  await openObserver(page);

  const clock = page.locator("#time");
  const initialTime = await clock.getAttribute("datetime");
  await page.keyboard.press("Space");
  await expect.poll(() => clock.getAttribute("datetime")).not.toBe(initialTime);

  await page.keyboard.press("Space");
  await expect(page.getByRole("button", { name: /Resume playback/ })).toBeVisible();
  await expect(page.locator("#speed-value")).toHaveText("Paused");
  const pausedTime = await clock.getAttribute("datetime");
  await page.waitForTimeout(1_100);
  await expect(clock).toHaveAttribute("datetime", pausedTime!);

  await page.keyboard.press("Space");
  await activate(page, "#speed-badge");
  await page.getByRole("button", { name: /Run at 8×/ }).evaluate((element: HTMLElement) =>
    element.click(),
  );
  await expect(page.locator("#speed-value")).toHaveText("8×");
});

test("filters services, navigates the archive, and spectates a complete route", async ({ page }) => {
  await openObserver(page);

  const display = page.getByRole("button", { name: /Train display/ });
  await display.evaluate((element: HTMLElement) => element.click());
  await expect(display).toHaveAttribute("aria-expanded", "true");
  const sBahn = page.getByRole("button", { name: "S-Bahn", exact: true });
  await sBahn.evaluate((element: HTMLElement) => element.click());
  await expect(sBahn).not.toHaveClass(/\bon\b/);
  await expect(page.locator("#filter-summary")).toContainText("5 of 6 services");
  await page
    .getByRole("button", { name: "Delay", exact: true })
    .evaluate((element: HTMLElement) => element.click());
  await expect(page.locator("#filter-summary")).toContainText("delay colours");
  await activate(page, "#filters-close");

  await page.keyboard.press("Control+k");
  const input = page.locator("#command-input");
  await input.fill("2018-01-02 07:30");
  await page
    .locator("#command-results .command-item")
    .evaluate((element: HTMLElement) => element.click());
  await expect(page.locator("#time")).toHaveAttribute("datetime", /^2018-01-02T/);

  await spectateFromSearch(page);
  await expect
    .poll(() => page.evaluate(() => window.__BERG_E2E__?.selectedRouteIds().length ?? 0), {
      timeout: 30_000,
    })
    .toBeGreaterThan(1);
  await page
    .getByRole("button", { name: "Stop spectating" })
    .evaluate((element: HTMLElement) => element.click());
  await expect(page.locator("#details")).toBeHidden();
  expect(await page.evaluate(() => window.__BERG_E2E__?.selectedJourneyId() ?? null)).toBeNull();
});

test("clicks train arrows and preserves unspectate controls on station boards", async ({ page }) => {
  await openObserver(page);

  await expect.poll(() => trainPoints(page), { timeout: 30_000 }).not.toHaveLength(0);
  const trains = await trainPoints(page);
  const train = trains.find(({ x, y }) => x > 80 && x < 1_000 && y > 210 && y < 820);
  expect(train, "expected a clickable train outside the controls").toBeTruthy();
  await page.mouse.click(train!.x, train!.y);
  await expect(page.locator("#details .eyebrow")).toContainText("Spectating train", {
    timeout: 30_000,
  });

  // Zoom out while the camera keeps following the selected train. This makes several stations
  // available without relying on a hard-coded station or pixel coordinate.
  await page.mouse.move(700, 520);
  await page.mouse.wheel(0, 1_600);
  await page.waitForTimeout(800);

  const liveTrains = await trainPoints(page);
  const stations = (await stationPoints(page)).filter(({ x, y }) => {
    if (x < 80 || x > 980 || y < 220 || y > 820) return false;
    return liveTrains.every((trainPoint) => Math.hypot(trainPoint.x - x, trainPoint.y - y) > 24);
  });
  expect(stations, "expected a visible station away from train arrows").not.toHaveLength(0);

  await page.mouse.click(stations[0]!.x, stations[0]!.y);
  await expect(page.locator("#details .eyebrow")).toContainText("Station", { timeout: 10_000 });

  const stop = page.getByRole("button", { name: "Stop spectating" });
  await expect(stop).toBeVisible();
  await stop.evaluate((element: HTMLElement) => element.click());
  await expect(page.locator("#details")).toBeVisible();
  await expect(page.locator("#details .eyebrow")).toContainText("Station");
  await expect(stop).toBeHidden();
  expect(await page.evaluate(() => window.__BERG_E2E__?.selectedJourneyId() ?? null)).toBeNull();
});
