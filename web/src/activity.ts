/**
 * "Trains running" chart: how many trains the map drew at each moment of the displayed day.
 *
 * Sampled from the render loop rather than queried, so it costs no range requests and always
 * agrees with the map. The curve fills in as the day plays. Samples are only comparable while
 * the same countries and services are drawn, so a change of either starts a fresh curve.
 */

const BUCKET_S = 300;
const DAY_S = 86_400;
const MAX_GAP_S = 3 * BUCKET_S;
const RENDER_EVERY_MS = 250;
const PAD = { top: 6, right: 4, bottom: 16, left: 30 };
const SVG_NS = "http://www.w3.org/2000/svg";

const clockFormats = new Map<string, Intl.DateTimeFormat>();

/** Seconds since local midnight in `timeZone`. */
function secondsIntoDay(epoch: number, timeZone: string): number {
  let format = clockFormats.get(timeZone);
  if (!format) {
    format = new Intl.DateTimeFormat("en-GB", {
      timeZone,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hourCycle: "h23",
    });
    clockFormats.set(timeZone, format);
  }
  const parts = format.formatToParts(new Date(epoch * 1000));
  const part = (type: string) => Number(parts.find((p) => p.type === type)?.value ?? 0);
  return part("hour") * 3600 + part("minute") * 60 + part("second");
}

/** A round axis maximum: 1, 2, 2.5 or 5 times a power of ten. */
function niceMax(value: number): number {
  if (value <= 0) return 10;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  for (const step of [1, 2, 2.5, 5, 10]) if (step * magnitude >= value) return step * magnitude;
  return 10 * magnitude;
}

const svg = <K extends keyof SVGElementTagNameMap>(
  tag: K,
  attrs: Record<string, string | number> = {},
): SVGElementTagNameMap[K] => {
  const element = document.createElementNS(SVG_NS, tag);
  for (const [key, value] of Object.entries(attrs)) element.setAttribute(key, String(value));
  return element;
};

export class ActivityChart {
  private samples = new Map<number, number>();
  private signature = "";
  private lastRender = 0;
  private hoverX: number | null = null;
  private view: { dayStart: number; now: number; timeZone: string } | null = null;

  private readonly root: SVGSVGElement;
  private readonly tooltip: HTMLDivElement;

  constructor(
    private readonly container: HTMLElement,
    private readonly formatTime: (epoch: number, timeZone: string) => string,
  ) {
    this.root = svg("svg", { role: "img", "aria-label": "Trains running over the displayed day" });
    this.tooltip = document.createElement("div");
    this.tooltip.className = "chart-tooltip hidden";
    container.append(this.root, this.tooltip);
    container.addEventListener("pointermove", (event) => {
      this.hoverX = event.clientX - container.getBoundingClientRect().left;
      this.draw();
    });
    container.addEventListener("pointerleave", () => {
      this.hoverX = null;
      this.draw();
    });
  }

  /** Record one frame. `signature` names what was counted; a new one discards old samples. */
  record(time: number, count: number, signature: string, timeZone: string): void {
    if (signature !== this.signature) {
      this.signature = signature;
      this.samples.clear();
    }
    const bucket = Math.floor(time / BUCKET_S) * BUCKET_S;
    this.samples.set(bucket, count);
    const dayStart = Math.floor(time) - secondsIntoDay(time, timeZone);
    this.view = { dayStart, now: time, timeZone };

    const wall = performance.now();
    if (wall - this.lastRender < RENDER_EVERY_MS) return;
    this.lastRender = wall;
    for (const key of this.samples.keys()) {
      if (key < dayStart - DAY_S || key > dayStart + 2 * DAY_S) this.samples.delete(key);
    }
    this.draw();
  }

  private draw(): void {
    if (!this.view) return;
    const { dayStart, now, timeZone } = this.view;
    const width = this.container.clientWidth;
    const height = this.container.clientHeight;
    if (width === 0 || height === 0) return;
    const plotW = width - PAD.left - PAD.right;
    const plotH = height - PAD.top - PAD.bottom;

    const points: Array<[number, number]> = [];
    for (let t = dayStart; t < dayStart + DAY_S; t += BUCKET_S) {
      const count = this.samples.get(t);
      if (count !== undefined) points.push([t, count]);
    }
    const yMax = niceMax(Math.max(0, ...points.map(([, count]) => count)));
    const x = (t: number) => PAD.left + ((t - dayStart) / DAY_S) * plotW;
    const y = (count: number) => PAD.top + plotH - (count / yMax) * plotH;

    const nodes: SVGElement[] = [];
    for (const value of [0, yMax / 2, yMax]) {
      nodes.push(svg("line", { class: "grid", x1: PAD.left, x2: width - PAD.right, y1: y(value), y2: y(value) }));
      const label = svg("text", { class: "axis-label", x: PAD.left - 6, y: y(value) + 3, "text-anchor": "end" });
      label.textContent = value >= 1000 ? `${value / 1000}k` : String(value);
      nodes.push(label);
    }
    for (const hour of [0, 6, 12, 18, 24]) {
      const label = svg("text", {
        class: "axis-label",
        x: x(dayStart + hour * 3600),
        y: height - 2,
        "text-anchor": hour === 0 ? "start" : hour === 24 ? "end" : "middle",
      });
      label.textContent = `${String(hour).padStart(2, "0")}:00`;
      nodes.push(label);
    }

    // A slow frame at high speed can skip a bucket or two; a longer gap (a seek, a pause of the
    // tab) splits the curve, so missing time never draws as a confident straight line.
    const runs: Array<Array<[number, number]>> = [];
    for (const point of points) {
      const run = runs.at(-1);
      if (run && point[0] - run.at(-1)![0] <= MAX_GAP_S) run.push(point);
      else runs.push([point]);
    }
    for (const run of runs) {
      const line = run.map(([t, count], i) => `${i ? "L" : "M"}${x(t).toFixed(1)},${y(count).toFixed(1)}`).join("");
      const baseline = y(0).toFixed(1);
      nodes.push(svg("path", {
        class: "area",
        d: `${line}L${x(run.at(-1)![0]).toFixed(1)},${baseline}L${x(run[0]![0]).toFixed(1)},${baseline}Z`,
      }));
      nodes.push(svg("path", { class: "line", d: line }));
      if (run.length === 1) nodes.push(svg("circle", { class: "hover-dot", cx: x(run[0]![0]), cy: y(run[0]![1]), r: 1.5 }));
    }
    if (points.length === 0) {
      const note = svg("text", { class: "empty-note", x: PAD.left + plotW / 2, y: PAD.top + plotH / 2, "text-anchor": "middle" });
      note.textContent = "Collecting samples…";
      nodes.push(note);
    }

    const nowX = x(Math.min(Math.max(now, dayStart), dayStart + DAY_S));
    nodes.push(svg("line", { class: "now", x1: nowX, x2: nowX, y1: PAD.top, y2: PAD.top + plotH }));
    const nowCount = this.samples.get(Math.floor(now / BUCKET_S) * BUCKET_S);
    if (nowCount !== undefined) nodes.push(svg("circle", { class: "now-dot", cx: nowX, cy: y(nowCount), r: 3.5 }));

    this.tooltip.classList.add("hidden");
    if (this.hoverX !== null && points.length > 0) {
      const t = dayStart + ((this.hoverX - PAD.left) / plotW) * DAY_S;
      const nearest = points.reduce((best, point) => (Math.abs(point[0] - t) < Math.abs(best[0] - t) ? point : best));
      const hx = x(nearest[0]);
      nodes.push(svg("line", { class: "hover-line", x1: hx, x2: hx, y1: PAD.top, y2: PAD.top + plotH }));
      nodes.push(svg("circle", { class: "hover-dot", cx: hx, cy: y(nearest[1]), r: 4 }));
      this.tooltip.innerHTML = `${this.formatTime(nearest[0], timeZone)} · <strong>${nearest[1].toLocaleString("en-GB")}</strong> trains`;
      this.tooltip.style.left = `${Math.min(Math.max(hx, 60), width - 60)}px`;
      this.tooltip.classList.remove("hidden");
    }
    this.root.replaceChildren(...nodes);
  }
}
