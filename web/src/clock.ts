/**
 * Simulation clock.
 *
 * Advances on wall-clock delta, never on frame count. That is what makes 8x feel smooth on a
 * janky frame and what stops speed changes from lurching: a dropped frame costs you nothing,
 * because the next tick measures how long it actually took.
 */
export class Clock {
  /** Simulated time, epoch seconds UTC. */
  simTime: number;
  speed: number;
  paused = true;

  private last: number | null = null;

  constructor(simTime: number, speed = 1) {
    this.simTime = simTime;
    this.speed = speed;
  }

  /** Call once per frame. Returns the new simTime. */
  tick(now: number = performance.now()): number {
    if (this.last !== null && !this.paused) {
      this.simTime += ((now - this.last) / 1000) * this.speed;
    }
    this.last = now;
    return this.simTime;
  }

  /** Jump to an exact instant. Precision is free — position is a lerp, not a lookup. */
  seek(simTime: number): void {
    this.simTime = simTime;
    this.last = null;
  }

  setSpeed(speed: number): void {
    this.speed = speed;
  }

  play(): void {
    this.paused = false;
    this.last = null; // don't bank the paused wall-clock time as sim time
  }

  pause(): void {
    this.paused = true;
  }
}

export const SPEEDS = [1, 8, 60, 150, 600] as const;
