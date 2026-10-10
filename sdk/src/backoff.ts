/**
 * Backoff with jitter.
 *
 * The jitter is the important part and it is not decoration. When a gateway
 * restarts, every connection it held drops at the same instant, and the whole
 * population reconnects together. Fixed intervals make that a synchronised
 * reconnect storm: the fresh instance receives a thundering herd on top of the
 * load it was just started to serve. Randomising the interval spreads the
 * recovery across the whole window, which is the difference between an instance
 * coming back up and being knocked over again.
 */
export interface BackoffOptions {
  /** First retry delay. Doubles on each consecutive failure. */
  initialMs: number;
  /** Ceiling for the delay. */
  maxMs: number;
  /**
   * Fraction of the interval that is randomised. 0 gives a fixed delay and
   * loses the spread; 1 gives full jitter, which can retry almost immediately.
   */
  jitterRatio: number;
  /** Attempts before the SDK gives up. */
  maxAttempts: number;
}

export class Backoff {
  private attempt = 0;
  private readonly random: () => number;
  private readonly opts: BackoffOptions;

  constructor(opts: BackoffOptions, random: () => number = Math.random) {
    this.opts = opts;
    this.random = random;
  }

  /** Number of consecutive failures recorded. */
  get attempts(): number {
    return this.attempt;
  }

  /** Whether the retry budget is exhausted. */
  get exhausted(): boolean {
    return this.attempt >= this.opts.maxAttempts;
  }

  /** Records a failure and returns the delay before the next attempt. */
  next(): number {
    const base = Math.min(this.opts.maxMs, this.opts.initialMs * 2 ** this.attempt);
    this.attempt += 1;

    // The jitter is symmetric around the base delay, so the exponential growth
    // still holds on average while individual clients spread out.
    const spread = base * this.opts.jitterRatio;
    const delta = (this.random() * 2 - 1) * spread;
    return Math.max(0, Math.round(base + delta));
  }

  /** Clears the attempt count after a successful connection. */
  reset(): void {
    this.attempt = 0;
  }
}