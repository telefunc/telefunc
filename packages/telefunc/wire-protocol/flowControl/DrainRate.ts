export { DrainRate, DRAIN_SPAN_MS }

/** The least span a rate is measured over: a frame arrives whole, so a shorter span can count one that took longer. */
const DRAIN_SPAN_MS = 100
/** A wire drains at the fastest of this many of the newest such rates. */
const DRAIN_RATES = 8

/** A rate a wire drains at, in bytes per ms: the fastest of the newest `DRAIN_RATES` measured. */
class DrainRate {
  private fastest = 0
  private readonly rates: number[] = []
  /** The bytes and ms of a span not yet measured. */
  private spanBytes = 0
  private spanMs = 0

  /** 0 before any rate. */
  get rate(): number {
    return this.fastest
  }

  /** The newest rate measured, 0 before any. */
  get newest(): number {
    return this.rates.at(-1) ?? 0
  }

  /** Takes a rate measured over at least `DRAIN_SPAN_MS`. */
  measured(rate: number): void {
    this.rates.push(rate)
    if (this.rates.length > DRAIN_RATES) this.rates.shift()
    let fastest = rate
    for (const each of this.rates) if (each > fastest) fastest = each
    this.fastest = fastest
  }

  /** Takes `bytes` drained over `ms`, and measures a rate once the span reaches `DRAIN_SPAN_MS`. */
  spanned(bytes: number, ms: number): void {
    this.spanBytes += bytes
    this.spanMs += ms
    if (this.spanMs < DRAIN_SPAN_MS) return
    this.measured(this.spanBytes / this.spanMs)
    this.dropSpan()
  }

  /** Drops the span not yet measured. */
  dropSpan(): void {
    this.spanBytes = this.spanMs = 0
  }
}
