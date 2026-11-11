export { Refetcher, REFETCH_WINDOW }

/** The least time between the starts of two refetches of one value, in ms, when it keeps going stale. */
const REFETCH_WINDOW = 100

/**
 * When a client fetches a value again after the server said it went stale. A value going stale once (a single write)
 * is fetched at once. Under load, while a refetch runs or less than `window` ms after the last one started, further
 * stale signals merge into one refetch, which starts once both are over: a value changing a thousand times a second
 * is fetched about ten times a second, and the last change is always fetched.
 */
class Refetcher {
  private readonly refetch: () => Promise<unknown>
  private readonly window: number
  private running = false
  private waiting: ReturnType<typeof setTimeout> | undefined
  private pending = false
  private lastStart = Number.NEGATIVE_INFINITY
  private stopped = false

  constructor(refetch: () => Promise<unknown>, window = REFETCH_WINDOW) {
    this.refetch = refetch
    this.window = window
  }

  /** The value went stale. */
  stale(): void {
    if (this.stopped) return
    if (this.running || this.waiting) {
      this.pending = true
      return
    }
    const wait = this.lastStart + this.window - performance.now()
    if (wait <= 0) return this.start()
    this.waiting = setTimeout(() => {
      this.waiting = undefined
      this.start()
    }, wait)
  }

  stop(): void {
    this.stopped = true
    clearTimeout(this.waiting)
  }

  private start(): void {
    this.pending = false
    this.running = true
    this.lastStart = performance.now()
    // A failure stays unhandled: `refetch` reports its own errors, and a usage error must show.
    void this.refetch().finally(() => {
      this.running = false
      if (this.pending) this.stale()
    })
  }
}
