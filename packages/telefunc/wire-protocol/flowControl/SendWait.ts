export { Waits }
export type { SendWait }

/** How long a wait stays in the history a download's window reads (see `SendWait`). */
const WAITS_KEPT_MS = 60_000

/** How long the frames this side sends at once wait behind its own data on its wire, where it can tell. */
type SendWait = {
  /** What a frame sent now waits. */
  waitNow(): number
  /** The longest such a frame waited since `at`, on `performance.now()`'s clock, or one sent now waits. */
  longestWaitSince(at: number): number
}

/** Waits by when they were seen, for `WAITS_KEPT_MS`; each drops the earlier ones no longer than it. */
class Waits {
  private entries: { at: number; wait: number }[] = []
  private head = 0

  note(wait: number, at: number): void {
    while (this.entries.length > this.head && this.entries[this.entries.length - 1]!.wait <= wait) this.entries.pop()
    this.entries.push({ at, wait })
    while (this.entries[this.head]!.at < at - WAITS_KEPT_MS) this.head++
    if (this.head > 16 && this.head >= this.entries.length >>> 1) {
      this.entries = this.entries.slice(this.head)
      this.head = 0
    }
  }

  /** The longest wait seen at or after `at`, 0 for none. */
  since(at: number): number {
    let lo = this.head
    let hi = this.entries.length
    while (lo < hi) {
      const mid = (lo + hi) >>> 1
      if (this.entries[mid]!.at < at) lo = mid + 1
      else hi = mid
    }
    return lo < this.entries.length ? this.entries[lo]!.wait : 0
  }
}
