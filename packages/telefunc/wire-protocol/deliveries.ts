export { Deliveries }

/** The deliveries a wire keeps: enough to tell a few frames of other channels from a queue. A power of two. */
const KEPT = 32

/** What a wire delivered, numbered from 0 in the order it arrived. */
class Deliveries {
  private readonly at = new Float64Array(KEPT)
  /** The bytes delivered up to and including each. */
  private readonly through = new Float64Array(KEPT)
  private count = 0

  /** The number of the last delivery, -1 before the first. */
  get last(): number {
    return this.count - 1
  }

  note(at: number, bytes: number): void {
    const slot = this.slot(this.count)
    this.through[slot] = (this.count === 0 ? 0 : this.through[this.slot(this.count - 1)]!) + bytes
    this.at[slot] = at
    this.count++
  }

  /** The bytes delivered in the `ms` before delivery `n`, not counting it. Where that reaches past the deliveries kept,
   *  the rate over those kept times `ms`, and `Infinity` where none before `n` is kept. */
  before(n: number, ms: number): number {
    if (n === 0) return 0
    const oldest = Math.max(0, this.count - KEPT)
    if (n <= oldest) return Infinity
    const arrived = this.at[this.slot(n)]!
    const through = this.through[this.slot(n - 1)]!
    let i = n - 1
    while (i >= oldest && this.at[this.slot(i)]! >= arrived - ms) i--
    if (i >= oldest) return through - this.through[this.slot(i)]!
    if (oldest === 0) return through
    const span = arrived - this.at[this.slot(oldest)]!
    return span > 0 ? ((through - this.through[this.slot(oldest)]!) * ms) / span : Infinity
  }

  /** Where delivery `n` is kept. */
  private slot(n: number): number {
    return n & (KEPT - 1)
  }
}
