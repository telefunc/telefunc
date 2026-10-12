export { Deliveries }

/** The deliveries a wire keeps: enough to tell a few frames of other channels from a queue. A power of two. */
const KEPT = 32

/** What a wire delivered, numbered from 0 in the order it arrived. */
class Deliveries {
  private readonly _at = new Float64Array(KEPT)
  /** The bytes delivered up to and including each. */
  private readonly _through = new Float64Array(KEPT)
  private _count = 0

  /** The number of the last delivery, -1 before the first. */
  get last(): number {
    return this._count - 1
  }

  note(at: number, bytes: number): void {
    const slot = this._count & (KEPT - 1)
    this._through[slot] = (this._count === 0 ? 0 : this._through[(this._count - 1) & (KEPT - 1)]!) + bytes
    this._at[slot] = at
    this._count++
  }

  /** The bytes delivered in the `ms` before delivery `n`, not counting it. Where that reaches past the deliveries kept,
   *  the rate over those kept times `ms`, and `Infinity` where none before `n` is kept. */
  before(n: number, ms: number): number {
    if (n === 0) return 0
    const oldest = Math.max(0, this._count - KEPT)
    if (n <= oldest) return Infinity
    const at = this._at[n & (KEPT - 1)]!
    const through = this._through[(n - 1) & (KEPT - 1)]!
    let i = n - 1
    while (i >= oldest && this._at[i & (KEPT - 1)]! >= at - ms) i--
    if (i >= oldest) return through - this._through[i & (KEPT - 1)]!
    if (oldest === 0) return through
    const span = at - this._at[oldest & (KEPT - 1)]!
    return span > 0 ? ((through - this._through[oldest & (KEPT - 1)]!) * ms) / span : Infinity
  }
}
