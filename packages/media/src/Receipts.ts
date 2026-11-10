export { Receipts }

/** A track's publishes still waiting for the server's receipt. Their age says whether the page's uplink holds a
 *  queue: the quickest receipt is one that waited behind none. */
class Receipts {
  /** When each unanswered publish left, oldest first; one answered out of order leaves its place empty till then. */
  private readonly pending: (number | null)[] = []
  private head = 0
  private quickest = Number.POSITIVE_INFINITY

  add(receipt: Promise<unknown>): void {
    const sentAt = performance.now()
    const slot = this.head + this.pending.length
    this.pending.push(sentAt)
    const settle = () => this.settle(slot, sentAt)
    receipt.then(settle, settle)
  }

  /** Whether the oldest unanswered publish waited longer than a round trip and `slackMs`: a newer one would wait too. */
  behind(slackMs: number): boolean {
    const oldest = this.pending[0]
    return oldest != null && performance.now() - oldest > this.quickest + slackMs
  }

  private settle(slot: number, sentAt: number): void {
    this.quickest = Math.min(this.quickest, performance.now() - sentAt)
    const at = slot - this.head
    if (at < 0 || at >= this.pending.length) return
    this.pending[at] = null
    while (this.pending.length > 0 && this.pending[0] === null) {
      this.pending.shift()
      this.head++
    }
  }
}
