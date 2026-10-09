export { SubscriberFence }

/** The read-never-misses owner. It captures the graph sequence beside the initial read, replays a change
 *  that landed before subscriber activation, and carries a reseed race folded into the new baseline to
 *  subscribers that were already live. */
class SubscriberFence {
  constructor(readonly seqAtRead: number) {}

  replayAtActivation(currentSeq: number, notify: () => void): void {
    if (currentSeq > this.seqAtRead) notify()
  }

  static notifyReseedRace(raced: boolean, advance: () => void, notify?: () => void): void {
    if (!raced) return
    advance()
    notify?.()
  }
}
