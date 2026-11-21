export { EndedSessions }

import type { SessionCause } from '../endCause.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'

/** The sessions the server ended, each with its cause, for as long as their page may come back: a page that missed its
 *  wire's last word reopens naming its session, and is answered with the same end. */
class EndedSessions {
  private readonly ended = new Map<string, { cause: SessionCause; timer: ReturnType<typeof setTimeout> }>()

  /** Keeps `sessionId`'s end for `waitMs`. */
  keep(sessionId: string, cause: SessionCause, waitMs: number): void {
    const kept = this.ended.get(sessionId)
    if (kept) clearTimeout(kept.timer)
    const timer = unrefTimer(setTimeout(() => this.ended.delete(sessionId), waitMs))
    this.ended.set(sessionId, { cause, timer })
  }

  /** The cause `sessionId` ended with, while it is kept. */
  causeOf(sessionId: string): SessionCause | undefined {
    return this.ended.get(sessionId)?.cause
  }
}
