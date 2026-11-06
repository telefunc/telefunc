export { creditWindowOf }
export type { CreditOwner, Urgency }

import { CREDIT_WINDOW_MAX_BYTES } from '../constants.js'

/** How soon a credit frame goes (`urgent-flow-control` at once, an `acknowledgement` rides); also its page frame kind. */
type Urgency = 'urgent-flow-control' | 'acknowledgement' | 'flow-control'

/** What a channel's credits send through and read: the frames they decide on, and what the channel's wire holds. */
interface CreditOwner {
  // Limits go out mod 2^32; a byte limit carries the last seq this side has, which acknowledges what arrived.
  sendByteLimit(limit: number, urgency: Urgency): void
  sendMessageLimit(limit: number, urgency: Urgency): void
  sendBdpPing(probe: number): void
  /** Bytes waiting to go out on the channel's wire, `undefined` where the runtime can't tell. */
  backlogBytes(): number | undefined
  /** The bytes the channel's wire delivered in the `ms` before the frame being handled arrived. */
  deliveredBefore(ms: number): number
}

/** The largest credit window, each way, of a server with this `maxMemory`: what a small host holds is small. */
function creditWindowOf(maxMemory: number): number {
  return Math.min(CREDIT_WINDOW_MAX_BYTES, maxMemory / 2)
}
