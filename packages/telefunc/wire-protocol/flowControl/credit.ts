export { replayWindow }
export type { CreditOwner, Urgency }

import { MESSAGE_FRAME_OVERHEAD_MAX_BYTES } from '../wire/frame.js'

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

/** The largest window a sender whose replay queues hold `text` and `binary` bytes gets: what it holds in flight fits. */
function replayWindow(text: number, binary: number): number {
  return Math.max(1, Math.floor(Math.min(text, binary) / 2) - MESSAGE_FRAME_OVERHEAD_MAX_BYTES)
}
