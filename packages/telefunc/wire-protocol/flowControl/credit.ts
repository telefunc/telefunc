export { replayWindow }
export type { CreditOwner, WindowKind }

import { MESSAGE_FRAME_OVERHEAD_MAX_BYTES } from '../wire/frame.js'

/** A batching wire sends `urgent-flow-control` at once; an `acknowledgement` may ride the next message it sends. */
type WindowKind = 'urgent-flow-control' | 'acknowledgement' | 'flow-control'

/** The channel a credit belongs to: it sends the frames its credit decides on, and says what its wire holds. */
interface CreditOwner {
  // Limits go out mod 2^32; a byte limit carries the last seq this side has, which acknowledges what arrived.
  _sendByteLimit(limit: number, kind: WindowKind): void
  _sendMessageLimit(limit: number, kind: WindowKind): void
  _sendBdpPing(probe: number): void
  /** Bytes waiting to go out on the channel's wire, `undefined` where the runtime can't tell. */
  _backlogBytes(): number | undefined
}

/** The largest window a sender whose replay queues hold `text` and `binary` bytes gets: what it holds in flight fits. */
function replayWindow(text: number, binary: number): number {
  return Math.max(1, Math.floor(Math.min(text, binary) / 2) - MESSAGE_FRAME_OVERHEAD_MAX_BYTES)
}
