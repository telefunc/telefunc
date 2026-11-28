export { countWaitingPublishes, droppingLossy, droppingPublishes, droppingFrom }
export type { WireQueue }

import { SEQ_OFFSET, SEQ_SPAN, readTag, readU16, readU32 } from '../../wire/header.js'
import { TAG, isPublishBody, isSequencedTag } from '../../wire/tags.js'

/** What a server wire holds back for its page, which a broadcast falling behind, or a channel its page left, can let go of. */
interface WireQueue {
  /** How many of channel `ix`'s publishes wait here, counted by their heads: its newest ones. */
  waitingPublishes(ix: number): number
  /** Drops channel `ix`'s publishes at `seqs`, oldest first, that wait here. */
  dropWaitingPublishes(ix: number, seqs: readonly number[]): void
  /** Drops what of channel `ix` waits here, receipts too: from its oldest publish on, or from the start. */
  dropWaitingFrom(ix: number, from: 'publish' | 'start'): void
  /** For an aborted channel: drops channel `ix`'s lossy sends and publishes of which the socket has been given no byte,
   *  frames gathered for it included. A body whose head the socket has stays. Returns how many went. */
  dropUngiven(ix: number): number
}

function isHeadOf(frame: Uint8Array, ix: number): boolean {
  return frame[0] === TAG.PUBLISH_TO && readU16(frame, 1) === ix
}

/** A body whose head left stays, which is never counted: it goes in the order it was sent. */
function countWaitingPublishes(waiting: Iterable<Uint8Array>, ix: number): number {
  let count = 0
  for (const frame of waiting) if (isHeadOf(frame, ix)) count++
  return count
}

/** Tells, in the queue's order, whether each frame goes: channel `ix`'s sequenced frames, from its first head on or from
 *  the start, and the body after each head. */
function droppingFrom(ix: number, from: 'publish' | 'start'): (frame: Uint8Array) => boolean {
  let dropping = from === 'start'
  let bodyNext = false
  return (frame) => {
    if (bodyNext) {
      bodyNext = false
      return true
    }
    if (isHeadOf(frame, ix)) dropping = bodyNext = true
    const tag = readTag(frame)
    return dropping && isSequencedTag(tag) && !isPublishBody(tag) && readU16(frame, 1) === ix
  }
}

/** Tells, in the queue's order, whether each frame goes: channel `ix`'s heads at `seqs`, oldest first, and the body
 *  after each. A head carries its seq's low 32 bits, which no two of a queue's publishes share. */
function droppingPublishes(ix: number, seqs: readonly number[]): (frame: Uint8Array) => boolean {
  let next = 0
  let bodyNext = false
  return (frame) => {
    if (bodyNext) {
      bodyNext = false
      return true
    }
    const seq = seqs[next]
    if (seq === undefined || !isHeadOf(frame, ix) || readU32(frame, SEQ_OFFSET) !== seq % SEQ_SPAN) return false
    next++
    bodyNext = true
    return true
  }
}

/** `WireQueue.dropUngiven`'s walk: channel `ix`'s heads, and the body after each; `count` then says how many went. */
function droppingLossy(ix: number): { drops: (frame: Uint8Array) => boolean; count: () => number } {
  let count = 0
  let bodyNext = false
  const drops = (frame: Uint8Array): boolean => {
    if (bodyNext) {
      bodyNext = false
      return true
    }
    if (!isHeadOf(frame, ix)) return false
    count++
    bodyNext = true
    return true
  }
  return { drops, count: () => count }
}
