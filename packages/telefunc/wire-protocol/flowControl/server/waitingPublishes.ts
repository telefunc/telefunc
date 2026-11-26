export { countWaitingPublishes, droppingLossy, droppingPublishes, droppingFrom }
export type { WireQueue }

import { readTag, readU16 } from '../../wire/header.js'
import { TAG, isPublishBody, isSequencedTag } from '../../wire/tags.js'

/** What a server wire holds back for its page, which a broadcast falling behind, or a channel its page left, can let go of. */
interface WireQueue {
  /** How many of channel `ix`'s publishes wait here, counted by their heads: its newest ones. */
  waitingPublishes(ix: number): number
  /** Drops channel `ix`'s publishes that wait here, but its oldest `keep`; with `upTo`, only those among its oldest
   *  `upTo`. */
  dropWaitingPublishes(ix: number, keep: number, upTo?: number): void
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

/** Tells, in the queue's order, whether each frame goes: channel `ix`'s heads past its first `keep` and among its first
 *  `upTo`, and the body after each. */
function droppingPublishes(ix: number, keep: number, upTo = Number.POSITIVE_INFINITY): (frame: Uint8Array) => boolean {
  let heads = 0
  let bodyNext = false
  return (frame) => {
    if (bodyNext) {
      bodyNext = false
      return true
    }
    if (!isHeadOf(frame, ix)) return false
    const goes = heads >= keep && heads < upTo
    heads++
    bodyNext = goes
    return goes
  }
}

/** `WireQueue.dropUngiven`'s walk, as `droppingPublishes` with none kept; `count` then says how many went. */
function droppingLossy(ix: number): { drops: (frame: Uint8Array) => boolean; count: () => number } {
  const dropping = droppingPublishes(ix, 0)
  let count = 0
  const drops = (frame: Uint8Array): boolean => {
    if (isHeadOf(frame, ix)) count++
    return dropping(frame)
  }
  return { drops, count: () => count }
}
