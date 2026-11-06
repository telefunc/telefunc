export { countWaitingPublishes, droppingPublishes }
export type { WireQueue }

import { readU16 } from '../../wire/header.js'
import { TAG } from '../../wire/tags.js'

/** What a server wire holds back for its page, which a broadcast falling behind can let go of. */
interface WireQueue {
  /** How many of channel `ix`'s publishes wait here, heads and bodies both: its newest ones. */
  waitingPublishes(ix: number): number
  /** Drops channel `ix`'s oldest `count` publishes that wait here. */
  dropWaitingPublishes(ix: number, count: number): void
  /** The rate the page takes data at, in bytes per ms, 0 before one was measured. */
  drainRate(): number
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

/** Tells, in the queue's order, whether each frame goes: the first `count` heads of channel `ix`, and the body after each. */
function droppingPublishes(ix: number, count: number): (frame: Uint8Array) => boolean {
  let left = count
  let bodyNext = false
  return (frame) => {
    if (bodyNext) {
      bodyNext = false
      return true
    }
    if (left === 0 || !isHeadOf(frame, ix)) return false
    left--
    bodyNext = true
    return true
  }
}
