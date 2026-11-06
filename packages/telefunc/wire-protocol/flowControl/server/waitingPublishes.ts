export { countWaitingPublishes, droppingPublishes, overtakes }
export type { WireQueue }

import { readU16 } from '../../wire/header.js'
import { TAG, isSequencedTag } from '../../wire/tags.js'

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

function isPublishBody(tag: number): boolean {
  return tag === TAG.PUBLISH || tag === TAG.PUBLISH_BINARY
}

/** Whether data `frame` goes ahead of `ahead`, the frames its wire holds back: a channel orders only its own frames, so a
 *  slow page's publishes don't hold up its other channels. Publishes keep their line, being what falls behind. A channel's
 *  closing answer passes that channel's publishes: a subscriber that closed reads no more of them. */
function overtakes(frame: Uint8Array, ahead: Iterable<Uint8Array>): boolean {
  const tag = frame[0]!
  if (!isSequencedTag(tag) || tag === TAG.PUBLISH_TO || isPublishBody(tag)) return false
  const ix = readU16(frame, 1)
  for (const other of ahead) {
    const otherTag = other[0]!
    // A body's index names no channel; its head does.
    if (!isSequencedTag(otherTag) || isPublishBody(otherTag) || readU16(other, 1) !== ix) continue
    if (tag !== TAG.CLOSE_ACK || otherTag !== TAG.PUBLISH_TO) return false
  }
  return true
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
