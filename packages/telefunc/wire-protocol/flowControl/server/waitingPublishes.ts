export { countWaitingPublishes, droppingPublishes, droppingFromPublish, overtakes }
export type { WireQueue }

import type { PathEstimate } from '../PathEstimate.js'
import type { PiecesAck } from '../../wire/frame.js'
import { SEQ_OFFSET, SEQ_SPAN, readU16, readU32 } from '../../wire/header.js'
import { TAG, isClosingTag, isSequencedTag } from '../../wire/tags.js'

/** What a server wire holds back for its page, which a broadcast falling behind can let go of. */
interface WireQueue {
  /** How many of channel `ix`'s publishes wait here, counted by their heads: its newest ones. */
  waitingPublishes(ix: number): number
  /** Drops channel `ix`'s publishes at `seqs`, oldest first, that wait here. */
  dropWaitingPublishes(ix: number, seqs: readonly number[]): void
  /** Drops what of channel `ix` waits here from its oldest publish on, receipts too. */
  dropWaitingFromPublish(ix: number): void
  /** The bytes it holds back, as the page counts them. */
  readonly unhandedBytes: number
  /** The rate the page takes data at, in bytes per ms, 0 before one was measured. */
  drainRate(): number
  /** What the path to the page delivers, from the page's reports of what it took. */
  readonly path: PathEstimate
  /** Takes the page's report of what it took; false if it reports more than was sent. */
  acknowledged(ack: PiecesAck): boolean
}

function isHeadOf(frame: Uint8Array, ix: number): boolean {
  return frame[0] === TAG.PUBLISH_TO && readU16(frame, 1) === ix
}

function isPublishBody(tag: number): boolean {
  return tag === TAG.PUBLISH || tag === TAG.PUBLISH_BINARY
}

/** Whether data `frame` goes ahead of `ahead`, the frames its wire holds back: a channel orders only its own frames, so a
 *  slow page's publishes don't hold up its other channels. Publishes keep their line, being what falls behind. What ends a
 *  channel passes that channel's publishes too: a channel that ends reads no more of them. */
function overtakes(frame: Uint8Array, ahead: Iterable<Uint8Array>): boolean {
  const tag = frame[0]!
  if (!isSequencedTag(tag) || tag === TAG.PUBLISH_TO || isPublishBody(tag)) return false
  const ix = readU16(frame, 1)
  for (const other of ahead) {
    const otherTag = other[0]!
    // A body's index names no channel; its head does.
    if (!isSequencedTag(otherTag) || isPublishBody(otherTag) || readU16(other, 1) !== ix) continue
    if (!isClosingTag(tag) || otherTag !== TAG.PUBLISH_TO) return false
  }
  return true
}

/** A body whose head left stays, which is never counted: it goes in the order it was sent. */
function countWaitingPublishes(waiting: Iterable<Uint8Array>, ix: number): number {
  let count = 0
  for (const frame of waiting) if (isHeadOf(frame, ix)) count++
  return count
}

/** Tells, in the queue's order, whether each frame goes: channel `ix`'s sequenced frames from its first head on, and the
 *  body after each head. */
function droppingFromPublish(ix: number): (frame: Uint8Array) => boolean {
  let dropping = false
  let bodyNext = false
  return (frame) => {
    if (bodyNext) {
      bodyNext = false
      return true
    }
    if (isHeadOf(frame, ix)) dropping = bodyNext = true
    return dropping && isSequencedTag(frame[0]!) && !isPublishBody(frame[0]!) && readU16(frame, 1) === ix
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
    if (next === seqs.length || !isHeadOf(frame, ix) || readU32(frame, SEQ_OFFSET) !== seqs[next]! % SEQ_SPAN) {
      return false
    }
    next++
    bodyNext = true
    return true
  }
}
