export { ReplayBuffer }
export type { Stall }

import type { OutboundFrame, OutboundFrameKind } from './ClientConnection.js'
import { replayKindOf, type ReplayKind } from '../../wire/tags.js'
import { assert } from '../../../utils/assert.js'
import { SeqQueue } from '../SeqQueue.js'

/** The frames every channel of the page handed, counted in the order it handed them. */
let handed = 0

/** Sent sequenced frames, each with the kind it was sent with, till their peer acks them, in queues by tag; a data
 *  queue evicts its oldest past its budget. */
class ReplayBuffer {
  /** Each kind's queue, made at its first frame. */
  private readonly queues: Partial<Record<ReplayKind, ReplayQueue>> = {}
  private readonly budgets: Record<ReplayKind, number>
  private issuedSeq = 0
  /** The highest seq pushed. */
  private pushedSeq = 0
  /** The highest seq the peer acknowledged. */
  private acknowledgedSeq = 0
  private stallDate: Stall | null = null

  /** Since when what was handed out past what the peer acknowledged has waited for it: dated at a loss, and only the
   *  peer acknowledging more clears it. */
  get stall(): Stall | null {
    return this.stallDate
  }

  /** The highest seq the peer acknowledged. */
  get acknowledged(): number {
    return this.acknowledgedSeq
  }

  /** A try was lost at `at`: what the peer hasn't acknowledged is dated from then, unless it is already. */
  dateStall(at: number): void {
    if (this.stallDate === null && this.acknowledgedSeq < this.issuedSeq) this.stallDate = { since: at }
  }

  /** The last seq handed out (see `nextSeq`). */
  get seq(): number {
    return this.issuedSeq
  }

  constructor(maxBytes: number) {
    // The frame that ends a channel is small, and none is dropped for size.
    this.budgets = { text: maxBytes, binary: maxBytes, closing: Infinity }
  }

  /** Hands out the next seq. */
  nextSeq(): number {
    return ++this.issuedSeq
  }

  /** Stores a frame and its kind, its seq no less than the last pushed, in its queue; none larger than the queue's
   *  budget. */
  push(seq: number, frame: Uint8Array<ArrayBuffer>, kind: OutboundFrameKind): void {
    assert(seq <= this.issuedSeq)
    assert(seq >= this.pushedSeq)
    this.pushedSeq = seq
    const replayKind = replayKindOf(frame[0]!)
    const queue = (this.queues[replayKind] ??= new ReplayQueue(this.budgets[replayKind]))
    queue.push(seq, { kind, frame }, ++handed)
  }

  /** The peer has every frame through `lastSeq`, which a reconnect never asks for again: they go. */
  acknowledge(lastSeq: number): void {
    // Nothing new: its queues dropped through it.
    if (lastSeq <= this.acknowledgedSeq) return
    this.acknowledgedSeq = lastSeq
    this.stallDate = null
    for (const kind of REPLAY_KINDS) this.queues[kind]?.acknowledge(lastSeq)
  }

  /** The frames with afterSeq < seq <= throughSeq, by seq, each with when the page handed it among all its channels'
   *  frames, or null if it dropped one of them. */
  getAfter(afterSeq: number, throughSeq: number): { frames: OutboundFrame[]; handed: number[] } | null {
    let run: Run = { seqs: [], frames: [], handed: [] }
    for (const kind of REPLAY_KINDS) {
      const queue = this.queues[kind]
      if (queue) run = mergeBySeq(run, queue.getAfter(afterSeq, throughSeq))
    }
    // Every seq through the highest pushed was pushed, each to one queue, so one missing was dropped.
    return run.frames.length >= Math.min(throughSeq, this.pushedSeq) - afterSeq ? run : null
  }

  get length(): number {
    let length = 0
    for (const kind of REPLAY_KINDS) length += this.queues[kind]?.length ?? 0
    return length
  }

  /** Lets go of every frame it holds; its seqs stay. */
  dispose(): void {
    for (const kind of REPLAY_KINDS) this.queues[kind]?.dispose()
    this.stallDate = null
  }
}

/** When a channel's unacknowledged data was dated: a frame the page read from the server after it shows a server that
 *  could have acknowledged it. */
type Stall = { since: number }

const REPLAY_KINDS: readonly ReplayKind[] = ['text', 'binary', 'closing']

type Run = { seqs: number[]; frames: OutboundFrame[]; handed: number[] }

/** Merges two sorted runs by their stored seq values. */
function mergeBySeq(a: Run, b: Run): Run {
  if (a.frames.length === 0) return b
  if (b.frames.length === 0) return a
  const length = a.frames.length + b.frames.length
  const seqs: number[] = new Array(length)
  const frames: OutboundFrame[] = new Array(length)
  const handed: number[] = new Array(length)
  let ai = 0
  let bi = 0
  for (let ri = 0; ri < length; ri++) {
    if (bi >= b.frames.length || (ai < a.frames.length && a.seqs[ai]! <= b.seqs[bi]!)) {
      seqs[ri] = a.seqs[ai]!
      handed[ri] = a.handed[ai]!
      frames[ri] = a.frames[ai++]!
    } else {
      seqs[ri] = b.seqs[bi]!
      handed[ri] = b.handed[bi]!
      frames[ri] = b.frames[bi++]!
    }
  }
  return { seqs, frames, handed }
}

/** One kind's frames, oldest first, within a byte budget; `ReplayBuffer` keeps one per replay kind, made at its first
 *  frame. */
class ReplayQueue {
  private readonly frames = new SeqQueue<OutboundFrame>()
  /** When the page handed each frame, among all its channels' frames. */
  private readonly handed = new SeqQueue<number>()
  private totalBytes = 0
  private readonly maxBytes: number

  constructor(maxBytes: number) {
    this.maxBytes = maxBytes
  }

  get length(): number {
    return this.frames.length
  }

  /** Stores an encoded frame and evicts the oldest while over the budget; none larger than the budget. */
  push(seq: number, entry: OutboundFrame, handed: number): void {
    const bytes = entry.frame.byteLength
    if (bytes > this.maxBytes) return
    this.frames.push(seq, entry)
    this.handed.push(seq, handed)
    this.totalBytes += bytes
    let evicted = 0
    while (this.totalBytes > this.maxBytes) this.totalBytes -= this.frames.at(evicted++).frame.byteLength
    this.frames.dropOldest(evicted)
    this.handed.dropOldest(evicted)
  }

  acknowledge(lastSeq: number): void {
    const acknowledged = this.frames.indexAfter(lastSeq)
    for (let i = 0; i < acknowledged; i++) this.totalBytes -= this.frames.at(i).frame.byteLength
    this.frames.dropOldest(acknowledged)
    this.handed.dropOldest(acknowledged)
  }

  /** The stored frames with afterSeq < seq <= throughSeq. */
  getAfter(afterSeq: number, throughSeq: number): Run {
    const run: Run = { seqs: [], frames: [], handed: [] }
    for (let i = this.frames.indexAfter(afterSeq); i < this.frames.length && this.frames.seqAt(i) <= throughSeq; i++) {
      run.seqs.push(this.frames.seqAt(i))
      run.frames.push(this.frames.at(i))
      run.handed.push(this.handed.at(i))
    }
    return run
  }

  dispose(): void {
    this.frames.clear()
    this.handed.clear()
    this.totalBytes = 0
  }
}
