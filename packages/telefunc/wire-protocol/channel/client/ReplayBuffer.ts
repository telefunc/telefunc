export { ReplayBuffer }

import { replayKindOf, type ReplayKind } from '../../wire/tags.js'
import { assert } from '../../../utils/assert.js'
import { SeqQueue } from '../SeqQueue.js'

/** Sent sequenced frames till their peer acks them, in queues by tag; a data queue evicts its oldest past its budget. */
class ReplayBuffer {
  /** Each kind's queue, made at its first frame. */
  private readonly queues: Partial<Record<ReplayKind, ReplayQueue>> = {}
  private readonly budgets: Record<ReplayKind, number>
  private issuedSeq = 0
  /** The highest seq pushed. */
  private pushedSeq = 0
  /** The highest seq the peer acknowledged. */
  private acknowledgedSeq = 0

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

  /** Stores a frame, its seq no less than the last pushed, in its queue; none larger than the queue's budget. */
  push(seq: number, frame: Uint8Array<ArrayBuffer>): void {
    assert(seq <= this.issuedSeq)
    assert(seq >= this.pushedSeq)
    this.pushedSeq = seq
    const kind = replayKindOf(frame[0]!)
    const queue = (this.queues[kind] ??= new ReplayQueue(this.budgets[kind]))
    queue.push(seq, frame)
  }

  /** The peer has every frame through `lastSeq`, which a reconnect never asks for again: they go. */
  acknowledge(lastSeq: number): void {
    // Nothing new: its queues dropped through it.
    if (lastSeq <= this.acknowledgedSeq) return
    this.acknowledgedSeq = lastSeq
    for (const kind of REPLAY_KINDS) this.queues[kind]?.acknowledge(lastSeq)
  }

  /** The frames with afterSeq < seq <= throughSeq, by seq, or null if it dropped one of them. */
  getAfter(afterSeq: number, throughSeq: number): Uint8Array<ArrayBuffer>[] | null {
    let run: Run = { seqs: [], frames: [] }
    for (const kind of REPLAY_KINDS) {
      const queue = this.queues[kind]
      if (queue) run = mergeBySeq(run, queue.getAfter(afterSeq, throughSeq))
    }
    // Every seq through the highest pushed was pushed, each to one queue, so one missing was dropped.
    return run.frames.length >= Math.min(throughSeq, this.pushedSeq) - afterSeq ? run.frames : null
  }

  get length(): number {
    let length = 0
    for (const kind of REPLAY_KINDS) length += this.queues[kind]?.length ?? 0
    return length
  }

  dispose(): void {
    for (const kind of REPLAY_KINDS) this.queues[kind]?.dispose()
    this.issuedSeq = 0
    this.pushedSeq = 0
    this.acknowledgedSeq = 0
  }
}

const REPLAY_KINDS: readonly ReplayKind[] = ['text', 'binary', 'closing']

type Run = { seqs: number[]; frames: Uint8Array<ArrayBuffer>[] }

/** Merges two sorted runs by their stored seq values. */
function mergeBySeq(a: Run, b: Run): Run {
  if (a.frames.length === 0) return b
  if (b.frames.length === 0) return a
  const length = a.frames.length + b.frames.length
  const seqs: number[] = new Array(length)
  const frames: Uint8Array<ArrayBuffer>[] = new Array(length)
  let ai = 0
  let bi = 0
  for (let ri = 0; ri < length; ri++) {
    if (bi >= b.frames.length || (ai < a.frames.length && a.seqs[ai]! <= b.seqs[bi]!)) {
      seqs[ri] = a.seqs[ai]!
      frames[ri] = a.frames[ai++]!
    } else {
      seqs[ri] = b.seqs[bi]!
      frames[ri] = b.frames[bi++]!
    }
  }
  return { seqs, frames }
}

/** One kind's frames, oldest first, within a byte budget; `ReplayBuffer` keeps one per replay kind, made at its first
 *  frame. */
class ReplayQueue {
  private readonly frames = new SeqQueue<Uint8Array<ArrayBuffer>>()
  private totalBytes = 0
  private readonly maxBytes: number

  constructor(maxBytes: number) {
    this.maxBytes = maxBytes
  }

  get length(): number {
    return this.frames.length
  }

  /** Stores an encoded frame and evicts the oldest while over the budget; none larger than the budget. */
  push(seq: number, frame: Uint8Array<ArrayBuffer>): void {
    const bytes = frame.byteLength
    if (bytes > this.maxBytes) return
    this.frames.push(seq, frame)
    this.totalBytes += bytes
    let evicted = 0
    while (this.totalBytes > this.maxBytes) this.totalBytes -= this.frames.at(evicted++).byteLength
    this.frames.dropOldest(evicted)
  }

  acknowledge(lastSeq: number): void {
    const acknowledged = this.frames.indexAfter(lastSeq)
    for (let i = 0; i < acknowledged; i++) this.totalBytes -= this.frames.at(i).byteLength
    this.frames.dropOldest(acknowledged)
  }

  /** The stored frames with afterSeq < seq <= throughSeq. */
  getAfter(afterSeq: number, throughSeq: number): Run {
    const run: Run = { seqs: [], frames: [] }
    for (let i = this.frames.indexAfter(afterSeq); i < this.frames.length && this.frames.seqAt(i) <= throughSeq; i++) {
      run.seqs.push(this.frames.seqAt(i))
      run.frames.push(this.frames.at(i))
    }
    return run
  }

  dispose(): void {
    this.frames.clear()
    this.totalBytes = 0
  }
}
