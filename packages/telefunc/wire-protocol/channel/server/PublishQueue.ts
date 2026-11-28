export { PublishQueue }

import { assert } from '../../../utils/assert.js'
import type { PublishBody } from './channelMemory.js'

/** A replay's publish records, kept in seq order and dropped from either end. */
class PublishQueue {
  private readonly bodies: (PublishBody | undefined)[] = []
  private readonly seqs: number[] = []
  private readonly indexes: number[] = []
  private readonly sentAt: number[] = []
  private readonly confirmed: number[] = []
  private readonly alone: boolean[] = []
  private head = 0

  get length(): number {
    return this.bodies.length - this.head
  }

  seqAt(i: number): number {
    const seq = this.seqs[this.head + i]
    assert(seq !== undefined)
    return seq
  }

  bodyAt(i: number): PublishBody {
    const body = this.bodies[this.head + i]
    assert(body !== undefined)
    return body
  }

  indexAt(i: number): number {
    const index = this.indexes[this.head + i]
    assert(index !== undefined)
    return index
  }

  timeAt(i: number): number {
    const time = this.sentAt[this.head + i]
    assert(time !== undefined)
    return time
  }

  confirmedBefore(i: number): number {
    const confirmed = this.confirmed[this.head + i]
    assert(confirmed !== undefined)
    return confirmed
  }

  wasAlone(i: number): boolean {
    return this.alone[this.head + i] === true
  }

  indexAfter(seq: number): number {
    const seqs = this.seqs
    let i = this.head
    let issued = seqs[i]
    while (issued !== undefined && issued <= seq) issued = seqs[++i]
    return i - this.head
  }

  push(seq: number, index: number, body: PublishBody, at: number, alone: boolean, confirmedBefore: number): void {
    this.bodies.push(body)
    this.seqs.push(seq)
    this.indexes.push(index)
    this.sentAt.push(at)
    this.alone.push(alone)
    this.confirmed.push(confirmedBefore)
  }

  dropOldest(count: number): void {
    if (count === 0) return
    const head = this.head + count
    if (head >= this.bodies.length) return this.clear()
    if (head >= this.bodies.length - head) {
      discardPrefix(this.bodies, head)
      discardPrefix(this.seqs, head)
      discardPrefix(this.indexes, head)
      discardPrefix(this.sentAt, head)
      discardPrefix(this.alone, head)
      discardPrefix(this.confirmed, head)
      this.head = 0
      return
    }
    for (let i = this.head; i < head; i++) this.bodies[i] = undefined
    this.head = head
  }

  pop(): PublishBody {
    const body = this.bodies.pop()
    assert(body !== undefined)
    if (this.head >= this.bodies.length) this.clear()
    else {
      this.seqs.pop()
      this.indexes.pop()
      this.sentAt.pop()
      this.alone.pop()
      this.confirmed.pop()
    }
    return body
  }

  private clear(): void {
    this.bodies.length = 0
    this.seqs.length = 0
    this.indexes.length = 0
    this.sentAt.length = 0
    this.alone.length = 0
    this.confirmed.length = 0
    this.head = 0
  }
}

/** Moves the live slots forward before releasing the discarded tail. */
function discardPrefix<T>(items: (T | undefined)[], count: number): void {
  const remaining = items.length - count
  for (let i = 0; i < remaining; i++) items[i] = items[count + i]
  items.length = remaining
}
