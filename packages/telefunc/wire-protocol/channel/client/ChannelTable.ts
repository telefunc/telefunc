export { ChannelTable }
export type { ChannelRecord, ClosedState }

import type { ReplayBuffer } from '../ReplayBuffer.js'
import type { MuxChannel } from './connection.js'
import { assert } from '../../../utils/assert.js'
import { seqThrough } from '../../wire/seq.js'

/** `closed`: unregistered, and kept till released, as the RECONCILE releasing it, or a new wire's leaving it out, ends
 *  it on the server. */
type ChannelState =
  | { tag: 'pending'; initial: boolean }
  | { tag: 'open' }
  | {
      tag: 'closed'
      initial: boolean
      /** The server has all the page sent on it, or ended it. */
      delivered: boolean
      /** A PING named it. */
      reported: boolean
    }

type ClosedState = Extract<ChannelState, { tag: 'closed' }>

/** A channel the connection carries, at its wire index. */
type ChannelRecord = {
  readonly ix: number
  readonly channel: MuxChannel
  state: ChannelState
  /** What the page sent on it, till the server has it. */
  readonly replay: ReplayBuffer
  /** The newest seq the server sent on it. */
  lastSeq: number
  /** An initial channel a RECONCILED left out, till its ATTACH_RESULT: what it queues waits, so its replay goes first. */
  awaited: boolean
}

/** The channels a page's connection carries, one record per wire index. */
class ChannelTable {
  private readonly records = new Map<number, ChannelRecord>()
  /** The channels not closed: `close` takes one out while `records` still lists it. */
  private readonly byChannel = new Map<MuxChannel, ChannelRecord>()
  /** Channels no RECONCILE named on the wire yet: the next one there names them. */
  private readonly unnamed = new Set<ChannelRecord>()
  /** Closed channels not let go: a RECONCILE names them, as the server's answer tells when it has all they sent. */
  private readonly closed = new Set<ChannelRecord>()
  /** Channels let go since the last RECONCILE: the next one on the same wire releases them on the server. */
  private released: number[] = []

  get size(): number {
    return this.records.size
  }

  /** Every channel registered is closed. */
  get idle(): boolean {
    return this.byChannel.size === 0
  }

  get hasUnnamed(): boolean {
    return this.unnamed.size > 0
  }

  get(ix: number): ChannelRecord | undefined {
    return this.records.get(ix)
  }

  /** `channel`'s record, while it isn't closed. */
  of(channel: MuxChannel): ChannelRecord | undefined {
    return this.byChannel.get(channel)
  }

  values(): IterableIterator<ChannelRecord> {
    return this.records.values()
  }

  /** The channels a RECONCILE on the wire that has the session names: the unnamed and the closed. */
  changed(): Set<ChannelRecord> {
    return new Set([...this.unnamed, ...this.closed])
  }

  isUnnamed(record: ChannelRecord): boolean {
    return this.unnamed.has(record)
  }

  /** A RECONCILE names `record`. */
  name(record: ChannelRecord): void {
    this.unnamed.delete(record)
  }

  /** The channels let go since the last call. */
  takeReleased(): number[] {
    const released = this.released
    this.released = []
    return released
  }

  /** The wire carries a seq's low 32 bits: the server's acknowledgement is the latest seq with them the page sent. */
  sentThrough(ix: number, lastSeq: number): number {
    return seqThrough(lastSeq, this.records.get(ix)?.replay.seq ?? 0)
  }

  /** The server has what the page sent on channel `ix` through `lastSeq`, which its replay lets go. */
  serverHasThrough(ix: number, lastSeq: number): void {
    this.records.get(ix)?.replay.acknowledge(lastSeq)
  }

  // ── Per-channel state transitions: every write of a `record.state`, or of its fields, goes through these. ──

  add(ix: number, channel: MuxChannel, replay: ReplayBuffer): ChannelRecord {
    const record: ChannelRecord = {
      ix,
      channel,
      state: { tag: 'pending', initial: true },
      replay,
      lastSeq: 0,
      awaited: false,
    }
    this.records.set(ix, record)
    this.byChannel.set(channel, record)
    this.unnamed.add(record)
    return record
  }

  open(record: ChannelRecord): void {
    assert(record.state.tag === 'pending')
    record.state = { tag: 'open' }
  }

  close(record: ChannelRecord): void {
    assert(record.state.tag !== 'closed')
    this.byChannel.delete(record.channel)
    const initial = record.state.tag === 'pending' && record.state.initial
    record.state = { tag: 'closed', initial, delivered: false, reported: false }
    this.closed.add(record)
  }

  /** A closed channel the server attached has what the page sent on it through `lastSeq`. */
  serverHas({ replay }: ChannelRecord, state: ClosedState, lastSeq: number): void {
    state.initial = false
    replay.acknowledge(lastSeq)
    if (lastSeq >= replay.seq) state.delivered = true
  }

  /** The server ended the closed channel. */
  serverEnded(state: ClosedState): void {
    state.delivered = true
  }

  /** A PING named the closed channel. */
  reported(state: ClosedState): void {
    state.reported = true
  }

  /** Drops all the channel holds, its ATTACH_RESULT wait too. */
  release(record: ChannelRecord): void {
    if (!this.unnamed.delete(record)) this.released.push(record.ix)
    this.closed.delete(record)
    this.records.delete(record.ix)
    this.byChannel.delete(record.channel)
    record.replay.dispose()
  }

  /** No channel waits for its ATTACH_RESULT. */
  stopAwaiting(): void {
    for (const record of this.records.values()) record.awaited = false
  }

  clear(): void {
    for (const { replay } of this.records.values()) replay.dispose()
    this.records.clear()
    this.byChannel.clear()
    this.unnamed.clear()
    this.closed.clear()
    this.released = []
  }
}
