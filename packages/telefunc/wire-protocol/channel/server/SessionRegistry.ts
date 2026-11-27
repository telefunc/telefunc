export { SessionRegistry }
export type { ChannelHandle, Session }

import type { ServerChannel } from './ServerChannel.js'
import type { IndexedPeer } from './IndexedPeer.js'
import type { WireEntry } from './ChannelMux.js'
import type { SessionCause } from '../endCause.js'
import { DeadlineTimer } from '../DeadlineTimer.js'
import { catchUpDroppedTimer } from '../../../workarounds/workerdDroppedTimers.js'

/** A channel as one session holds it: its index there and the peer it sends through, none once its wire went. */
type ChannelHandle = { channel: ServerChannel; ix: number; peer: IndexedPeer | null }

/** A page's session, under one id for its life: its channels by index, and the one wire that holds it, which alone
 *  speaks for its channels. */
type Session = {
  readonly id: string
  readonly channels: Map<number, ChannelHandle>
  holder: WireEntry | null
  /** The page's number for the newest of its wires that took the session: an older attempt's RECONCILE or PREPARE,
   *  read later, takes nothing. */
  high: number
}

/** What the server keeps of a session that left the registry, under its id, until `leavesAt`: why the server ended it,
 *  or `retired` where it emptied with no wire holding it or its page left; and the highest attempt of its page's that
 *  took it or that its end answered, which an attempt read later must be above to take anything, and which only rises.
 *  A reopen answered from it raises that attempt in place and nothing more: no record is written again, and its instant
 *  never moves. */
type KeptEnd = {
  readonly end: SessionCause | 'retired'
  high: number
  readonly leavesAt: number
}

type SessionRegistryOptions = {
  /** The instant, on the `performance` clock, a session that retires now leaves its record at. */
  retiredLeavesAt: () => number
}

/** Each session by id, and the sessions each channel is in: a frame finds its channel by session and index, and an
 *  ending channel leaves every session in O(its sessions). Every change writes both maps. A session is stored while a
 *  wire holds it or it has a channel. One that leaves is kept under its id as a record, written once, until an instant
 *  fixed as it is written: a page that missed its wire's last word reopens naming its session, and is answered with
 *  the same end; an older attempt of the page's, read after a newer one, takes nothing. Records are written with one
 *  window, so the map's insertion order is their instants' order, and one timer frees them; where a host dropped that
 *  timer, the next record written does. */
class SessionRegistry {
  private readonly bySession = new Map<string, Session>()
  private readonly byChannel = new Map<string, Map<string, number>>()
  private readonly kept = new Map<string, KeptEnd>()
  private readonly sweep = new DeadlineTimer({ unref: true })
  /** The instant the sweep waits for, while it waits. */
  private sweepAt: number | null = null
  private readonly retiredLeavesAt: () => number

  constructor({ retiredLeavesAt }: SessionRegistryOptions) {
    this.retiredLeavesAt = retiredLeavesAt
  }

  /** Keeps `end` under `sessionId` until `leavesAt`, where no record is kept under it; one kept already stands. */
  keepEnd(sessionId: string, end: KeptEnd['end'], high: number, leavesAt: number): void {
    if (this.keptOf(sessionId) !== undefined) return
    catchUpDroppedTimer(this.sweepAt, this.drain)
    // One past its instant that the sweep hasn't freed yet goes, so that the map stays in the order of the instants.
    this.kept.delete(sessionId)
    this.kept.set(sessionId, { end, high, leavesAt })
    if (this.kept.size === 1) this.waitFor(leavesAt)
  }

  /** The record kept under `sessionId`, before its instant; from the instant on, none. */
  keptOf(sessionId: string): KeptEnd | undefined {
    const kept = this.kept.get(sessionId)
    return kept !== undefined && performance.now() < kept.leavesAt ? kept : undefined
  }

  /** A newer attempt of the page's takes the retired session `sessionId` again: its record goes. */
  revive(sessionId: string): void {
    this.kept.delete(sessionId)
  }

  /** Frees the records whose instant passed, oldest first, and waits for the first one that is not yet due. */
  private readonly drain = (): void => {
    this.sweepAt = null
    const now = performance.now()
    for (const [sessionId, { leavesAt }] of this.kept) {
      if (leavesAt > now) return this.waitFor(leavesAt)
      this.kept.delete(sessionId)
    }
  }

  private waitFor(at: number): void {
    this.sweepAt = at
    this.sweep.arm(at, this.drain)
  }

  get(sessionId: string): Session | undefined {
    return this.bySession.get(sessionId)
  }

  /** A new session under `sessionId`, which no wire holds yet. */
  create(sessionId: string): Session {
    const session: Session = { id: sessionId, channels: new Map(), holder: null, high: 0 }
    this.bySession.set(sessionId, session)
    return session
  }

  /** The index the session holds channel `channelId` at. */
  indexOf(sessionId: string, channelId: string): number | undefined {
    return this.byChannel.get(channelId)?.get(sessionId)
  }

  /** `entry`, the page's wire numbered `attempt`, holds the session from now on; another wire that held it holds
   *  nothing, and is returned. */
  handOver(session: Session, entry: WireEntry, attempt: number): WireEntry | null {
    const previous = session.holder === entry ? null : session.holder
    if (previous !== null) previous.holds = null
    session.holder = entry
    session.high = Math.max(session.high, attempt)
    entry.holds = session
    return previous
  }

  /** `entry` holds nothing from now on; a session that no wire holds and that has no channel goes. */
  release(entry: WireEntry): void {
    const session = entry.holds
    if (session === null) return
    entry.holds = null
    session.holder = null
    this.dropIfEmpty(session)
  }

  /** Replaces the session's channels with `handles`, and returns those it had. */
  setChannels(session: Session, handles: Iterable<ChannelHandle>): Map<number, ChannelHandle> {
    const previous = new Map(session.channels)
    for (const handle of previous.values()) this.unbind(session.id, handle)
    session.channels.clear()
    for (const h of handles) this.add(session, h)
    this.dropIfEmpty(session)
    return previous
  }

  add(session: Session, h: ChannelHandle): void {
    session.channels.set(h.ix, h)
    if (this.bySession.get(session.id) !== session) this.bySession.set(session.id, session)
    let bindings = this.byChannel.get(h.channel.id)
    if (!bindings) {
      bindings = new Map()
      this.byChannel.set(h.channel.id, bindings)
    }
    bindings.set(session.id, h.ix)
  }

  /** Takes the channel at `ix` out of the session and returns it; an emptied session goes, as in `removeChannel`. */
  remove(session: Session, ix: number): ChannelHandle | undefined {
    const handle = session.channels.get(ix)
    if (handle === undefined) return undefined
    session.channels.delete(ix)
    this.unbind(session.id, handle)
    this.dropIfEmpty(session)
    return handle
  }

  /** Takes the session out, its holder releasing it, and returns its channels, for the caller to detach. */
  removeSession(session: Session): Map<number, ChannelHandle> {
    if (this.bySession.get(session.id) === session) this.leave(session)
    if (session.holder !== null) session.holder.holds = null
    session.holder = null
    const channels = new Map(session.channels)
    for (const handle of channels.values()) this.unbind(session.id, handle)
    session.channels.clear()
    return channels
  }

  removeChannel(channelId: string): void {
    const bindings = this.byChannel.get(channelId)
    if (!bindings) return
    this.byChannel.delete(channelId)
    for (const [sessionId, ix] of bindings) {
      const session = this.bySession.get(sessionId)
      if (!session) continue
      session.channels.delete(ix)
      // Last channel gone and no wire holding it: drop the session, or it outlives every reconcile that could ever
      // name it (a session whose wire was lost is otherwise only removed by a reconcile or its page's leaving).
      this.dropIfEmpty(session)
    }
  }

  private unbind(sessionId: string, handle: ChannelHandle): void {
    const bindings = this.byChannel.get(handle.channel.id)
    bindings?.delete(sessionId)
    if (bindings?.size === 0) this.byChannel.delete(handle.channel.id)
  }

  private dropIfEmpty(session: Session): void {
    if (session.channels.size === 0 && session.holder === null && this.bySession.get(session.id) === session) {
      this.leave(session)
    }
  }

  /** The session leaves the registry. Unless the server ended it, whose end is kept already, it is kept as retired. */
  private leave(session: Session): void {
    this.bySession.delete(session.id)
    this.keepEnd(session.id, 'retired', session.high, this.retiredLeavesAt())
  }
}
