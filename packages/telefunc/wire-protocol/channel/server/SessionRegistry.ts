export { SessionRegistry }
export type { ChannelHandle, Session }

import type { ServerChannel } from './ServerChannel.js'
import type { IndexedPeer } from './IndexedPeer.js'
import type { WireEntry } from './ChannelMux.js'

/** A channel as one session holds it: its index there and the peer it sends through. */
type ChannelHandle = { channel: ServerChannel; ix: number; peer: IndexedPeer }

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

/** Each session by id, and the sessions each channel is in: a frame finds its channel by session and index, and an
 *  ending channel leaves every session in O(its sessions). Every change writes both maps. A session is stored while a
 *  wire holds it or it has a channel. */
class SessionRegistry {
  private readonly bySession = new Map<string, Session>()
  private readonly byChannel = new Map<string, Map<string, number>>()

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

  /** `entry`, the page's wire numbered `attempt`, holds the session from now on; the wire that held it holds nothing,
   *  and is returned. */
  handOver(session: Session, entry: WireEntry, attempt: number): WireEntry | null {
    const previous = session.holder
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
    if (this.bySession.get(session.id) === session) this.bySession.delete(session.id)
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
      this.bySession.delete(session.id)
    }
  }
}
