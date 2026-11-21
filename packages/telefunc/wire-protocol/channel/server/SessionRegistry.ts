export { SessionRegistry }
export type { ChannelHandle, Session }

import type { ServerChannel } from './ServerChannel.js'
import type { IndexedPeer } from './IndexedPeer.js'
import type { WireEntry } from './ChannelMux.js'

/** A channel as one session holds it: its index there and the peer it sends through. */
type ChannelHandle = { channel: ServerChannel; ix: number; peer: IndexedPeer }

/** A page's session: its channels by index, and the wire that holds it, the last to reconcile it, until another
 *  reconciles it or it closes. */
type Session = {
  readonly id: string
  readonly channels: Map<number, ChannelHandle>
  holder: WireEntry | null
}

/** Each session by id, and the sessions each channel is in: a frame finds its channel by session and index, and an
 *  ending channel leaves every session in O(its sessions). Every change writes both maps. A session is stored while a
 *  wire holds it or it has a channel. */
class SessionRegistry {
  private readonly bySession = new Map<string, Session>()
  private readonly byChannel = new Map<string, Map<string, number>>()

  get(sessionId: string, ix: number): ChannelHandle | undefined {
    return this.bySession.get(sessionId)?.channels.get(ix)
  }

  /** The index the session holds channel `channelId` at. */
  indexOf(sessionId: string, channelId: string): number | undefined {
    return this.byChannel.get(channelId)?.get(sessionId)
  }

  /** The session's channels, left in place. */
  peekSession(sessionId: string): Map<number, ChannelHandle> | undefined {
    return this.bySession.get(sessionId)?.channels
  }

  /** The wire that holds the session, if one does. */
  holderOf(sessionId: string): WireEntry | null {
    return this.bySession.get(sessionId)?.holder ?? null
  }

  /** `entry` holds the session from now on. */
  hold(sessionId: string, entry: WireEntry): void {
    this.sessionOf(sessionId).holder = entry
  }

  /** `entry` holds the session no more, if it did; a session that no wire holds and that has no channel goes. */
  release(sessionId: string, entry: WireEntry): void {
    const session = this.bySession.get(sessionId)
    if (session?.holder !== entry) return
    session.holder = null
    this.dropIfEmpty(session)
  }

  /** No wire holds the session from now on. */
  unhold(sessionId: string): void {
    const session = this.bySession.get(sessionId)
    if (session === undefined) return
    session.holder = null
    this.dropIfEmpty(session)
  }

  /** Replaces the session's channels, and forgets its holder. */
  setSession(sessionId: string, handles: Iterable<ChannelHandle>): void {
    this.removeSession(sessionId)
    for (const h of handles) this.add(sessionId, h)
  }

  add(sessionId: string, h: ChannelHandle): void {
    this.sessionOf(sessionId).channels.set(h.ix, h)
    let bindings = this.byChannel.get(h.channel.id)
    if (!bindings) {
      bindings = new Map()
      this.byChannel.set(h.channel.id, bindings)
    }
    bindings.set(sessionId, h.ix)
  }

  /** Takes the channel at `ix` out of the session and returns it; an emptied session goes, as in `removeChannel`. */
  remove(sessionId: string, ix: number): ChannelHandle | undefined {
    const session = this.bySession.get(sessionId)
    const handle = session?.channels.get(ix)
    if (session === undefined || handle === undefined) return undefined
    session.channels.delete(ix)
    this.dropIfEmpty(session)
    const bindings = this.byChannel.get(handle.channel.id)
    bindings?.delete(sessionId)
    if (bindings?.size === 0) this.byChannel.delete(handle.channel.id)
    return handle
  }

  /** Takes the session out, its holder with it, and returns its channels, for the caller to detach. */
  removeSession(sessionId: string): Map<number, ChannelHandle> | undefined {
    const session = this.bySession.get(sessionId)
    if (!session) return undefined
    this.bySession.delete(sessionId)
    for (const handle of session.channels.values()) {
      const bindings = this.byChannel.get(handle.channel.id)
      if (!bindings) continue
      bindings.delete(sessionId)
      if (bindings.size === 0) this.byChannel.delete(handle.channel.id)
    }
    return session.channels
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
      // name it (transient-closed sessions are otherwise only removed by reconcile).
      this.dropIfEmpty(session)
    }
  }

  private sessionOf(sessionId: string): Session {
    let session = this.bySession.get(sessionId)
    if (!session) {
      session = { id: sessionId, channels: new Map(), holder: null }
      this.bySession.set(sessionId, session)
    }
    return session
  }

  private dropIfEmpty(session: Session): void {
    if (session.channels.size === 0 && session.holder === null) this.bySession.delete(session.id)
  }
}
