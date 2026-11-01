export { SessionRegistry }
export type { ChannelHandle }

import type { ServerChannel } from './channel.js'
import type { IndexedPeer } from './IndexedPeer.js'

/** A channel as one session holds it: its index there and the peer it sends through. */
type ChannelHandle = { channel: ServerChannel; ix: number; peer: IndexedPeer }

/** Each session's channels by index, and the sessions each channel is in: a frame finds its channel by session and
 *  index, and an ending channel leaves every session in O(its sessions). Every change writes both maps. */
class SessionRegistry {
  private readonly bySession = new Map<string, Map<number, ChannelHandle>>()
  private readonly byChannel = new Map<string, Map<string, number>>()

  get(sessionId: string, ix: number): ChannelHandle | undefined {
    return this.bySession.get(sessionId)?.get(ix)
  }

  /** The session's channels, left in place. */
  peekSession(sessionId: string): Map<number, ChannelHandle> | undefined {
    return this.bySession.get(sessionId)
  }

  /** Replaces the session's channels. An empty session isn't stored: a transient close keeps sessions, and no
   *  channel's removal would delete an empty one. */
  setSession(sessionId: string, handles: Iterable<ChannelHandle>): void {
    this.removeSession(sessionId)
    for (const h of handles) this.add(sessionId, h)
  }

  add(sessionId: string, h: ChannelHandle): void {
    let session = this.bySession.get(sessionId)
    if (!session) {
      session = new Map()
      this.bySession.set(sessionId, session)
    }
    session.set(h.ix, h)
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
    const handle = session?.get(ix)
    if (session === undefined || handle === undefined) return undefined
    session.delete(ix)
    if (session.size === 0) this.bySession.delete(sessionId)
    const bindings = this.byChannel.get(handle.channel.id)!
    bindings.delete(sessionId)
    if (bindings.size === 0) this.byChannel.delete(handle.channel.id)
    return handle
  }

  /** Takes the session out and returns its channels, for the caller to detach. */
  removeSession(sessionId: string): Map<number, ChannelHandle> | undefined {
    const session = this.bySession.get(sessionId)
    if (!session) return undefined
    this.bySession.delete(sessionId)
    for (const handle of session.values()) {
      const bindings = this.byChannel.get(handle.channel.id)
      if (!bindings) continue
      bindings.delete(sessionId)
      if (bindings.size === 0) this.byChannel.delete(handle.channel.id)
    }
    return session
  }

  removeChannel(channelId: string): void {
    const bindings = this.byChannel.get(channelId)
    if (!bindings) return
    this.byChannel.delete(channelId)
    for (const [sessionId, ix] of bindings) {
      const session = this.bySession.get(sessionId)
      if (!session) continue
      session.delete(ix)
      // Last channel gone: drop the session, or it outlives every reconcile that could
      // ever name it (transient-closed sessions are otherwise only removed by reconcile).
      if (session.size === 0) this.bySession.delete(sessionId)
    }
  }
}
