export { SessionRegistry }
export type { ChannelHandle }

import type { ServerChannel } from './channel.js'
import type { IndexedPeer } from './IndexedPeer.js'

type ChannelHandle = { channel: ServerChannel; ix: number; peer: IndexedPeer }

/** Forward (`bySession`: sessionId → ix → handle) for per-frame routing; reverse
 *  (`byChannel`: channelId → sessionId → ix) for O(bindings) channel eviction.
 *  Mutations stay atomic across both maps. */
class SessionRegistry {
  private readonly bySession = new Map<string, Map<number, ChannelHandle>>()
  private readonly byChannel = new Map<string, Map<string, number>>()

  get(sessionId: string, ix: number): ChannelHandle | undefined {
    return this.bySession.get(sessionId)?.get(ix)
  }

  /** Read without mutating. Used by transient-close handling so the next reconcile's
   *  prev-comparison can still detect channels the client dropped. */
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

  /** Returns the removed session so callers can drive per-handle lifecycle side effects. */
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
