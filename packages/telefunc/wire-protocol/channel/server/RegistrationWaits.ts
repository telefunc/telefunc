export { RegistrationWaits }

import { expireAwaited, type AwaitedChannel } from './AwaitedChannels.js'

/** The awaited channels on a mux's wires that wait for their channel's registration, by id, in the order they began;
 *  one that waits `CHANNEL_CONNECT_TTL_MS` in vain expires. */
class RegistrationWaits {
  private readonly byId = new Map<string, Set<AwaitedChannel>>()

  /** `awaited` waits for its channel's registration, `ttlMs` at most. */
  wait(awaited: AwaitedChannel, ttlMs: number): void {
    let waiting = this.byId.get(awaited.channelId)
    if (waiting === undefined) this.byId.set(awaited.channelId, (waiting = new Set()))
    waiting.add(awaited)
    awaited.ttl = setTimeout(() => {
      if (this.stop(awaited)) expireAwaited(awaited)
    }, ttlMs)
  }

  /** Those that wait for channel `id`, in the order they began: each still waits until `stop`. */
  take(id: string): Iterable<AwaitedChannel> {
    const waiting = this.byId.get(id)
    if (waiting === undefined) return NONE
    this.byId.delete(id)
    return waiting
  }

  /** `awaited` waits no more; false where it didn't. */
  stop(awaited: AwaitedChannel): boolean {
    if (awaited.ttl === null) return false
    clearTimeout(awaited.ttl)
    awaited.ttl = null
    const waiting = this.byId.get(awaited.channelId)
    if (waiting?.delete(awaited) && waiting.size === 0) this.byId.delete(awaited.channelId)
    return true
  }
}

const NONE: readonly AwaitedChannel[] = []
