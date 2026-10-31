export { RegistrationWaits }
export type { RegistrationWaitsOwner }

import type { AwaitedChannel } from './AwaitedChannels.js'

/** The mux, which expires an awaited channel whose channel didn't register in time. */
interface RegistrationWaitsOwner {
  onRegistrationTimeout(awaited: AwaitedChannel): void
}

type RegistrationWaitsOptions = {
  owner: RegistrationWaitsOwner
}

/** The awaited channels on a mux's wires that wait for their channel's registration, by id, in the order they began. */
class RegistrationWaits {
  private readonly owner: RegistrationWaitsOwner
  private readonly byId = new Map<string, Set<AwaitedChannel>>()

  constructor({ owner }: RegistrationWaitsOptions) {
    this.owner = owner
  }

  /** `awaited` waits for its channel's registration, `ttlMs` at most. */
  wait(awaited: AwaitedChannel, ttlMs: number): void {
    let waiting = this.byId.get(awaited.channelId)
    if (waiting === undefined) this.byId.set(awaited.channelId, (waiting = new Set()))
    waiting.add(awaited)
    awaited.ttl = setTimeout(() => {
      if (this.stop(awaited)) this.owner.onRegistrationTimeout(awaited)
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
