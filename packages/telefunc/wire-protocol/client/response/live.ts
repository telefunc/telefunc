export { liveReviver }

import type { ClientReviverContext, LiveContract, ReviverType } from '../../types.js'
import type { ClientChannel } from '../../channel.js'
import { SERIALIZER_PREFIX_LIVE } from '../../constants.js'
import type { Live, LiveEvent, LiveSubscription } from '../../../node/server/live/live.js'

/** The revived consumer handle. Publicly it is just `Live<T>` — a user reads `.data`, seeded from the
 *  wire snapshot. Internally it carries the invalidation tap an adapter binds (to refetch), plus the
 *  channel lifecycle. */
type ClientLiveHandle<T> = Live<T> &
  LiveSubscription & {
    onClose(callback: (err?: Error) => void): void
    readonly isClosed: boolean
  }

const liveReviver: ReviverType<LiveContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_LIVE,
  revive(metadata, context) {
    const channel = context.createChannel<never, LiveEvent>({ channelId: metadata.channelId })
    return {
      value: createClientLive(metadata.data, channel),
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue, abortError.message)
      },
    }
  },
}

/** Build the client-side consumer end over the revived channel: `.data` is the wire snapshot (the
 *  primitive is invalidation-only, so it never changes in place); `onInvalidate` observes the channel's
 *  stale signals, on which an adapter refetches. */
function createClientLive<T>(data: T, channel: ClientChannel<never, LiveEvent>): ClientLiveHandle<T> {
  const invalidateTaps: Array<() => void> = []
  channel.listen(() => {
    for (const tap of [...invalidateTaps]) tap()
  })
  return {
    get data() {
      return data
    },
    onInvalidate(callback) {
      return addTap(invalidateTaps, callback)
    },
    onClose(callback) {
      channel.onClose(callback)
    },
    close() {
      return channel.close().then(() => undefined)
    },
    get isClosed() {
      return channel.isClosed
    },
  }
}

/** Register a tap and return an idempotent unsubscribe that removes exactly one registration. */
function addTap<F>(taps: Array<F>, callback: F): () => void {
  taps.push(callback)
  let removed = false
  return () => {
    if (removed) return
    removed = true
    const index = taps.indexOf(callback)
    if (index >= 0) taps.splice(index, 1)
  }
}
