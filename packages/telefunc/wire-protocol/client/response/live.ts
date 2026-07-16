export { liveReviver }

import type { ClientReviverContext, LiveContract, ReviverType } from '../../types.js'
import type { ClientChannel } from '../../channel.js'
import { SERIALIZER_PREFIX_LIVE } from '../../constants.js'
import type { ClientLive, LiveEvent } from '../../../node/server/live/live.js'

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

/** Build the client-side consumer end over the revived channel: `.data` is the wire snapshot (fixed —
 *  invalidation-only, no delta push); `onInvalidate` observes the channel's stale signals. */
function createClientLive<T>(initialData: T, channel: ClientChannel<never, LiveEvent>): ClientLive<T> {
  const invalidateTaps: Array<() => void> = []
  channel.listen(() => {
    for (const tap of [...invalidateTaps]) tap()
  })
  return {
    get data() {
      return initialData
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
