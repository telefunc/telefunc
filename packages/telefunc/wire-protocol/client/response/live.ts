export { liveReviver }

import type { ClientReviverContext, LiveContract, ReviverType } from '../../types.js'
import type { ClientChannel } from '../../channel.js'
import { SERIALIZER_PREFIX_LIVE } from '../../constants.js'
import type { Live, LiveEvent, LiveSubscription } from '../../../node/server/live/live.js'

/** The revived consumer handle. Publicly it is just `Live<T>` — a user reads `.data`. Internally it
 *  carries the observation taps (the producer's minus its authority): adapters reach `onData`/
 *  `onInvalidate`/`close` via `liveSubscription()`, and the channel lifecycle rounds out the end. */
type ClientLiveHandle<T> = Live<T> &
  LiveSubscription<T> & {
    onClose(callback: (err?: Error) => void): void
    readonly isClosed: boolean
  }

const liveReviver: ReviverType<LiveContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_LIVE,
  revive(metadata, context) {
    const channel = context.createChannel<never, LiveEvent<unknown>>({ channelId: metadata.channelId })
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

/** Build the client-side consumer end over the revived channel: `.data` seeds from the wire snapshot
 *  and tracks pushed `data` events; `onData`/`onInvalidate` observe the channel's events. */
function createClientLive<T>(initialData: T, channel: ClientChannel<never, LiveEvent<T>>): ClientLiveHandle<T> {
  let data = initialData
  const dataTaps: Array<(data: T) => void> = []
  const invalidateTaps: Array<() => void> = []
  channel.listen((event) => {
    if (event.kind === 'data') {
      data = event.data
      for (const tap of [...dataTaps]) tap(data)
    } else {
      for (const tap of [...invalidateTaps]) tap()
    }
  })
  return {
    get data() {
      return data
    },
    onData(callback) {
      return addTap(dataTaps, callback)
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
