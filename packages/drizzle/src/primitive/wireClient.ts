export { installLiveReviver, liveReviver }

import { config } from 'telefunc/client'
import type { ClientReviverContext, ReviverType, TypeContract } from 'telefunc/client'
import type { Live, LiveEvent, LiveSubscription } from './live.js'
import { SERIALIZER_PREFIX_LIVE } from './wireConstants.js'

// THE CLIENT HALF OF THE WIRE, registered through telefunc's PUBLIC client extension seam
// (`config.extensions.push({ responseTypes })`). Core's built-in reviver registry does not know a Live
// exists.
//
// BROWSER-SAFE BY CONSTRUCTION: this module imports only `telefunc/client`, TYPES from live.ts, and an
// import-free constant — nothing here can drag the server graph into a client bundle.

/** The client's half of the contract. Only the revived shape and the metadata matter here; the server's
 *  producer type is deliberately not referenced, so this module needs nothing server-side. */
type LiveWireContract = TypeContract<unknown, Live<unknown>, { data: unknown; channelId: string }>

/** The revived consumer handle. Publicly it is just `Live<T>` — a user reads `.data`, seeded from the
 *  wire snapshot. Internally it carries the invalidation tap the adapter binds (to refetch), plus the
 *  channel lifecycle. */
type ClientLiveHandle<T> = Live<T> & LiveSubscription

/** The channel type the reviver context hands back, derived from the context itself rather than imported
 *  — one less name to keep in step with core. */
type RevivedChannel = ReturnType<ClientReviverContext['createChannel']>

const liveReviver: ReviverType<LiveWireContract, ClientReviverContext> = {
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
function createClientLive<T>(data: T, channel: RevivedChannel): ClientLiveHandle<T> {
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
    close() {
      return channel.close().then(() => undefined)
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

let installed = false

/** Register the Live reviver with telefunc's client, at most once. Called at IMPORT time of the
 *  `./tanstack-query` subpath — before any `live()` queryFn can run, which is the ordering the revival
 *  depends on: a response carrying a Live must never arrive before its reviver is registered. */
function installLiveReviver(): void {
  if (installed) return
  installed = true
  config.extensions.push({
    name: '@telefunc/drizzle-experimental',
    responseTypes: [liveReviver as unknown as ReviverType<TypeContract, ClientReviverContext>],
  })
}
