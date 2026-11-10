export { LIVE_PREFIX, EXTENSION_NAME, SUBSCRIPTION, subscriptionOf, closeSubscription }
export type { Subscription }

import type { Live } from '../live.js'

// Shared by both halves of the wire and by the client APIs, and free of runtime imports: neither side may pull in the other's,
// and importing `watch()` or the TanStack adapter registers nothing.

const LIVE_PREFIX = '!TelefuncLive:'
const EXTENSION_NAME = '@telefunc/live'

/** On a `Live` that came from a telefunction: how to learn it went stale, and how to stop listening. */
const SUBSCRIPTION = Symbol.for('@telefunc/live/subscription')

type Subscription = {
  /** Calls `onStale` each time the server says the value went stale. Returns `stop`. */
  onStale(onStale: () => void): () => void
  close(): Promise<void>
}

/** The subscription of a `Live` a telefunction returned in the browser; none on one made on the server (SSR). Whoever
 *  listens keeps the `Live` itself: telefunc closes the channels of a value the app no longer holds. */
function subscriptionOf(live: Live<unknown>): Subscription | undefined {
  return (live as { [SUBSCRIPTION]?: Subscription })[SUBSCRIPTION]
}

function closeSubscription(live: Live<unknown> | undefined): void {
  if (live) void subscriptionOf(live)?.close()
}
