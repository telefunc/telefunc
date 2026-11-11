export { watch }

import { assertUsage } from './assert.js'
import type { Live } from './live.js'
import { Refetcher } from './refetcher.js'
import { closeSubscription, subscriptionOf } from './wire/shared.js'

/**
 * Calls `telefunction(...args)`, gives its value to `onData`, and calls it again each time the value goes stale on the
 * server. At most one call runs at a time, and a value that keeps changing is fetched again at most every 100 ms.
 * Returns `stop`.
 *
 * ```ts
 * const stop = watch(onGetTodos, [userId], (todos) => render(todos))
 * ```
 *
 * On the server (SSR) it does nothing: the browser starts watching once it runs.
 */
function watch<T, TArgs extends unknown[]>(
  telefunction: (...args: TArgs) => Promise<Live<T>>,
  args: TArgs,
  onData: (data: T) => void,
  onError: (err: unknown) => void = console.error,
): () => void {
  if (typeof window === 'undefined') return () => {}
  // The value watched now: holding it keeps its channel open.
  let current: Live<T> | undefined
  let stopped = false
  const refetcher = new Refetcher(fetchOnce)
  refetcher.stale()
  return () => {
    stopped = true
    refetcher.stop()
    closeSubscription(current)
  }

  async function fetchOnce(): Promise<void> {
    let live: Live<T>
    try {
      live = await telefunction(...args)
    } catch (err) {
      if (!stopped) onError(err)
      return
    }
    const subscription = subscriptionOf(live)
    assertUsage(subscription, 'watch() takes a telefunction returning a `Live`, e.g. a query ending with `.live()`.')
    if (stopped) {
      closeSubscription(live)
      return
    }
    closeSubscription(current)
    current = live
    subscription.onStale(() => refetcher.stale())
    onData(live.data)
  }
}
