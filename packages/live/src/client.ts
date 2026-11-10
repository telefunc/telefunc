export { watch }

import { assertUsage } from './assert.js'
import type { Live } from './live.js'
import { closeSubscription, subscriptionOf } from './wire/shared.js'

/**
 * Calls `telefunction(...args)`, gives its value to `onData`, and calls it again each time the value goes stale on the
 * server. At most one call runs at a time. Returns `stop`.
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
  let running = false
  let staleWhileRunning = false
  void refetch()
  return () => {
    stopped = true
    closeSubscription(current)
  }

  async function refetch(): Promise<void> {
    if (running) {
      staleWhileRunning = true
      return
    }
    running = true
    do {
      staleWhileRunning = false
      await fetchOnce()
    } while (staleWhileRunning && !stopped)
    running = false
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
    subscription.onStale(() => void refetch())
    onData(live.data)
  }
}
