export { createStopRace }

import { createDeferred } from './createDeferred.js'
import type { Deferred } from './createDeferred.js'

/**
 * Returns `race(next)`, which is `Promise.race([stop, next])` for a loop waiting once per chunk. `Promise.race()` itself
 * would leave a reaction on `stop` per chunk until `stop` settles, so the heap would grow for as long as a stream runs.
 */
function createStopRace<S>(stop: Promise<S>): <T>(next: Promise<T>) => Promise<S | T> {
  let stopped = false
  let wait: Omit<Deferred<S>, 'promise'> | undefined
  void settleWaitOnStop()
  return race

  async function settleWaitOnStop(): Promise<void> {
    try {
      const value = await stop
      stopped = true
      wait?.resolve(value)
    } catch (err) {
      stopped = true
      wait?.reject(err)
    }
  }

  function race<T>(next: Promise<T>): Promise<S | T> {
    const waiting = createDeferred<S | T>()
    wait = waiting
    if (stopped) stop.then(waiting.resolve, waiting.reject)
    next.then(waiting.resolve, waiting.reject)
    return waiting.promise
  }
}
