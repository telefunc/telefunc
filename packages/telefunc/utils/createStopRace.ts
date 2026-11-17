export { createStopRace }

import type { Deferred } from './createDeferred.js'

/**
 * Returns `race(next)`, which is `Promise.race([stop, next])` for a loop waiting once per chunk. `Promise.race()` itself
 * would leave a reaction on `stop` per chunk until `stop` settles, so the heap would grow for as long as a stream runs.
 */
function createStopRace<S>(stop: Promise<S>): <T>(next: Promise<T>) => Promise<S | T> {
  let stopped = false
  let wait: Omit<Deferred<S>, 'promise'> | undefined
  stop.then(
    (value) => {
      stopped = true
      wait?.resolve(value)
    },
    (err) => {
      stopped = true
      wait?.reject(err)
    },
  )
  return <T>(next: Promise<T>) =>
    new Promise<S | T>((resolve, reject) => {
      wait = { resolve, reject }
      if (stopped) stop.then(resolve, reject)
      next.then(resolve, reject)
    })
}
