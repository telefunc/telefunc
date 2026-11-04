export { raceTimeout }

import { createDeferred } from 'telefunc/__internal'
import { unrefTimer } from './unrefTimer.js'

/** Settles like `promise`, or like `onTimeout()` once `ms` pass first. */
function raceTimeout<T>(promise: Promise<T>, ms: number, onTimeout: () => T): Promise<T> {
  const timeout = createDeferred<T>()
  const timer = unrefTimer(setTimeout(() => timeout.resolve(Promise.resolve().then(onTimeout)), ms))
  return Promise.race([promise, timeout.promise]).finally(() => clearTimeout(timer))
}
