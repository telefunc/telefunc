export { raceTimeout }

import { createDeferred, unrefTimer } from 'telefunc/__internal'

/** Settles like `promise`, or like `onTimeout()` once `ms` pass first. */
async function raceTimeout<T>(promise: Promise<T>, ms: number, onTimeout: () => T): Promise<T> {
  const timeout = createDeferred<T>()
  const timer = unrefTimer(setTimeout(() => timeout.resolve(Promise.resolve().then(onTimeout)), ms))
  try {
    return await Promise.race([promise, timeout.promise])
  } finally {
    clearTimeout(timer)
  }
}
