export { raceTimeout }

import { unrefTimer } from './unrefTimer.js'

/** Settles like `promise`, or like `onTimeout()` once `ms` pass first; a spent budget times out at once. */
function raceTimeout<T>(promise: Promise<T>, ms: number, onTimeout: () => T): Promise<T> {
  if (ms <= 0) return Promise.resolve().then(onTimeout)
  let timer!: ReturnType<typeof setTimeout>
  const timeout = new Promise<T>((resolve) => {
    timer = unrefTimer(setTimeout(() => resolve(Promise.resolve().then(onTimeout)), ms))
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}
