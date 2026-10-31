export { raceTimeout }

import { unrefTimer } from './unrefTimer.js'

/** Settles like `promise`, or like `onTimeout()` once `ms` pass first. */
function raceTimeout<T>(promise: Promise<T>, ms: number, onTimeout: () => T): Promise<T> {
  let timer!: ReturnType<typeof setTimeout>
  const timeout = new Promise<T>((resolve) => {
    timer = unrefTimer(setTimeout(() => resolve(Promise.resolve().then(onTimeout)), ms))
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}
