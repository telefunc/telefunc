export { markHandled }

import { isPromise } from './isPromise.js'

/** A fire-and-forget call that fails leaves no unhandled rejection; a caller that awaits it still sees the error. */
function markHandled<T>(result: Promise<T>): Promise<T>
function markHandled<T>(result: T | Promise<T>): T | Promise<T>
function markHandled<T>(result: T | Promise<T>): T | Promise<T> {
  if (isPromise(result)) result.catch(() => {})
  return result
}
