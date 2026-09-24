export { unrefTimer }

import { hasProp } from './hasProp.js'

/** Unref a timer if the runtime supports it (Node.js), then return it unchanged. */
function unrefTimer<T extends ReturnType<typeof setTimeout>>(timer: T): T {
  if (hasProp(timer, 'unref', 'function')) timer.unref()
  return timer
}
