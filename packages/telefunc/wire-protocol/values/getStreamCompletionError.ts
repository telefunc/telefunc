export { getStreamCompletionError }

import type { StreamReadOptions } from './types.js'

function getStreamCompletionError(opts: {
  wasCancelled: boolean
  received: number
  readOptions?: StreamReadOptions
}): Error | null {
  const { wasCancelled, received, readOptions } = opts
  if (wasCancelled && (readOptions?.cancelBehavior ?? 'error') === 'error') {
    return new Error('Stream cancelled before all bytes were received')
  }
  const expectedSize = readOptions?.expectedSize
  if (expectedSize !== undefined && expectedSize >= 0 && received !== expectedSize) {
    return new Error(`Stream truncated — received ${received} of ${expectedSize} expected bytes`)
  }
  return null
}
