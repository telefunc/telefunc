export { makePublishInfo, invokeChannelListener }

import type { ChannelPublishInfo } from 'telefunc/__internal'
import { isPromise } from './utils/isPromise.js'

function makePublishInfo(key: string, seq: number, timestamp: number): ChannelPublishInfo {
  return { key, seq, timestamp }
}

/** Invoke a fire-and-forget listener while routing both thrown and rejected failures. Returns what `handleError`
 *  returned for a synchronous throw only, which a caller can use to stop iterating; a rejection can't stop it. */
function invokeChannelListener<Args extends unknown[]>(
  listener: (...args: Args) => unknown,
  args: Args,
  handleError: (error: unknown) => boolean | void,
): boolean | void {
  try {
    const result = listener(...args)
    if (isPromise(result)) void result.catch(handleError)
  } catch (error) {
    return handleError(error)
  }
}
