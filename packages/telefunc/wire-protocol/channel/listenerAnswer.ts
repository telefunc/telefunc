export { listenerAnswer, forListener }

import { HEADER } from '../wire/header.js'
import type { Registration } from '../../utils/Listeners.js'
import { stringifyValue } from './channel.js'

/** What `listeners[i]` gets of `value`: bytes of its own, which it may keep or transfer without touching another
 *  listener's. The last takes `value` itself where its buffer holds only it and the `ahead` bytes its frame has before
 *  it: a frame on Node can be a view of a socket read. */
function forListener<T>(value: T, listeners: readonly unknown[], i: number, ahead = HEADER): T {
  if (!(value instanceof Uint8Array)) return value
  const ownsBuffer = i === listeners.length - 1 && value.buffer.byteLength === ahead + value.byteLength
  return ownsBuffer ? value : (new Uint8Array(value) as T)
}

/** Calls each of `listeners` with `data` in turn, awaiting each, and returns the serialized value of the one that
 *  returned something: `undefined` answers nothing, so listeners that only observe can stand beside the one that
 *  answers. Null once two listeners answered. What a listener throws propagates, and so does the refusal of an answer
 *  JSON can't hold: bytes in it, which it would turn into an object. */
async function listenerAnswer<T>(
  listeners: readonly Registration<(data: T) => unknown>[],
  data: T,
): Promise<string | null> {
  let answer: unknown
  for (const [i, { listener }] of listeners.entries()) {
    if (!listener) continue
    const value = await listener(forListener(data, listeners, i))
    if (value === undefined) continue
    if (answer !== undefined) return null
    answer = value
  }
  return stringifyValue(answer, 'answer')
}
