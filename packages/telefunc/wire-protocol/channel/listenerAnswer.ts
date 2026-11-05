export { listenerAnswer, forListener }

import { stringify } from '@brillout/json-serializer/stringify'
import { HEADER } from '../wire/header.js'

/** What `listeners[i]` gets of `value`: bytes of its own, which it may keep or transfer without touching another
 *  listener's. The last takes the frame itself where its buffer holds nothing else: a frame on Node can be a view of a
 *  socket read. */
function forListener<T>(value: T, listeners: readonly unknown[], i: number): T {
  if (!(value instanceof Uint8Array)) return value
  const ownsBuffer = i === listeners.length - 1 && value.buffer.byteLength === value.byteLength + HEADER
  return ownsBuffer ? value : (new Uint8Array(value) as T)
}

/** Calls each of `listeners` with `data` in turn, awaiting each, and returns the serialized value of the one that
 *  returned something: `undefined` answers nothing, so listeners that only observe can stand beside the one that
 *  answers. Null once two listeners answered. What a listener throws, or `stringify` of the answer, propagates. */
async function listenerAnswer<T>(listeners: readonly ((data: T) => unknown)[], data: T): Promise<string | null> {
  let answer: unknown
  for (const [i, cb] of listeners.entries()) {
    const value = await cb(forListener(data, listeners, i))
    if (value === undefined) continue
    if (answer !== undefined) return null
    answer = value
  }
  return stringify(answer)
}
