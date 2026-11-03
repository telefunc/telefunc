export { listenerAnswer, forListener }

import { stringify } from '@brillout/json-serializer/stringify'

/** What one listener gets of `value`: bytes of its own, which it may keep or transfer without touching another
 *  listener's. */
function forListener<T>(value: T): T {
  return value instanceof Uint8Array ? (new Uint8Array(value) as T) : value
}

/** Calls each of `listeners` with `data` in turn, awaiting each, and returns the serialized value of the one that
 *  returned something: `undefined` answers nothing, so listeners that only observe can stand beside the one that
 *  answers. Null once two listeners answered. What a listener throws, or `stringify` of the answer, propagates. */
async function listenerAnswer<T>(listeners: readonly ((data: T) => unknown)[], data: T): Promise<string | null> {
  let answer: unknown
  for (const cb of listeners) {
    const value = await cb(forListener(data))
    if (value === undefined) continue
    if (answer !== undefined) return null
    answer = value
  }
  return stringify(answer)
}
