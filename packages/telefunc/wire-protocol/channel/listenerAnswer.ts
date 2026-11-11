export { listenerAnswer, forListener, soleAnswer, TWO_ANSWERS }

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

/** What `soleAnswer` returns once two listeners answered. */
const TWO_ANSWERS = Symbol('TWO_ANSWERS')

/** Calls each of `listeners` with `data` and `args` in turn, awaiting each, and returns the value of the one that
 *  returned something: `undefined` answers nothing, so listeners that only observe can stand beside the one that
 *  answers. `TWO_ANSWERS` once two answered. What a listener throws propagates. */
async function soleAnswer<T, A extends unknown[]>(
  listeners: readonly ((data: T, ...args: A) => unknown)[],
  data: T,
  ...args: A
): Promise<unknown> {
  let answer: unknown
  for (const [i, cb] of listeners.entries()) {
    const value = await cb(forListener(data, listeners, i), ...args)
    if (value === undefined) continue
    if (answer !== undefined) return TWO_ANSWERS
    answer = value
  }
  return answer
}

/** `soleAnswer`, serialized: null once two listeners answered. What `stringify` throws propagates. */
async function listenerAnswer<T>(listeners: readonly ((data: T) => unknown)[], data: T): Promise<string | null> {
  const answer = await soleAnswer(listeners, data)
  return answer === TWO_ANSWERS ? null : stringify(answer)
}
