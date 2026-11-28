export { listenerAnswer, forListener, soleAnswer, TWO_ANSWERS }

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

/** What `soleAnswer` returns once two listeners answered. */
const TWO_ANSWERS = Symbol('TWO_ANSWERS')

/** Calls each of `listeners` with `data` and `args` in turn, awaiting each, and returns the value of the one that
 *  returned something: `undefined` answers nothing, so listeners that only observe can stand beside the one that
 *  answers. `TWO_ANSWERS` once two answered. What a listener throws propagates. */
async function soleAnswer<T, A extends unknown[]>(
  listeners: readonly Registration<(data: T, ...args: A) => unknown>[],
  data: T,
  ...args: A
): Promise<unknown> {
  let answer: unknown
  for (const [i, { listener }] of listeners.entries()) {
    if (!listener) continue
    const value = await listener(forListener(data, listeners, i), ...args)
    if (value === undefined) continue
    if (answer !== undefined) return TWO_ANSWERS
    answer = value
  }
  return answer
}

/** `soleAnswer`, as `serialize` writes it: null once two listeners answered. What `serialize` throws propagates: by
 *  default, the refusal of bytes in the answer, which JSON would turn into an object. */
async function listenerAnswer<T>(
  listeners: readonly Registration<(data: T) => unknown>[],
  data: T,
  serialize: (answer: unknown) => string = (answer) => stringifyValue(answer, 'answer'),
): Promise<string | null> {
  const answer = await soleAnswer(listeners, data)
  return answer === TWO_ANSWERS ? null : serialize(answer)
}
