export { PendingValue, isPendingValue, resolvePendingValues }

import { isObject } from '../../../../utils/isObject.js'
import { isPlainObject } from '../../../../utils/isPlainObject.js'

const PENDING_VALUE = Symbol.for('telefuncPendingValue')

/** Revived value known only once `promise` resolves: the call waits for it and hands out the resolved value in its slot. */
class PendingValue<T> {
  readonly [PENDING_VALUE] = true
  constructor(readonly promise: Promise<T>) {}
}

function isPendingValue(value: unknown): value is PendingValue<unknown> {
  return isObject(value) && value[PENDING_VALUE] === true
}

/** Awaits `pendingValues`, then swaps each one for its resolved value in place. Walks the containers json-serializer builds (arrays, plain objects, Map, Set); revived values are opaque. */
async function resolvePendingValues(
  parsed: unknown,
  pendingValues: PendingValue<unknown>[],
  isRevived: (value: object) => boolean,
): Promise<unknown> {
  const resolved = new Map<unknown, unknown>(
    await Promise.all(pendingValues.map(async (pending) => [pending, await pending.promise] as const)),
  )
  return resolve(parsed)

  function resolve(value: unknown): unknown {
    if (isPendingValue(value)) return resolved.get(value)
    if (typeof value !== 'object' || value === null || isRevived(value)) return value
    if (Array.isArray(value)) {
      value.forEach((item, i) => {
        value[i] = resolve(item)
      })
    } else if (value instanceof Map) {
      const entries = [...value].map(([key, item]) => [resolve(key), resolve(item)] as const)
      value.clear()
      for (const [key, item] of entries) value.set(key, item)
    } else if (value instanceof Set) {
      const items = [...value].map(resolve)
      value.clear()
      for (const item of items) value.add(item)
    } else if (isPlainObject(value)) {
      for (const key of Object.keys(value)) value[key] = resolve(value[key])
    }
    return value
  }
}
