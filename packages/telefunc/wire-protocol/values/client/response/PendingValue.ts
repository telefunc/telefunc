export { PendingValue, isPendingValue, resolvePendingValues }

import { isObject } from '../../../../utils/isObject.js'
import { replaceParsedValues } from './replaceParsedValues.js'

const PENDING_VALUE = Symbol.for('telefuncPendingValue')

/** Revived value known only once `promise` resolves: the call waits for it and hands out the resolved value in its slot. */
class PendingValue<T> {
  readonly [PENDING_VALUE] = true
  constructor(readonly promise: Promise<T>) {}
}

function isPendingValue(value: unknown): value is PendingValue<unknown> {
  return isObject(value) && value[PENDING_VALUE] === true
}

async function resolvePendingValues(
  parsed: unknown,
  pendingValues: PendingValue<unknown>[],
  isRevived: (value: object) => boolean,
): Promise<unknown> {
  const resolved = new Map<object, unknown>()
  await Promise.all(pendingValues.map(async (pending) => resolved.set(pending, await pending.promise)))
  return replaceParsedValues(parsed, resolved, isRevived)
}
