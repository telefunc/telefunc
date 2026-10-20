export { deepFreeze }

/** Freezes `value` and every object its properties hold. */
function deepFreeze<T>(value: T): T {
  if (typeof value !== 'object' || value === null) return value
  Object.freeze(value)
  for (const key of Reflect.ownKeys(value)) deepFreeze((value as Record<PropertyKey, unknown>)[key])
  return value
}
