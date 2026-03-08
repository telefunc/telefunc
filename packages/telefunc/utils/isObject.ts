export { isObject, isObjectOrFunction }

function isObject(value: unknown): value is Record<string | symbol, unknown> {
  return typeof value === 'object' && value !== null
}

function isObjectOrFunction(value: unknown): value is object {
  return value !== null && (typeof value === 'object' || typeof value === 'function')
}
