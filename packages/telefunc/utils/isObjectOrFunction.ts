export { isObjectOrFunction }

function isObjectOrFunction(value: unknown): value is object {
  return value !== null && (typeof value === 'object' || typeof value === 'function')
}
