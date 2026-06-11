export { isObject }

function isObject(value: unknown): value is Record<string | symbol, unknown> {
  return typeof value === 'object' && value !== null
}
