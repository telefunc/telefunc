export { isObject, isObjectOrFunction }

function isObject(value: unknown): value is Record<string | symbol, unknown> {
  return typeof value === 'object' && value !== null
}

// TODO/ai move this function to utils/isObjectOrFunction.ts
function isObjectOrFunction(value: unknown): value is object {
  return value !== null && (typeof value === 'object' || typeof value === 'function')
}
