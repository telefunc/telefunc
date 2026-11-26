export { defined }

/** `value`, which a test expects to be there: a clear failure where it isn't. */
function defined<T>(value: T | null | undefined, what = 'a value'): T {
  if (value === null || value === undefined) throw new Error(`Expected ${what}`)
  return value
}
