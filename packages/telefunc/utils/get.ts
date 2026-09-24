export { get }

/** Read a nested value following `segments` into `value`. Returns `defaultValue` if the walk hits
 *  `null`/`undefined` or the final slot is `undefined`. Takes raw segments — no escape knowledge. */
function get<TDefault = unknown>(value: unknown, segments: readonly string[], defaultValue?: TDefault): TDefault {
  let current: any = value
  for (const key of segments) {
    if (current === null || current === undefined) return defaultValue as TDefault
    current = current[key]
  }
  return current === undefined ? (defaultValue as TDefault) : current
}
