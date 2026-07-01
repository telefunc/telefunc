export { set }

/** Write `value` into the slot identified by `segments` in `root`. Mutates in place; intermediate
 *  segments must already exist. Takes raw segments — no escape knowledge. */
function set(root: any, segments: readonly string[], value: unknown): void {
  let current = root
  for (let i = 0; i < segments.length - 1; i++) current = current[segments[i]!]
  current[segments[segments.length - 1]!] = value
}
