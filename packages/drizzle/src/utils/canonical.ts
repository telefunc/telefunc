export { frame, canonicalValue }

// Collision-free string encoding, shared by identity keys and schema fingerprints so
// there is exactly one framing scheme. Every variable-length token is length-prefixed
// (`frame`), which makes any concatenation of framed tokens unambiguously decodable —
// the property that makes the composed keys injective. There is no delimiter that a
// payload could forge, because there is no delimiter: lengths bound every field.

/** Length-prefix a string: `frame('a.b') !== frame('a') + frame('.b')`. */
function frame(text: string): string {
  return `${text.length}:${text}`
}

/** Canonical encoding of a value: a distinct type tag plus a framed payload, so
 *  structurally-equal values collapse and structurally-different values never do —
 *  number vs bigint vs string, `['a','b']` vs `['a,b']`, `null` vs `'null'`, Date vs
 *  its string all encode differently, with no possible delimiter injection. */
function canonicalValue(value: unknown): string {
  if (value === null) return 'z'
  if (value === undefined) return 'u'
  const type = typeof value
  if (type === 'number') return `n${frame(String(value))}`
  if (type === 'bigint') return `g${frame((value as bigint).toString())}`
  if (type === 'boolean') return value ? 'bT' : 'bF'
  if (value instanceof Date) return `d${frame(String(value.getTime()))}`
  if (type === 'string') return `s${frame(value as string)}`
  if (Array.isArray(value)) return `a${frame(value.map(canonicalValue).join(''))}`
  const record = value as Record<string, unknown>
  return `o${frame(
    Object.keys(record)
      .sort()
      .map((key) => frame(key) + canonicalValue(record[key]))
      .join(''),
  )}`
}
