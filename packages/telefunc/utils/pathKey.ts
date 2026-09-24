export { toPathKey, fromPathKey }

/** A path key is a dotted string used for map-style lookups (e.g. shield metadata keyed by path).
 *  Literal `.` and `\` in any segment are escaped so round-tripping is lossless:
 *    toPathKey(['a.b', 'c']) === 'a\\.b.c'
 *    fromPathKey('a\\.b.c')  === ['a.b', 'c']
 *  Callers that walk a tree (e.g. get/set) use the segment array directly and never see the escape. */

function toPathKey(segments: readonly string[]): string {
  return segments.map(escapeSegment).join('.')
}

function fromPathKey(path: string): string[] {
  const segments: string[] = []
  let current = ''
  for (let i = 0; i < path.length; i++) {
    const c = path[i]
    if (c === '\\' && i + 1 < path.length) {
      current += path[i + 1]
      i++
    } else if (c === '.') {
      segments.push(current)
      current = ''
    } else {
      current += c
    }
  }
  segments.push(current)
  return segments
}

function escapeSegment(s: string): string {
  return s.replace(/\\/g, '\\\\').replace(/\./g, '\\.')
}
