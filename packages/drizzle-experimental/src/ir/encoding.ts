export { sortedContent, equiKey, correlationKey, keyedRow, canonicalRow, positionalRow }

import { frame, canonicalValue } from '../utils/canonical.js'
import type { Row } from '../router/events.js'

// HOW A ROW BECOMES A STRING. Every incremental operator decides "same or not same" by comparing encodings,
// so these functions are what make a retraction cancel the insert it should cancel — and what makes two
// genuinely different rows fail to cancel. They live together because their DIFFERENCES are the design, and
// those differences were previously discoverable only by diffing near-identical copies in four files.
//
// The variants, and why each exists:
//
//   keyedRow        ordered by NAME, framed, PRESENCE-marked. The general row identity: an absent column and
//                   a NULL one are different observations, and framing keeps `{ab: 1}` from colliding with
//                   `{a: 'b1'}`.
//   canonicalRow    ordered by NAME, framed, NO presence marker — for comparing two images of the SAME row,
//                   where the column set is fixed and only values can differ.
//   positionalRow   ordered by POSITION, unframed, presence-marked. UNION dedup compares projections
//                   positionally: `SELECT a, b` and `SELECT b, a` are different projections of the same row
//                   and must encode differently, which a name-sorted encoding would erase.
//
// And the two NULL-safe key encoders, which differ in what a NULL must not match:
//
//   equiKey         a composite join key over several columns.
//   correlationKey  a single semi-join correlation value.
//
// Both answer a NULL (or an uncaptured column) with a per-content SENTINEL rather than a shared "null key":
// SQL says `NULL <> NULL`, so such a row must match nothing — but a re-fed IDENTICAL row has to key
// identically or its retraction would not cancel the insert. Content-derived sentinels give both. Their
// prefixes differ (`\0` vs a space) because they are separate keyspaces; nothing compares one against the
// other, and keeping them distinct means a future merge cannot silently make them collide.

/** A row's fields in name order — the shared input to the sentinel encodings below. */
function sortedContent(row: Row): Record<string, unknown> {
  const sorted: Record<string, unknown> = {}
  for (const key of Object.keys(row).sort()) sorted[key] = row[key]
  return sorted
}

/** The composite equi-key. A SQL NULL (or an uncaptured column) in any component yields a per-content
 *  sentinel that no cross-side row shares — the row can never equi-match, matching SQL's `NULL <> NULL`,
 *  while a re-fed identical row keys identically so its retraction still cancels. */
function equiKey(row: Row, keys: string[]): string {
  const values: unknown[] = []
  for (const key of keys) {
    const cell = row[key]
    if (!Object.hasOwn(row, key) || cell === null) return `\0N${canonicalValue(sortedContent(row))}`
    values.push(cell)
  }
  return `\0K${canonicalValue(values)}`
}

/** The semi-join key of a correlation value; a NULL / uncaptured value becomes a sentinel that shares no
 *  key, so it never matches (SQL `NULL IN (…)` is not true). */
function correlationKey(row: Row, key: string): string {
  const value = row[key]
  if (!Object.hasOwn(row, key) || value === null) return ` N${canonicalValue(sortedContent(row))}`
  return ` K${canonicalValue(value)}`
}

/** Collision-free encoding of a row restricted to `keys`: equal projections encode identically (so
 *  consolidate/distinct cancel them) and any observable difference — a value change or a presence change —
 *  encodes differently.
 *
 *  `frame` on the key is defence in depth rather than the thing that makes this injective: `canonicalValue`
 *  length-prefixes its own payloads, so no value can extend across into the next field's name and a mutation
 *  removing the framing here is not killable by any constructible collision. It stays because the injectivity
 *  argument should not depend on reading a second module's internals. */
function keyedRow(row: Row, keys: string[]): string {
  return keys
    .slice()
    .sort()
    .map((key) => frame(key) + (Object.hasOwn(row, key) ? `P${canonicalValue(row[key])}` : 'A'))
    .join('')
}

/** Encoding of a whole row for comparing two IMAGES of it. No presence marker: both images carry the same
 *  column set, so only values can differ. */
function canonicalRow(row: Row): string {
  return Object.keys(row)
    .sort()
    .map((key) => frame(key) + canonicalValue(row[key]))
    .join('')
}

/** Encoding of a projection in the order it was SELECTED. Unframed because the key list is fixed and
 *  positional, so there is no boundary to forge. */
function positionalRow(row: Row, keys: string[]): string {
  return keys.map((key) => (Object.hasOwn(row, key) ? `P${canonicalValue(row[key])}` : 'A')).join('')
}
