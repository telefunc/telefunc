// The terminal stage: project each row to a canonical string, optionally DISTINCT, consolidate,
// observe. The projected string ALWAYS carries the ORDER BY + opaque-contributing columns, so a
// reorder or opaque-value change emits a delta instead of cancelling. The PK rides through the
// stateful operators for retraction identity but is dropped here unless actually selected — so a
// plain SELECT of a non-key column collapses duplicates into bag multiplicity exactly as SQL does.

export { projectionKeysOf, projectFnOf, wholeRowFn }

import { canonicalValue } from '../utils/canonical.js'
import type { SelectShape } from '../ir/types.js'
import { type Row, qualifiedKey, rowString } from './rowSpace.js'

/** The observable projection keys in output ORDER: projected + opaque-contributing columns, then
 *  the ORDER BY columns (so a reorder rides in the tuple). `'*'` = whole row (SELECT *). */
function projectionKeysOf(shape: SelectShape): string[] | '*' {
  if (shape.projection.star) return '*'
  const keys: string[] = []
  const push = (key: string) => {
    if (!keys.includes(key)) keys.push(key)
  }
  for (const item of shape.projection.items) {
    if (item.kind === 'col') push(qualifiedKey(item.ref.table, item.ref.column))
    else if (item.kind === 'opaque') for (const ref of item.columns) push(qualifiedKey(ref.table, ref.column))
  }
  for (const key of shape.orderBy) {
    if (key.expr.kind === 'col') push(qualifiedKey(key.expr.ref.table, key.expr.ref.column))
    else for (const ref of key.expr.columns) push(qualifiedKey(ref.table, ref.column))
  }
  return keys
}

/** The projection function for a non-aggregate SELECT. The encoding is POSITIONAL (values in
 *  output order, not keyed by source column) so UNION-compatible branches over different source
 *  columns dedupe by value tuple exactly as SQL does. */
function projectFnOf(shape: SelectShape): (row: Row) => string {
  const keys = projectionKeysOf(shape)
  return keys === '*' ? wholeRowFn : (row) => positional(row, keys)
}

function positional(row: Row, keys: string[]): string {
  return keys.map((key) => (Object.hasOwn(row, key) ? `P${canonicalValue(row[key])}` : 'A')).join('')
}

/** A projection over every key present in the row (by name) — used for SELECT * and for
 *  the already-final aggregate record produced by the aggregate stage. */
function wholeRowFn(row: Row): string {
  return rowString(row, Object.keys(row))
}
