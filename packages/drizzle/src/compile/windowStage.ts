// ORDER BY + LIMIT/OFFSET. ORDER BY without LIMIT is the identity — sound because the
// projection already carries the sort columns, so a reorder emits a delta. With a LIMIT
// it is a topK, but an EXACT topK is licensed ONLY when the order is provably total (the
// comparator never ties two distinct rows) and aligned with SQL — otherwise the graph's
// private tie-break can disagree with SQL at the boundary (the D3 blocker), so the window
// degrades to a live coarse (dirty) plan. Likewise a placeholder bound, an OFFSET without
// ORDER BY, or an opaque ORDER expression cannot be honored precisely and degrade. The
// default NULL ordering follows the dialect matrix: pg ASC→last/DESC→first;
// sqlite ASC→first/DESC→last; an explicit NULLS FIRST/LAST overrides.

export { applyWindow, comparatorFor, isTotalOrder }

import { type IStreamBuilder, keyBy, map, topK } from '../graph/ivm.js'
import { compareValues } from '../ir/eval.js'
import type { Bound, Dialect, OrderKey, SelectShape } from '../ir/types.js'
import type { DirtySink } from './dirty.js'
import { type Row, qualifiedKey } from './rowSpace.js'

/** Apply the window. Returns the (possibly topK-limited) row stream; degradations tap the
 *  dirty witness and leave the stream unlimited so the data path also over-fires — never
 *  a miss. */
function applyWindow(shape: SelectShape, stream: IStreamBuilder<Row>, dirty: DirtySink): IStreamBuilder<Row> {
  const limit = valueOf(shape.limit)
  const offset = valueOf(shape.offset)
  if (shape.limit === undefined && shape.offset === undefined) return stream // ORDER BY without LIMIT is identity

  const placeholder = isPlaceholder(shape.limit) || isPlaceholder(shape.offset)
  const opaqueOrder = shape.orderBy.some((key) => key.expr.kind === 'opaque')
  const total = isTotalOrder(shape)
  const exact = !placeholder && !opaqueOrder && shape.orderBy.length > 0 && total

  if (exact) {
    const comparator = comparatorFor(shape.orderBy, shape.dialect)
    return stream.pipe(
      keyBy(() => 0),
      topK(comparator, { limit: limit ?? Number.POSITIVE_INFINITY, offset: offset ?? 0 }),
      map(([, row]: [number, Row]) => row),
    )
  }
  // Placeholder bound / OFFSET-without-ORDER / opaque order / non-total order → dirty.
  dirty.tap(stream)
  return stream
}

/** The order is total when no two distinct rows can tie. A grouped query's rows are unique
 *  per group key, so ordering by every group column is total over the groups; a base-table
 *  order is total when it includes the whole primary key. Anything else is treated as
 *  non-total (dirty-licensed). */
function isTotalOrder(shape: SelectShape): boolean {
  if (shape.orderBy.length === 0) return false
  const orderCols = new Set<string>()
  for (const key of shape.orderBy) {
    if (key.expr.kind !== 'col') return false
    orderCols.add(qualifiedKey(key.expr.ref.table, key.expr.ref.column))
  }
  if (shape.groupBy.length > 0 && !shape.groupByOpaque) {
    return shape.groupBy.every((ref) => orderCols.has(qualifiedKey(ref.table, ref.column)))
  }
  if (shape.joins.length > 0 || shape.from.primaryKey.length === 0) return false
  return shape.from.primaryKey.every((pk) => orderCols.has(qualifiedKey(shape.from.alias, pk)))
}

/** A comparator over rows honoring direction and the dialect-default (or explicit) NULL
 *  ordering per key. */
function comparatorFor(orderBy: OrderKey[], dialect: Dialect): (a: Row, b: Row) => number {
  const keys = orderBy.map((key) => ({
    key: key.expr.kind === 'col' ? qualifiedKey(key.expr.ref.table, key.expr.ref.column) : '',
    descending: key.direction === 'desc',
    nullsFirst: nullsFirstOf(key, dialect),
  }))
  return (a, b) => {
    for (const { key, descending, nullsFirst } of keys) {
      const va = a[key]
      const vb = b[key]
      const aNull = va === null || va === undefined
      const bNull = vb === null || vb === undefined
      if (aNull && bNull) continue
      if (aNull) return nullsFirst ? -1 : 1
      if (bNull) return nullsFirst ? 1 : -1
      const order = compareValues(va, vb)
      if (order === undefined || order === 0) continue
      return descending ? -order : order
    }
    return 0
  }
}

function nullsFirstOf(key: OrderKey, dialect: Dialect): boolean {
  if (key.nulls) return key.nulls === 'first'
  const ascending = key.direction === 'asc'
  // pg: ASC→last / DESC→first; sqlite: ASC→first / DESC→last.
  return dialect === 'pg' ? !ascending : ascending
}

function valueOf(bound: Bound | undefined): number | undefined {
  return bound && bound.kind === 'value' ? bound.value : undefined
}

function isPlaceholder(bound: Bound | undefined): boolean {
  return bound?.kind === 'placeholder'
}
