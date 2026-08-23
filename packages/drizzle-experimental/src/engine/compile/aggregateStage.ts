// GROUP BY + aggregate compilation. Each group keys by the canonical group-by tuple
// (GROUP BY NULL is its own group) and db-ivm's retraction-correct `reduce` recomputes the
// whole group from its current rows on every change — so MIN/MAX reveal the next value
// when the extremum is retracted, and SUM/AVG/COUNT stay exact. SQL NULL semantics: count(*)
// counts every row, count(col)/sum/avg/min/max ignore NULL, an all-NULL or empty input
// aggregates to NULL. The stage emits aggregate RECORDS (rows keyed by the selected group
// columns + synthetic aggregate keys + the ORDER BY group columns), so the shared window /
// projection pipeline can order and limit them exactly like any other row stream — there is
// no aggregate-specific terminal. HAVING over a group column filters exactly; HAVING that
// compares an aggregate is unprovable from the column IR, so it widens and taps dirty. An
// UNGROUPED aggregate seeds its unit-group zero row (count 0 / sum NULL) before any input,
// so a first NULL row that leaves the value unchanged emits no delta.

export { applyAggregate }

import {
  type D2,
  type IStreamBuilder,
  type RootStreamBuilder,
  concat,
  filter,
  keyBy,
  map,
  reduce,
} from '../graph/ivm.js'
import { compareValues } from '../../ir/eval.js'
import type { AggCall, OrderKey, ProjItem, SelectShape } from '../../ir/types.js'
import { type DirtySink, containsUnknown } from './dirty.js'
import { type Row, qualifiedKey, qualifiedRowView, requalify, sigmaMatch } from './rowSpace.js'
import { keyedRow } from '../../ir/encoding.js'
import { canonicalValue } from '../../ir/canonical.js'

type AggregateResult = {
  stream: IStreamBuilder<Row>
  /** Present for an ungrouped aggregate: fed a net-zero pair at instantiate to materialize
   *  the unit-group zero row before any real input. */
  genesis?: RootStreamBuilder<Row>
}

function applyAggregate(graph: D2, shape: SelectShape, stream: IStreamBuilder<Row>, dirty: DirtySink): AggregateResult {
  const grouped = shape.groupBy.length > 0 || shape.groupByOpaque
  const groupKeys = shape.groupBy.map((ref) => qualifiedKey(ref.table, ref.column))
  const outputItems = shape.projection.items.filter((item) => item.kind !== 'opaque')
  const orderCols = orderColumns(shape.orderBy)
  const havingExact = shape.having && !containsUnknown(shape.having) ? requalify(shape.having) : undefined
  const havingDirty = shape.having ? containsUnknown(shape.having) : false

  let input = stream
  let genesis: RootStreamBuilder<Row> | undefined
  if (!grouped) {
    genesis = graph.newInput<Row>()
    input = input.pipe(concat(genesis)) // seed the unit-group zero state (fed at instantiate)
  }

  const reduced = input.pipe(
    keyBy((row: Row) => keyedRow(row, groupKeys)),
    reduce<string, Row, Row, [string, Row]>((values) => {
      const present = values.filter(([, multiplicity]) => multiplicity > 0)
      if (present.length === 0) return grouped ? [] : [[emptyRecord(outputItems, orderCols), 1]]
      return [[groupRecord(outputItems, orderCols, present), 1]]
    }),
  )
  // HAVING over an aggregate is unprovable from the column IR, so it widens and taps dirty AFTER
  // the reduce — the reduced record carries the aggregate value HAVING filters. An opaque GROUP BY
  // is witnessed UPSTREAM at the input adapter instead (its base inputs are dirtyActive — see
  // opaqueAliases): a membership move between hidden groups cancels inside the reduce, so it cannot
  // be caught from the post-grouping stream (column pruning also folds the group column away here).
  if (havingDirty) dirty.tap(reduced)

  let records = reduced.pipe(map(([, record]) => record))
  if (havingExact) records = records.pipe(filter((row: Row) => sigmaMatch(havingExact, qualifiedRowView(row))))
  return { stream: records, genesis }
}

// ── Group record + aggregates ───────────────────────────────────────

/** The projected record for a non-empty group: selected group columns take their (group-
 *  constant) value, aggregates fold over the present rows, and ORDER BY group columns ride
 *  so the window stage can order groups exactly. */
function groupRecord(items: ProjItem[], orderCols: string[], present: Array<[Row, number]>): Row {
  const first = present[0]![0]
  const record: Row = {}
  let aggIndex = 0
  for (const item of items) {
    if (item.kind === 'col') {
      const key = qualifiedKey(item.ref.table, item.ref.column)
      record[key] = valueOf(first, key)
    } else if (item.kind === 'agg') {
      record[` agg${aggIndex++}`] = aggregate(item.call, present)
    }
  }
  for (const key of orderCols) if (!Object.hasOwn(record, key)) record[key] = valueOf(first, key)
  return record
}

/** The zero row of an ungrouped aggregate over an empty input (count → 0, others → NULL). */
function emptyRecord(items: ProjItem[], orderCols: string[]): Row {
  const record: Row = {}
  let aggIndex = 0
  for (const item of items) {
    if (item.kind === 'agg') record[` agg${aggIndex++}`] = item.call.star || item.call.fn === 'count' ? 0 : null
    else if (item.kind === 'col') record[qualifiedKey(item.ref.table, item.ref.column)] = null
  }
  for (const key of orderCols) if (!Object.hasOwn(record, key)) record[key] = null
  return record
}

function orderColumns(orderBy: OrderKey[]): string[] {
  const keys: string[] = []
  for (const key of orderBy) {
    if (key.expr.kind === 'col') keys.push(qualifiedKey(key.expr.ref.table, key.expr.ref.column))
  }
  return keys
}

function valueOf(row: Row, key: string): unknown {
  return Object.hasOwn(row, key) ? row[key] : null
}

function aggregate(call: AggCall, present: Array<[Row, number]>): unknown {
  if (call.star) {
    let count = 0
    for (const [, multiplicity] of present) count += multiplicity
    return count
  }
  const key = call.arg ? qualifiedKey(call.arg.table, call.arg.column) : ''
  const nonNull: Array<[unknown, number]> = []
  for (const [row, multiplicity] of present) {
    if (!Object.hasOwn(row, key)) continue
    const value = row[key]
    if (value !== null && value !== undefined) nonNull.push([value, multiplicity])
  }
  const values = call.distinct ? distinctValues(nonNull) : nonNull
  switch (call.fn) {
    case 'count':
      return countOf(values)
    case 'sum':
      return sumOf(values)
    case 'avg': {
      const count = countOf(values)
      return count === 0 ? null : sumOf(values)! / count
    }
    case 'min':
      return extremum(values, -1)
    case 'max':
      return extremum(values, 1)
  }
}

/** The distinct values, by the SAME identity every other operator uses.
 *
 *  This asks "are these the same value?", which the engine already has one answer to (`canonicalValue`), and
 *  answering it a second way here made DISTINCT disagree with the joins and shadows around it. The previous
 *  `JSON.stringify(v) + typeof v` diverged three ways, each a wrong count rather than a stale one:
 *  `JSON.stringify` renders NaN and ±Infinity all as `null` (three values counted as one — a MISS), it
 *  preserves key INSERTION order (one logical object counted twice, where every other identity sorts), and
 *  it THROWS on a bigint, which crashed the stage on an int8 column. */
function distinctValues(values: Array<[unknown, number]>): Array<[unknown, number]> {
  const seen = new Map<string, unknown>()
  for (const [value] of values) {
    const key = canonicalValue(value)
    if (!seen.has(key)) seen.set(key, value)
  }
  return [...seen.values()].map((value) => [value, 1])
}

function countOf(values: Array<[unknown, number]>): number {
  let count = 0
  for (const [, multiplicity] of values) count += multiplicity
  return count
}

function sumOf(values: Array<[unknown, number]>): number | null {
  if (values.length === 0) return null
  let sum = 0
  for (const [value, multiplicity] of values) sum += Number(value) * multiplicity
  return sum
}

function extremum(values: Array<[unknown, number]>, direction: number): unknown {
  let best: unknown
  let seen = false
  for (const [value] of values) {
    if (!seen) {
      best = value
      seen = true
      continue
    }
    const order = compareValues(value, best)
    if (order !== undefined && Math.sign(order) === direction) best = value
  }
  return seen ? best : null
}
