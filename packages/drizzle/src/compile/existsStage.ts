// EXISTS / IN decorrelation. A positive `EXISTS (…)` / `col IN (subquery)` with a single
// equi-correlation becomes a semi-join: the outer rows are kept when their correlation key
// appears among the inner rows' keys (db-ivm `filterBy`). A SQL NULL correlation key is
// sent to a per-content sentinel that never matches, so `NULL IN (…)` correctly excludes.
// A NEGATED exists (`NOT EXISTS` / `NOT IN`) cannot be a plain anti-join without the
// NULL counterexample — removing an inner NULL flips SQL from excluded to included —
// so it degrades to a live coarse (dirty) plan whose witness fires on any inner OR outer
// change. Scalar subqueries are handled the same way by the caller: their inner tables are
// registered as inputs and any inner change taps dirty.

export { type ExistsSpec, applyExists, correlationKey }

import { type IStreamBuilder, filterBy, keyBy, map } from '../graph/ivm.js'
import type { Row } from './rowSpace.js'
import { canonicalValue } from '../utils/canonical.js'

type ExistsSpec = {
  /** The outer correlation column (qualified key) whose value keys the semi-join. */
  outerKey: string
  /** The inner rows, already σ-filtered, keyed by their correlation value. */
  innerKeys: IStreamBuilder<[string, Row]>
}

/** Apply each decorrelated positive EXISTS/IN as a semi-join, threading the surviving outer rows
 *  through. Negated EXISTS / NOT IN are NOT decorrelated here — the extractor leaves them in the
 *  residual predicate, which taps dirty (the inner-NULL counterexample never misses). */
function applyExists(outer: IStreamBuilder<Row>, specs: ExistsSpec[]): IStreamBuilder<Row> {
  let stream = outer
  for (const spec of specs) {
    stream = stream
      .pipe(keyBy((row: Row) => correlationKey(row, spec.outerKey)))
      .pipe(filterBy(spec.innerKeys))
      .pipe(map(([, row]: [string, Row]) => row))
  }
  return stream
}

/** The semi-join key of a correlation value; a NULL / uncaptured value becomes a sentinel
 *  that shares no key, so it never matches (SQL `NULL IN (…)` is not true). */
function correlationKey(row: Row, key: string): string {
  const value = row[key]
  if (!Object.hasOwn(row, key) || value === null) return ` N${canonicalValue(sortedContent(row))}`
  return ` K${canonicalValue(value)}`
}

function sortedContent(row: Row): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  for (const k of Object.keys(row).sort()) out[k] = row[k]
  return out
}
