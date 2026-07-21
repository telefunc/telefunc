export { captureMismatch, captureOrCoarse, captureBothOrCoarse, changesFromRows, emitSafely, coarse }
export type { CaptureSink, CaptureMismatch, Images }

import { report } from '../bus/captureReport.js'
import type { Op, Plan, PrecisePlan, SubstitutionPlan } from './writePlan.js'
import type { Row, TableChange } from '../bus/router/events.js'

// Turning what a write statement returned into the `TableChange`s the graphs read. Everything here runs
// AFTER the database has applied the write, so nothing in this module may fail the caller: a row set that
// cannot be trusted becomes ONE coarse marker for the table, never a fabricated row.
//
// What a captured change carries:
//   INSERT  → { kind:'insert', new: full row }
//   DELETE  → { kind:'delete', key: PK }        (retraction by old PK)
//   UPDATE  → { kind:'update', new: full row, key: PK }   (key = old PK = new PK; non-PK-changing only)
//
// …and, where the connection returned BOTH images of a changed row in the write statement itself:
//   DELETE  → { kind:'delete', old: full row, key: PK }
//   UPDATE  → { kind:'update', old: full row, new: full row, key: OLD PK }
// which makes two previously-coarse classes exact: an update that MOVES the primary key (the old key is
// right there), and any update a STATELESS live query must decide membership for (it can compare the two
// images instead of assuming the row may have entered or left).

/** Where a captured batch goes: straight to the db's graphs (autocommit) or a transaction buffer. */
type CaptureSink = (changes: TableChange[]) => void

/** A row's two images, as the write statement returned them. */
type Images = { old: Row; new: Row }

/** Why a captured row set cannot be trusted as a full, identifiable image. `undefined` = it can. */
type CaptureMismatch = { rowIndex: number; reason: 'missing-columns' | 'missing-key'; detail: string } | undefined

/** THE CAPTURE OBSERVATION SEAM — the last check between a captured row set and precise change events.
 *
 *  Exported so the mismatch decision is independently observable: it is the one place a shape/identity
 *  disagreement is caught, and it has to be assertable without contriving a driver that returns malformed
 *  rows. A mismatch fails CLOSED (one coarse marker) — never a fabricated row or a key of `undefined`s.
 *
 *  Checks EVERY row, not just the first: a partial projection or a driver quirk can widen the first row and
 *  narrow a later one, and `changesOf` would then emit a change whose `new` silently lacks columns.
 *
 *  `requireKey` is whether a retraction will be keyed off these rows: an absent or NULL key value would key
 *  it to nothing. Callers whose rows carry the whole fact and retract nothing pass `false` — an insert
 *  (`captureOrCoarse` derives it from the op), and a both-images NEW image, whose retraction key is the OLD
 *  image's to provide.
 *
 *  What is deliberately NOT claimed here: a row-COUNT cross-check. On both RETURNING paths the only
 *  affected-row count available (`reconstruct`'s `affectedRows`) is DERIVED from these same rows, so there
 *  is no independent oracle to disagree with — a "count check" against it could never fail. Where an
 *  independent count does exist (a SQLite direct terminal's `changes`) the write already fails closed to
 *  coarse for other reasons. Asserting a check that cannot fail would be verification theatre. */
function captureMismatch(rows: Row[], columns: string[], pk: string[], requireKey: boolean): CaptureMismatch {
  for (const [rowIndex, row] of rows.entries()) {
    const missing = columns.filter((column) => !(column in row))
    if (missing.length > 0) return { rowIndex, reason: 'missing-columns', detail: missing.join(', ') }
    if (!requireKey) continue
    const unkeyed = pk.filter((field) => row[field] === undefined || row[field] === null)
    if (unkeyed.length > 0) return { rowIndex, reason: 'missing-key', detail: unkeyed.join(', ') }
  }
  return undefined
}

/** Precise changes when the captured rows are a trustworthy full image, else ONE coarse marker. */
function captureOrCoarse(op: Op, relationId: string, rows: Row[], plan: PrecisePlan): TableChange[] {
  const mismatch = captureMismatch(rows, plan.columns, plan.pk, op !== 'insert')
  if (!mismatch) return changesOf(op, relationId, rows, plan)
  report('capture-mismatch', { relation: relationId, mismatch })
  return [coarse(relationId)]
}

/** Precise changes from BOTH images, else one coarse marker. The image that has to be trustworthy is the
 *  one the change is built from — the OLD image always (it carries the retraction key), plus whatever the
 *  layout says is decisive for this op (`ImageLayout.decisive`: an update's NEW images; nothing for a
 *  delete, whose NEW is the all-NULL non-row). */
function captureBothOrCoarse(
  op: Op,
  relationId: string,
  pairs: Images[],
  plan: Extract<SubstitutionPlan, { strategy: 'bothImages' }>,
): TableChange[] {
  const emitted = (row: Row) => physicalRow(row, plan.physical)
  const mismatch =
    captureMismatch(
      pairs.map((pair) => pair.old),
      plan.columns,
      plan.pk,
      true, // the retraction key is the OLD image's to provide — op is never 'insert' on this path
    ) ?? captureMismatch(plan.images.decisive(pairs, op), plan.columns, plan.pk, /* requireKey */ false)
  if (mismatch) {
    report('capture-mismatch', { relation: relationId, mismatch })
    return [coarse(relationId)]
  }
  return pairs.map(({ old, new: fresh }) =>
    op === 'delete'
      ? { table: relationId, kind: 'delete', old: emitted(old), key: keyOf(old, plan) }
      : // The key is taken from the OLD image on purpose: it addresses the row as the graph knows it, which
        // is what makes an update that MOVES the key describable rather than coarse.
        { table: relationId, kind: 'update', old: emitted(old), new: emitted(fresh), key: keyOf(old, plan) },
  )
}

function changesOf(op: Op, table: string, rows: Row[], plan: PrecisePlan): TableChange[] {
  return rows.map((row) => {
    const emitted = physicalRow(row, plan.physical)
    if (op === 'insert') return { table, kind: 'insert', new: emitted }
    if (op === 'delete') return { table, kind: 'delete', key: keyOf(row, plan) }
    return { table, kind: 'update', new: emitted, key: keyOf(row, plan) }
  })
}

// ── rows a terminal returned directly ───────────────────────────────

/** Capture from rows a terminal returned DIRECTLY, without capture having chosen the statement's RETURNING.
 *  Only a caller's own full `.returning()` qualifies: widening is not available here, because the caller's
 *  result must come back with its exact shape and sync/async-ness and the statement runs as they wrote it. */
function changesFromRows(result: unknown, plan: Plan, relationId: string, op: Op): TableChange[] {
  if (plan.strategy !== 'callerReturning' || !Array.isArray(result)) return [coarse(relationId)]
  return captureOrCoarse(op, relationId, namedRows(result, plan), plan)
}

/** Driver rows as NAMED rows. `.all()`/`.get()`/`await` already yield objects; SQLite's `.values()` terminal
 *  yields POSITIONAL arrays. Naming those is not the forbidden guess it looks like: on a `callerReturning`
 *  plan a non-empty `callerOrder` means the selection IS the full image, and the statement's RETURNING list
 *  was BUILT from that ordered selection — so position i is its i-th column by construction (verified
 *  against node:sqlite — a `.returning({ n: name, i: id })` comes back `["z", 9]`). A positional row of a
 *  length the selection does not explain is left alone and fails closed downstream. */
function namedRows(rows: unknown[], plan: PrecisePlan): Row[] {
  const { callerOrder } = plan
  return rows.map((row) => {
    if (!Array.isArray(row) || callerOrder.length === 0 || row.length !== callerOrder.length) return row as Row
    const named: Row = {}
    callerOrder.forEach((field, index) => {
      named[field] = row[index]
    })
    return named
  })
}

// ── the row-key space a change is emitted in ────────────────────────
//
// A captured row arrives keyed by drizzle FIELD names (`teamId`), because that is what a `.returning()`
// row is. The graph reads PHYSICAL column names (`team_id`) — `rowSpace.projectRaw` looks up the seed
// descriptor's columns, which the compiler derived from SQL. Emitting field keys therefore fired changes
// into a space the graphs do not read: a mapped column silently matched nothing.
//
// This translation is deliberately the LAST step and applies ONLY to what is emitted. Verification
// (`captureMismatch`) and the caller's own result stay in field space, where the driver's rows live — the
// two spaces are kept apart rather than one being converted into the other early and used for both.

/** A captured row re-keyed by physical column name. A field with no known column is dropped rather than
 *  passed through under a name the graph would misread. */
function physicalRow(row: Row, physical: Record<string, string>): Row {
  const out: Row = {}
  for (const [field, value] of Object.entries(row)) {
    const column = physical[field]
    if (column !== undefined) out[column] = value
  }
  return out
}

/** The retraction key, in the same physical space as the row it addresses. */
function keyOf(row: Row, plan: PrecisePlan): Row {
  const key: Row = {}
  for (const field of plan.pk) key[plan.physical[field] ?? field] = row[field]
  return key
}

const coarse = (table: string): TableChange => ({ table, kind: 'coarse' })

// ── emission, which must never fail the caller's write ──────────────

/** Emit captured changes WITHOUT ever failing the caller's write. By the time we get here the DATABASE HAS
 *  ALREADY COMMITTED, so a sink/router/transport fault must never turn a committed write into a caller-visible
 *  rejection — the caller's result and failure behaviour stay exactly plain Drizzle's. A precise feed that
 *  throws part-way is retried ONCE as a single coarse marker, so the graphs end up soundly over-fired rather
 *  than half-applied; if even that fails the fault is reported and dropped (live queries on the table may be
 *  stale until the next write, which is the honest degradation). */
function emitSafely(sink: CaptureSink, changes: TableChange[]): void {
  try {
    sink(changes)
  } catch (error) {
    const tables = [...new Set(changes.map((change) => change.table))]
    report('capture-failed', { relation: tables.join(', '), cause: error })
    if (changes.every((change) => change.kind === 'coarse')) return // the coarse fallback itself failed
    try {
      sink(tables.map(coarse)) // degrade: coarsen EVERY touched table rather than leave a feed half-applied
    } catch (fallbackError) {
      report('capture-failed', { relation: tables.join(', '), cause: fallbackError })
    }
  }
}
