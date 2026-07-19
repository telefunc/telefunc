export { captureMismatch, captureOrCoarse, captureBothOrCoarse, changesFromRows, emitSafely, coarse }
export { reportCaptureFault }
export type { CaptureSink, CaptureMismatch, Images }

import { describeRelationId } from '../ir/relation.js'
import { report } from './captureReport.js'
import type { Op, Plan, PrecisePlan } from './writePlan.js'
import type { Row, TableChange } from '../router/events.js'

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
 *  What is deliberately NOT claimed here: a row-COUNT cross-check. On both RETURNING paths the only
 *  affected-row count available (`reconstruct`'s `affectedRows`) is DERIVED from these same rows, so there
 *  is no independent oracle to disagree with — a "count check" against it could never fail. Where an
 *  independent count does exist (a SQLite direct terminal's `changes`) the write already fails closed to
 *  coarse for other reasons. Asserting a check that cannot fail would be verification theatre. */
function captureMismatch(rows: Row[], columns: string[], pk: string[], op: Op): CaptureMismatch {
  for (const [rowIndex, row] of rows.entries()) {
    const missing = columns.filter((column) => !(column in row))
    if (missing.length > 0) return { rowIndex, reason: 'missing-columns', detail: missing.join(', ') }
    // A retraction is keyed by PK, so an absent or NULL key value would key it to nothing. Inserts carry the
    // whole row and need no key.
    if (op === 'insert') continue
    const unkeyed = pk.filter((field) => row[field] === undefined || row[field] === null)
    if (unkeyed.length > 0) return { rowIndex, reason: 'missing-key', detail: unkeyed.join(', ') }
  }
  return undefined
}

/** Precise changes when the captured rows are a trustworthy full image, else ONE coarse marker. */
function captureOrCoarse(op: Op, relationId: string, rows: Row[], plan: PrecisePlan): TableChange[] {
  const mismatch = captureMismatch(rows, plan.columns, plan.pk, op)
  if (!mismatch) return changesOf(op, relationId, rows, plan)
  reportCaptureMismatch(relationId, mismatch)
  return [coarse(relationId)]
}

function reportCaptureMismatch(relationId: string, mismatch: NonNullable<CaptureMismatch>): void {
  report(
    `[telefunc] live write-capture fell back to COARSE for table "${describeRelationId(relationId)}": captured row ${mismatch.rowIndex} has ${mismatch.reason} (${mismatch.detail}). The write is unaffected; live queries on this table over-invalidate rather than receive a partial row.`,
  )
}

/** Precise changes from BOTH images, else one coarse marker. The image that has to be trustworthy is the
 *  one the change is built from: an UPDATE is decided by both, a DELETE only by the row that was removed
 *  (PostgreSQL returns NEW as all-NULL for a delete, which is not a row and is never treated as one). */
function captureBothOrCoarse(op: Op, relationId: string, pairs: Images[], plan: PrecisePlan): TableChange[] {
  const emitted = (row: Row) => physicalRow(row, plan.physical)
  const decisive = op === 'delete' ? [] : pairs.map((pair) => pair.new)
  const mismatch =
    captureMismatch(
      pairs.map((pair) => pair.old),
      plan.columns,
      plan.pk,
      op,
    ) ?? captureMismatch(decisive, plan.columns, plan.pk, 'insert')
  if (mismatch) {
    reportCaptureMismatch(relationId, mismatch)
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
  if (plan.mode !== 'precise' || !plan.callerReturning || !Array.isArray(result)) return [coarse(relationId)]
  return captureOrCoarse(op, relationId, namedRows(result, plan), plan)
}

/** Driver rows as NAMED rows. `.all()`/`.get()`/`await` already yield objects; SQLite's `.values()` terminal
 *  yields POSITIONAL arrays. Naming those is not the forbidden guess it looks like: the statement's RETURNING
 *  list was BUILT from the builder's own ordered selection, so position i is that selection's i-th column by
 *  construction (verified against node:sqlite — a `.returning({ n: name, i: id })` comes back `["z", 9]`).
 *  A positional row of a length the selection does not explain is left alone and fails closed downstream. */
function namedRows(rows: unknown[], plan: PrecisePlan): Row[] {
  const { positional } = plan
  return rows.map((row) => {
    if (!Array.isArray(row) || positional === undefined || row.length !== positional.length) return row as Row
    const named: Row = {}
    positional.forEach((field, index) => {
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
    reportCaptureFault(error, tables.join(', '))
    if (changes.every((change) => change.kind === 'coarse')) return // the coarse fallback itself failed
    try {
      sink(tables.map(coarse)) // degrade: coarsen EVERY touched table rather than leave a feed half-applied
    } catch (fallbackError) {
      reportCaptureFault(fallbackError, tables.join(', '))
    }
  }
}

/** The diagnostics seam for a contained capture fault. Takes a relation IDENTITY and renders it the way a
 *  human wrote it (`a.users`), since this reaches an operator's logs. */
function reportCaptureFault(error: unknown, relationId: string): void {
  report(
    `[telefunc] live write-capture failed for table "${describeRelationId(relationId)}". The write COMMITTED and its result is unaffected; live queries on this table may be stale until the next write.`,
    error,
  )
}
