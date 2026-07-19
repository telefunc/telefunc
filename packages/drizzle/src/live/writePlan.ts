export { planCapture, callerPositionsOf, writeConfigOf, UNMAPPABLE }
export type { Op, Plan, PrecisePlan, WriteConfig }

import { type Column, SQL, type Table, getTableColumns, is } from 'drizzle-orm'
import { dialectOf, driverOf } from '../binding/database.js'
import { oldNewReturningOf } from './writeCapabilities.js'
import { primaryKeyOf } from '../extract/columns.js'
import type { Dialect } from '../ir/types.js'
import type { Row } from '../router/events.js'

// How ONE write is classified before anything runs: precise (capture can name the changed rows) or coarse
// (it cannot, so the table over-invalidates). Everything here is a decision made from the BUILDER's shape —
// no statement has executed yet, and nothing in this module touches a connection.
//
// PRECISION is gated + fails closed (emit one {table, kind:'coarse'}) — safe over-fire, never a wrong row:
//   - a resolvable PK (single OR composite) for UPDATE/DELETE, whose retraction is keyed by it; an INSERT
//     carries its whole row and retracts nothing, so a PK-less table is still exact for it;
//   - not an UPSERT / ON CONFLICT (a returned row does not say whether it was inserted or updated), and not
//     a raw-SQL values clause;
//   - not a PK-CHANGING update, UNLESS both images are available: without the old image, the key a
//     retraction is addressed by is gone the moment the statement runs, and no RETURNING of the new row can
//     recover it → coarse;
//   - the caller's result must be REBUILDABLE from the full row image the capture actually runs. A full
//     `.returning()` is its own answer. A PARTIAL or aliased `.returning({ id })` is widened internally to
//     the whole row and the caller's own columns are projected back out of it — nothing is invented, every
//     value returned is a real column of the row the database changed. Where the projection selects a raw
//     `SQL` expression the database computed (`sql`id + 1``), it cannot be recomputed from the row → coarse.
//     For a write with NO `.returning()` at all, the plain driver result must be faithfully reproducible
//     from a hidden full RETURNING (verified: PGlite → `{rows:[],fields:[],affectedRows:N}`); other drivers
//     + SQLite (its `lastInsertRowid` is not recoverable for update/delete) → coarse.
//
// NOT a gate: whether the connection is pooled. A `.returning()` row image is produced by the exact session
// that executed the statement, so "which pool connection ran it" cannot make those rows less real. Session
// authority (role / search_path / RLS) is load-bearing for READ hydration identity — which keeps its own
// pooled gate in readCapture.ts — not for returned-row capture.

type Op = 'insert' | 'update' | 'delete'

/** What a precise plan carries into capture: the PK fields a retraction is keyed by, the columns a full
 *  image must contain, and — only when the RETURNING selection already IS the full image — the table field
 *  each returned POSITION carries, which is what lets SQLite's positional `.values()` rows be named. */
type PrecisePlan = {
  pk: string[]
  columns: string[]
  positional?: string[]
  /** field name → PHYSICAL column name, for translating an emitted change into the graph's row space. */
  physical: Record<string, string>
  /** The table fields the CALLER's own RETURNING asked for, IN THE ORDER their selection lists them — which
   *  is the order their own mapper reads positions in, and so is what lets their result be rebuilt from raw
   *  rows when capture's mapping of the same statement fails (see the count-observation tap). Empty when
   *  they asked for no rows at all, and when their projection is not rebuildable from a row image. */
  callerOrder: readonly string[]
}

type Plan =
  | { mode: 'coarse' }
  | ({ mode: 'precise'; callerReturning: true } & PrecisePlan)
  | ({
      mode: 'precise'
      callerReturning: false
      /** The caller's own result, rebuilt from the rows capture chose to fetch. */
      reconstruct: (rows: Row[]) => unknown
      /** The caller's own result from a row COUNT alone — present exactly when their result carries no
       *  values, which is the case capture's tap can answer without decoding anything. */
      reconstructCount?: (count: number) => unknown
      /** Present when the statement asks for BOTH images: the table fields, in the order the positional
       *  `o<i>`/`n<i>` aliases carry them. */
      images?: string[]
    } & PrecisePlan)

/** Decide precise vs coarse for one write — every ambiguity fails closed to coarse. */
function planCapture(builder: unknown, table: Table, op: Op, db: object): Plan {
  const dialect = dialectOf(db)
  const pk = pkFieldsOf(table)
  // A retraction is keyed by the PK, so UPDATE and DELETE need one. An INSERT retracts nothing — it carries
  // its whole new row — so a table with no primary key is still captured exactly for it. (A STATEFUL graph
  // over a PK-less input is born coarse in liveGraph and ignores the precision either way; a STATELESS one
  // evaluates the new row against its predicate, which is where this win lands.)
  if (pk.length === 0 && op !== 'insert') return { mode: 'coarse' }
  const config = writeConfigOf(builder)
  if (!config) return { mode: 'coarse' } // unrecognized builder shape (version drift) → coarse
  if (hasOnConflict(config, dialect)) return { mode: 'coarse' } // UPSERT / ON CONFLICT
  if (op === 'insert' && hasRawValues(config)) return { mode: 'coarse' } // raw-SQL values clause

  const tableColumns = getTableColumns(table)
  const columns = Object.keys(tableColumns)
  // field name → physical column name. Built here so every precise plan carries it and no emission path can
  // forget to translate (see `physicalRow`).
  const physical = Object.fromEntries(
    Object.entries(tableColumns).map(([field, column]) => [field, (column as Column).name]),
  )
  const noCallerOrder: readonly string[] = EMPTY_FIELDS
  // BOTH images, where the connection is known to produce them (`RETURNING old.*, new.*`, PostgreSQL 18+).
  // An INSERT has no old image and needs none. The capability is probed once per db and is only ever `true`
  // when a real statement proved it — so this branch adds precision where it exists and changes nothing
  // anywhere else.
  const images =
    op !== 'insert' && dialect === 'pg' && oldNewReturningOf(db) ? Object.keys(getTableColumns(table)) : undefined
  // A PK-CHANGING update moves the very key a retraction is addressed by. Without the old image that key is
  // gone the moment the statement runs, and no RETURNING of the NEW row can recover it → coarse. WITH the
  // old image it is simply there, so the case stops being special (fork #2 closed on this lane).
  if (op === 'update' && !images && setTouchesPk(config, pk)) return { mode: 'coarse' }
  // The caller asked the DATABASE for the changed rows, and the database answered from the very session that
  // executed the statement — so a POOLED connection is precise here too, and this check sits ABOVE any
  // single-session gate deliberately. Session authority is a READ-HYDRATION argument: a pooled read can be
  // answered by a connection with a different role / search_path / RLS view than the one that was probed,
  // which is why `readCapture.ts` keeps its own pooled gate. No such probe is involved in a returned row, so
  // borrowing that gate here coarsens pooled PostgreSQL writes for a reason that does not apply to them.
  //
  if (config.returning !== undefined) {
    const selection = callerSelection(config.returning, table)
    // Not reproducible from a row image — a nested alias path, or a raw `SQL` expression the DATABASE
    // computed. Run exactly what the caller wrote and let `captureMismatch` fail it closed, as before.
    if (!selection) return { mode: 'precise', callerReturning: true, pk, columns, physical, callerOrder: noCallerOrder }
    const project = (rows: Row[]) => rows.map((row) => projectRow(row, selection))
    const callerOrder = selection.map((entry) => entry.field)
    // Both images are worth SUBSTITUTING for even when the caller's own RETURNING would have sufficed:
    // their result is reproducible from the image they would have been handed, and the old row is what
    // makes a stateless update exact and a key change describable.
    if (images)
      return {
        mode: 'precise',
        callerReturning: false,
        pk,
        columns,
        physical,
        callerOrder,
        reconstruct: project,
        images,
      }
    // Already the full image: nothing to widen, and the returned order is what names SQLite's positional rows.
    if (isFullImage(selection, columns))
      return {
        mode: 'precise',
        callerReturning: true,
        pk,
        columns,
        physical,
        callerOrder,
        positional: selection.map((entry) => entry.field),
      }
    // A PARTIAL or aliased projection is not a reason to give up. Widen the executed RETURNING to the whole
    // row, capture THAT, and project the caller's own columns back out of it. The caller sees exactly the
    // result they asked for; capture sees a real full row. No column is invented — this is the same row.
    return { mode: 'precise', callerReturning: false, pk, columns, physical, callerOrder, reconstruct: project }
  }

  // No caller returning → capture must SUBSTITUTE a hidden full RETURNING and hand the caller back a
  // reconstructed plain result, so reconstruction must be provably faithful for THIS driver. (PGlite is one
  // in-process connection by construction, so there is no pooled variant of this branch to gate.)
  if (dialect === 'pg' && driverOf(db) === 'PgliteDatabase') {
    return {
      mode: 'precise',
      callerReturning: false,
      pk,
      columns,
      physical,
      callerOrder: noCallerOrder,
      // PGlite's plain no-returning result is `{ rows: [], fields: [], affectedRows: N }`; affectedRows =
      // the RETURNING row count. Verified empirically against PGlite 18.3.
      reconstruct: (rows) => ({ rows: [], fields: [], affectedRows: rows.length }),
      // The same result from the count ALONE. It is a separate function rather than `reconstruct` over N
      // placeholder rows because the two are asked different questions: `reconstruct` is handed rows that
      // were decoded, this one is handed a number that was OBSERVED and no rows at all. Faking rows to reach
      // the first would be inventing the very values this path exists to avoid inventing.
      reconstructCount: (count) => ({ rows: [], fields: [], affectedRows: count }),
      images,
    }
  }
  // SQLite's `lastInsertRowid` is not recoverable for update/delete, and other drivers are unverified → coarse.
  return { mode: 'coarse' }
}

// ── where the caller's own columns sit in capture's layout ──────────

/** Capture's layout cannot produce one of the caller's own columns. */
const UNMAPPABLE = Symbol('telefunc: the caller projection has no position in capture’s layout')

/** Where each of the caller's ordered columns sits in the raw rows capture's statement returns — or
 *  `undefined` when their result carries no values and nothing is decoded on their behalf. */
function callerPositionsOf(
  plan: Extract<Plan, { callerReturning: false }>,
  op: Op,
): number[] | undefined | typeof UNMAPPABLE {
  if (plan.callerOrder.length === 0) return undefined
  const positions = plan.callerOrder.map((field) => rawPositionOf(field, plan, op))
  return positions.some((position) => position < 0) ? UNMAPPABLE : positions
}

/** Where in the substituted statement's raw row the given table field's value sits.
 *
 *  Both layouts are capture's own, so both are known exactly: a full-row RETURNING selects the table's
 *  columns in order, and a BOTH-IMAGES RETURNING interleaves them as `o0, n0, o1, n1, …` (see
 *  `substituteOldNew`) — of which the caller would have been handed the OLD image for a delete and the NEW
 *  one otherwise, exactly as `reconstruct` picks them. */
function rawPositionOf(field: string, plan: PrecisePlan & { images?: string[] }, op: Op): number {
  const index = (plan.images ?? plan.columns).indexOf(field)
  if (index < 0) return -1
  if (!plan.images) return index
  return op === 'delete' ? index * 2 : index * 2 + 1
}

/** Shared empty list — a write with no caller RETURNING has no fields the caller owns. */
const EMPTY_FIELDS: readonly string[] = []

// ── builder introspection (version-brittle, guarded — mirrors drizzleShape.ts) ───────────────────────

/** The PK columns' FIELD names (the keys `.returning()` rows use) — single OR composite. `primaryKeyOf`
 *  resolves the PK's DATABASE column names (composite keys live in the table's extra-config builder); those
 *  are translated back to field names via the table's column map, since a `.returning()` row is keyed by
 *  field, not db column. A PK column that doesn't resolve to a field (or no PK at all) yields `[]` → the
 *  caller coarsens (fail-closed). */
function pkFieldsOf(table: Table): string[] {
  const columnNames = primaryKeyOf(table)
  if (columnNames.length === 0) return []
  const fieldByColumnName = new Map<string, string>()
  for (const [field, column] of Object.entries(getTableColumns(table))) {
    fieldByColumnName.set((column as Column).name, field)
  }
  const fields = columnNames.map((name) => fieldByColumnName.get(name))
  return fields.every((field): field is string => field !== undefined) ? (fields as string[]) : []
}

type WriteConfig = {
  values?: unknown
  set?: Record<string, unknown>
  onConflict?: unknown
  returning?: unknown
  /** What `.returning()` records ALONGSIDE `returning` — the selection as written, which `getSelectedFields()`
   *  reports. Restored with it, so a substituted builder never describes a selection it no longer runs. */
  returningFields?: unknown
  select?: unknown
}

/** Read a write builder's `config` (typed-protected on PG, runtime-only on SQLite — cast either way),
 *  or `null` when the shape isn't the pinned one → caller falls back to coarse. */
function writeConfigOf(builder: unknown): WriteConfig | null {
  const config = (builder as { config?: unknown }).config
  return config !== null && typeof config === 'object' ? (config as WriteConfig) : null
}

/** ON CONFLICT / UPSERT: PG carries a single `SQL`; SQLite a non-empty `SQL[]`. */
function hasOnConflict(config: WriteConfig, dialect: Dialect): boolean {
  if (dialect === 'sqlite') return Array.isArray(config.onConflict) && config.onConflict.length > 0
  return config.onConflict !== undefined
}

/** A fully-raw values clause (`insert(t).values(sql\`…\`)`): the rows going IN are opaque, and no shape for
 *  the caller's result is pinned for it, so coarsen. Per-column raw values are fine (the RETURNING row is
 *  still the real row).
 *
 *  An insert-from-SELECT (`config.select === true`) is deliberately NOT caught by this: where the rows CAME
 *  from says nothing about the rows that went IN. `RETURNING` on an insert-from-select yields the real
 *  inserted rows (verified on PGlite), and its plain no-returning result is byte-identical in shape to an
 *  ordinary insert's — so both capture paths already handle it. */
function hasRawValues(config: WriteConfig): boolean {
  return is(config.values, SQL)
}

// ── the caller's RETURNING selection ────────────────────────────────

/** One entry of drizzle's ordered RETURNING selection: the alias path the returned row is keyed by, and
 *  what was selected there. `.returning()` with no argument builds the identity selection over every column.
 *  (Pinned against drizzle 1.0.0-rc.4, like the rest of this module's builder introspection.) */
type ReturningEntry = { path: string[]; field: unknown }

/** The caller's RETURNING selection as alias → table FIELD pairs, in the order the statement returns them,
 *  or `null` when their result could not be rebuilt from a full row image:
 *
 *   - a nested alias path (`.returning({ a: { b: col } })`) — the result shape is not a flat row;
 *   - anything that is not a plain column of THIS table, above all a raw `SQL` expression
 *     (`.returning({ n: sql\`id + 1\` })`): the DATABASE computed that value, and re-deriving it from the
 *     row would mean re-implementing SQL. That case keeps its old behaviour — run as written, fail closed.
 *
 *  Columns are matched by OBJECT IDENTITY against the table's own column map, so a same-named column of a
 *  different table cannot masquerade as one of ours. */
function callerSelection(returning: unknown, table: Table): { alias: string; field: string }[] | null {
  if (!Array.isArray(returning) || returning.length === 0) return null
  const fieldByColumn = new Map<unknown, string>()
  for (const [field, column] of Object.entries(getTableColumns(table))) fieldByColumn.set(column, field)
  const selection: { alias: string; field: string }[] = []
  for (const entry of returning as ReturningEntry[]) {
    if (!Array.isArray(entry?.path) || entry.path.length !== 1) return null
    const field = fieldByColumn.get(entry.field)
    if (field === undefined) return null
    selection.push({ alias: entry.path[0]!, field })
  }
  return selection
}

/** Whether a selection already yields exactly the image capture needs: every column, keyed by its own field
 *  name, and NOTHING else. An extra or renamed member (`.returning({ mine: users.id, … })`) is not a defect
 *  — it just means the row is widened and projected instead, so the emitted image is the table's row rather
 *  than the caller's bag of aliases. */
function isFullImage(selection: { alias: string; field: string }[], columns: string[]): boolean {
  if (selection.length !== columns.length) return false
  const identity = new Set(selection.filter((entry) => entry.alias === entry.field).map((entry) => entry.field))
  return columns.every((column) => identity.has(column))
}

/** The caller's requested result, taken out of a full row image. */
function projectRow(row: Row, selection: { alias: string; field: string }[]): Row {
  const projected: Row = {}
  for (const { alias, field } of selection) projected[alias] = row[field]
  return projected
}

/** Whether an UPDATE's SET assigns any PK column — a PK-changing update, whose old PK a RETURNING (new)
 *  can't recover on PG/SQLite (fork #2 → coarse). */
function setTouchesPk(config: WriteConfig, pk: string[]): boolean {
  return config.set !== undefined && pk.some((field) => field in config.set!)
}
