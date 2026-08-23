export { planCapture, callerPositionsOf, writeConfigOf, AS_WRITTEN, UNMAPPABLE }
export type { Op, Plan, PrecisePlan, SubstitutionPlan, WriteConfig }

import { type Column, SQL, type Table, getTableColumns, is } from 'drizzle-orm'
import { dialectOf, driverOf } from './binding/database.js'
import { oldNewReturningOf } from './writeCapabilities.js'
import { primaryKeyOf } from './extract/columns.js'
import type { Dialect } from '../ir/types.js'
import type { Row } from '../bus/router/events.js'
import { type ImageLayout, imageLayoutOf } from './imageLayout.js'
import type { Substituted } from './writeSubstitution.js'

// How ONE write is classified before anything runs — into exactly ONE of five strategies (see `Plan`):
// capture can name the changed rows (from the caller's own full image, or by substituting a fuller
// RETURNING), or it cannot and the table over-invalidates (`asWritten`). Everything here is a decision made
// from the BUILDER's shape — no statement has executed yet, and nothing in this module touches a connection.
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

/** What every capturing plan carries: the PK fields a retraction is keyed by, the columns a full image
 *  must contain, and the field→physical translation an emitted change needs. */
type PrecisePlan = {
  pk: string[]
  columns: string[]
  /** field name → PHYSICAL column name, for translating an emitted change into the graph's row space. */
  physical: Record<string, string>
  /** The table fields the CALLER's own RETURNING asked for, IN THE ORDER their selection lists them — which
   *  is the order their own mapper reads positions in. Empty when they asked for no rows at all, and when
   *  their projection is not rebuildable from a row image. On a `callerReturning` plan a NON-EMPTY order
   *  means the selection is exactly the full image, which is what licenses naming SQLite's positional
   *  `.values()` rows by it (see `namedRows`); on a substitution plan it is what lets their result be
   *  rebuilt from raw rows when capture's mapping of the same statement fails (see the count-observation
   *  tap). */
  callerOrder: readonly string[]
}

/** ONE strategy per write, decided before anything runs. The tag names both the statement that executes and
 *  how the caller is answered:
 *
 *    `asWritten`       the caller's statement untouched + ONE coarse marker. Every ambiguity lands here —
 *                      and so does the single-shot recovery from a refused substitution (see `AS_WRITTEN`).
 *    `callerReturning` the caller's statement untouched; their own returned rows are the captured image
 *                      (precise only when they carry the FULL image — verified after the fact, never trusted).
 *    `fullRow`         substitute capture's full-row RETURNING; the caller's own columns are projected back
 *                      out of the full image.
 *    `bothImages`      substitute the OLD/NEW both-images RETURNING (PostgreSQL 18+); the caller gets their
 *                      projection — or their plain count-shaped result where they asked for no rows.
 *    `countOnly`       substitute capture's full-row RETURNING; the caller's plain no-returning result is
 *                      reconstructed from the row count alone (driver-verified shape). */
type Plan = { strategy: 'asWritten' } | ({ strategy: 'callerReturning' } & PrecisePlan) | SubstitutionPlan

/** A plan whose statement capture REWRITES (`writeSubstitution.ts`), carrying its own reconstruction:
 *  `deliver` answers the caller from what the substituted execution produced. Reconstruction is CO-LOCATED
 *  with the branch that chose the strategy, so what a strategy substitutes and how it answers the caller
 *  are one decision — not one module's definition driven from another. */
type SubstitutionPlan = ({ strategy: 'fullRow' | 'countOnly'; images?: undefined } | BothImagesPlan) &
  PrecisePlan & {
    /** The caller's result — their tapped value/error where their result carries values, else rebuilt from
     *  capture's mapped rows, else from the tap's observed row count. Built by `delivery`. */
    deliver: (outcome: Substituted, mapped: Row[] | undefined) => unknown
  }

type BothImagesPlan = {
  strategy: 'bothImages'
  /** The OLD/NEW both-images convention, owned whole — alias emission, position math, split, and the
   *  delete/update asymmetry (see imageLayout.ts). */
  images: ImageLayout
}

/** Run the caller's statement exactly as written and coarsen — the fail-closed floor every ambiguity drops
 *  to, and the WHOLE of the recovery from a refused substitution: a plan that cannot substitute is what
 *  makes that recovery single-shot BY CONSTRUCTION rather than by discipline. One fact, one representation
 *  — the recovery is this value, not a flag threaded beside a plan that contradicts it. */
const AS_WRITTEN: Plan = { strategy: 'asWritten' }

// ── the table's field↔column relationship, built once per plan ──────

/** Everything planning needs to know about how the table's FIELDS relate to its physical COLUMNS. This
 *  relationship used to be constructed four times in this module — `planCapture`'s `physical` map,
 *  `pkFieldsOf`'s name→field map, `callerSelection`'s identity→field map, and a second `getTableColumns`
 *  for the images list. One relationship, one build, shared by every consumer of one plan. */
type TableShape = {
  /** Field names, in the table's declared column order — what a full image must contain. */
  columns: string[]
  /** field name → PHYSICAL column name, for translating an emitted change into the graph's row space. */
  physical: Record<string, string>
  /** PHYSICAL column name → field name — how a PK, resolved in DB-column space, translates back to the
   *  field keys a `.returning()` row uses. */
  fieldOfColumnName: Map<string, string>
  /** Column OBJECT IDENTITY → field name — how a caller's RETURNING selection is matched. Identity, so a
   *  same-named column of a different table cannot masquerade as one of ours. */
  fieldOfColumn: Map<unknown, string>
}

function tableShapeOf(table: Table): TableShape {
  const columns: string[] = []
  const physical: Record<string, string> = {}
  const fieldOfColumnName = new Map<string, string>()
  const fieldOfColumn = new Map<unknown, string>()
  for (const [field, column] of Object.entries(getTableColumns(table))) {
    const columnName = (column as Column).name
    columns.push(field)
    physical[field] = columnName
    fieldOfColumnName.set(columnName, field)
    fieldOfColumn.set(column, field)
  }
  return { columns, physical, fieldOfColumnName, fieldOfColumn }
}

/** Decide precise vs coarse for one write — every ambiguity fails closed to coarse. */
function planCapture(builder: unknown, table: Table, op: Op, db: object): Plan {
  const dialect = dialectOf(db)
  const shape = tableShapeOf(table)
  const pk = pkFieldsOf(table, shape)
  // A retraction is keyed by the PK, so UPDATE and DELETE need one. An INSERT retracts nothing — it carries
  // its whole new row — so a table with no primary key is still captured exactly for it. (A STATEFUL graph
  // over a PK-less input is born coarse in liveGraph and ignores the precision either way; a STATELESS one
  // evaluates the new row against its predicate, which is where this win lands.)
  if (pk.length === 0 && op !== 'insert') return AS_WRITTEN
  const config = writeConfigOf(builder)
  if (!config) return AS_WRITTEN // unrecognized builder shape (version drift) → coarse
  if (hasOnConflict(config, dialect)) return AS_WRITTEN // UPSERT / ON CONFLICT
  if (op === 'insert' && hasRawValues(config)) return AS_WRITTEN // raw-SQL values clause

  const { columns, physical } = shape
  // BOTH images, where the connection is known to produce them (`RETURNING old.*, new.*`, PostgreSQL 18+).
  // An INSERT has no old image and needs none. The capability is probed once per db and is only ever `true`
  // when a real statement proved it — so this branch adds precision where it exists and changes nothing
  // anywhere else.
  const images = op !== 'insert' && dialect === 'pg' && oldNewReturningOf(db) ? imageLayoutOf(shape.columns) : undefined
  // A PK-CHANGING update moves the very key a retraction is addressed by. Without the old image that key is
  // gone the moment the statement runs, and no RETURNING of the NEW row can recover it → coarse. WITH the
  // old image it is simply there, so the case stops being special (fork #2 closed on this lane).
  if (op === 'update' && !images && setTouchesPk(config, pk)) return AS_WRITTEN
  // The caller asked the DATABASE for the changed rows, and the database answered from the very session that
  // executed the statement — so a POOLED connection is precise here too, and this check sits ABOVE any
  // single-session gate deliberately. Session authority is a READ-HYDRATION argument: a pooled read can be
  // answered by a connection with a different role / search_path / RLS view than the one that was probed,
  // which is why `readCapture.ts` keeps its own pooled gate. No such probe is involved in a returned row, so
  // borrowing that gate here coarsens pooled PostgreSQL writes for a reason that does not apply to them.
  //
  if (config.returning !== undefined) {
    const selection = callerSelection(config.returning, shape)
    // Not reproducible from a row image — a nested alias path, or a raw `SQL` expression the DATABASE
    // computed. Run exactly what the caller wrote and let `captureMismatch` fail it closed, as before.
    if (!selection) return { strategy: 'callerReturning', pk, columns, physical, callerOrder: [] }
    const project = (rows: Row[]) => rows.map((row) => projectRow(row, selection))
    const callerOrder = selection.map((entry) => entry.field)
    // Both images are worth SUBSTITUTING for even when the caller's own RETURNING would have sufficed:
    // their result is reproducible from the image they would have been handed, and the old row is what
    // makes a stateless update exact and a key change describable.
    if (images)
      return { strategy: 'bothImages', pk, columns, physical, callerOrder, images, deliver: delivery(project) }
    // Already the full image: nothing to widen, and the returned order is what names SQLite's positional rows.
    if (isFullImage(selection, columns)) return { strategy: 'callerReturning', pk, columns, physical, callerOrder }
    // A PARTIAL or aliased projection is not a reason to give up. Widen the executed RETURNING to the whole
    // row, capture THAT, and project the caller's own columns back out of it. The caller sees exactly the
    // result they asked for; capture sees a real full row. No column is invented — this is the same row.
    return { strategy: 'fullRow', pk, columns, physical, callerOrder, deliver: delivery(project) }
  }

  // No caller returning → capture must SUBSTITUTE a hidden full RETURNING and hand the caller back a
  // reconstructed plain result, so reconstruction must be provably faithful for THIS driver. (PGlite is one
  // in-process connection by construction, so there is no pooled variant of this branch to gate.)
  if (dialect === 'pg' && driverOf(db) === 'PgliteDatabase') {
    // PGlite's plain no-returning result is `{ rows: [], fields: [], affectedRows: N }`; affectedRows =
    // the RETURNING row count. Verified empirically against PGlite 18.3. Two closures rather than one over
    // N placeholder rows, because they are asked different questions: `reconstruct` is handed rows that
    // were decoded; `reconstructCount` a number that was OBSERVED and no rows at all. Faking rows to reach
    // the first would be inventing the very values this path exists to avoid inventing.
    const reconstruct = (rows: Row[]) => ({ rows: [], fields: [], affectedRows: rows.length })
    const deliver = delivery(reconstruct, (count) => ({ rows: [], fields: [], affectedRows: count }))
    return images
      ? { strategy: 'bothImages', pk, columns, physical, callerOrder: [], images, deliver }
      : { strategy: 'countOnly', pk, columns, physical, callerOrder: [], deliver }
  }
  // SQLite's `lastInsertRowid` is not recoverable for update/delete, and other drivers are unverified → coarse.
  return AS_WRITTEN
}

// ── how a substituted execution answers the caller ──────────────────

/** The caller's result out of ONE substituted execution — reconstruction CO-LOCATED with the plan branch
 *  that decides it, driven through `SubstitutionPlan.deliver`.
 *
 *  Their own tapped result wins wherever their result carries values: theirs to receive, or theirs to be
 *  thrown (a decoder on a column THEY selected, failing exactly where their own statement would have).
 *  A caller who asked for no rows gets the count-shaped result — from capture's mapped rows when capture
 *  mapped them, and otherwise from the rows the tap OBSERVED. Where neither exists there is no measured
 *  number, so the failure is rethrown rather than answered. */
function delivery(reconstruct: (rows: Row[]) => unknown, reconstructCount?: (count: number) => unknown) {
  return (outcome: Substituted, mapped: Row[] | undefined): unknown => {
    const caller = outcome.caller
    if (caller) {
      if (caller.ok) return caller.value
      throw caller.error
    }
    if (mapped) return reconstruct(mapped)
    if (outcome.raw && reconstructCount) return reconstructCount(outcome.raw.length)
    throw outcome.captureError
  }
}

// ── where the caller's own columns sit in capture's layout ──────────

/** Capture's layout cannot produce one of the caller's own columns. */
const UNMAPPABLE = Symbol('telefunc: the caller projection has no position in capture’s layout')

/** Where each of the caller's ordered columns sits in the raw rows capture's statement returns — or
 *  `undefined` when their result carries no values and nothing is decoded on their behalf. */
function callerPositionsOf(plan: SubstitutionPlan, op: Op): number[] | undefined | typeof UNMAPPABLE {
  if (plan.callerOrder.length === 0) return undefined
  const positions = plan.callerOrder.map((field) => rawPositionOf(field, plan, op))
  return positions.some((position) => position < 0) ? UNMAPPABLE : positions
}

/** Where in the substituted statement's raw row the given table field's value sits.
 *
 *  Both layouts are capture's own, so both are known exactly: a full-row RETURNING selects the table's
 *  columns in order, and the both-images interleaving is the `ImageLayout`'s to answer. */
function rawPositionOf(field: string, plan: SubstitutionPlan, op: Op): number {
  if (plan.images) return plan.images.rawPositionOf(field, op)
  return plan.columns.indexOf(field)
}

// ── builder introspection (version-brittle, guarded — mirrors drizzleShape.ts) ───────────────────────

/** The PK columns' FIELD names (the keys `.returning()` rows use) — single OR composite. `primaryKeyOf`
 *  resolves the PK's DATABASE column names (composite keys live in the table's extra-config builder); those
 *  are translated back to field names via the shape's column-name map, since a `.returning()` row is keyed
 *  by field, not db column. A PK column that doesn't resolve to a field (or no PK at all) yields `[]` → the
 *  caller coarsens (fail-closed). */
function pkFieldsOf(table: Table, shape: TableShape): string[] {
  const columnNames = primaryKeyOf(table)
  if (columnNames.length === 0) return []
  const fields = columnNames.map((name) => shape.fieldOfColumnName.get(name))
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
 *  Columns are matched by OBJECT IDENTITY (the shape's identity map), so a same-named column of a
 *  different table cannot masquerade as one of ours. */
function callerSelection(returning: unknown, shape: TableShape): { alias: string; field: string }[] | null {
  if (!Array.isArray(returning) || returning.length === 0) return null
  const selection: { alias: string; field: string }[] = []
  for (const entry of returning as ReturningEntry[]) {
    if (!Array.isArray(entry?.path) || entry.path.length !== 1) return null
    const field = shape.fieldOfColumn.get(entry.field)
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
