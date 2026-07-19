export { captureMutation, emitSafely, captureRawSql, planCapture, captureMismatch }
export type { CaptureSink }

import { type Column, SQL, type Table, getTableColumns, is, isTable, sql } from 'drizzle-orm'
import {
  demoteOldNewReturning,
  dialectOf,
  driverOf,
  markOldNewProven,
  oldNewProvenOf,
  oldNewReturningOf,
} from '../binding/database.js'
import { primaryKeyOf, relationKeyOf } from '../extract/columns.js'
import { describeRelationId } from '../ir/relation.js'
import { ingestWrite, registryFor } from './dbRuntime.js'
import { publishCoarseAll } from './writeTransport.js'
import type { Row, TableChange } from '../router/events.js'

// The write-capture engine. `reactiveDrizzle`'s proxy routes insert/update/delete here. The write runs as
// plain Drizzle; the terminal is intercepted to capture the changed rows and feed a `ChangeBatch` to the
// db's graphs (via the sink).
//
// Every emitted `TableChange.table` is a schema-qualified relation IDENTITY (`relationKeyOf`, see
// ir/relation.ts) — NOT the bare table name. The read side registers its graphs under the same identity, so
// a write to `a.users` reaches live queries on `a.users` and not the different physical table `b.users`.
//
// What a captured change carries:
//   INSERT  → { kind:'insert', new: full row }
//   DELETE  → { kind:'delete', key: PK }        (retraction by old PK)
//   UPDATE  → { kind:'update', new: full row, key: PK }   (key = old PK = new PK; non-PK-changing only)
//
// …and, where the connection can return BOTH images of a changed row in the write statement itself
// (`RETURNING old.*, new.*`, PostgreSQL 18+, capability-probed once per db — see `oldNewReturningOf`):
//   DELETE  → { kind:'delete', old: full row, key: PK }
//   UPDATE  → { kind:'update', old: full row, new: full row, key: OLD PK }
// which costs no extra round trip and makes two previously-coarse classes exact: an update that MOVES the
// primary key (the old key is right there), and any update a STATELESS live query must decide membership
// for (it can compare the two images instead of assuming the row may have entered or left).
//
// PRECISION is gated + fails closed (emit one {table, kind:'coarse'}) — safe over-fire, never a wrong row:
//   - PG/SQLite only (MySQL has no RETURNING → precise MySQL needs a pre-write SELECT + a live MySQL test
//     lane that does not exist in this package yet → deferred; MySQL stays sound-coarse);
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
// pooled gate in readCapture.ts — not for returned-row capture. See `planCapture`.

type Op = 'insert' | 'update' | 'delete'
/** Where a captured batch goes: straight to the db's graphs (autocommit) or a transaction buffer. */
type CaptureSink = (changes: TableChange[]) => void

function captureMutation(
  op: Op,
  baseMethod: (...a: unknown[]) => unknown,
  db: object,
  emit?: CaptureSink,
): (...a: unknown[]) => unknown {
  const sink: CaptureSink = emit ?? ((changes) => ingestWrite(db, { changes }))
  return (...args: unknown[]) => {
    const table = args[0]
    // insert/update/delete all take the target table as their first argument.
    if (!isTable(table)) return baseMethod(...args)
    return wrapWrite(baseMethod(...args), table, op, db, sink)
  }
}

/** Wrap a mutation builder so its terminal (`await` / `.execute()`) runs the write and captures its change;
 *  chain methods (`values`/`set`/`where`/`returning`/…) re-wrap so the terminal stays captured. */
function wrapWrite(builder: unknown, table: Table, op: Op, db: object, sink: CaptureSink): unknown {
  return new Proxy(builder as object, {
    get(target, prop, receiver) {
      // EVERY promise terminal routes through the captured run. `.catch()`/`.finally()` used to reach the
      // raw QueryPromise and execute the write uncaptured — a systematic missed invalidation.
      //
      // Each is gated on the underlying builder ACTUALLY having it. An insert builder before `.values()`
      // has none of them, so synthesizing them made `db.insert(t)` spuriously thenable: `await` on an
      // unfinished chain would run a write the caller never asked for, and `typeof b.then === 'function'`
      // lied about a proxy that is transparent except for `.live()`.
      const has = (name: string): boolean => typeof Reflect.get(target, name, receiver) === 'function'
      if (prop === 'then' && has('then')) {
        return (onFulfilled?: (v: unknown) => unknown, onRejected?: (e: unknown) => unknown) =>
          runWrite(target, table, op, db, sink).then(onFulfilled, onRejected)
      }
      if (prop === 'catch' && has('catch')) {
        return (onRejected?: (e: unknown) => unknown) => runWrite(target, table, op, db, sink).catch(onRejected)
      }
      if (prop === 'finally' && has('finally')) {
        return (onFinally?: () => void) => runWrite(target, table, op, db, sink).finally(onFinally)
      }
      if (prop === 'execute' && has('execute')) {
        return (...args: unknown[]) => runWrite(target, table, op, db, sink, args)
      }
      // Driver terminals that execute DIRECTLY (SQLite's run/all/get/values — SYNCHRONOUS on node:sqlite).
      // Gated on the underlying builder ACTUALLY having the member: PG and MySQL write builders have no
      // `run`/`all`/`get`, and synthesizing one made `typeof builder.get === 'function'` report true on a
      // proxy that is supposed to be transparent except for `.live()` — then died inside the interceptor
      // ("Cannot read properties of undefined") instead of with the driver's own error. Mirrors the
      // `prepare` guard below and the raw-SQL guard in reactiveDrizzle.
      if (typeof prop === 'string' && (DIRECT_TERMINALS.has(prop) || isTerminalValues(prop, target)) && has(prop)) {
        return (...args: unknown[]) => runDirectTerminal(target, prop, args, table, op, db, sink)
      }
      // A prepared write executes LATER; hand back a wrapped prepared query so each execution invalidates.
      if (prop === 'prepare') {
        const prepare = Reflect.get(target, prop, receiver)
        if (typeof prepare === 'function') {
          // Planned HERE, from the builder as it stands at `prepare()` — after that the shape is frozen, so
          // one plan covers every execution of the prepared statement.
          const plan = planCapture(target, table, op, db)
          return (...args: unknown[]) =>
            wrapPrepared((prepare as (...a: unknown[]) => unknown).apply(target, args), table, op, sink, plan)
        }
      }
      const value = Reflect.get(target, prop, receiver)
      if (typeof value !== 'function') return value
      return (...args: unknown[]) => {
        const next = (value as (...a: unknown[]) => unknown).apply(target, args)
        return isWriteBuilder(next) ? wrapWrite(next, table, op, db, sink) : next
      }
    },
  })
}

/** Driver terminals that run the statement immediately rather than through the QueryPromise (SQLite).
 *  `values` is NOT here because the name is overloaded — see `isTerminalValues`. (At the DB level
 *  `db.values(sql`…`)` IS a raw execution surface — see `isRawSqlOp` in reactiveDrizzle.) */
const DIRECT_TERMINALS = new Set(['run', 'all', 'get'])

/** `values` names TWO different things on a write chain, and only one of them executes:
 *
 *    db.insert(t).values(rows)              — the insert BUILDER's own method: supplies the rows, no SQL runs
 *    db.insert(t).values(rows).returning()
 *                             .values()     — a SQLite driver TERMINAL: runs the statement, returns rows
 *
 *  Discriminated by the RECEIVING OBJECT's surface, not by the argument shape: the chain builder
 *  (`SQLiteInsertBuilder`) exposes `values` and nothing else, while an executable statement also exposes the
 *  execution surface (`execute`/`then`). Argument shape alone is not enough — `.values()` with no arguments
 *  is exactly what the terminal looks like, and a caller could equally pass an empty row list.
 *
 *  Verified against node:sqlite: the chain form inserts nothing until a terminal runs, while the terminal
 *  form executes and returns positional row arrays. Those positional rows are why this path stays COARSE —
 *  mapping `[2, 'b']` back to named columns would mean assuming projection order, which is the kind of guess
 *  the capture contract forbids. `captureMismatch` catches it and fails closed. */
function isTerminalValues(prop: string, target: object): boolean {
  if (prop !== 'values') return false
  const candidate = target as { execute?: unknown; then?: unknown }
  return typeof candidate.execute === 'function' || typeof candidate.then === 'function'
}

const isThenable = (value: unknown): value is PromiseLike<unknown> =>
  typeof value === 'object' && value !== null && typeof (value as { then?: unknown }).then === 'function'

/** Run a DIRECT driver terminal and capture it. The caller's result must come back with its exact shape AND
 *  sync/async-ness (node:sqlite is synchronous), so we cannot substitute a hidden RETURNING here: the write
 *  fails closed to COARSE — except when the caller's own `.returning()` already yields a full row image,
 *  which is precise for free. Sync in, sync out; thenable in, thenable out. */
function runDirectTerminal(
  builder: object,
  prop: string,
  args: unknown[],
  table: Table,
  op: Op,
  db: object,
  sink: CaptureSink,
): unknown {
  const base = (builder as Record<string, (...a: unknown[]) => unknown>)[prop]!
  const result = base.apply(builder, args)
  const emit = (rows: unknown) => emitSafely(sink, directChanges(rows, builder, table, op, db))
  if (isThenable(result)) {
    return Promise.resolve(result).then((rows) => {
      emit(rows)
      return rows
    })
  }
  emit(result)
  return result
}

function directChanges(result: unknown, builder: object, table: Table, op: Op, db: object): TableChange[] {
  const relationId = relationKeyOf(table)
  return changesFromRows(result, planCapture(builder, table, op, db), relationId, op)
}

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

/** A PREPARED write: the statement executes later, possibly many times, but it is FROZEN at `prepare()` —
 *  the builder it came from can no longer change shape, so the plan made for that builder describes every
 *  execution. Each one is captured under it: a caller's own full `.returning()` yields real rows per
 *  execution, and anything else fails closed to COARSE for the target table rather than running uncaptured.
 *
 *  Only the caller's own returning qualifies, for the same reason as a direct terminal: capture never chose
 *  this statement's RETURNING and cannot widen it after the fact. Result shape and sync/async-ness are
 *  preserved either way. */
function wrapPrepared(prepared: unknown, table: Table, op: Op, sink: CaptureSink, plan: Plan): unknown {
  const relationId = relationKeyOf(table)
  return new Proxy(prepared as object, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver)
      if (typeof value !== 'function') return value
      // A prepared statement IS the executable surface, so `values` here is unambiguously the driver
      // terminal — there is no chain builder left to confuse it with.
      const isTerminal =
        prop === 'then' ||
        prop === 'catch' ||
        prop === 'finally' ||
        prop === 'execute' ||
        prop === 'values' ||
        (typeof prop === 'string' && DIRECT_TERMINALS.has(prop))
      if (!isTerminal) return (...args: unknown[]) => (value as (...a: unknown[]) => unknown).apply(target, args)
      return (...args: unknown[]) => {
        const result = (value as (...a: unknown[]) => unknown).apply(target, args)
        if (isThenable(result)) {
          return Promise.resolve(result).then((rows) => {
            emitSafely(sink, changesFromRows(rows, plan, relationId, op))
            return rows
          })
        }
        emitSafely(sink, changesFromRows(result, plan, relationId, op))
        return result
      }
    },
  })
}

/** Wrap a RAW-SQL execution surface on the reactive db (`db.run(sql\`…\`)`, `db.execute(sql\`…\`)`, …). The
 *  touched tables are unknowable without parsing SQL, so — per the owner disposition — this coarsens EVERY
 *  table that currently has a registered graph on this db (safe over-fire, keeps raw SQL usable) as ONE batch
 *  at completion. One honest bound: a raw READ also over-fires (sound, just noisy).
 *
 *  `announce` is how OTHER instances hear about it — they may watch tables this db does not, which a batch
 *  of THIS db's tables could never reach. It defaults to publishing a coarse-all announcement immediately,
 *  which is right for an autocommit statement. INSIDE A TRANSACTION the caller passes an announce that only
 *  records the intent, so nothing is published until the outer COMMIT and a rollback announces nothing —
 *  publishing mid-transaction would tell other instances to refetch state that may never exist. */
function captureRawSql(
  base: (...a: unknown[]) => unknown,
  db: object,
  sink: CaptureSink,
  announce: () => void = () => publishCoarseAll(db),
) {
  return (...args: unknown[]) => {
    const result = base(...args)
    const emit = () => {
      const tables = registryFor(db).router.watchedTables()
      if (tables.length > 0)
        emitSafely(
          sink,
          tables.map((table) => ({ table, kind: 'coarse' as const })),
        )
      try {
        announce()
      } catch (error) {
        reportCaptureFault(error, '*')
      }
    }
    if (isThenable(result)) {
      return Promise.resolve(result).then((rows) => {
        emit()
        return rows
      })
    }
    emit()
    return result
  }
}

async function runWrite(
  builder: unknown,
  table: Table,
  op: Op,
  db: object,
  sink: CaptureSink,
  executeArgs?: unknown[],
  allowImages = true,
): Promise<unknown> {
  const relationId = relationKeyOf(table)
  const plan = planCapture(builder, table, op, db, allowImages)

  if (plan.mode === 'coarse') {
    const result = await runBase(builder, executeArgs) // run the caller's write untouched
    emitSafely(sink, [{ table: relationId, kind: 'coarse' }]) // fail-closed: over-invalidate, never guess
    return result
  }

  if (plan.callerReturning) {
    // The caller asked for rows — run their write, and capture only if the rows carry the FULL image.
    const rows = (await runBase(builder, executeArgs)) as Row[]
    emitSafely(sink, captureOrCoarse(op, relationId, rows, plan))
    return rows
  }

  // Capture CHOOSES this statement's RETURNING: a full row image, or — where the connection can produce it
  // — both images of the row. The caller's own result is then rebuilt from what came back (verified faithful
  // for this driver / reproducible from their projection). The returned rows are expected to be full, but
  // that is still VERIFIED rather than trusted: this path once built changes unchecked, so a driver
  // returning a narrowed row would have emitted a partial image as precise.
  if (plan.images) {
    // An UNPROVEN capability was derived from the server's version number rather than from a statement that
    // ran (the temp-table probe was refused for lack of privilege). A fork can report version 18 and still
    // reject `RETURNING old.*, new.*`, so the first write that actually depends on it is guarded: believe
    // the statement over the version, demote permanently, and run the caller's write as they wrote it.
    //
    // The retry is safe because a statement that ERRORED had no effect — PostgreSQL applies a statement
    // wholly or not at all, so this is the caller's write happening for the first time, not a second time.
    // HONEST LIMIT: inside an already-open transaction the failed statement aborts that transaction, so the
    // retry cannot rescue it — that write fails, and every later one is correct. The window is one write on
    // a database whose version number lied.
    const proven = oldNewProvenOf(db)
    const config = writeConfigOf(builder)
    const callerReturning = config?.returning
    let rows: Row[]
    try {
      rows = (await runBase(substituteOldNew(builder, table, plan.images), executeArgs)) as Row[]
    } catch (error) {
      if (proven) throw error // this connection has DONE this — the failure is the caller's, untouched
      demoteOldNewReturning(db)
      reportOldNewDemotion(relationId, error)
      // `substituteOldNew` overwrote the builder's RETURNING in place, so the caller's own statement has to
      // be put back before re-planning — otherwise the retry would replay the very statement that failed.
      if (config) config.returning = callerReturning
      // `allowImages: false` — the retry cannot substitute again, so this is one fallback and not a loop,
      // by construction rather than by trusting the demotion above to have taken effect.
      return runWrite(builder, table, op, db, sink, executeArgs, false)
    }
    if (!proven) markOldNewProven(db) // it worked; later writes pay nothing for the guard
    const pairs = rows.map((row) => splitImages(row, plan.images!))
    emitSafely(sink, captureBothOrCoarse(op, relationId, pairs, plan))
    // PostgreSQL's plain RETURNING on a DELETE is the row that was deleted — the OLD image. On an UPDATE it
    // is the NEW one. The caller's result is rebuilt from whichever they would have been given.
    return plan.reconstruct(pairs.map((pair) => (op === 'delete' ? pair.old : pair.new)))
  }
  const full = (builder as { returning: () => unknown }).returning()
  const rows = (await runBase(full, executeArgs)) as Row[]
  emitSafely(sink, captureOrCoarse(op, relationId, rows, plan))
  return plan.reconstruct(rows)
}

// ── both images (`RETURNING old.*, new.*`) ──────────────────────────

/** A row's two images, as the write statement returned them. */
type Images = { old: Row; new: Row }

/** Replace the statement's RETURNING with one that asks for BOTH images of every column.
 *
 *  Aliases are POSITIONAL (`o0`/`n0`, …) rather than name-derived: capture owns every alias in this list, so
 *  positional ones are injective by construction, while any name-based scheme (`old_id`) could be made to
 *  collide by a table that has a column of that name. Each expression is mapped through its own column, so
 *  values arrive decoded exactly as drizzle would decode them anywhere else (a `timestamp` comes back a
 *  `Date`, not a string). */
function substituteOldNew(builder: unknown, table: Table, fields: string[]): unknown {
  const columns = getTableColumns(table)
  const selection: Record<string, SQL.Aliased | SQL> = {}
  fields.forEach((field, index) => {
    const column = columns[field] as Column
    const name = sql.identifier(column.name)
    selection[`o${index}`] = sql`old.${name}`.mapWith(column)
    selection[`n${index}`] = sql`new.${name}`.mapWith(column)
  })
  return (builder as { returning: (selection: unknown) => unknown }).returning(selection)
}

function splitImages(row: Row, fields: string[]): Images {
  const images: Images = { old: {}, new: {} }
  fields.forEach((field, index) => {
    images.old[field] = row[`o${index}`]
    images.new[field] = row[`n${index}`]
  })
  return images
}

// ── capture-fault isolation ─────────────────────────────────────────

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

/** The server rejected a statement that its own version number said it would accept. Reported once per db,
 *  because it changes how every later write on it is captured. */
function reportOldNewDemotion(relationId: string, error: unknown): void {
  console.error(
    `[telefunc] live: this PostgreSQL server reports version 18 or newer but rejected "RETURNING old.*, new.*" (first seen writing "${describeRelationId(relationId)}"). Falling back to new-image capture for this database; the write itself is unaffected. Live queries stay correct, with more coarse invalidation than a genuine PostgreSQL 18 would need.`,
    error,
  )
}

/** The diagnostics seam for a contained capture fault. Takes a relation IDENTITY and renders it the way a
 *  human wrote it (`a.users`), since this reaches an operator's logs. */
function reportCaptureFault(error: unknown, relationId: string): void {
  console.error(
    `[telefunc] live write-capture failed for table "${describeRelationId(relationId)}". The write COMMITTED and its result is unaffected; live queries on this table may be stale until the next write.`,
    error,
  )
}

// ── capture planning ────────────────────────────────────────────────

/** What a precise plan carries into capture: the PK fields a retraction is keyed by, the columns a full
 *  image must contain, and — only when the RETURNING selection already IS the full image — the table field
 *  each returned POSITION carries, which is what lets SQLite's positional `.values()` rows be named. */
type PrecisePlan = { pk: string[]; columns: string[]; positional?: string[] }

type Plan =
  | { mode: 'coarse' }
  | ({ mode: 'precise'; callerReturning: true } & PrecisePlan)
  | ({
      mode: 'precise'
      callerReturning: false
      /** The caller's own result, rebuilt from the rows capture chose to fetch. */
      reconstruct: (rows: Row[]) => unknown
      /** Present when the statement asks for BOTH images: the table fields, in the order the positional
       *  `o<i>`/`n<i>` aliases carry them. */
      images?: string[]
    } & PrecisePlan)

/** Decide precise vs coarse for one write — every ambiguity fails closed to coarse.
 *
 *  `allowImages` is how the OLD/NEW retry path forbids a second substitution. It makes that retry
 *  STRUCTURALLY single-shot instead of relying on the demotion side-effect to break the recursion. */
function planCapture(builder: unknown, table: Table, op: Op, db: object, allowImages = true): Plan {
  const dialect = dialectOf(db)
  if (dialect === 'mysql') return { mode: 'coarse' } // no RETURNING; precise MySQL (pre-write SELECT) deferred
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

  const columns = Object.keys(getTableColumns(table))
  // BOTH images, where the connection is known to produce them (`RETURNING old.*, new.*`, PostgreSQL 18+).
  // An INSERT has no old image and needs none. The capability is probed once per db and is only ever `true`
  // when a real statement proved it — so this branch adds precision where it exists and changes nothing
  // anywhere else.
  const images =
    allowImages && op !== 'insert' && dialect === 'pg' && oldNewReturningOf(db)
      ? Object.keys(getTableColumns(table))
      : undefined
  // A PK-CHANGING update moves the very key a retraction is addressed by. Without the old image that key is
  // gone the moment the statement runs, and no RETURNING of the NEW row can recover it → coarse. WITH the
  // old image it is simply there, so the case stops being special (fork #2 closed on this lane).
  if (op === 'update' && !images && setTouchesPk(config, pk)) return { mode: 'coarse' }
  // The caller asked the DATABASE for the changed rows, and the database answered from the very session that
  // executed the statement — so a POOLED connection is precise here too. This used to sit below a blanket
  // `!isSingleSession(db) → coarse` gate, which coarsened pooled PostgreSQL writes using an argument that
  // belongs to read hydration (a pooled read can be answered by a connection with a different role /
  // search_path / RLS view than the one that was probed). No such probe is involved in a returned row.
  //
  if (config.returning !== undefined) {
    const selection = callerSelection(config.returning, table)
    // Not reproducible from a row image — a nested alias path, or a raw `SQL` expression the DATABASE
    // computed. Run exactly what the caller wrote and let `captureMismatch` fail it closed, as before.
    if (!selection) return { mode: 'precise', callerReturning: true, pk, columns }
    const project = (rows: Row[]) => rows.map((row) => projectRow(row, selection))
    // Both images are worth SUBSTITUTING for even when the caller's own RETURNING would have sufficed:
    // their result is reproducible from the image they would have been handed, and the old row is what
    // makes a stateless update exact and a key change describable.
    if (images) return { mode: 'precise', callerReturning: false, pk, columns, reconstruct: project, images }
    // Already the full image: nothing to widen, and the returned order is what names SQLite's positional rows.
    if (isFullImage(selection, columns))
      return { mode: 'precise', callerReturning: true, pk, columns, positional: selection.map((s) => s.field) }
    // A PARTIAL or aliased projection is not a reason to give up. Widen the executed RETURNING to the whole
    // row, capture THAT, and project the caller's own columns back out of it. The caller sees exactly the
    // result they asked for; capture sees a real full row. No column is invented — this is the same row.
    return { mode: 'precise', callerReturning: false, pk, columns, reconstruct: project }
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
      // PGlite's plain no-returning result is `{ rows: [], fields: [], affectedRows: N }`; affectedRows =
      // the RETURNING row count. Verified empirically against PGlite 18.3.
      reconstruct: (rows) => ({ rows: [], fields: [], affectedRows: rows.length }),
      images,
    }
  }
  // SQLite's `lastInsertRowid` is not recoverable for update/delete, and other drivers are unverified → coarse.
  return { mode: 'coarse' }
}

// ── change construction ─────────────────────────────────────────────

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
  if (!mismatch) return changesOf(op, relationId, rows, plan.pk)
  reportCaptureMismatch(relationId, mismatch)
  return [coarse(relationId)]
}

function reportCaptureMismatch(relationId: string, mismatch: NonNullable<CaptureMismatch>): void {
  console.error(
    `[telefunc] live write-capture fell back to COARSE for table "${describeRelationId(relationId)}": captured row ${mismatch.rowIndex} has ${mismatch.reason} (${mismatch.detail}). The write is unaffected; live queries on this table over-invalidate rather than receive a partial row.`,
  )
}

/** Precise changes from BOTH images, else one coarse marker. The image that has to be trustworthy is the
 *  one the change is built from: an UPDATE is decided by both, a DELETE only by the row that was removed
 *  (PostgreSQL returns NEW as all-NULL for a delete, which is not a row and is never treated as one). */
function captureBothOrCoarse(op: Op, relationId: string, pairs: Images[], plan: PrecisePlan): TableChange[] {
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
      ? { table: relationId, kind: 'delete', old, key: keyOf(old, plan.pk) }
      : // The key is taken from the OLD image on purpose: it addresses the row as the graph knows it, which
        // is what makes an update that MOVES the key describable rather than coarse.
        { table: relationId, kind: 'update', old, new: fresh, key: keyOf(old, plan.pk) },
  )
}

function changesOf(op: Op, table: string, rows: Row[], pk: string[]): TableChange[] {
  return rows.map((row) => {
    if (op === 'insert') return { table, kind: 'insert', new: row }
    if (op === 'delete') return { table, kind: 'delete', key: keyOf(row, pk) }
    return { table, kind: 'update', new: row, key: keyOf(row, pk) }
  })
}

function keyOf(row: Row, pk: string[]): Row {
  const key: Row = {}
  for (const field of pk) key[field] = row[field]
  return key
}

const coarse = (table: string): TableChange => ({ table, kind: 'coarse' })

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
  select?: unknown
}

/** Read a write builder's `config` (typed-protected on PG/MySQL, runtime-only on SQLite — cast either way),
 *  or `null` when the shape isn't the pinned one → caller falls back to coarse. */
function writeConfigOf(builder: unknown): WriteConfig | null {
  const config = (builder as { config?: unknown }).config
  return config !== null && typeof config === 'object' ? (config as WriteConfig) : null
}

/** ON CONFLICT / UPSERT: PG/MySQL carry a single `SQL`; SQLite a non-empty `SQL[]`. */
function hasOnConflict(config: WriteConfig, dialect: 'pg' | 'sqlite'): boolean {
  if (dialect === 'sqlite') return Array.isArray(config.onConflict) && config.onConflict.length > 0
  return config.onConflict !== undefined
}

/** A fully-raw values clause (`insert(t).values(sql\`…\`)`): the rows going IN are opaque, and no shape for
 *  the caller's result is pinned for it, so coarsen. Per-column raw values are fine (the RETURNING row is
 *  still the real row).
 *
 *  An insert-from-SELECT (`config.select === true`) used to coarsen here too, on the same "raw" reflex. It
 *  does not belong: where the rows CAME from says nothing about the rows that went IN. `RETURNING` on an
 *  insert-from-select yields the real inserted rows (verified on PGlite), and its plain no-returning result
 *  is byte-identical in shape to an ordinary insert's — so both capture paths already handle it. */
function hasRawValues(config: WriteConfig): boolean {
  return is(config.values, SQL)
}

// ── the caller's RETURNING selection ────────────────────────────────

/** One entry of drizzle's ordered RETURNING selection: the alias path the returned row is keyed by, and
 *  what was selected there. `.returning()` with no argument builds the identity selection over every column.
 *  (Pinned against drizzle 1.0.0-rc.4, like the rest of this file's builder introspection.) */
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

// ── terminal execution ──────────────────────────────────────────────

function runBase(builder: unknown, executeArgs?: unknown[]): Promise<unknown> {
  return executeArgs
    ? (builder as { execute: (...a: unknown[]) => Promise<unknown> }).execute(...executeArgs)
    : Promise.resolve(builder as PromiseLike<unknown>)
}

/** A drizzle write query builder, distinguished from a plain method return (a `toSQL()` object, a Promise). */
function isWriteBuilder(value: unknown): boolean {
  return typeof value === 'object' && value !== null && typeof (value as { toSQL?: unknown }).toSQL === 'function'
}
