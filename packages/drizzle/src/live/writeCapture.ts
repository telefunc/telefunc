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
  tx?: object,
): (...a: unknown[]) => unknown {
  const sink: CaptureSink = emit ?? ((changes) => ingestWrite(db, { changes }))
  return (...args: unknown[]) => {
    const table = args[0]
    // insert/update/delete all take the target table as their first argument.
    if (!isTable(table)) return baseMethod(...args)
    return wrapWrite(baseMethod(...args), table, op, db, sink, tx)
  }
}

/** Wrap a mutation builder so its terminal (`await` / `.execute()`) runs the write and captures its change;
 *  chain methods (`values`/`set`/`where`/`returning`/…) re-wrap so the terminal stays captured. */
function wrapWrite(builder: unknown, table: Table, op: Op, db: object, sink: CaptureSink, tx?: object): unknown {
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
      // ONE write at a time per transaction. The serialization sits HERE, around the whole captured write,
      // rather than around the substituted statement alone: a savepoint's `ROLLBACK TO` rewinds everything
      // done after it, so an interleaved write's already-completed recovery statement would be undone by a
      // neighbour rewinding. Observed — three concurrent inserts on one transaction committed one row.
      // Outside a transaction there is nothing to serialize and writes stay fully concurrent.
      const start = (args?: unknown[]): Promise<unknown> => {
        const run = () => runWrite(target, table, op, db, sink, tx, args)
        return tx ? serializePerTx(tx, run) : run()
      }
      if (prop === 'then' && has('then')) {
        return (onFulfilled?: (v: unknown) => unknown, onRejected?: (e: unknown) => unknown) =>
          start().then(onFulfilled, onRejected)
      }
      if (prop === 'catch' && has('catch')) {
        return (onRejected?: (e: unknown) => unknown) => start().catch(onRejected)
      }
      if (prop === 'finally' && has('finally')) {
        return (onFinally?: () => void) => start().finally(onFinally)
      }
      if (prop === 'execute' && has('execute')) {
        return (...args: unknown[]) => start(args)
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
        return isWriteBuilder(next) ? wrapWrite(next, table, op, db, sink, tx) : next
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
  tx: object | undefined,
  executeArgs?: unknown[],
  asWritten = false,
): Promise<unknown> {
  const relationId = relationKeyOf(table)
  // `asWritten` is the recovery from a refused substitution: run the caller's statement and nothing else.
  // Expressed as a coarse plan rather than a flag threaded through planning, so the recovery provably
  // cannot substitute again — that is what makes it single-shot by construction rather than by discipline.
  const plan: Plan = asWritten ? { mode: 'coarse' } : planCapture(builder, table, op, db)

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

  // From here capture CHOOSES the statement — a full row image, or both images of the row where the
  // connection can produce them. THE CALLER DID NOT ASK FOR THIS, so it must never be what fails their
  // write: `runSubstituted` puts their own statement back and runs that instead if it does.
  //
  // The caller's result is then rebuilt from what came back (verified faithful for this driver, or
  // reproducible from their own projection). The rows are expected to be full, but that is still VERIFIED
  // rather than trusted: this path once built changes unchecked, so a driver returning a narrowed row would
  // have emitted a partial image as precise.
  if (plan.images) {
    const rows = await runSubstituted(
      () => substituteOldNew(builder, table, plan.images!),
      builder,
      tx,
      executeArgs,
      relationId,
    )
    if (rows === SUBSTITUTION_REFUSED) {
      // An UNPROVEN capability came from the server's version number rather than from a statement that ran
      // (the temp-table probe was refused for lack of privilege). A fork can report 18 and still reject
      // `RETURNING old.*, new.*` — so believe the statement over the version, permanently.
      if (!oldNewProvenOf(db)) {
        demoteOldNewReturning(db)
        reportOldNewDemotion(relationId)
      }
      return recoverAsWritten(builder, table, op, db, sink, tx, executeArgs)
    }
    if (!oldNewProvenOf(db)) markOldNewProven(db) // it worked; later writes pay nothing for the guard
    const pairs = rows.map((row) => splitImages(row, plan.images!))
    emitSafely(sink, captureBothOrCoarse(op, relationId, pairs, plan))
    // PostgreSQL's plain RETURNING on a DELETE is the row that was deleted — the OLD image. On an UPDATE it
    // is the NEW one. The caller's result is rebuilt from whichever they would have been given.
    return plan.reconstruct(pairs.map((pair) => (op === 'delete' ? pair.old : pair.new)))
  }
  const rows = await runSubstituted(() => substituteFullRow(builder, table), builder, tx, executeArgs, relationId)
  if (rows === SUBSTITUTION_REFUSED) return recoverAsWritten(builder, table, op, db, sink, tx, executeArgs)
  emitSafely(sink, captureOrCoarse(op, relationId, rows, plan))
  return plan.reconstruct(rows)
}

// ── capture's own statement must never fail the caller's write ──────

/** The substituted statement was refused for a reason the SUBSTITUTION introduced. */
const SUBSTITUTION_REFUSED = Symbol('telefunc: capture substitution refused')

/** Run a statement CAPTURE chose in place of the caller's, and tell a refusal of the substitution apart from
 *  a refusal of the write.
 *
 *  The caller never asked for a `RETURNING` clause, so it must never be the reason their write fails — and
 *  it genuinely can be: a role with INSERT but not SELECT on the table commits a plain insert and is
 *  refused `INSERT … RETURNING *` with 42501. That is capture breaking a write that was fine.
 *
 *  Refusals are told apart by SQLSTATE CLASS, not by guessing. A substituted RETURNING can only ever produce
 *  an access, syntax or unsupported-feature error; it cannot make a row violate a constraint, deadlock, or
 *  fail a trigger. So classes 42 (syntax/access), 0A (feature not supported) and 28/22 are treated as
 *  capture's fault and recovered from, while a class-23 integrity violation is the CALLER'S error and is
 *  re-thrown untouched — retrying that would only mean two failed statements in their log, and inside a
 *  transaction it would replace their real error with "current transaction is aborted".
 *
 *  Also restores the caller's own RETURNING before returning the refusal: the substitution overwrote it IN
 *  PLACE, so without this the recovery would replay the very statement that just failed. */
async function runSubstituted(
  substitute: () => unknown,
  builder: unknown,
  tx: object | undefined,
  executeArgs: unknown[] | undefined,
  relationId: string,
): Promise<Row[] | typeof SUBSTITUTION_REFUSED> {
  const config = writeConfigOf(builder)
  const callerReturning = config?.returning
  // Restored on EVERY exit, not just the failing ones. The substitution overwrites the builder's RETURNING
  // in place, so leaving it overwritten after a SUCCESSFUL capture means a second `await` of the same
  // builder re-runs capture's statement and hands the caller capture's rows instead of their own result.
  const restore = () => {
    if (config) config.returning = callerReturning
  }
  // INSIDE A TRANSACTION the recovery needs a savepoint, because PostgreSQL aborts the whole transaction on
  // the refused statement and re-running would only get "current transaction is aborted". Without one — an
  // unverified dialect, or a SAVEPOINT the driver would not issue — there is nothing to recover to, so the
  // substitution is not attempted at all and the caller's statement runs as written (coarse).
  const attempt = async (): Promise<Row[] | typeof SUBSTITUTION_REFUSED> => {
    const savepoint = tx ? await openSavepoint(tx, relationId) : NO_SAVEPOINT_NEEDED
    if (savepoint === SAVEPOINT_UNAVAILABLE) {
      restore()
      return SUBSTITUTION_REFUSED
    }
    try {
      const rows = (await runBase(substitute(), executeArgs)) as Row[]
      await savepoint.release()
      return rows
    } catch (error) {
      if (!isSubstitutionFault(error)) {
        // The caller's own error. Their transaction is aborted exactly as plain Drizzle would have left it,
        // so the savepoint is abandoned rather than rolled back to — rewinding would HIDE their failure.
        await savepoint.abandon()
        throw error
      }
      await savepoint.rewind() // un-abort the transaction so the caller's statement can still run
      reportSubstitutionRefused(relationId, error)
      return SUBSTITUTION_REFUSED
    } finally {
      restore()
    }
  }
  // Serialization happens one level up, around the WHOLE write — see `wrapWrite`. It has to: this attempt's
  // `ROLLBACK TO` would otherwise undo another write's already-completed recovery statement.
  return attempt()
}

/** Re-run the caller's write EXACTLY as they wrote it, and coarsen. Single-shot by construction: this path
 *  plans with substitution forbidden, so it cannot land back here.
 *
 *  Safe to re-run because the refused statement had no effect — PostgreSQL applies a statement wholly or not
 *  at all, so this is the caller's write happening for the first time, not a second time.
 *
 *  HONEST LIMIT, in-transaction only: inside an already-open transaction the refused statement aborts the
 *  whole transaction, so this re-run cannot rescue it and the caller's write is lost with a "current
 *  transaction is aborted" error. Closing that needs a SAVEPOINT around the substituted attempt, which needs
 *  the executing transaction handle — and capture is deliberately handed the TOP db (`txProxy` passes
 *  `topDb` so registry keying stays with the db that owns the graphs), so no such handle reaches here.
 *  Autocommit writes — the overwhelming majority, and every write on a db with no transaction open — are
 *  fully covered. */
async function recoverAsWritten(
  builder: unknown,
  table: Table,
  op: Op,
  db: object,
  sink: CaptureSink,
  tx: object | undefined,
  executeArgs: unknown[] | undefined,
): Promise<unknown> {
  return runWrite(builder, table, op, db, sink, tx, executeArgs, true)
}

// ── savepoints, so the in-transaction recovery has something to rewind to ──
//
// Established through the RAW transaction handle, never the tx proxy: the proxy treats `execute` as raw SQL
// (`isRawSqlOp`), so a SAVEPOINT issued through it would be recorded as raw INTENT and coarsen every watched
// graph in the transaction at commit — this recovery would then degrade every transaction it touched, which
// is worse than the problem it solves. (Verified against drizzle 1.0.0-rc.4, which issues its own nested-tx
// savepoints through exactly this handle.)

/** The bracket around one substituted statement. `release` on success, `rewind` to undo a refusal, `abandon`
 *  when the caller's own statement failed and their transaction should stay aborted. */
type Savepoint = { release: () => Promise<void>; rewind: () => Promise<void>; abandon: () => Promise<void> }

/** No transaction: the statement stands alone, and a failure aborts nothing to recover from. */
const NO_SAVEPOINT_NEEDED: Savepoint = {
  release: async () => {},
  rewind: async () => {},
  abandon: async () => {},
}
/** A savepoint could not be established, so the substitution must not be attempted. */
const SAVEPOINT_UNAVAILABLE = Symbol('telefunc: no savepoint available')

/** Serializes whole substituted ATTEMPTS per transaction — savepoint, statement and release as one unit.
 *
 *  `RELEASE SAVEPOINT` destroys every savepoint established after it, so two concurrent writes on one
 *  transaction (`Promise.all` over the same tx) interleave as save-A, save-B, release-A: B's savepoint is
 *  gone, B's release errors, and a failed statement aborts the transaction. Observed, not theorised — three
 *  concurrent inserts committed zero rows. Unique names do not help; it is establishment ORDER, not naming.
 *
 *  PostgreSQL executes one statement at a time per connection anyway, so nothing real is lost by queueing. */
const txSerial = new WeakMap<object, Promise<unknown>>()
let savepointCount = 0

function serializePerTx<T>(tx: object, attempt: () => Promise<T>): Promise<T> {
  const previous = txSerial.get(tx) ?? Promise.resolve()
  const next = previous.then(attempt, attempt)
  // Kept off the chain's failure path: one attempt's rejection must not reject every later one.
  txSerial.set(
    tx,
    next.then(
      () => {},
      () => {},
    ),
  )
  return next
}

/** Open a savepoint on the executing transaction, or report that none is available.
 *
 *  SAVEPOINT-FIRST and fail-safe: if establishing it fails, the substituted statement is never run, so this
 *  machinery cannot cause the very harm it exists to prevent. Scoped to PostgreSQL, the only dialect this is
 *  driven against — SQLite has savepoints too, but its raw surface is `run` rather than `execute` and
 *  node:sqlite is synchronous, so symmetry is assumed nowhere. */
async function openSavepoint(tx: object, relationId: string): Promise<Savepoint | typeof SAVEPOINT_UNAVAILABLE> {
  if (dialectOf(tx as { dialect?: unknown }) !== 'pg') return SAVEPOINT_UNAVAILABLE
  const execute = (tx as { execute?: (query: SQL) => Promise<unknown> }).execute
  if (typeof execute !== 'function') return SAVEPOINT_UNAVAILABLE
  // NOT `sp<n>` — that is drizzle's own nested-transaction namespace.
  const name = `telefunc_cap_${savepointCount++}`
  const run = (statement: string) => execute.call(tx, sql.raw(statement)) as Promise<unknown>
  try {
    await run(`savepoint ${name}`)
  } catch (error) {
    reportSavepointUnavailable(relationId, error)
    return SAVEPOINT_UNAVAILABLE
  }
  // A failed SAVEPOINT statement aborts the transaction like any other, so bookkeeping failures cannot be
  // swallowed: doing that once turned a silently-destroyed savepoint into a transaction that committed
  // nothing while every assertion about the writes still passed. Contained (the caller's write is already
  // decided by this point) but never silent — and the caller still sees it, because their COMMIT will fail.
  const bookkeeping = (statement: string) =>
    run(statement).then(
      () => {},
      (error: unknown) => reportSavepointBookkeepingFailed(relationId, statement, error),
    )
  return {
    release: () => bookkeeping(`release savepoint ${name}`),
    // RELEASE after ROLLBACK TO as well: PostgreSQL keeps the savepoint alive otherwise, and a long
    // transaction full of recovered writes would accumulate them.
    rewind: async () => {
      await bookkeeping(`rollback to savepoint ${name}`)
      await bookkeeping(`release savepoint ${name}`)
    },
    abandon: async () => {},
  }
}

function reportSavepointUnavailable(relationId: string, error: unknown): void {
  console.error(
    `[telefunc] live: could not open a savepoint to capture a write on "${describeRelationId(relationId)}" inside a transaction. The write runs exactly as you wrote it and is unaffected; live queries on this table over-invalidate.`,
    error,
  )
}

function reportSavepointBookkeepingFailed(relationId: string, statement: string, error: unknown): void {
  console.error(
    `[telefunc] live: "${statement}" failed while capturing a write on "${describeRelationId(relationId)}". The surrounding transaction is likely aborted and its COMMIT will fail — this is a bug in Telefunc's capture, please report it.`,
    error,
  )
}

/** Whether an error is one capture's substituted RETURNING could have caused.
 *
 *  Two questions, in order. Did the DATABASE reject the statement — that is a SQLSTATE, and its class says
 *  whose fault it is. Or did nothing reach the database at all, in which case the failure came from code
 *  that only ran BECAUSE capture asked for rows.
 *
 *  That second case is not hypothetical: a `customType` whose `fromDriver` throws explodes while decoding
 *  rows the caller never requested. The write COMMITS and the caller is handed the decoder's error — a
 *  committed write reported as a failure, which is the worst form of this whole class of bug. Such an error
 *  carries no SQLSTATE, because no database refused anything. */
function isSubstitutionFault(error: unknown): boolean {
  const state = sqlStateOf(error)
  // NO SQLSTATE means the database never refused anything — so the statement may well have COMMITTED and
  // the failure came afterwards, in capture's own handling of the result. That case must never be recovered
  // by re-running the write: it would apply it a second time. It is handled where it arises instead, in the
  // isolated post-commit domain (`captureResult`), and cannot reach here.
  if (state === undefined) return false
  return SUBSTITUTION_FAULT_CLASSES.has(state.slice(0, 2))
}

/** The first SQLSTATE in the cause chain — drizzle re-throws driver errors wrapped, and the code lives on
 *  the original. SQLSTATEs are exactly five characters, which is what tells one apart from a transport
 *  code like `ECONNRESET`. */
function sqlStateOf(error: unknown): string | undefined {
  for (let current = error, depth = 0; current != null && depth < 5; depth++) {
    const code = (current as { code?: unknown }).code
    if (typeof code === 'string' && code.length === 5) return code
    current = (current as { cause?: unknown }).cause
  }
  return undefined
}

/** `42` insufficient privilege / syntax error / undefined column, `0A` feature not supported, `28` invalid
 *  authorization. Deliberately NOT `23` (integrity constraint): no RETURNING clause can cause one, so such
 *  an error belongs to the caller's write and must reach them unchanged. */
const SUBSTITUTION_FAULT_CLASSES = new Set(['42', '0A', '28'])

// ── both images (`RETURNING old.*, new.*`) ──────────────────────────

/** A row's two images, as the write statement returned them. */
type Images = { old: Row; new: Row }

/** Replace the statement's RETURNING with one that asks for BOTH images of every column.
 *
 *  The two images are bound to CORRELATION NAMES via `RETURNING WITH (OLD AS …, NEW AS …)` rather than
 *  written as bare `old.col` / `new.col`. Bare names are resolved against the query's own scope first, so
 *  on a table literally named `old` they silently select that TABLE's post-update row: an update to
 *  `old` returned {old: 11, new: 11} where the truth was {old: 10, new: 11} — a wrong row image reported
 *  as precise. Verified on PGlite. One form, applied unconditionally: a rule that only engages for names
 *  that look dangerous is a rule that is never exercised until it matters.
 *
 *  `WITH (…)` must sit immediately after RETURNING, which drizzle's builder has no clause for — so it
 *  rides the FIRST selected expression, where it lands in exactly that position. The emitted SQL is
 *  `returning with (old as "tf_old__", new as "tf_new__") "tf_old__"."id", …`.
 *
 *  Aliases are POSITIONAL (`o0`/`n0`, …): capture owns every alias here, so positional ones are injective
 *  by construction where a name-derived scheme could be made to collide by a column of that name.
 *
 *  Each expression is decoded through its own column, so values arrive exactly as drizzle would decode them
 *  anywhere else — and through `safeDecoder`, so a user's `fromDriver` cannot throw into a write that has
 *  already committed (see `DECODE_FAILED`). */
const OLD_IMAGE = 'tf_old__'
const NEW_IMAGE = 'tf_new__'

// ── the post-commit failure domain ──────────────────────────────────
//
// Once the substituted statement SUCCEEDS the write is committed and irreversible. Everything capture does
// with the result after that point — decoding values, splitting the two images, verifying the row, building
// changes — happens for capture's benefit, not the caller's, and must not be able to fail their write.
//
// The sharp edge is DECODING. A `customType` whose `fromDriver` throws explodes while mapping rows the
// caller never asked for: the row commits and they are handed the decoder's error, a successful write
// reported as a failure. And it cannot be recovered by re-running, because re-running would write twice.
//
// So capture decodes through its OWN mappers, which never throw. A value that will not decode becomes
// `DECODE_FAILED`, the image is then untrustworthy, and the write coarsens — while the caller still gets
// their faithfully reconstructed plain result, because the ROW COUNT survived.

/** A value whose own decoder threw. Never emitted: its presence coarsens the write. */
const DECODE_FAILED = Symbol('telefunc: value could not be decoded')

/** Whether any captured value failed to decode — checked before a row is trusted as an image. */
function hasUndecodable(row: Row): boolean {
  for (const value of Object.values(row)) if (value === DECODE_FAILED) return true
  return false
}

function substituteOldNew(builder: unknown, table: Table, fields: string[]): unknown {
  const columns = getTableColumns(table)
  const selection: Record<string, SQL.Aliased | SQL> = {}
  fields.forEach((field, index) => {
    const column = columns[field] as Column
    const name = sql.identifier(column.name)
    const oldRef = sql`${sql.identifier(OLD_IMAGE)}.${name}`
    selection[`o${index}`] = (
      index === 0
        ? sql`with (old as ${sql.identifier(OLD_IMAGE)}, new as ${sql.identifier(NEW_IMAGE)}) ${oldRef}`
        : oldRef
    ).mapWith(column)
    selection[`n${index}`] = sql`${sql.identifier(NEW_IMAGE)}.${name}`.mapWith(column)
  })
  return (builder as { returning: (selection: unknown) => unknown }).returning(selection)
}

/** Replace the RETURNING with capture's own full-row selection.
 *
 *  Built explicitly rather than via a bare `.returning()` so the DECODERS can be chosen per column: a
 *  column the CALLER asked for keeps its real decoder, because their own statement would have decoded it
 *  identically and must fail identically; every other column — the ones capture added — decodes safely and
 *  can never throw into a committed write. */
function substituteFullRow(builder: unknown, table: Table): unknown {
  const selection: Record<string, SQL.Aliased | SQL> = {}
  for (const [field, column] of Object.entries(getTableColumns(table))) {
    selection[field] = sql`${sql.identifier((column as Column).name)}`.mapWith(column as Column)
  }
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

/** The server refused the RETURNING clause CAPTURE added — most often a role that may write the table but
 *  not read it. The caller's write is re-run as they wrote it, so the only cost is a coarse invalidation. */
function reportSubstitutionRefused(relationId: string, error: unknown): void {
  console.error(
    `[telefunc] live: the database refused the RETURNING clause Telefunc added to a write on "${describeRelationId(relationId)}" (a role that can write a table but not SELECT from it does this). The write is being re-run exactly as you wrote it and is unaffected; live queries on this table over-invalidate rather than lose the write.`,
    error,
  )
}

/** The server rejected a statement its own version number said it would accept. Reported once per db,
 *  because it changes how every later write on it is captured. */
function reportOldNewDemotion(relationId: string): void {
  console.error(
    `[telefunc] live: this PostgreSQL server reports version 18 or newer but refused "RETURNING old.*, new.*" (first seen writing "${describeRelationId(relationId)}"). Falling back to new-image capture for this database. Live queries stay correct, with more coarse invalidation than a genuine PostgreSQL 18 would need.`,
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
type PrecisePlan = {
  pk: string[]
  columns: string[]
  positional?: string[]
  /** field name → PHYSICAL column name, for translating an emitted change into the graph's row space. */
  physical: Record<string, string>
  /** The table fields the CALLER's own RETURNING asked for. Those keep their real decoders; everything else
   *  in capture's substituted selection decodes safely. Empty when they asked for no rows at all. */
  callerFields: ReadonlySet<string>
}

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

/** Decide precise vs coarse for one write — every ambiguity fails closed to coarse. */
function planCapture(builder: unknown, table: Table, op: Op, db: object): Plan {
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

  const tableColumns = getTableColumns(table)
  const columns = Object.keys(tableColumns)
  // field name → physical column name. Built here so every precise plan carries it and no emission path can
  // forget to translate (see `physicalRow`).
  const physical = Object.fromEntries(
    Object.entries(tableColumns).map(([field, column]) => [field, (column as Column).name]),
  )
  const noCallerFields: ReadonlySet<string> = EMPTY_FIELDS
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
  // executed the statement — so a POOLED connection is precise here too. This used to sit below a blanket
  // `!isSingleSession(db) → coarse` gate, which coarsened pooled PostgreSQL writes using an argument that
  // belongs to read hydration (a pooled read can be answered by a connection with a different role /
  // search_path / RLS view than the one that was probed). No such probe is involved in a returned row.
  //
  if (config.returning !== undefined) {
    const selection = callerSelection(config.returning, table)
    // Not reproducible from a row image — a nested alias path, or a raw `SQL` expression the DATABASE
    // computed. Run exactly what the caller wrote and let `captureMismatch` fail it closed, as before.
    if (!selection)
      return { mode: 'precise', callerReturning: true, pk, columns, physical, callerFields: noCallerFields }
    const project = (rows: Row[]) => rows.map((row) => projectRow(row, selection))
    const callerFields = new Set(selection.map((entry) => entry.field))
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
        callerFields,
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
        callerFields,
        positional: selection.map((entry) => entry.field),
      }
    // A PARTIAL or aliased projection is not a reason to give up. Widen the executed RETURNING to the whole
    // row, capture THAT, and project the caller's own columns back out of it. The caller sees exactly the
    // result they asked for; capture sees a real full row. No column is invented — this is the same row.
    return { mode: 'precise', callerReturning: false, pk, columns, physical, callerFields, reconstruct: project }
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
      callerFields: noCallerFields,
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
type CaptureMismatch =
  | { rowIndex: number; reason: 'missing-columns' | 'missing-key' | 'undecodable'; detail: string }
  | undefined

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
    // A value whose own decoder threw. Capture asked for it, so the failure is contained here rather than
    // raised at the caller — but an image with a hole in it is not an image, so the write coarsens.
    const undecodable = columns.filter((column) => row[column] === DECODE_FAILED)
    if (undecodable.length > 0) return { rowIndex, reason: 'undecodable', detail: undecodable.join(', ') }
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
  console.error(
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

/** Shared empty set — a write with no caller RETURNING has no fields the caller owns. */
const EMPTY_FIELDS: ReadonlySet<string> = new Set<string>()

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
