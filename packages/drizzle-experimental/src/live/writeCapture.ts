export { captureMutation, captureRawSql }

import { type Table, isTable } from 'drizzle-orm'
import { demoteOldNewReturning, markOldNewProven, oldNewProvenOf } from './writeCapabilities.js'
import { relationKeyOf } from '../extract/columns.js'
import { report } from './captureReport.js'
import { announceCoarse, ingestWrite } from './dbRuntime.js'
import {
  type CaptureSink,
  captureBothOrCoarse,
  captureOrCoarse,
  changesFromRows,
  coarse,
  emitSafely,
} from './writeChanges.js'
import { UNMAPPABLE, type Op, type Plan, callerPositionsOf, planCapture } from './writePlan.js'
import {
  SUBSTITUTION_REFUSED,
  type Substituted,
  deliverCaller,
  deliverCount,
  runBase,
  runSubstituted,
  serializeOn,
  splitImages,
  substituteFullRow,
  substituteOldNew,
} from './writeSubstitution.js'
import type { Row, TableChange } from '../router/events.js'

// The write-capture INTERCEPTION facade. `reactiveDrizzle`'s proxy routes insert/update/delete here. The
// write runs as plain Drizzle; the terminal is intercepted to capture the changed rows and feed a
// `ChangeBatch` to the db's graphs (via the sink).
//
// One write reads as: classify the terminal → build a plan (`writePlan.ts`) → execute through one strategy
// (as written, or through the substitution adapter in `writeSubstitution.ts`) → emit the captured batch
// (`writeChanges.ts`). Each of those modules owns its own invariants; this file owns the routing between
// them and nothing else.
//
// Every emitted `TableChange.table` is a schema-qualified relation IDENTITY (`relationKeyOf`, see
// ir/relation.ts) — NOT the bare table name. The read side registers its graphs under the same identity, so
// a write to `a.users` reaches live queries on `a.users` and not the different physical table `b.users`.

function captureMutation(
  op: Op,
  baseMethod: (...a: unknown[]) => unknown,
  db: object,
  emit?: CaptureSink,
  tx?: object,
  txRoot?: object,
): (...a: unknown[]) => unknown {
  const sink: CaptureSink = emit ?? ((changes) => ingestWrite(db, { changes }))
  return (...args: unknown[]) => {
    const table = args[0]
    // insert/update/delete all take the target table as their first argument.
    if (!isTable(table)) return baseMethod(...args)
    return wrapWrite(baseMethod(...args), table, op, db, sink, tx, txRoot)
  }
}

/** Wrap a mutation builder so its terminal (`await` / `.execute()`) runs the write and captures its change;
 *  chain methods (`values`/`set`/`where`/`returning`/…) re-wrap so the terminal stays captured. */
function wrapWrite(
  builder: unknown,
  table: Table,
  op: Op,
  db: object,
  sink: CaptureSink,
  tx?: object,
  txRoot?: object,
): unknown {
  return new Proxy(builder as object, {
    get(target, prop, receiver) {
      // EVERY promise terminal routes through the captured run — `.catch()`/`.finally()` included. A
      // terminal left out reaches the raw QueryPromise and executes the write uncaptured, which is a
      // systematic missed invalidation rather than a one-off.
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
        // INSIDE A TRANSACTION, keyed by the ROOT of the physical transaction rather than by this scope's own
        // handle: a nested transaction is a SAVEPOINT on the same connection, so keying per handle gave
        // parent and child separate queues, let their savepoints interleave, and turned a transaction plain
        // Drizzle commits into "savepoint does not exist" → 25P02. That queue already covers everything on
        // the connection, this builder included.
        //
        // OUTSIDE one, keyed by the BUILDER. Substitution rewrites the builder's RETURNING in place and puts
        // it back when the statement finishes, so two executions of the SAME builder overlapping in time see
        // each other's half-applied state: `Promise.all([b, b])` had the second execution plan against
        // CAPTURE's full RETURNING and hand that caller capture's rows where plain Drizzle returns the plain
        // count-shape twice — and their two taps shadowed and restored one `_prepare` out of order. Different
        // builders keep different queues, so unrelated autocommit writes stay fully concurrent.
        return serializeOn(tx ? (txRoot ?? tx) : target, run)
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
      // Gated on the underlying builder ACTUALLY having the member: PG write builders have no
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
        return isWriteBuilder(next) ? wrapWrite(next, table, op, db, sink, tx, txRoot) : next
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
 *  touched tables are unknowable without parsing SQL, so — per the owner disposition — the statement is
 *  ANNOUNCED once it completes rather than captured (safe over-fire, keeps raw SQL usable). One honest
 *  bound: a raw READ also over-fires (sound, just noisy).
 *
 *  What that announcement IS belongs to `announceCoarse` (dbRuntime), which owns both of its halves — this
 *  only decides WHEN. INSIDE A TRANSACTION the caller passes an announce that records the intent instead, so
 *  nothing is announced until the outer COMMIT and a rollback announces nothing: announcing mid-transaction
 *  would tell every other instance to refetch state that may never exist. */
function captureRawSql(
  base: (...a: unknown[]) => unknown,
  db: object,
  announce: () => void = () => announceCoarse(db),
) {
  return (...args: unknown[]) => {
    const result = base(...args)
    // The statement has already run, so its announcement is never allowed to fail the caller for it.
    const announceSafely = () => {
      try {
        announce()
      } catch (error) {
        report('capture-failed', { relation: '*', cause: error })
      }
    }
    if (isThenable(result)) {
      return Promise.resolve(result).then((rows) => {
        announceSafely()
        return rows
      })
    }
    announceSafely()
    return result
  }
}

// ── the one captured write ──────────────────────────────────────────

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
  // Where the caller's own result carries VALUES, capture decodes it first and separately — see the tap. An
  // unresolvable position means capture's layout cannot answer their projection, so the substitution is
  // refused rather than approximated.
  const callerPositions = callerPositionsOf(plan, op)
  if (callerPositions === UNMAPPABLE) return recoverAsWritten(builder, table, op, db, sink, tx, executeArgs)

  if (plan.images) {
    const outcome = await runSubstituted(
      () => substituteOldNew(builder, table, plan.images!),
      builder,
      tx,
      executeArgs,
      relationId,
      callerPositions,
    )
    if (outcome === SUBSTITUTION_REFUSED) {
      // An UNPROVEN capability came from the server's version number rather than from a statement that ran
      // (the temp-table probe was refused for lack of privilege). A fork can report 18 and still reject
      // `RETURNING old.*, new.*` — so believe the statement over the version, permanently.
      if (!oldNewProvenOf(db)) {
        demoteOldNewReturning(db)
        report('old-new-demoted', { relation: relationId })
      }
      return recoverAsWritten(builder, table, op, db, sink, tx, executeArgs)
    }
    if (!oldNewProvenOf(db)) markOldNewProven(db) // it worked; later writes pay nothing for the guard
    // PostgreSQL's plain RETURNING on a DELETE is the row that was deleted — the OLD image. On an UPDATE it
    // is the NEW one. Both are already accounted for in the positions the caller's decoding read.
    const pairs = outcome.rows?.map((row) => splitImages(row, plan.images!))
    emitCaptured(sink, relationId, outcome, pairs ? captureBothOrCoarse(op, relationId, pairs, plan) : undefined)
    if (outcome.caller) return deliverCaller(outcome.caller)
    return deliverCount(
      outcome,
      plan,
      pairs?.map((pair) => (op === 'delete' ? pair.old : pair.new)),
    )
  }

  const outcome = await runSubstituted(
    () => substituteFullRow(builder, table),
    builder,
    tx,
    executeArgs,
    relationId,
    callerPositions,
  )
  if (outcome === SUBSTITUTION_REFUSED) return recoverAsWritten(builder, table, op, db, sink, tx, executeArgs)
  emitCaptured(sink, relationId, outcome, outcome.rows && captureOrCoarse(op, relationId, outcome.rows, plan))
  if (outcome.caller) return deliverCaller(outcome.caller)
  return deliverCount(outcome, plan, outcome.rows)
}

/** Emit what capture managed to see. Where its own mapping of the rows failed there is no image to emit, but
 *  the write HAPPENED — so the table coarsens rather than going unmentioned.
 *
 *  Emission follows the DATABASE, not the caller's outcome: this runs before the caller is answered and
 *  regardless of whether they are about to receive a result or an error, because a live query never told
 *  about an applied write is silently stale. (A write that never ran — a constraint violation, a refused
 *  statement — still emits nothing, because nothing changed.) */
function emitCaptured(
  sink: CaptureSink,
  relationId: string,
  outcome: Substituted,
  changes: TableChange[] | undefined,
): void {
  if (changes) return emitSafely(sink, changes)
  emitSafely(sink, [coarse(relationId)])
  report('post-commit-decode-failed', { relation: relationId, cause: outcome.captureError })
}

/** Re-run the caller's write EXACTLY as they wrote it, and coarsen. Single-shot by construction: this path
 *  plans with substitution forbidden, so it cannot land back here.
 *
 *  Safe to re-run because the refused statement left nothing behind, and that holds in both worlds:
 *
 *   - AUTOCOMMIT: PostgreSQL applies a statement wholly or not at all, so this is the caller's write
 *     happening for the first time, not a second time.
 *   - INSIDE A TRANSACTION: the refused statement aborts the whole transaction, so a bare re-run would only
 *     get "current transaction is aborted". `runSubstituted` therefore brackets every in-transaction
 *     substitution in a SAVEPOINT and has already REWOUND to it before returning the refusal — the
 *     transaction is un-aborted by the time this runs, and the caller's own statement executes normally.
 *     Where no savepoint could be established (a non-PG dialect, or a driver that would not issue one) the
 *     substitution is never attempted at all, so there is no refusal to recover from.
 *
 *  The transaction handle is threaded through for that bracket; capture is still keyed on the TOP db
 *  (`txProxy` passes `topDb`) so registry ownership of the graphs stays with the db that owns them. */
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

/** A drizzle write query builder, distinguished from a plain method return (a `toSQL()` object, a Promise). */
function isWriteBuilder(value: unknown): boolean {
  return typeof value === 'object' && value !== null && typeof (value as { toSQL?: unknown }).toSQL === 'function'
}
