export { captureMutation, captureRawSql }
export type { WriteContext }

import { type Table, isTable } from 'drizzle-orm'
import { demoteOldNewReturning, markOldNewProven, oldNewProvenOf } from './writeCapabilities.js'
import { relationKeyOf } from './extract/columns.js'
import { report } from '../bus/captureReport.js'
import {
  type CaptureSink,
  captureBothOrCoarse,
  captureOrCoarse,
  changesFromRows,
  coarse,
  emitSafely,
} from './writeChanges.js'
import { isBuilderTerminal, isDriverTerminal, isPreparedTerminal } from './writeTerminals.js'
import { AS_WRITTEN, UNMAPPABLE, type Op, type Plan, callerPositionsOf, planCapture } from './writePlan.js'
import {
  SUBSTITUTION_REFUSED,
  type Substituted,
  runBase,
  runSubstituted,
  serializeOn,
  substituteFullRow,
  substituteOldNew,
} from './writeSubstitution.js'
import type { Row, TableChange } from '../bus/router/events.js'

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

/** Everything ONE captured write needs to know about the world it runs in, discriminated by which delivery
 *  world that is — the two identities that used to thread through here as `db`/`tx`/`txRoot` positionals
 *  (with optional parameters silently encoding autocommit-vs-transaction) are structural members now, so
 *  the raw-handle-vs-registry-key rule is enforced by the type rather than documented at three call sites. */
type WriteContext =
  | {
      /** AUTOCOMMIT: statements run on the identity db itself; whole captured writes serialize per builder
       *  (`wrapWrite`), and there is no savepoint to bracket a substitution with. */
      sinkMode: 'autocommit'
      /** The db capture PLANS against and keys the registry to — the graphs live on the db the reads
       *  acquired from. */
      identityDb: object
      /** Where a captured batch goes — the db's publishing ingest (local feed + cross-instance publish). */
      sink: CaptureSink
      executionHandle?: undefined
      serializationKey?: undefined
    }
  | {
      /** INSIDE A TRANSACTION: writes buffer until the commit boundary flushes them as one tick. */
      sinkMode: 'transaction'
      /** Still the TOP db, never a tx handle — a tx db is not recognized as its own driver. */
      identityDb: object
      /** The transaction's buffer. */
      sink: CaptureSink
      /** The RAW tx db, passed ONLY as the execution handle for the capture-recovery savepoint. NEVER the
       *  tx proxy: `execute` on the proxy is intercepted as raw SQL, so a SAVEPOINT through it would
       *  coarsen the whole transaction. */
      executionHandle: object
      /** The PHYSICAL transaction root every captured write on this connection serializes on. A nested
       *  transaction is a SAVEPOINT on the same connection: keying per handle gave parent and child
       *  separate queues, let their savepoints interleave, and turned a transaction plain Drizzle commits
       *  into "savepoint does not exist" → 25P02. */
      serializationKey: object
    }

function captureMutation(
  op: Op,
  baseMethod: (...a: unknown[]) => unknown,
  context: WriteContext,
): (...a: unknown[]) => unknown {
  return (...args: unknown[]) => {
    const table = args[0]
    // insert/update/delete all take the target table as their first argument.
    if (!isTable(table)) return baseMethod(...args)
    return wrapWrite(baseMethod(...args), table, op, context)
  }
}

/** Wrap a mutation builder so its terminal (`await` / `.execute()`) runs the write and captures its change;
 *  chain methods (`values`/`set`/`where`/`returning`/…) re-wrap so the terminal stays captured. */
function wrapWrite(builder: unknown, table: Table, op: Op, context: WriteContext): unknown {
  return new Proxy(builder as object, {
    get(target, prop, receiver) {
      // Preserve the builder's actual terminal surface (`writeCapture.terminals.spec.ts`: "does not SYNTHESIZE").
      const has = (name: string): boolean => typeof Reflect.get(target, name, receiver) === 'function'
      const start = (args?: unknown[]): Promise<unknown> => {
        const run = () => runWrite(target, table, op, context, args)
        // Queue ownership is pinned by writeCapture.mechanism.spec.ts (same builder) and
        // writeCapture.transaction.spec.ts (concurrent + nested physical transaction).
        return serializeOn(context.serializationKey ?? target, run)
      }
      // `writeTerminals` classifies the verbs; `has` keeps the proxy surface transparent (same terminals pin).
      if (isBuilderTerminal(prop) && has(prop as string)) {
        if (prop === 'then') {
          return (onFulfilled?: (v: unknown) => unknown, onRejected?: (e: unknown) => unknown) =>
            start().then(onFulfilled, onRejected)
        }
        if (prop === 'catch') return (onRejected?: (e: unknown) => unknown) => start().catch(onRejected)
        if (prop === 'finally') return (onFinally?: () => void) => start().finally(onFinally)
        return (...args: unknown[]) => start(args) // execute
      }
      // Driver terminals execute DIRECTLY rather than through the QueryPromise — SYNCHRONOUS on node:sqlite.
      if (isDriverTerminal(prop, target) && has(prop as string)) {
        return (...args: unknown[]) => runDirectTerminal(target, prop as string, args, table, op, context)
      }
      // A prepared write executes LATER; hand back a wrapped prepared query so each execution invalidates.
      if (prop === 'prepare') {
        const prepare = Reflect.get(target, prop, receiver)
        if (typeof prepare === 'function') {
          // Planned HERE, from the builder as it stands at `prepare()` — after that the shape is frozen, so
          // one plan covers every execution of the prepared statement.
          const plan = planCapture(target, table, op, context.identityDb)
          return (...args: unknown[]) =>
            wrapPrepared((prepare as (...a: unknown[]) => unknown).apply(target, args), table, op, context.sink, plan)
        }
      }
      const value = Reflect.get(target, prop, receiver)
      if (typeof value !== 'function') return value
      return (...args: unknown[]) => {
        const next = (value as (...a: unknown[]) => unknown).apply(target, args)
        return isWriteBuilder(next) ? wrapWrite(next, table, op, context) : next
      }
    },
  })
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
  context: WriteContext,
): unknown {
  const base = (builder as Record<string, (...a: unknown[]) => unknown>)[prop]!
  const result = base.apply(builder, args)
  const emit = (rows: unknown) => emitSafely(context.sink, directChanges(rows, builder, table, op, context.identityDb))
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
      if (!isPreparedTerminal(prop)) {
        return (...args: unknown[]) => (value as (...a: unknown[]) => unknown).apply(target, args)
      }
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
function captureRawSql(base: (...a: unknown[]) => unknown, announce: () => void) {
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
  context: WriteContext,
  executeArgs?: unknown[],
  // The strategy this write runs under — freshly classified by default. The recovery from a refused
  // substitution passes `AS_WRITTEN` instead: the one plan that provably cannot substitute, which is what
  // makes that recovery single-shot by construction. The plan value IS the recovery state — one fact, one
  // representation, never a flag beside a plan that contradicts it.
  plan: Plan = planCapture(builder, table, op, context.identityDb),
): Promise<unknown> {
  const { identityDb: db, sink, executionHandle } = context
  const relationId = relationKeyOf(table)

  if (plan.strategy === 'asWritten') {
    const result = await runBase(builder, executeArgs) // run the caller's write untouched
    emitSafely(sink, [{ table: relationId, kind: 'coarse' }]) // fail-closed: over-invalidate, never guess
    return result
  }

  if (plan.strategy === 'callerReturning') {
    // The caller asked for rows — run their write, and capture only if the rows carry the FULL image.
    const rows = (await runBase(builder, executeArgs)) as Row[]
    emitSafely(sink, captureOrCoarse(op, relationId, rows, plan))
    return rows
  }

  // From here capture CHOOSES the statement — a full row image, or both images of the row where the
  // connection can produce them. THE CALLER DID NOT ASK FOR THIS, so it must never be what fails their
  // write: `runSubstituted` puts their own statement back and runs that instead if it does.
  //
  // The caller's result is then rebuilt from what came back (`plan.deliver` — verified faithful for this
  // driver, or reproducible from their own projection). The rows are expected to be full, but that is still
  // VERIFIED rather than trusted: this path once built changes unchecked, so a driver returning a narrowed
  // row would have emitted a partial image as precise.
  // Where the caller's own result carries VALUES, capture decodes it first and separately — see the tap. An
  // unresolvable position means capture's layout cannot answer their projection, so the substitution is
  // refused rather than approximated.
  const callerPositions = callerPositionsOf(plan, op)
  if (callerPositions === UNMAPPABLE) return recoverAsWritten(builder, table, op, context, executeArgs)

  if (plan.strategy === 'bothImages') {
    const { images } = plan
    const outcome = await runSubstituted(
      () => substituteOldNew(builder, table, images),
      builder,
      executionHandle,
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
      return recoverAsWritten(builder, table, op, context, executeArgs)
    }
    if (!oldNewProvenOf(db)) markOldNewProven(db) // it worked; later writes pay nothing for the guard
    // Which image is which answer (delete→old, update→new) is the layout's to say — see imageLayout.ts.
    const pairs = outcome.rows?.map((row) => images.split(row))
    emitCaptured(sink, relationId, outcome, pairs ? captureBothOrCoarse(op, relationId, pairs, plan) : undefined)
    return plan.deliver(
      outcome,
      pairs?.map((pair) => images.delivered(pair, op)),
    )
  }

  const outcome = await runSubstituted(
    () => substituteFullRow(builder, table),
    builder,
    executionHandle,
    executeArgs,
    relationId,
    callerPositions,
  )
  if (outcome === SUBSTITUTION_REFUSED) return recoverAsWritten(builder, table, op, context, executeArgs)
  emitCaptured(sink, relationId, outcome, outcome.rows && captureOrCoarse(op, relationId, outcome.rows, plan))
  return plan.deliver(outcome, outcome.rows)
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
 *  re-enters `runWrite` under the `AS_WRITTEN` plan, which cannot substitute, so it cannot land back here.
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
 *  The context's execution handle carries that bracket; capture stays keyed on the context's identity db
 *  so registry ownership of the graphs stays with the db that owns them. */
async function recoverAsWritten(
  builder: unknown,
  table: Table,
  op: Op,
  context: WriteContext,
  executeArgs: unknown[] | undefined,
): Promise<unknown> {
  return runWrite(builder, table, op, context, executeArgs, AS_WRITTEN)
}

/** A drizzle write query builder, distinguished from a plain method return (a `toSQL()` object, a Promise). */
function isWriteBuilder(value: unknown): boolean {
  return typeof value === 'object' && value !== null && typeof (value as { toSQL?: unknown }).toSQL === 'function'
}
