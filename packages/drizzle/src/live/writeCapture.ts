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
        reportOldNewDemotion(relationId)
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
  reportPostCommitDecodeFault(relationId, outcome.captureError)
}

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

// ── capture's own statement must never fail the caller's write ──────

/** The substituted statement was refused for a reason the SUBSTITUTION introduced. */
const SUBSTITUTION_REFUSED = Symbol('telefunc: capture substitution refused')

/** What ONE substituted execution produced — capture's view and the caller's, kept apart on purpose. */
type Substituted = {
  /** Capture's own view: the mapped rows, absent when drizzle's mapping of them threw. */
  readonly rows?: Row[]
  /** Why capture's mapping failed — present exactly when `rows` is absent. */
  readonly captureError?: unknown
  /** The RAW driver rows the tap observed. Only ever set when they arrived as an ARRAY, so a length taken
   *  from it is a length that was measured — see `tapRawRows`. */
  readonly raw?: readonly unknown[]
  /** The caller's own result, decoded FIRST and by their own mapper. Absent when their result carries no
   *  values at all, which is the case where nothing is decoded on their behalf. */
  readonly caller?: CallerResult
}

/** The caller's result, or the error their own statement would have raised producing it. */
type CallerResult = { readonly ok: true; readonly value: unknown } | { readonly ok: false; readonly error: unknown }

type SubstitutionOutcome = Substituted | typeof SUBSTITUTION_REFUSED

// ── the count-observation tap ───────────────────────────────────────
//
// A decoder can throw INSIDE drizzle's result mapping, AFTER the statement the database already applied.
// When that decoder was reached only because CAPTURE widened the RETURNING, the caller is handed an error for
// a write that happened — the worst shape this whole file exists to prevent.
//
// Neither obvious escape works. Re-running the statement writes twice. Decoding the rows ourselves diverges
// from drizzle's own mapping (a `mapWith` decoder that is not a real `Column` is ignored by rc.4's mapper
// generator, so the codec normalisation is skipped — a `timestamptz` comes back as the raw string
// `2020-03-04 06:06:07+01` instead of a `Date`), and a result that is subtly not drizzle's is not the
// caller's result.
//
// So capture OBSERVES instead of reconstructing. The raw driver rows exist BEFORE the mapper runs: drizzle's
// prepared query holds an `executor` and a `mapper` and composes them as `executor(params).then(mapper)`.
// A tap wrapped around that prepared query's `mapper` sees the exact array the mapper is about to map, and
// passes it through untouched. When the mapper then throws, the tap is holding the truth about the statement:
// how many rows it changed, and their raw values.
//
// From that the caller's own result is produced WITHOUT capture's decoding being involved at all:
//   - they asked for no rows → their result is count-shaped, and the count is the observed row count. Nothing
//     is decoded on their behalf, so there is no decoding for capture to disturb;
//   - they asked for a projection → drizzle's OWN mapper for THEIR selection runs over the raw values at
//     their columns' positions, and runs FIRST — before capture's mapper decodes anything.
//
// CALLER-FIRST is the part that is easy to get subtly wrong, and was. It is not enough to reuse drizzle's
// mapper; capture must not change how many times, in what order, or over which values the CALLER's decoders
// run. Capture's own selections do both of those things: a BOTH-IMAGES statement decodes every column TWICE
// (old and new), and a widened full-row statement decodes in TABLE order rather than the caller's. A
// stateful `customType.fromDriver` — a perfectly valid decoder — therefore handed the caller the value from
// its SECOND invocation where plain Drizzle gives the first. Decoding the caller's projection before capture
// touches anything makes their invocation sequence identical to plain Drizzle's, whatever capture does after.
//
// HONEST RESIDUAL, and the reason this is a bound rather than a proof: `fromDriver` is a MAPPING contract,
// and capture assumes it is pure with respect to caller-visible behaviour. An impure decoder can still
// OBSERVE capture's additional invocations (it just cannot colour the caller's result with them). That is
// inherent to capturing by RETURNING — the rows exist, so something must decode them. The alternative is to
// coarsen every table carrying a custom type, which would trade real precision on ordinary schemas for the
// protection of a pathological decoder class. Rejected deliberately.
//
// Scoped per execution, never a global client wrap: the tap shadows `_prepare` on THIS builder for the
// duration of THIS statement and restores it afterwards, and every `_prepare()` call mints a fresh prepared
// query, so the wrapped mapper belongs to one execution and nothing else. Concurrent executions of one
// builder cannot overlap here — `serializeOn` keys a queue on the builder precisely so two taps can never
// shadow one `_prepare` at once.
//
// Verified placed on PGlite, node-postgres (they share `pg-core/async`'s `PgAsyncPreparedQuery`) and
// node-sqlite (`SQLiteAsyncPreparedQuery`, whose executor shape differs but whose `mapper` does not — which
// is why the tap sits on the mapper rather than the executor).

/** What the tap saw. Both are `undefined` until the mapper is actually handed rows, so they answer "did this
 *  statement reach the mapper" — the only thing that licenses answering the caller from an observation. */
type Tap = {
  /** Whether the mapper was reached AT ALL — which proves the driver returned and the statement APPLIED,
   *  even where what it returned is a shape capture can make no use of. Emission keys off this; answering the
   *  caller keys off `observed`. The two are deliberately not the same question. */
  reached: () => boolean
  observed: () => readonly unknown[] | undefined
  callerResult: () => CallerResult | undefined
  release: () => void
}

/** The tap could not be placed, so this execution must not be substituted at all.
 *
 *  FAILS CLOSED, and that is the whole point of it being a value rather than an exception: the builder is put
 *  back and the caller's own statement runs as they wrote it (coarse). Installing the shadow used to happen
 *  outside the recovery's `try`, so a builder that could not be shadowed — `Object.preventExtensions()` on a
 *  finished builder is enough — rejected the caller's perfectly valid write before it ever reached the
 *  database, and left capture's RETURNING on the builder. That is a worse failure than the one this file
 *  exists to fix, produced by the machinery meant to fix it. */
const TAP_UNPLACEABLE = Symbol('telefunc: capture could not observe this statement')

/** Decode the CALLER's own projection out of the raw driver rows — drizzle's mapper for their selection, over
 *  the values at the positions capture's layout put them. */
type CallerDecode = (raw: readonly unknown[]) => unknown

function tapRawRows(builder: unknown, callerDecode: CallerDecode | undefined): Tap | typeof TAP_UNPLACEABLE {
  const target = builder as { _prepare?: unknown }
  const prepare = target._prepare
  if (typeof prepare !== 'function') return TAP_UNPLACEABLE
  const own = Object.getOwnPropertyDescriptor(target, '_prepare')
  let reached = false
  let seen: readonly unknown[] | undefined
  let caller: CallerResult | undefined
  const shadow = function (this: unknown, ...args: unknown[]): unknown {
    const prepared = (prepare as (...a: unknown[]) => unknown).apply(this, args) as { mapper?: unknown }
    const mapper = prepared?.mapper
    // No mapper means no mapping step to fail — the driver result IS the answer. Nothing to observe.
    if (typeof mapper !== 'function') return prepared
    prepared.mapper = (rows: readonly unknown[]) => {
      // Reaching the mapper at all is what proves the STATEMENT APPLIED — the driver answered. That is true
      // whatever shape the answer has, so it is recorded first and separately.
      reached = true
      // The rows themselves are recorded ONLY when they arrived as an array. Everything downstream reads a
      // LENGTH off this, and a length taken from a container of some other shape would be a number nobody
      // measured. A future mapper argument that is not an array therefore leaves the observation empty: the
      // table still coarsens, because the write happened, and the caller gets the error rather than a count.
      if (Array.isArray(rows)) {
        seen = rows // recorded BEFORE mapping, so a mapper that throws still leaves the observation behind
        // CALLER-FIRST: their decoders run here, over their columns, in their order, before capture's mapper
        // has decoded anything at all.
        if (callerDecode) {
          try {
            caller = { ok: true, value: callerDecode(rows) }
          } catch (error) {
            // A decoder on a column the CALLER selected themselves, failing exactly where their own statement
            // would have failed. Theirs to receive.
            caller = { ok: false, error }
          }
        }
      }
      return (mapper as (r: readonly unknown[]) => unknown)(rows)
    }
    return prepared
  }
  try {
    Object.defineProperty(target, '_prepare', {
      value: shadow,
      configurable: true,
      writable: true,
      enumerable: false,
    })
  } catch {
    return TAP_UNPLACEABLE // a frozen / non-extensible builder: refuse the substitution, never the write
  }
  return {
    reached: () => reached,
    observed: () => seen,
    callerResult: () => caller,
    release: () => {
      try {
        if (own) Object.defineProperty(target, '_prepare', own)
        else delete (target as Record<string, unknown>)._prepare
      } catch {
        // Placement succeeded, so this cannot normally fail. If it somehow does, the statement is already
        // decided and a restore fault must not become the caller's error.
      }
    },
  }
}

/** The caller's result as the tap produced it: theirs to receive, or theirs to be thrown. */
function deliverCaller(caller: CallerResult): unknown {
  if (caller.ok) return caller.value
  throw caller.error
}

/** The caller's COUNT-shaped result — the case where nothing is decoded on their behalf.
 *
 *  Taken from capture's mapped rows when capture mapped them, and otherwise from the rows the tap observed.
 *  Where neither exists there is no measured number, so the failure is rethrown rather than answered. */
function deliverCount(outcome: Substituted, plan: Extract<Plan, { callerReturning: false }>, mapped?: Row[]): unknown {
  if (mapped) return plan.reconstruct(mapped)
  if (outcome.raw && plan.reconstructCount) return plan.reconstructCount(outcome.raw.length)
  throw outcome.captureError
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

/** The caller's own decoding, as a function of the raw driver rows: their values at the positions capture's
 *  layout put them, mapped by DRIZZLE'S OWN mapper for their selection.
 *
 *  The mapper is obtained by preparing the builder while it still carries THEIR returning — which is what
 *  makes it the mapper their unsubstituted statement would have used, rather than a re-implementation of one.
 *  Preparing builds SQL and touches no connection. It is done once per substituted write that projects
 *  values, which is a second SQL build on that path; the alternative is reaching past `_prepare` into
 *  `dialect.mapperGenerators`, and one internal seam is enough. */
function callerDecoderFor(builder: unknown, positions: number[]): CallerDecode | undefined {
  const prepare = (builder as { _prepare?: unknown })._prepare
  if (typeof prepare !== 'function') return undefined
  let mapper: ((rows: unknown[][]) => Row[]) | undefined
  try {
    const prepared = (prepare as (...a: unknown[]) => unknown).call(builder) as { mapper?: unknown }
    if (typeof prepared?.mapper === 'function') mapper = prepared.mapper as (rows: unknown[][]) => Row[]
  } catch {
    return undefined // version drift in `_prepare`: refuse the substitution, never guess at their result
  }
  if (!mapper) return undefined
  const decode = mapper
  return (raw) => {
    const projected: unknown[][] = []
    for (const row of raw) {
      // A driver whose raw rows are not positional. Throwing here reaches the caller as their own error
      // rather than as a wrong value, and the same statement's capture side will have failed too.
      if (!Array.isArray(row)) throw new TypeError('[telefunc] live: capture observed a non-positional row')
      projected.push(positions.map((position) => row[position]))
    }
    return decode(projected)
  }
}

/** BUILD the substituted statement and its tap — the whole of what capture does to the builder BEFORE any
 *  statement runs, gathered into one fallible phase.
 *
 *  Two things can refuse here, and they refuse the same way. Rewriting the builder's RETURNING is an
 *  assignment into drizzle's own `config`, which a caller is free to have frozen — `Object.freeze` on it
 *  makes `.returning()` throw. Installing the tap is a property definition on the builder, which a
 *  non-extensible builder rejects. Neither is a reason the caller's write should fail: nothing has run, the
 *  statement they wrote is still perfectly executable, and the only thing lost is capture's precision.
 *
 *  So neither throws. Both come back as a refusal, the builder is put back to whatever it actually was, and
 *  the caller's own statement runs as written (coarse). Returning a VALUE rather than throwing is what keeps
 *  a construction failure structurally incapable of reaching the SQLSTATE classification below and being read
 *  as the caller's database error — it never enters that path at all. */
function buildSubstitution(
  substitute: () => unknown,
  callerDecode: CallerDecode | undefined,
  relationId: string,
): { substituted: unknown; tap: Tap } | typeof SUBSTITUTION_REFUSED {
  let substituted: unknown
  try {
    substituted = substitute()
  } catch (error) {
    reportSubstitutionUnbuildable(relationId, error)
    return SUBSTITUTION_REFUSED
  }
  const tap = tapRawRows(substituted, callerDecode)
  if (tap === TAP_UNPLACEABLE) {
    reportTapUnplaceable(relationId)
    return SUBSTITUTION_REFUSED
  }
  return { substituted, tap }
}

/** Capture could not even WRITE its RETURNING onto the builder — a frozen or read-only drizzle `config`, or
 *  a builder shape that has drifted from the pinned one. No statement ran. */
function reportSubstitutionUnbuildable(relationId: string, error: unknown): void {
  report(
    `[telefunc] live: Telefunc could not build the statement it uses to capture a write on "${describeRelationId(relationId)}" (its RETURNING clause could not be applied to the query builder). The write runs exactly as you wrote it and is unaffected; live queries on this table over-invalidate.`,
    error,
  )
}

/** The builder could not be put back the way the caller left it. Contained — the statement is already decided
 *  — but it means a LATER execution of this same builder would run capture's selection, so it is never
 *  silent. */
function reportRestoreFailed(relationId: string, error: unknown): void {
  report(
    `[telefunc] live: Telefunc could not restore the query builder it borrowed to capture a write on "${describeRelationId(relationId)}". This write is unaffected, but re-executing that same builder may return Telefunc's columns instead of yours — this is a bug in Telefunc's capture, please report it.`,
    error,
  )
}

/** The tap could not be placed, so this write runs exactly as the caller wrote it and capture coarsens. Rare
 *  enough to be worth saying out loud: it means the builder shape drifted from the one capture is pinned to. */
function reportTapUnplaceable(relationId: string): void {
  report(
    `[telefunc] live: Telefunc could not observe the statement it uses to capture a write on "${describeRelationId(relationId)}", so it did not substitute one. The write runs exactly as you wrote it and is unaffected; live queries on this table over-invalidate.`,
  )
}

function reportPostCommitDecodeFault(relationId: string, error: unknown): void {
  report(
    `[telefunc] live: decoding the rows Telefunc added to a write on "${describeRelationId(relationId)}" failed AFTER the database applied it (a column decoder threw). The write HAPPENED and your result is unaffected; live queries on this table over-invalidate rather than receive a row capture could not read.`,
    error,
  )
}

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
  callerPositions: number[] | undefined,
): Promise<SubstitutionOutcome> {
  const config = writeConfigOf(builder)
  const callerReturning = config?.returning
  const callerReturningFields = config?.returningFields
  // Restored on EVERY exit, not just the failing ones. The substitution overwrites the builder's RETURNING
  // in place, so leaving it overwritten after a SUCCESSFUL capture means a second `await` of the same
  // builder re-runs capture's statement and hands the caller capture's rows instead of their own result.
  //
  // BOTH halves of it: `.returning()` writes `returning` (what executes) and `returningFields` (what
  // `getSelectedFields()` reports), so restoring only the first leaves the builder describing a selection it
  // no longer runs.
  //
  // ONLY WHAT ACTUALLY CHANGED, never a blind assignment. A frozen or read-only `config` rejects assignment,
  // and restoring is bookkeeping that runs on the way OUT of a decided statement — so a restore that throws
  // would replace the caller's result with capture's own housekeeping failure, which is this file's whole
  // failure class arriving one phase later. Comparing first means a config nothing was written to is a config
  // nothing is written back to, so the frozen case never assigns at all.
  const restore = () => {
    if (!config) return
    try {
      if (config.returning !== callerReturning) config.returning = callerReturning
      if (config.returningFields !== callerReturningFields) config.returningFields = callerReturningFields
    } catch (error) {
      // Contained, never silent: the builder is left describing capture's selection, so a later execution of
      // this same builder would run capture's statement. Reported rather than raised — see `report`.
      reportRestoreFailed(relationId, error)
    }
  }
  // INSIDE A TRANSACTION the recovery needs a savepoint, because PostgreSQL aborts the whole transaction on
  // the refused statement and re-running would only get "current transaction is aborted". Without one — an
  // unverified dialect, or a SAVEPOINT the driver would not issue — there is nothing to recover to, so the
  // substitution is not attempted at all and the caller's statement runs as written (coarse).
  // Taken from the PRISTINE builder, before the substitution rewrites its RETURNING — this has to be the
  // mapper the caller's OWN statement would have carried, and after `substitute()` the builder no longer
  // describes their statement. Where their projection needs values and no such mapper can be had, the
  // substitution is refused rather than attempted: without it there is no way to keep their decoding first,
  // and a substitution that cannot guarantee that is one this file should not be making.
  const callerDecode = callerPositions ? callerDecoderFor(builder, callerPositions) : undefined
  if (callerPositions && !callerDecode) return SUBSTITUTION_REFUSED

  const attempt = async (): Promise<SubstitutionOutcome> => {
    const savepoint = tx ? await openSavepoint(tx, relationId) : NO_SAVEPOINT_NEEDED
    if (savepoint === SAVEPOINT_UNAVAILABLE) {
      restore()
      return SUBSTITUTION_REFUSED
    }
    // ── PHASE ONE: BUILD the substituted statement. Fallible, and NOTHING HAS RUN — which is what makes
    //    every failure in it capture's alone. Kept a separate phase from execution precisely so that can be
    //    true by construction rather than by classification: a synchronous failure here cannot reach the
    //    SQLSTATE reasoning below and be mistaken for the caller's database error, because there is no
    //    database error to mistake it for. The answer to all of them is the same — do not substitute.
    const built = buildSubstitution(substitute, callerDecode, relationId)
    if (built === SUBSTITUTION_REFUSED) {
      await savepoint.release() // nothing ran, so there is nothing to rewind
      restore() // whatever the construction managed to write before failing, and nothing else
      return SUBSTITUTION_REFUSED
    }
    const { substituted, tap } = built

    // ── PHASE TWO: RUN it. From here a failure may or may not be the caller's, which is what the rest of
    //    this function is about.
    try {
      const rows = (await runBase(substituted, executeArgs)) as Row[]
      await savepoint.release()
      return { rows, raw: tap.observed(), caller: tap.callerResult() }
    } catch (error) {
      // THE STATEMENT RAN. The mapper was reached, so the driver answered and the database applied the write;
      // the failure came afterwards, inside drizzle's mapping of a RETURNING capture chose. Re-running would
      // write twice and rewinding would undo a write the caller asked for, so the savepoint is RELEASED
      // exactly as on success and the caller is answered from what the tap already decoded for them.
      //
      // Keyed on REACHED rather than on the rows: a driver that answered with a shape capture cannot read
      // still applied the write, and the table must coarsen for it even though no count can be given.
      if (tap.reached()) {
        await savepoint.release()
        return { captureError: error, raw: tap.observed(), caller: tap.callerResult() }
      }
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
      tap.release()
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

/** Serializes whole captured WRITES on one key — savepoint, statement, mapping and restore as one unit.
 *
 *  Keyed by a TRANSACTION, this is about savepoints: `RELEASE SAVEPOINT` destroys every savepoint
 *  established after it, so two concurrent writes on one transaction (`Promise.all` over the same tx)
 *  interleave as save-A, save-B, release-A: B's savepoint is gone, B's release errors, and a failed statement
 *  aborts the transaction. Observed, not theorised — three concurrent inserts committed zero rows. Unique
 *  names do not help; it is establishment ORDER, not naming. PostgreSQL executes one statement at a time per
 *  connection anyway, so nothing real is lost by queueing.
 *
 *  Keyed by a BUILDER, it is about the builder's own mutable state: capture's RETURNING lives on the shared
 *  builder across an await, so overlapping executions of one builder read each other's temporary shape.
 *
 *  SERIALIZED rather than executed against a per-execution CLONE, which was the alternative: drizzle's write
 *  builders carry `execute` as an own ARROW property bound to the instance it was constructed on, so a clone
 *  would build its own SQL and then execute the ORIGINAL builder — the copy would silently not be the thing
 *  that runs. Serialization changes only the timing of concurrent reuse, which is exactly the state that was
 *  wrong, and leaves every result identical to plain Drizzle's. */
const serialQueues = new WeakMap<object, Promise<unknown>>()
let savepointCount = 0

function serializeOn<T>(key: object, attempt: () => Promise<T>): Promise<T> {
  const previous = serialQueues.get(key) ?? Promise.resolve()
  const next = previous.then(attempt, attempt)
  // Kept off the chain's failure path: one attempt's rejection must not reject every later one.
  serialQueues.set(
    key,
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
  report(
    `[telefunc] live: could not open a savepoint to capture a write on "${describeRelationId(relationId)}" inside a transaction. The write runs exactly as you wrote it and is unaffected; live queries on this table over-invalidate.`,
    error,
  )
}

function reportSavepointBookkeepingFailed(relationId: string, statement: string, error: unknown): void {
  report(
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
 *  rows the caller never requested. The write is APPLIED and the caller is handed the decoder's error — a
 *  committed write reported as a failure, which is the worst form of this whole class of bug. Such an error
 *  carries no SQLSTATE, because no database refused anything.
 *
 *  This is only ever asked once the TAP has already been consulted, so "the statement ran and its mapping
 *  threw" has been separated out before we get here. What remains is a failure with no observed rows. */
function isSubstitutionFault(error: unknown): boolean {
  const state = sqlStateOf(error)
  // NO SQLSTATE and NO observed rows: the database never refused anything, and nothing came back. The
  // statement may still have run, so re-running it could write twice — the error is the caller's to see.
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
 *  anywhere else. A decoder that throws is handled by OBSERVING the statement rather than by decoding
 *  differently — see the count-observation tap. */
const OLD_IMAGE = 'tf_old__'
const NEW_IMAGE = 'tf_new__'

// ── the post-commit failure domain ──────────────────────────────────
//
// Once the substituted statement SUCCEEDS the write is applied and irreversible. Everything capture does
// with the result after that point — decoding values, splitting the two images, verifying the row, building
// changes — happens for capture's benefit, not the caller's, and must not be able to fail their write.
//
// The sharp edge is DECODING, and it is sharper than it looks: the decoding happens INSIDE drizzle, before
// any of capture's own code sees a row, so it cannot be caught by being careful here. That is what the
// count-observation tap above is for.

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
 *  Built explicitly rather than via a bare `.returning()` so the selection's LAYOUT is capture's own and
 *  exactly known — the table's columns in order — which is what lets a caller's projection be taken back out
 *  of raw rows by position when drizzle's mapping of them fails (see `rawPositionOf`). Every column keeps its
 *  real decoder, so a successful mapping is drizzle's own in every respect. */
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

/** Every diagnostic in this file, and the only place `console.error` is called.
 *
 *  Non-throwing by construction, because each of these reports sits IMMEDIATELY BEFORE a recovery: the
 *  substitution retry, a savepoint rewind, a coarse degradation. A host whose console throws — a patched or
 *  instrumented console, a logger that rejects a circular argument — would take the recovery down with it
 *  and turn "the write is safe" into "the write failed", which is precisely the failure this file exists to
 *  prevent. Telling the operator is best-effort; the recovery is not. */
function report(...args: unknown[]): void {
  try {
    console.error(...args)
  } catch {
    // A console that cannot be written to is not a reason to fail a committed write.
  }
}

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
  report(
    `[telefunc] live: the database refused the RETURNING clause Telefunc added to a write on "${describeRelationId(relationId)}" (a role that can write a table but not SELECT from it does this). The write is being re-run exactly as you wrote it and is unaffected; live queries on this table over-invalidate rather than lose the write.`,
    error,
  )
}

/** The server rejected a statement its own version number said it would accept. Reported once per db,
 *  because it changes how every later write on it is captured. */
function reportOldNewDemotion(relationId: string): void {
  report(
    `[telefunc] live: this PostgreSQL server reports version 18 or newer but refused "RETURNING old.*, new.*" (first seen writing "${describeRelationId(relationId)}"). Falling back to new-image capture for this database. Live queries stay correct, with more coarse invalidation than a genuine PostgreSQL 18 would need.`,
  )
}

/** The diagnostics seam for a contained capture fault. Takes a relation IDENTITY and renders it the way a
 *  human wrote it (`a.users`), since this reaches an operator's logs. */
function reportCaptureFault(error: unknown, relationId: string): void {
  report(
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
  // executed the statement — so a POOLED connection is precise here too. This used to sit below a blanket
  // `!isSingleSession(db) → coarse` gate, which coarsened pooled PostgreSQL writes using an argument that
  // belongs to read hydration (a pooled read can be answered by a connection with a different role /
  // search_path / RLS view than the one that was probed). No such probe is involved in a returned row.
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
