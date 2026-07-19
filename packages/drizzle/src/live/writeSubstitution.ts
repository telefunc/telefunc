export { runSubstituted, substituteOldNew, substituteFullRow, splitImages }
export { deliverCaller, deliverCount, serializeOn, runBase, SUBSTITUTION_REFUSED }
export type { Substituted, SubstitutionOutcome }

import { type Column, SQL, type Table, getTableColumns, sql } from 'drizzle-orm'
import { dialectOf } from '../binding/database.js'
import { describeRelationId } from '../ir/relation.js'
import { report } from './captureReport.js'
import type { Images } from './writeChanges.js'
import { type Plan, writeConfigOf } from './writePlan.js'
import type { Row } from '../router/events.js'

// The adapter around drizzle's execution machinery for a write whose RETURNING capture CHOSE. The caller
// never asked for that clause, so the whole of this module exists to make one guarantee: capture's own
// statement must never be what fails the caller's write.
//
// Three mechanisms hold it up, and they are here together because they are one story —
//   · a BUILD phase that is separate from execution, so a construction failure cannot be mistaken for the
//     caller's database error;
//   · a TAP on drizzle's mapper, so the caller's result can be answered from an observation of the raw rows
//     rather than reconstructed by capture;
//   · a SAVEPOINT bracket, so an in-transaction refusal can be rewound and the caller's own statement run.

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
// a write that happened — the worst shape this whole path exists to prevent.
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
 *  database, and left capture's RETURNING on the builder. That is a worse failure than the one this path
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
  // would replace the caller's result with capture's own housekeeping failure, which is this module's whole
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
  // and a substitution that cannot guarantee that is one this module should not be making.
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

/** The server refused the RETURNING clause CAPTURE added — most often a role that may write the table but
 *  not read it. The caller's write is re-run as they wrote it, so the only cost is a coarse invalidation. */
function reportSubstitutionRefused(relationId: string, error: unknown): void {
  report(
    `[telefunc] live: the database refused the RETURNING clause Telefunc added to a write on "${describeRelationId(relationId)}" (a role that can write a table but not SELECT from it does this). The write is being re-run exactly as you wrote it and is unaffected; live queries on this table over-invalidate rather than lose the write.`,
    error,
  )
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

// ── the statements capture substitutes ──────────────────────────────
//
// Once the substituted statement SUCCEEDS the write is applied and irreversible. Everything capture does
// with the result after that point — decoding values, splitting the two images, verifying the row, building
// changes — happens for capture's benefit, not the caller's, and must not be able to fail their write.
//
// The sharp edge is DECODING, and it is sharper than it looks: the decoding happens INSIDE drizzle, before
// any of capture's own code sees a row, so it cannot be caught by being careful here. That is what the
// count-observation tap above is for.

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

// ── terminal execution ──────────────────────────────────────────────

function runBase(builder: unknown, executeArgs?: unknown[]): Promise<unknown> {
  return executeArgs
    ? (builder as { execute: (...a: unknown[]) => Promise<unknown> }).execute(...executeArgs)
    : Promise.resolve(builder as PromiseLike<unknown>)
}
