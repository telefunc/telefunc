export { report }
export type { Fault }

import { describeRelationId } from '../ir/relation.js'

// The one diagnostics seam. Every fault this package tells an operator about is a ROW in the table below,
// and `report` is the only place in the package that calls `console.error` (pinned by captureReport.spec.ts).
//
// Why a table rather than a builder function per fault: these messages are not free prose, they are a
// CONTRACT with the operator, and the contract is the same one every time — name what Telefunc could not
// do, then say what it cost. Read down the table and that promise is checkable at a glance; spread across
// twelve builders in five files it was not, and two of those files had stopped using the seam at all.
//
// Every row sits IMMEDIATELY BEFORE a recovery — the substitution retry, a savepoint rewind, a coarse
// degradation, a coarse-all publication — which is why `report` cannot throw. A host whose console throws
// (a patched or instrumented console, a logger that rejects a circular argument) would otherwise take the
// recovery down with it and turn "the write is safe" into "the write failed", which is precisely the
// failure the capture path exists to prevent. Rendering the message is inside that guarantee too: it
// interpolates caller-shaped values, so it is not obviously total. Telling the operator is best-effort;
// the recovery is not.

/** The fault inventory: what happened, and — in each doc line — the recovery it precedes. A row that
 *  declares `cause` is reported with that error as `console.error`'s second argument. */
const FAULTS = {
  // ── capture, before the statement runs ──

  /** Capture could not even WRITE its RETURNING onto the builder — a frozen or read-only drizzle `config`,
   *  or a builder shape that has drifted from the pinned one. No statement ran → run the caller's own. */
  'substitution-unbuildable': ({ relation }: { relation: string; cause: unknown }) =>
    `[telefunc] live: Telefunc could not build the statement it uses to capture a write on "${describeRelationId(relation)}" (its RETURNING clause could not be applied to the query builder). The write runs exactly as you wrote it and is unaffected; live queries on this table over-invalidate.`,

  /** The tap could not be placed, so this write runs exactly as the caller wrote it and capture coarsens.
   *  Rare enough to be worth saying out loud: the builder shape drifted from the one capture is pinned to. */
  'tap-unplaceable': ({ relation }: { relation: string }) =>
    `[telefunc] live: Telefunc could not observe the statement it uses to capture a write on "${describeRelationId(relation)}", so it did not substitute one. The write runs exactly as you wrote it and is unaffected; live queries on this table over-invalidate.`,

  /** No savepoint to rewind to inside a transaction, so the substitution is not attempted → as-written. */
  'savepoint-unavailable': ({ relation }: { relation: string; cause: unknown }) =>
    `[telefunc] live: could not open a savepoint to capture a write on "${describeRelationId(relation)}" inside a transaction. The write runs exactly as you wrote it and is unaffected; live queries on this table over-invalidate.`,

  // ── capture, once the statement has been decided ──

  /** The server refused the RETURNING clause CAPTURE added — most often a role that may write the table but
   *  not read it. The caller's write is re-run as they wrote it, so the only cost is a coarse invalidation. */
  'substitution-refused': ({ relation }: { relation: string; cause: unknown }) =>
    `[telefunc] live: the database refused the RETURNING clause Telefunc added to a write on "${describeRelationId(relation)}" (a role that can write a table but not SELECT from it does this). The write is being re-run exactly as you wrote it and is unaffected; live queries on this table over-invalidate rather than lose the write.`,

  /** The builder could not be put back the way the caller left it. Contained — the statement is already
   *  decided — but a LATER execution of that same builder would run capture's selection, so it is never
   *  silent. */
  'restore-failed': ({ relation }: { relation: string; cause: unknown }) =>
    `[telefunc] live: Telefunc could not restore the query builder it borrowed to capture a write on "${describeRelationId(relation)}". This write is unaffected, but re-executing that same builder may return Telefunc's columns instead of yours — this is a bug in Telefunc's capture, please report it.`,

  /** The savepoint bracket itself failed. Nothing left to recover TO: the surrounding transaction is the
   *  caller's to lose, and this says so rather than pretending the rewind happened. */
  'savepoint-bookkeeping-failed': ({ relation, statement }: { relation: string; statement: string; cause: unknown }) =>
    `[telefunc] live: "${statement}" failed while capturing a write on "${describeRelationId(relation)}". The surrounding transaction is likely aborted and its COMMIT will fail — this is a bug in Telefunc's capture, please report it.`,

  // ── after the database applied the write ──

  /** A column decoder threw on rows Telefunc asked for. The write HAPPENED → coarsen the table. */
  'post-commit-decode-failed': ({ relation }: { relation: string; cause: unknown }) =>
    `[telefunc] live: decoding the rows Telefunc added to a write on "${describeRelationId(relation)}" failed AFTER the database applied it (a column decoder threw). The write HAPPENED and your result is unaffected; live queries on this table over-invalidate rather than receive a row capture could not read.`,

  /** The rows came back, but they are not a trustworthy full image → ONE coarse marker for the table. */
  'capture-mismatch': ({
    relation,
    mismatch,
  }: { relation: string; mismatch: { rowIndex: number; reason: string; detail: string } }) =>
    `[telefunc] live write-capture fell back to COARSE for table "${describeRelationId(relation)}": captured row ${mismatch.rowIndex} has ${mismatch.reason} (${mismatch.detail}). The write is unaffected; live queries on this table over-invalidate rather than receive a partial row.`,

  /** Feeding the captured changes threw. Contained: the sink is retried with coarse markers, and if even
   *  that fails the fault is dropped — live queries may be stale until the next write, which is honest. */
  'capture-failed': ({ relation }: { relation: string; cause: unknown }) =>
    `[telefunc] live write-capture failed for table "${describeRelationId(relation)}". The write COMMITTED and its result is unaffected; live queries on this table may be stale until the next write.`,

  /** The server rejected a statement its own version number said it would accept. Reported once per db,
   *  because it changes how every later write on it is captured → new-image capture from here on. */
  'old-new-demoted': ({ relation }: { relation: string }) =>
    `[telefunc] live: this PostgreSQL server reports version 18 or newer but refused "RETURNING old.*, new.*" (first seen writing "${describeRelationId(relation)}"). Falling back to new-image capture for this database. Live queries stay correct, with more coarse invalidation than a genuine PostgreSQL 18 would need.`,

  /** Announcing a committed raw-SQL transaction to the other instances failed. Local graphs already saw it. */
  'announce-failed': (_: { cause: unknown }) =>
    '[telefunc] live: announcing a committed raw-SQL transaction failed; other instances may hold stale live queries until the next write.',

  // ── carrying the batch to the other instances ──

  /** A value the codec cannot carry must not cost the invalidation itself → publish the value-free
   *  coarse-all envelope instead, keeping the seq the stream already took. */
  'encode-failed': (_: { cause: unknown }) =>
    '[telefunc] live: a change batch could not be encoded for the transport, so a COARSE invalidation was published instead. Remote live queries over-fetch rather than miss the write.',

  /** The transport refused the payload. Nothing to recover to — the write is already committed. */
  'publish-failed': (_: { cause: unknown }) =>
    '[telefunc] live: publishing a change batch to the changeTransport failed. The write COMMITTED and its result is unaffected; live queries on other instances may be stale until the next write touching those tables.',

  /** Detachment is unconfirmed, so the listener may still be attached → live reads FAIL CLOSED and retry. */
  'detach-failed': (_: { cause: unknown }) =>
    '[telefunc] live: detaching the change subscription failed, so its listener may still be attached. Live reads on this db FAIL CLOSED until the transport confirms detachment (each one retries it); admitting them would risk applying a change twice.',

  /** Unreadable, and `origin` is unreadable with it — so this may even be our own echo → coarsen
   *  everything watched rather than interpret a guessed row. */
  'undecodable-payload': ({ topic }: { topic: string }) =>
    `[telefunc] live: an undecodable payload arrived on "${topic}" (unknown codec version, or a transport that does not deliver the exact string it was given). Coarsening every watched table rather than interpreting it.`,

  // ── routing a batch into the graphs ──

  /** A routed apply threw, leaving that graph's precise state possibly corrupt → it is PERMANENTLY faulted
   *  to coarse, so no later batch can miss over it. Never swallowed, or the degrade would be invisible. */
  'apply-threw': (_: { cause: unknown }) =>
    '[@telefunc/drizzle-experimental] a routed graph apply threw; faulting it to coarse:',
}

type Fault = keyof typeof FAULTS
type DetailOf<K extends Fault> = Parameters<(typeof FAULTS)[K]>[0]

/** Tell the operator about a fault, and never fail the caller for it. See the module header. */
function report<K extends Fault>(kind: K, detail: DetailOf<K>): void {
  try {
    const message = (FAULTS[kind] as (detail: DetailOf<K>) => string)(detail)
    // Arity is part of what a row declares: a fault WITHOUT a cause must not log a trailing `undefined`.
    if ('cause' in detail) console.error(message, (detail as { cause: unknown }).cause)
    else console.error(message)
  } catch {
    // A console that cannot be written to — or a value that cannot be rendered — is not a reason to fail a
    // committed write.
  }
}
