export { isWriteOp, isRawSqlOp, captureTransactions }

import { registryFor } from './dbRuntime.js'
import { publishCoarseAll } from './changeRuntime.js'
import { type CaptureSink, captureMutation, captureRawSql, emitSafely } from './writeCapture.js'
import type { TableChange } from '../router/events.js'

// THE WRITE-SIDE PROXY MACHINERY: which db members are writes, which are raw execution surfaces, and the
// whole of what a `db.transaction(cb)` has to do so that one committed transaction is one atomic graph tick.
//
// Kept out of the `reactiveDrizzle` entry because it is a different altitude: the entry routes a member
// access to a strategy, while everything here is the strategy — buffering, savepoint scoping, commit-time
// marker computation, and the rollback semantics that make each of those safe.

function isWriteOp(prop: string | symbol): prop is 'insert' | 'update' | 'delete' {
  return prop === 'insert' || prop === 'update' || prop === 'delete'
}

/** The db-level RAW SQL execution surfaces — they bypass the builder entirely, so they need their own
 *  fail-closed capture (coarsen this db's watched tables). */
function isRawSqlOp(prop: string | symbol): boolean {
  return (
    prop === 'run' ||
    prop === 'execute' ||
    prop === 'all' ||
    prop === 'get' ||
    prop === 'values' ||
    // Not SQL execution, but a MUTATION with the same problem: it changes what a live query on the view
    // reads, and its effect is unknowable without resolving the view's definition. It used to fall through
    // to plain Drizzle and commit with nothing captured and nothing published — the same silent bypass
    // `tx.execute` had. Coarsening it is sound and it is a rare, deliberate operation.
    prop === 'refreshMaterializedView' ||
    // `db.batch([...])` runs a list of statements that may include writes (libSQL, D1, Neon-HTTP,
    // sqlite-proxy). Reconstructing which rows each item touched would mean re-planning every statement in
    // the list, so it takes the same coarse-all path — AFTER the batch completes, with the caller's result
    // handed back untouched. Rejecting it before execution was the alternative and is worse: batch is a
    // legitimate API, and coarse-all is sound.
    prop === 'batch'
  )
}

/** Everything ONE transaction scope needs to know about the world enclosing it.
 *
 *  This used to be seven positional parameters on `wrapTransaction`, which is what a concern sitting too
 *  high looks like: the entry had to build the announcement closure and the commit-marker closure itself,
 *  so a caller that only wanted "capture transactions on this db" had to know how a savepoint differs from
 *  a top-level commit. The scope is owned here, and the two shapes it can take are named below. */
type TransactionScope = {
  /** The session capture PLANS against and keys the registry to — always the top db, never a tx handle.
   *  A tx db is not recognized as its own driver, and the graphs live on the db the reads acquired from. */
  topDb: object
  /** Where a committed buffer flushes: the top db's graphs, or a parent tx's buffer for a savepoint. */
  sink: CaptureSink
  /** The SAME destination minus the remote publication. A transaction that ran raw SQL announces itself
   *  remotely as ONE coarse-all — which reseeds every remotely-watched graph — so also publishing its batch
   *  would make every remote watcher pay twice for one commit. The batch still flushes locally in one tick;
   *  only the remote hop is dropped, because the coarse-all supersedes it. For a SAVEPOINT both sinks are
   *  the parent's buffer, and the top level makes the choice once. */
  localSink: CaptureSink
  /** How a committed raw statement reaches other instances. At the top level it publishes; a savepoint
   *  promotes the intent into its parent instead. */
  announce: () => void
  /** The coarse markers a committed raw statement owes THIS db's own graphs, computed AT COMMIT — the top
   *  level supplies the real thing; a savepoint supplies nothing and promotes intent, so the markers are
   *  computed exactly once, against the watch-set as it stands when the transaction actually lands. */
  commitCoarseMarkers: () => TableChange[]
  /** The ROOT of the physical transaction this scope belongs to — undefined at the top level, where the
   *  transaction drizzle is about to open BECOMES the root. A nested scope is a SAVEPOINT inside the same
   *  physical transaction, not a transaction of its own, so it inherits the root rather than starting one. */
  root?: object
}

/** Capture every transaction opened on a reactive db. This is the whole seam the entry needs: hand it the
 *  host and the db's two sinks, and the top-level scope — including what a committed raw statement
 *  announces and which markers it owes — is constructed here rather than at the call site. */
function captureTransactions(txHost: object, topDb: object, sink: CaptureSink, localSink: CaptureSink) {
  return wrapTransaction(txHost, {
    topDb,
    sink,
    localSink,
    announce: () => publishCoarseAll(topDb),
    commitCoarseMarkers: () =>
      registryFor(topDb)
        .router.watchedTables()
        .map((table) => ({ table, kind: 'coarse' as const })),
  })
}

/** Wrap `db.transaction(cb)`: the callback gets a proxied tx db whose writes BUFFER; on outer COMMIT the
 *  whole buffer flushes as ONE `ChangeBatch` to the scope's sink (the top db's graphs, or a parent tx's
 *  buffer for a savepoint), so a committed transaction is one atomic graph tick. A rollback rejects and
 *  never flushes → its changes are discarded. */
function wrapTransaction(txHost: object, scope: TransactionScope) {
  return (callback: (tx: unknown) => unknown, config?: unknown) => {
    const buffer: TableChange[] = []
    const buffered: CaptureSink = (changes) => {
      for (const change of changes) buffer.push(change)
    }
    // A raw statement inside the transaction cannot be published when it runs: the transaction may still roll
    // back, and a coarse-all already broadcast would have told every other instance to refetch state that
    // never existed. Record the intent and hand it to the enclosing scope only if we COMMIT.
    let announcePending = false
    const bufferedAnnounce = () => {
      announcePending = true
    }
    const baseTransaction = (txHost as { transaction: (cb: unknown, c?: unknown) => unknown }).transaction.bind(txHost)
    return Promise.resolve(
      baseTransaction(
        (tx: object) => callback(txProxy(tx, scope.topDb, buffered, bufferedAnnounce, scope.root ?? tx)),
        config,
      ),
    ).then((result) => {
      // Reached ONLY on COMMIT (a rollback / savepoint-rollback rejects and skips this) → flush once.
      // Isolated: the transaction has COMMITTED, so a capture/publish fault must not reject it (it degrades
      // to a coarse ingest and is reported) — the caller's result stays exactly plain Drizzle's.
      //
      // A transaction that ran raw SQL flushes LOCAL-ONLY: its remote announcement is the single coarse-all
      // below, and publishing the batch as well would deliver two messages — and two refetches — for one
      // commit. This used to be defended as the price of atomicity ("splitting the raw markers out would
      // make two local ticks"), which was a false dichotomy: the whole buffer still lands in ONE local tick
      // here; only the redundant remote copy is dropped.
      //
      // The raw statement's coarse markers are computed HERE, at commit, not when the statement ran. The
      // buffer only ever carried INTENT for raw SQL, because statement-time markers snapshot the watch-set
      // of an earlier moment: a graph registering between the raw statement and this commit would be absent
      // from them, unreachable by this local flush — and the remote coarse-all below is its own publisher's
      // echo, origin-suppressed locally — so it would NEVER hear about the committed rows.
      if (announcePending) {
        const merged = [...buffer, ...scope.commitCoarseMarkers()]
        if (merged.length > 0) emitSafely(scope.localSink, merged)
      } else if (buffer.length > 0) {
        emitSafely(scope.sink, buffer)
      }
      // For a SAVEPOINT this promotes the intent into the parent's; at the top level it publishes.
      if (announcePending) {
        try {
          scope.announce()
        } catch (error) {
          console.error(
            '[telefunc] live: announcing a committed raw-SQL transaction failed; other instances may hold stale live queries until the next write.',
            error,
          )
        }
      }
      return result
    })
  }
}

/** A raw statement inside a transaction contributes NO markers at statement time — see the raw branch in
 *  txProxy. Named so the call site says what is happening rather than passing an anonymous no-op. */
const DROP_MARKERS: CaptureSink = () => {}

/** A SAVEPOINT's scope: it flushes into the PARENT's buffer (both sinks are that one buffer), promotes its
 *  raw-SQL intent rather than publishing, owes no markers of its own — the top level computes those once —
 *  and stays on the ROOT's write queue, because a savepoint shares one physical connection with its parent
 *  and giving it a queue of its own lets the two interleave and destroy each other's savepoints. */
function savepointScope(topDb: object, parentBuffer: CaptureSink, promote: () => void, root: object): TransactionScope {
  return {
    topDb,
    sink: parentBuffer,
    localSink: parentBuffer,
    announce: promote,
    commitCoarseMarkers: () => [],
    root,
  }
}

/** The proxy over a transaction db: writes buffer (via `sink`); a nested `transaction` is a SAVEPOINT whose
 *  buffer flushes into THIS one on release (and is discarded on savepoint-rollback); reads + everything else
 *  pass through as plain Drizzle (a live read inside a write transaction is out of scope). */
function txProxy(txDb: object, topDb: object, sink: CaptureSink, announce: () => void, root: object): unknown {
  return new Proxy(txDb, {
    get(target, prop, receiver) {
      if (isWriteOp(prop)) {
        const base = Reflect.get(target, prop, receiver) as (...a: unknown[]) => unknown
        // `target` is the RAW tx db, passed ONLY as an execution handle for the capture-recovery savepoint.
        // Planning and registry keying still read `topDb` — that invariant is what keeps this apart from
        // the separate question of which db owns session identity. Never the PROXY: `execute` on it is
        // intercepted as raw SQL, so a SAVEPOINT through it would coarsen the whole transaction.
        return captureMutation(prop, base.bind(target), topDb, sink, target, root) // plan on topDb; run on tx; buffer
      }
      if (prop === 'transaction') {
        return wrapTransaction(target, savepointScope(topDb, sink, announce, root))
      }
      if (isRawSqlOp(prop)) {
        // `tx.execute(sql`…`)` used to pass straight through, so a raw write committed with NOTHING
        // published — the same silent bypass the top-level db already fixed.
        //
        // Inside a transaction, raw SQL records INTENT ONLY (`announce`); its coarse markers are NOT
        // materialized here. Markers name the tables being watched, and the watch-set at statement time is
        // the wrong one — a live read admitted between this statement and the outer COMMIT would be missing
        // from them, unreachable by the commit flush, and origin-suppressed out of the remote coarse-all:
        // it would never hear about the committed rows at all. The outer commit computes the markers
        // against the watch-set that exists when the transaction actually lands, and flushes them with the
        // ORM buffer in one atomic local tick; the single coarse-all carries the remote side.
        const base = Reflect.get(target, prop, receiver)
        if (typeof base === 'function') {
          return captureRawSql((base as (...a: unknown[]) => unknown).bind(target), topDb, DROP_MARKERS, announce)
        }
      }
      return Reflect.get(target, prop, receiver)
    },
  })
}
