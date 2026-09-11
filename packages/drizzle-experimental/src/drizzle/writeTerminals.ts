export { isBuilderTerminal, isDriverTerminal, isPreparedTerminal, isCoarseAllSurface }

// WHICH MEMBER ACCESS EXECUTES SOMETHING. Every miss here is the same defect: a statement that runs while
// capture is looking the other way, committing rows with nothing published — a systematic missed
// invalidation, not a one-off. That knowledge was enumerated in four places, so a drizzle release adding a
// terminal could be caught in three of them and escape through the fourth.
//
// Three classes, and they answer three different questions:
//
//   BUILDER TERMINAL   the promise surface of a write chain (`await`, `.execute()`). Capture runs the write
//                      through its own plan and intercepts the result.
//   DRIVER TERMINAL    a member that executes IMMEDIATELY rather than through the QueryPromise — SQLite's
//                      `run`/`all`/`get`, and `values` where the receiver is executable. Synchronous on
//                      node:sqlite, which is why capture cannot substitute a RETURNING into it.
//   COARSE-ALL SURFACE a DB-level member that bypasses the builder entirely, so what it touched is
//                      unknowable without parsing SQL. Fails closed by announcing a coarse-all.
//
// The sets overlap on purpose and the overlap is not redundancy: `run`/`all`/`get`/`values` name a driver
// terminal ON A WRITE BUILDER and a raw execution surface ON THE DB. Same word, different receiver,
// different consequence — which is exactly why the discrimination belongs in one file rather than being
// re-derived at each call site.

/** The promise surface every write chain exposes. Each has a different signature, so callers still dispatch
 *  per verb — but whether a member is one of them is decided HERE. */
const BUILDER_TERMINALS = new Set(['then', 'catch', 'finally', 'execute'])

/** Members that execute the statement directly (SQLite). `values` is not here because it is contextual —
 *  see {@link isDriverTerminal}. */
const DRIVER_TERMINALS = new Set(['run', 'all', 'get'])

/** DB-level members that run SQL capture cannot see into, plus two mutations with the same problem.
 *
 *  `refreshMaterializedView` is not SQL execution but changes what a live query on the view reads, and its
 *  effect is unknowable without resolving the view's definition. `db.batch([...])` runs a list that may
 *  include writes (libSQL, D1, Neon-HTTP, sqlite-proxy); reconstructing which rows each item touched would
 *  mean re-planning every statement in it. Left off this list, either falls through to plain Drizzle and
 *  commits with nothing captured — the silent bypass this classification exists to close. Rejecting them
 *  before execution was the alternative and is worse: both are legitimate APIs, and coarse-all is sound. */
const COARSE_ALL_SURFACES = new Set(['run', 'execute', 'all', 'get', 'values', 'refreshMaterializedView', 'batch'])

function isBuilderTerminal(prop: string | symbol): boolean {
  return typeof prop === 'string' && BUILDER_TERMINALS.has(prop)
}

/** Whether a member executes the statement directly on the given receiver.
 *
 *  `values` names TWO things on a write chain, and only one of them executes:
 *
 *    db.insert(t).values(rows)              — the insert BUILDER's own method: supplies the rows, no SQL runs
 *    db.insert(t).values(rows).returning()
 *                             .values()     — a SQLite driver TERMINAL: runs the statement, returns rows
 *
 *  Discriminated by whether the RECEIVER is executable, not by the argument shape: the chain builder
 *  (`SQLiteInsertBuilder`) exposes `values` and nothing else, while an executable statement also exposes the
 *  execution surface. Argument shape alone is not enough — `.values()` with no arguments is exactly what the
 *  terminal looks like, and a caller could equally pass an empty row list.
 *
 *  Verified against node:sqlite: the chain form inserts nothing until a terminal runs, while the terminal
 *  form executes and returns positional row arrays. Those positional rows are why that path stays COARSE —
 *  mapping `[2, 'b']` back to named columns would mean assuming projection order, which is the kind of guess
 *  the capture contract forbids. `captureMismatch` catches it and fails closed. */
function isDriverTerminal(prop: string | symbol, receiver: object): boolean {
  if (typeof prop !== 'string') return false
  if (DRIVER_TERMINALS.has(prop)) return true
  return prop === 'values' && isExecutable(receiver)
}

/** A receiver that can run the statement it holds, as opposed to one still building it.
 *
 *  DELIBERATELY BROAD. Probed across both dialects, every executable drizzle surface exposes `execute`, so
 *  the `then` arm is currently redundant — a mutation removing it survives the whole suite. It stays because
 *  the two ways of being wrong here are not symmetric: reading an executable receiver as a builder lets the
 *  statement run UNCAPTURED (a silent missed invalidation), while reading a builder as executable breaks the
 *  chain loudly and immediately. Widening toward "this executes" fails in the direction that gets noticed. */
function isExecutable(receiver: object): boolean {
  const candidate = receiver as { execute?: unknown; then?: unknown }
  return typeof candidate.execute === 'function' || typeof candidate.then === 'function'
}

/** Whether a member of a PREPARED statement executes it. Every terminal of both other classes does, and
 *  `values` unconditionally: a prepared statement IS the executable surface, so there is no chain builder
 *  left for it to be ambiguous with. Asked separately rather than by probing the receiver, because that is
 *  a fact about the prepared surface itself and not something to re-derive per call. */
function isPreparedTerminal(prop: string | symbol): boolean {
  return isBuilderTerminal(prop) || (typeof prop === 'string' && (DRIVER_TERMINALS.has(prop) || prop === 'values'))
}

function isCoarseAllSurface(prop: string | symbol): boolean {
  return typeof prop === 'string' && COARSE_ALL_SURFACES.has(prop)
}
