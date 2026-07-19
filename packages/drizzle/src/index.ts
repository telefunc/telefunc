// Public surface of @telefunc/drizzle-experimental — reactive Drizzle queries for Telefunc.
// `reactiveDrizzle(db)` wraps a db once (module level is the intended shape) and binds to nothing
// request-scoped; its `select()` builds an ordinary Drizzle query you terminate with `.live()` to get a
// `Live` instead of rows. Writes through that same db (`insert`/`update`/`delete`, transactions, raw SQL)
// run as plain Drizzle AND are captured, so a live query goes stale exactly when a write touches what it
// reads — precisely where the changed rows can be identified, and fail-closed coarse otherwise. The IR /
// extraction / capture engine internals are implementation detail and are intentionally NOT re-exported.
//
// EXPERIMENTAL, and named so on purpose: the whole feature — including the `Live` primitive itself — lives
// in this package rather than in telefunc core, so abandoning it is a package deletion rather than a core
// migration. Nothing here requires a change to telefunc: the wire replacer and reviver register through
// telefunc's PUBLIC extension seams (see src/primitive/).
//
// `derived` composes a `Live` from other `Live`s. `Live<T>` is exported as a TYPE only — a user reads
// `.data`, and the producer verbs stay internal. The client-side query adapter is the separate
// `./tanstack-query` subpath, so importing this root pulls in no TanStack dependency.

export { reactiveDrizzle } from './live/reactiveDrizzle.js'
export { derived } from './primitive/live.js'
export type { Live } from './primitive/live.js'
