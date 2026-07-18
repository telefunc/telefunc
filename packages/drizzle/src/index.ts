// Public surface of @telefunc/drizzle — reactive Drizzle queries for Telefunc. Call `reactiveDrizzle(db)`
// at the top of a telefunction to get a per-request reactive db, whose `select()` builds an ordinary
// Drizzle query you terminate with `.live()` to get a `Live` instead of rows. Writes through that same db
// (`insert`/`update`/`delete`, transactions, raw SQL) run as plain Drizzle AND are captured, so a live
// query goes stale exactly when a write touches what it reads — precisely where the changed rows can be
// identified, and fail-closed coarse otherwise. The IR / extraction / capture engine internals are
// implementation detail and are intentionally NOT re-exported here.
//
// One function. The types describing what it returns are machinery for saying so — callers get them by
// inference and never name them, so exporting them would only add concepts to learn.

export { reactiveDrizzle } from './live/reactiveDrizzle.js'
