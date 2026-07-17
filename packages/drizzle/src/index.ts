// Public surface of @telefunc/drizzle — reactive Drizzle queries for Telefunc. Call `reactiveDrizzle(db)`
// at the top of a telefunction to get a per-request reactive db, whose `.live.select()` awaits to a
// `Live` instead of rows. Writes are not observed yet: `insert`/`update`/`delete` run as plain Drizzle,
// so a live query goes stale only when something invalidates it. The IR / extraction / read-capture
// engine internals are implementation detail and are intentionally NOT re-exported here.
//
// One function. The types describing what it returns are machinery for saying so — callers get them by
// inference and never name them, so exporting them would only add concepts to learn.

export { reactiveDrizzle } from './live/reactiveDrizzle.js'
