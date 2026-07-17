// Public surface of @telefunc/drizzle — reactive Drizzle queries for Telefunc. Call `reactiveDrizzle(db)`
// at the top of a telefunction to get a per-request reactive db (`.live.select()` awaits to a `Live`;
// plain `insert`/`update`/`delete` are auto-captured). The IR / extraction / read-capture
// engine internals are implementation detail and are intentionally NOT re-exported here.

export { reactiveDrizzle } from './live/reactiveDrizzle.js'
export type { Reactive, LiveOf, LiveNamespace } from './live/reactiveDrizzle.js'
