export type { TELEFUNC_SHIELDS } from '../shared/transformer/generateShield/shield-key.js'

// The post-serialize disposer seam — internal cross-package surface for reactive bindings,
// deliberately NOT on the public `telefunc` entry. `onPostSerialize` lets a binding (e.g. @telefunc/drizzle)
// register a request-scoped disposer that runs after the response is serialized (net-zero token release);
// `drainPostSerializeDisposers` is the core drain (also exposed here so bindings can gate it under test).
export { onPostSerialize, drainPostSerializeDisposers } from './context/postSerialize.js'

// The Live seams — internal cross-package surface for reactive bindings + query adapters, deliberately
// NOT on the public `telefunc` entry (where a Live is only `{ readonly data: T }` + `Live.derived`).
// `LiveCell` is the PRODUCER end a binding builds (e.g. @telefunc/drizzle's read-capture: construct
// around the initial rows, attach the graph as an invalidation source) — server-side, so a runtime
// export is fine. `LiveSubscription` is the CONSUMER end an adapter binds (e.g. @telefunc/tanstack-query:
// invalidate → refetch, data → cache write) — TYPE-ONLY on purpose: those adapters ship to the browser,
// and this is a server entry, so only an erased type import can cross that boundary safely.
export { LiveCell } from './live/live.js'
export type { LiveSubscription } from './live/live.js'
