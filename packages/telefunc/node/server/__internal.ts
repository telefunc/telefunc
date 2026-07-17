export type { TELEFUNC_SHIELDS } from '../shared/transformer/generateShield/shield-key.js'

// The R1 post-serialize disposer seam (Ticket 6) — internal cross-package surface for reactive bindings,
// deliberately NOT on the public `telefunc` entry. `onPostSerialize` lets a binding (e.g. @telefunc/drizzle)
// register a request-scoped disposer that runs after the response is serialized (net-zero token release);
// `drainPostSerializeDisposers` is the core drain (also exposed here so bindings can gate it under test).
export { onPostSerialize, drainPostSerializeDisposers } from './context/postSerialize.js'
