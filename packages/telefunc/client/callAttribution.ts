export { markContextConsumer, contextConsumerOf }

import { getGlobalObject } from '../utils/getGlobalObject.js'
import type { ClientCallContext } from './withContext.js'

/**
 * Which per-call context a given telefunction call actually consumed.
 *
 * `getPendingContext()` nulling the slot proves that SOMETHING took the context; it cannot say WHAT. A
 * closure that calls one telefunction and returns a different value consumes the context and hands back
 * something the signal never reached — the failure the whole check exists to catch, passing the check.
 * Attribution is the missing half: the call is stamped with the context it read, at the one point where
 * both exist, so a caller can ask about the value it was actually given rather than about a side effect.
 *
 * `@internal` — not public API. It exists so `withContextChecked` can attribute consumption; the shape of
 * the key (whatever the generated stub returns) is an implementation detail on both sides.
 *
 * Weak, so an entry dies with the call it describes. Only calls made INSIDE a context window are recorded,
 * so an app that never uses `withContext` never grows this map at all.
 *
 * Its own global key, deliberately. `getGlobalObject` is keyed by FILENAME and hands back whichever object
 * registered that key first, so adding a field to `withContext.ts`'s object would leave it silently missing
 * wherever a second copy of that module registered first — the exact trap that makes a consumption COUNTER
 * unworkable. A fresh key has no such incumbent: every copy agrees on one map.
 */
const globalObject = getGlobalObject<{ consumers: WeakMap<object, ClientCallContext> }>('callAttribution.ts', () => ({
  consumers: new WeakMap(),
}))

/** Record that `call` read `context`. Called where the generated stub creates the call. */
function markContextConsumer(call: object, context: ClientCallContext): void {
  globalObject.consumers.set(call, context)
}

/** The context `call` consumed, or undefined — including for a non-object, which `WeakMap.get` reports as
 *  undefined rather than throwing. Anything unattributable therefore reads as "did not consume ours". */
function contextConsumerOf(call: unknown): ClientCallContext | undefined {
  return globalObject.consumers.get(call as object)
}
