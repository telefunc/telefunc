export { acquireCarrier, captureMutation }

import { getRawContext } from 'telefunc'
import { getTelefuncHost } from './telefuncHost.js'
import type { DbLiveCarrier } from './reactiveDrizzle.js'
import { disposeUnredeemedReads, type ReadCarrier } from './readCapture.js'

// The per-request carrier every `db.live` read in one request shares. Acquiring it must happen before
// the body's first await, while the request context still exists; the cleanup that follows serialization
// (registered through telefunc's extension host, not its internals) releases anything a read reserved but
// never used.

/** The context symbol under which a request's db.live carrier is stashed — one carrier per request,
 *  reused by every `.live` read in that request. */
const REACTIVE_CARRIER = Symbol.for('telefunc.reactiveDrizzleCarrier')

/** The concrete carrier: the read half (the tokens each live read reserves) plus the opaque brand the
 *  client surface threads through. */
type CarrierImpl = ReadCarrier & { __dbLiveCarrier: true }

/** Acquire the per-request carrier — MUST run before the body's first await, while `getRawContext()`
 *  is still non-null (it nulls at the first macrotask in sync mode, the no-async_hooks reality). Get-or-
 *  create on the context, and register the finally-sweep (via the extension host) so any read token minted
 *  but never activated (a handle created but never serialized) is released post-serialize — net-zero, no
 *  leak. The host binds the cleanup to THIS request's context, so the drain finds it even after context
 *  nulls. */
function acquireCarrier(): DbLiveCarrier {
  const context = getRawContext()
  if (!context) {
    throw new Error('reactiveDrizzle(db) must be called inside a telefunction, before the body’s first await.')
  }
  const existing = context[REACTIVE_CARRIER] as CarrierImpl | undefined
  if (existing) return existing
  const carrier: CarrierImpl = { __dbLiveCarrier: true, mintedTokens: [] }
  context[REACTIVE_CARRIER] = carrier
  getTelefuncHost().onRequestCleanup(() => disposeUnredeemedReads(carrier))
  return carrier
}

/** Pass-through: mutations run as plain Drizzle and nothing observes them. The seam exists so writes
 *  route through one place once capture is built; today it changes nothing. */
function captureMutation(
  _op: 'insert' | 'update' | 'delete',
  baseMethod: (...a: unknown[]) => unknown,
  _carrier: DbLiveCarrier,
): (...a: unknown[]) => unknown {
  return baseMethod
}
