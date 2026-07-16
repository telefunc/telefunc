export { assembleDbLiveRuntime }

import { getRawContext } from 'telefunc'
import { onPostSerialize } from 'telefunc/__internal'
import type { DbLiveCarrier, DbLiveRuntime } from './reactiveDrizzle.js'
import { wrapLiveSelect, disposeUnredeemedReads, type ReadCarrier } from './readCapture.js'

// ─────────────────────────────────────────────────────────────────────────────
// Ticket 6 §U3 — the telefunc-side db.live RUNTIME assembly [GEN]. Owns the per-request CARRIER
// lifecycle (acquire before the first await; the R1 net-zero finally-sweep) + the pass-through
// captureMutation (U4 supplies the real write-capture). The engine half — `wrapLiveSelect` +
// `disposeUnredeemedReads` — is EngineFix's readCapture.ts; this module threads them behind the
// `DbLiveRuntime` seam, and server.ts installs the assembled runtime at extension setup.
// ─────────────────────────────────────────────────────────────────────────────

/** The context symbol under which a request's db.live carrier is stashed — one carrier per request,
 *  reused by every `.live` read + captured mutation in that request. */
const REACTIVE_CARRIER = Symbol.for('telefunc.reactiveDrizzleCarrier')

/** The concrete carrier: the read half (minted tokens — EngineFix's `ReadCarrier`) plus the opaque
 *  brand the client surface threads. The write tx-scope is added here by U4. */
type CarrierImpl = ReadCarrier & { __dbLiveCarrier: true }

/** Acquire the per-request carrier — MUST run before the body's first await, while `getRawContext()`
 *  is still non-null (it nulls at the first macrotask in sync mode, the no-async_hooks reality). Get-or-
 *  create on the context, and register the R1 finally-sweep so any read token minted but never activated
 *  (a handle created but never serialized) is released post-serialize — net-zero, no leak. The disposer
 *  binds to THIS context object (captured eagerly here), so the drain finds it even after context nulls. */
function acquireCarrier(): DbLiveCarrier {
  const context = getRawContext()
  if (!context) {
    throw new Error(
      'reactiveDrizzle: the reactive db accessor must be called inside a telefunction, before the body’s first await.',
    )
  }
  const existing = context[REACTIVE_CARRIER] as CarrierImpl | undefined
  if (existing) return existing
  const carrier: CarrierImpl = { __dbLiveCarrier: true, mintedTokens: [] }
  context[REACTIVE_CARRIER] = carrier
  onPostSerialize(context, () => disposeUnredeemedReads(carrier))
  return carrier
}

/** U3: pass-through — mutations run as plain Drizzle. The write-capture unit (U4) replaces this with
 *  the tx-scoped accumulator → `router.ingest` → Broadcast; the carrier already carries the write slot. */
function captureMutation(
  _op: 'insert' | 'update' | 'delete',
  baseMethod: (...a: unknown[]) => unknown,
  _carrier: DbLiveCarrier,
): (...a: unknown[]) => unknown {
  return baseMethod
}

/** Assemble the installable runtime: EngineFix's read-capture engine + the carrier lifecycle + the
 *  pass-through mutations. The opaque `DbLiveCarrier` the client surface threads is the concrete
 *  `ReadCarrier` at runtime, so the engine boundary casts through it. Installed once by server.ts. */
function assembleDbLiveRuntime(): DbLiveRuntime {
  return {
    acquireCarrier,
    wrapLiveSelect: (baseBuilder, carrier, db) =>
      wrapLiveSelect(baseBuilder, carrier as unknown as ReadCarrier, db as object),
    captureMutation,
  }
}
