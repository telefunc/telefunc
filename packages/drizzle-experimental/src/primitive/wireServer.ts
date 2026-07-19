export { installLiveReplacer, liveReplacer }

import { config } from 'telefunc'
import type { ReplacerType, ServerReplacerContext, TypeContract } from 'telefunc'
import type { Live, LiveCell, LiveEvent } from './live.js'
import { SERIALIZER_PREFIX_LIVE } from './wireConstants.js'

// THE SERVER HALF OF THE WIRE, registered from this package through telefunc's PUBLIC extension seam
// (`config.extensions.push({ responseTypes })`). Core's built-in replacer registry does not know a Live
// exists — which is the whole point: delete this package and core's wire protocol is untouched.

/** A live value on the wire: the initial snapshot `data` + the opaque channel ref. Serialize-time
 *  activation creates the channel only when a Live crosses the wire. Server side is the producer cell;
 *  the client revives the public `Live<T>` — a channel-backed view, not a cell. */
type LiveContract = TypeContract<LiveCell<unknown>, Live<unknown>, { data: unknown; channelId: string }>

// Reconstruct the PRIVATE Live brand locally (global-registry symbol) — detection is internal, never a
// public surface, so the brand is neither imported nor exported.
const LIVE_BRAND = Symbol.for('telefunc.Live')

const liveReplacer: ReplacerType<LiveContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_LIVE,
  detect(value): value is LiveContract['value'] {
    return typeof value === 'object' && value !== null && LIVE_BRAND in value
  },
  replace(live, context) {
    // Serialize-time single activation, with no reservation step: the channel is created HERE, only
    // because this Live is crossing the wire. A Live that never serializes reaches this never, so no
    // channel and no source subscription are ever created for it. That bounds what THIS layer holds; a
    // producer whose attached source reserved resources BEFORE serializing (as the read-capture engine's
    // does, to register its graph before the snapshot read) still has to reclaim those itself.
    const channel = context.createChannel<never, LiveEvent>()
    // The producer's coalesced invalidation rides the channel; the channel owns its teardown.
    //
    // The tap is installed BEFORE `activate()` so that subscription ordering can never matter. Inherited
    // from core with a stronger claim than it can carry ("MUST … otherwise the client keeps a stale
    // snapshot"): a mutation moving this below `activate()` SURVIVES the suite, because `invalidate()`
    // defers to a microtask — a source replaying a catch-up synchronously on subscribe sets the pending
    // flag and flushes after the tap is attached either way. Kept in this order regardless, since it costs
    // nothing and removes the question; recorded as defence in depth rather than a proven invariant.
    // The signal IS the message — its arrival is the event, so nothing is carried.
    const offInvalidate = live.onInvalidate(() => void channel.send(undefined))
    // Deferred activation, refcounted by cell-local leases: cascade-activate this cell's pending
    // deps/source (idempotent — a dep also returned elsewhere activates exactly once). This channel holds
    // one lease; its permanent close releases it, tearing down on the last owner.
    live.activate()
    channel.onClose(() => {
      offInvalidate()
      // `release` closes the cell only on the LAST owning channel (lease 0) — a shared dep whose own
      // channel closes stays live for the derived cells that hold it.
      live.release()
    })
    return {
      metadata: { data: live.data, channelId: channel.id },
      async close() {
        await channel.close()
      },
      abort(abortError) {
        channel.abort(abortError.abortValue)
      },
    }
  },
}

let installed = false

/** Register the Live replacer with telefunc, at most once per process. Called from `reactiveDrizzle()`
 *  rather than at import time, so merely importing this package registers nothing — and so the
 *  registration is an internal detail the public API never asks the user to perform. */
function installLiveReplacer(): void {
  if (installed) return
  installed = true
  config.extensions.push({
    name: '@telefunc/drizzle-experimental',
    responseTypes: [liveReplacer as ReplacerType<TypeContract, ServerReplacerContext>],
  })
}
