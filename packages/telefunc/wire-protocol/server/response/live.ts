export { liveReplacer }

import type { LiveContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_LIVE } from '../../constants.js'
import type { LiveEvent } from '../../../node/server/live/live.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
assertIsNotBrowser()

// Reconstruct the PRIVATE Live brand locally (global-registry symbol) — detection is internal, never
// a public surface, so the brand is neither imported nor exported.
const LIVE_BRAND = Symbol.for('telefunc.Live')

const liveReplacer: ReplacerType<LiveContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_LIVE,
  detect(value): value is LiveContract['value'] {
    return typeof value === 'object' && value !== null && LIVE_BRAND in value
  },
  replace(live, context) {
    // Serialize-time single activation, with no reservation step: the channel is created
    // HERE, only because this Live is crossing the wire. A Live that never serializes reaches this never, so
    // no channel and no source subscription are ever created for it. That bounds what THIS layer holds; a
    // producer whose attached source reserved resources BEFORE serializing (as @telefunc/drizzle's does, to
    // register its graph before the snapshot read) still has to reclaim those itself.
    const channel = context.createChannel<never, LiveEvent>()
    // The producer's coalesced invalidation rides the channel; the channel owns its teardown. This tap
    // MUST be installed BEFORE `activate()`: activation subscribes this Live's invalidation source, and
    // a source that replays a catch-up synchronously on subscribe would fire with no tap attached
    // otherwise — the client would keep a snapshot it should have refetched.
    // The signal IS the message — its arrival is the event, so nothing is carried. Wire-compatible in
    // both directions: no client version has ever branched on the payload (the reviver's listener
    // discards its argument), and `LiveEvent` is internal, never exported, so nothing outside this
    // package can be typed against the old shape.
    const offInvalidate = live.onInvalidate(() => void channel.send(undefined))
    // Deferred activation, refcounted by cell-local leases: cascade-activate this cell's pending
    // deps/source (idempotent — a dep also returned elsewhere activates exactly once). This channel
    // holds one lease; its permanent close releases it, tearing down on the last owner.
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
