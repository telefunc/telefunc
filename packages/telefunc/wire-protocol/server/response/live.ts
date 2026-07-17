export { liveReplacer }

import type { LiveContract, ReplacerType, ServerReplacerContext } from '../../types.js'
import { SERIALIZER_PREFIX_LIVE } from '../../constants.js'
import type { LiveEvent } from '../../../node/server/live/live.js'
import type { LiveSerializeContext } from '../../../node/server/live/tags.js'
import { assert } from '../../../utils/assert.js'
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
    // Serialize-time single activation (quota deleted — no reservation step): the channel is created
    // HERE, only because this Live is crossing the wire. A Live that never serializes reaches this
    // never — no channel, no subscription — so it activates nothing and cannot leak.
    const channel = context.createChannel<never, LiveEvent<unknown>>()
    // The producer's coalesced emissions ride the channel; the channel owns their teardown. These taps
    // MUST be installed BEFORE `activate()`: activation resolves this Live's tags against the request
    // fence and replays anything published since, and a catch-up invalidation that fires with no tap
    // attached reaches nothing — the client would keep a snapshot it should have refetched.
    const offData = live.onData((data) => void channel.send({ kind: 'data', data }))
    const offInvalidate = live.onInvalidate(() => void channel.send({ kind: 'invalidate' }))
    // Deferred activation, refcounted by cell-local leases: cascade-activate this cell's pending
    // deps/source (idempotent — a dep also returned elsewhere activates exactly once). This channel
    // holds one lease; its permanent close releases it, tearing down on the last owner.
    // The fence arrives explicitly on the context — on the Live-specific extension of it, since no
    // other extension's replacer has any business knowing this request has a fence. Every Live is
    // serialized as a telefunction's response, after the fence is stamped at that request's entry, so
    // a missing one means the plumbing that carries it is broken rather than that we're outside a
    // request. Guessing "start from now" there would hide exactly the writes the fence exists to
    // catch: everything that landed between the read and this moment, silently, forever.
    const { requestStartSeq } = context as ServerReplacerContext & Partial<LiveSerializeContext>
    assert(requestStartSeq !== undefined)
    live.activate(requestStartSeq)
    channel.onClose(() => {
      offData()
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
