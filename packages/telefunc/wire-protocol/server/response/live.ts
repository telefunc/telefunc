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
    // The producer's coalesced emissions ride the channel; the channel owns their teardown.
    // These taps MUST be installed BEFORE `activate()`. Activation resolves this Live's tags against the
    // request fence and replays anything published since — an invalidation that fires with no tap
    // attached reaches nothing, and the client silently keeps a snapshot it should have refetched.
    // Frames sent before the client connects wait in the channel's pre-peer buffer, which is byte-capped
    // and drops in two ways: an oversized frame clears the whole lane, and an ordinary overflow evicts
    // the OLDEST entries — which is exactly where the earliest emission sits. `send` swallows the
    // resulting rejection, so nothing here can tell whether a frame survived; the channel goes on
    // looking healthy while the client holds a snapshot it should have replaced.
    //
    // So remember, in one bit that lives outside the buffer, that something was emitted before anyone
    // was listening. It applies to data pushes as much as invalidations: a Live's truth may travel in
    // either, and a dropped push leaves the client on the wire snapshot just as silently.
    let peerConnected = false
    let missedUpdate = false
    // The snapshot's revision, filled in below when the metadata is built. A data emission only tells
    // this client something new if the value moved PAST what its snapshot already carries: a producer
    // that populates with `set(...)` before returning has its value in the metadata, and its coalesced
    // emission still lands here — pre-connect, but not news. Treating that as a miss would tell the
    // client to refetch what it has, and the refetch would build the same Live and say it again.
    // Revisions rather than value comparison, because `set` may hand back a mutated object.
    let snapshotRevision = Number.POSITIVE_INFINITY
    const offData = live.onData((data) => {
      if (!peerConnected && live.revision > snapshotRevision) missedUpdate = true
      void channel.send({ kind: 'data', data })
    })
    const offInvalidate = live.onInvalidate(() => {
      if (!peerConnected) missedUpdate = true
      void channel.send({ kind: 'invalidate' })
    })
    // `onOpen` fires once, on the first peer attach, AFTER the pre-peer buffer is flushed — so this
    // lands behind whatever did survive. Since a survivor is indistinguishable from a casualty here,
    // this may be redundant: the client refetches once more than it strictly had to. That is the cheap
    // direction. The other one loses the only signal that its data moved on.
    channel.onOpen(() => {
      peerConnected = true
      if (!missedUpdate) return
      missedUpdate = false
      void channel.send({ kind: 'invalidate' })
    })
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
    // Capture the revision beside the value it describes, in the same tick — the taps above only fire
    // on a later microtask, so by then this records exactly what the client was sent.
    snapshotRevision = live.revision
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
