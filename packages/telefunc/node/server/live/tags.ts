export { stampRequestStartFence, getRequestStartSeq, subscribeTag, invalidateTagStatic }
export type { LiveSerializeContext }

import { getRawContext } from '../context/context.js'
import type { Context } from '../context/context.js'
import { getTagHub } from './tagHub.js'
import type { TagHub } from './tagHub.js'

const TAGS = Symbol.for('telefunc.tagState')

type TagState = {
  hub: TagHub
  /** The fence: only tags published after this seq replay to a subscribing source. */
  requestStartSeq: number
}

/** Stamp the request's fence, once, before the body runs — and therefore before any read it performs.
 *  Key-agnostic: it records WHEN the request started observing, not what it cares about. Awaits the
 *  hub's readiness barrier so the subscription is proven live before that moment is recorded. */
async function stampRequestStartFence(): Promise<void> {
  const context = getRawContext()
  if (!context || context[TAGS]) return
  const hub = getTagHub()
  await hub.ready()
  context[TAGS] = {
    hub,
    requestStartSeq: hub.currentSeq(),
  } satisfies TagState
}

/** @internal What serialization carries so a Live can resolve its tags: the request's fence, and
 *  nothing else. Kept OFF the generic replacer context — that type is public and shared by every
 *  extension's replacer, none of which have any business knowing this request has a fence. */
type LiveSerializeContext = { requestStartSeq: number }

/** Read the stamped fence off an EXPLICIT context object. Serialization passes the context it already
 *  holds rather than reaching for the ambient one, which by then may be gone: in the default sync mode
 *  the request context is nulled at the first macrotask, so an ambient read at serialize is a race. */
function getRequestStartSeq(context: Context): number | undefined {
  return (context[TAGS] as TagState | undefined)?.requestStartSeq
}

/** Subscribe `tag`, replaying anything published since `requestStartSeq`.
 *
 *  Order is load-bearing: REGISTER FIRST, then scan. Registering first means a publish landing during
 *  the scan is delivered by the index rather than falling between the two steps; the reverse order has
 *  a window where an invalidation belongs to neither. An overlap is harmless — invalidation is
 *  idempotent, so the pair may deliver twice but can never deliver zero times. A journal overflow
 *  replays unconditionally, since "published since the fence" can no longer be answered.
 *
 *  Context-free by construction: everything it needs is an argument, so it runs at serialize time
 *  whether or not the request context still exists. */
function subscribeTag(tag: string, requestStartSeq: number, onInvalidate: () => void): () => void {
  const hub = getTagHub()
  const teardown = hub.registerTag(tag, onInvalidate)
  if (hub.hasOverflow(requestStartSeq) || hub.hasTagSince(requestStartSeq, tag)) onInvalidate()
  return teardown
}

/** Invalidate `tag` (fire-and-forget, `void`). Publishes IMMEDIATELY and knows nothing about requests
 *  or transactions: the caller decides when a change is real, and says so by calling this. Holding a
 *  write's invalidations back until its transaction commits belongs to whoever owns that transaction —
 *  nothing at this level can see one. */
function invalidateTagStatic(tag: string): void {
  void publishTagImmediate(tag)
}

/** Publish `tag` to every instance, falling back to this one if the transport can't be reached.
 *
 *  Getting the tag OUT is one operation with two ways to fail, so it gets one failure boundary.
 *  Confirming the subscription can fail (the transport is down, or never proves itself and fails
 *  closed) just as publishing can, and either way the outcome is the same: the fan-out hop is lost, so
 *  local subscribers are told directly. Leaving readiness outside the boundary meant a first
 *  invalidation against a broken transport was neither published, nor journaled, nor fired — and its
 *  rejection escaped a caller that, by design, is not awaiting it.
 *
 *  Never throws: the caller is fire-and-forget, so a throw here has nowhere to go but an unhandled
 *  rejection. A failed readiness probe is not cached (see `ready`), so a later invalidation re-probes
 *  and recovers on its own. */
async function publishTagImmediate(tag: string): Promise<void> {
  const hub = getTagHub()
  try {
    await hub.ready()
    await hub.publish([tag])
  } catch (err) {
    console.error('[telefunc:live] tag publish failed; firing local subscribers instead:', err)
    hub.fireLocal([tag])
  }
}
