export { liveTag, invalidateTag, stampRequestStartFence, publishQueuedTags }
export { subscribeTagFenced, captureTagFence, subscribeCapturedTag, invalidateTagStatic }
export type { TagFence }

import { assertUsage } from '../../../utils/assert.js'
import { getRawContext } from '../context/context.js'
import { addLiveSource } from './source.js'
import type { LiveSource } from './source.js'
import { getTagHub } from './tagHub.js'
import type { TagHub } from './tagHub.js'

const TAGS = Symbol.for('telefunc.tagState')

type TagState = {
  hub: TagHub
  /** The fence: only tags published after this seq replay to a subscribing source. */
  requestStartSeq: number
  /** invalidateTag() calls, published as one deduped batch at settle. */
  queued: Set<string>
}

/** Stamp the request-start fence and await the hub's readiness barrier once — runs before the body
 *  (core START step) so a tag published between the read and a later `liveTag()` still replays. */
async function stampRequestStartFence(): Promise<void> {
  const context = getRawContext()
  if (!context || context[TAGS]) return
  const hub = getTagHub()
  await hub.ready()
  context[TAGS] = {
    hub,
    requestStartSeq: hub.currentSeq(),
    queued: new Set(),
  } satisfies TagState
}

function getRequestTagState(): TagState {
  const context = getRawContext()
  assertUsage(context, 'liveTag()/invalidateTag() can only be used inside a telefunction.')
  const existing = context[TAGS] as TagState | undefined
  if (existing) return existing
  const hub = getTagHub()
  const state: TagState = { hub, requestStartSeq: hub.currentSeq(), queued: new Set() }
  context[TAGS] = state
  return state
}

/** A request's tag fence, captured while the sync context is live. Holds ONLY stable per-request refs
 *  (hub, request-start seq) and registers NOTHING on the hub, so `subscribeCapturedTag` can run
 *  context-free at serialize (rationale on `captureTagFence`). */
type TagFence = { tag: string; hub: TagHub; requestStartSeq: number }

/** Capture the fence for `tag`. MUST run inside the request context — BEFORE any real-I/O await (which,
 *  in the default sync context mode, nulls the context at the next macrotask) — else `getRequestTagState()`
 *  throws the "inside a telefunction" assert (the guard that enforces subscribe-before-fetch). Reads only
 *  stable refs and registers no hub listener → leak-safe by construction until an actual subscribe. */
function captureTagFence(tag: string): TagFence {
  const { hub, requestStartSeq } = getRequestTagState()
  return { tag, hub, requestStartSeq }
}

/** Subscribe a captured fence — CONTEXT-FREE. Register the index listener FIRST, then scan the journal
 *  from the request-start fence to catch a publish that landed between capture and now (a journal
 *  overflow replays unconditionally). Invalidation is idempotent, so a harmless overlap between the
 *  catch-up and the index needs no dedup. Returns the teardown; runs at serialization, after the
 *  context may be gone. */
function subscribeCapturedTag(fence: TagFence, onInvalidate: () => void): () => void {
  const { tag, hub, requestStartSeq } = fence
  const teardown = hub.registerTag(tag, onInvalidate)
  if (hub.hasOverflow(requestStartSeq) || hub.hasTagSince(requestStartSeq, tag)) onInvalidate()
  return teardown
}

/** Eager capture + subscribe in one context-dependent call — the `liveTag` (bag-source) convenience,
 *  where the subscription is established synchronously within the request body. */
function subscribeTagFenced(tag: string, onInvalidate: () => void): () => void {
  return subscribeCapturedTag(captureTagFence(tag), onInvalidate)
}

/** Declare the current telefunction's result live under `tag`. Identity is server-owned
 *  (`tag:<namespace>:<tag>`) — the client never names it. Registration acquires no resource, so the
 *  source has no `dispose`; its subscription is torn down via the returned teardown. */
function liveTag(tag: string): void {
  const { hub } = getRequestTagState()
  const source: LiveSource = {
    identity: `tag:${hub.namespace}:${tag}`,
    subscribe: (onInvalidate) => subscribeTagFenced(tag, onInvalidate),
  }
  addLiveSource(source)
}

/** Queue an invalidation for `tag`, published as one deduped batch at settle. Inside a request it
 *  queues into request state; outside a request it's a usage error. */
function invalidateTag(tag: string): void {
  getRequestTagState().queued.add(tag)
}

/** `Live.invalidate(key)` publish routing (fire-and-forget, `void`): inside a request it queues at
 *  settle (like `invalidateTag`); outside a request it publishes immediately. */
function invalidateTagStatic(tag: string): void {
  if (getRawContext()) {
    getRequestTagState().queued.add(tag)
  } else {
    void publishTagImmediate(tag)
  }
}

/** Publish `tag` immediately (the out-of-request path). Awaits readiness + publish internally; a
 *  transport failure is fired locally, never thrown to the fire-and-forget caller. */
async function publishTagImmediate(tag: string): Promise<void> {
  const hub = getTagHub()
  await hub.ready()
  await publishTags(hub, [tag])
}

/** Publish the request's queued tags as one deduped batch (called by settle), failure-safe. */
async function publishQueuedTags(): Promise<void> {
  const state = getRawContext()?.[TAGS] as TagState | undefined
  if (!state || state.queued.size === 0) return
  const tags = [...state.queued]
  state.queued = new Set()
  await publishTags(state.hub, tags)
}

/** Publish one Broadcast batch. A transport failure is detected (structured log) and the batch is
 *  fired to local subscribers anyway — never silent, never thrown. */
async function publishTags(hub: TagHub, tags: string[]): Promise<void> {
  if (tags.length === 0) return
  try {
    await hub.publish(tags)
  } catch (err) {
    console.error('[telefunc:live] tag publish failed; firing local subscribers instead:', err)
    hub.fireLocal(tags)
  }
}
