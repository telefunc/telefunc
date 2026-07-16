export { liveTag, invalidateTag, stampRequestStartFence, publishQueuedTags, subscribeTagFenced, invalidateTagStatic }

import { assertUsage } from '../../../utils/assert.js'
import { getRawContext } from '../context/context.js'
import { addLiveSource } from './source.js'
import type { LiveSource } from './source.js'
import { getTagHub } from './tagHub.js'
import type { TagHub } from './tagHub.js'
import { incrementCounter } from './telemetry.js'

const TAGS = Symbol.for('telefunc.tagState')

type TagState = {
  hub: TagHub
  /** The fence: only tags published after this seq replay to a subscribing source. */
  requestStartSeq: number
  /** invalidateTag() calls, published as one deduped batch at settle. */
  queued: Set<string>
  /** batchIds this request published — its own sources skip these (self-write ≠ self-refetch),
   *  including a transport's delayed echo. */
  ownBatchIds: Set<string>
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
    ownBatchIds: new Set(),
  } satisfies TagState
}

function getRequestTagState(): TagState {
  const context = getRawContext()
  assertUsage(context, 'liveTag()/invalidateTag() can only be used inside a telefunction.')
  const existing = context[TAGS] as TagState | undefined
  if (existing) return existing
  const hub = getTagHub()
  const state: TagState = { hub, requestStartSeq: hub.currentSeq(), queued: new Set(), ownBatchIds: new Set() }
  context[TAGS] = state
  return state
}

/** Subscribe `onInvalidate` to `tag` under the request fence: register the index listener FIRST, then
 *  scan the journal from the request-start fence to catch a publish that landed between the acquiring
 *  read and now — delivered exactly once (seq-deduped; the request's own echo is skipped by batchId; a
 *  journal overflow replays unconditionally). Returns the teardown. Shared by `liveTag` (bag source)
 *  and `Live.onInvalidate` (handle-bound source). */
function subscribeTagFenced(tag: string, onInvalidate: () => void): () => void {
  const state = getRequestTagState()
  const { hub, requestStartSeq } = state
  let lastDeliveredSeq = requestStartSeq
  const emit = (seq: number, batchId: string | undefined): void => {
    if (batchId !== undefined && state.ownBatchIds.has(batchId)) return
    if (seq <= lastDeliveredSeq) return
    lastDeliveredSeq = seq
    onInvalidate()
  }
  const teardown = hub.registerTag(tag, emit)
  if (hub.hasOverflow(requestStartSeq)) {
    incrementCounter('live.tagHub.journalOverflow')
    lastDeliveredSeq = hub.currentSeq()
    onInvalidate()
  } else {
    // Carry the matched publish's batchId so `emit` can skip this request's OWN echo (self-write ≠
    // self-refetch) — the same suppression the index path gets via `_notify`.
    const match = hub.scanSince(requestStartSeq, hub.currentSeq(), tag)
    if (match.seq > 0) emit(match.seq, match.batchId)
  }
  return teardown
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
 *  transport failure is counted + fired locally, never thrown to the fire-and-forget caller. */
async function publishTagImmediate(tag: string): Promise<void> {
  const hub = getTagHub()
  await hub.ready()
  await publishTags(hub, [tag], crypto.randomUUID())
}

/** Publish the request's queued tags as one deduped batch (called by settle). Records the batchId as
 *  the request's own so its sources skip the echo, then publishes failure-safe. */
async function publishQueuedTags(): Promise<void> {
  const state = getRawContext()?.[TAGS] as TagState | undefined
  if (!state || state.queued.size === 0) return
  const tags = [...state.queued]
  state.queued = new Set()
  const batchId = crypto.randomUUID()
  state.ownBatchIds.add(batchId)
  await publishTags(state.hub, tags, batchId)
}

/** Publish one Broadcast batch. A transport failure is detected (counter + structured log) and the
 *  batch is fired to local subscribers anyway — never silent, never thrown. */
async function publishTags(hub: TagHub, tags: string[], batchId: string): Promise<void> {
  if (tags.length === 0) return
  try {
    await hub.publish(tags, batchId)
  } catch (err) {
    incrementCounter('live.tagHub.publishFailure')
    console.error('[telefunc:live] tag publish failed; firing local subscribers instead:', err)
    hub.fireLocal(tags, batchId)
  }
}
