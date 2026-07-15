export { liveTag, invalidateTag, runWithLiveBatch, stampRequestStartFence, publishQueuedTags }

import { AsyncLocalStorage } from 'node:async_hooks'
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

// Per-async-scope active batch (runWithLiveBatch). AsyncLocalStorage isolates concurrent job batches
// from each other and from request state — no process-global mutable owner to corrupt.
const batchStore = new AsyncLocalStorage<Set<string>>()

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
  assertUsage(
    context,
    'liveTag()/invalidateTag() can only be used inside a telefunction; use runWithLiveBatch() outside a request.',
  )
  const existing = context[TAGS] as TagState | undefined
  if (existing) return existing
  const hub = getTagHub()
  const state: TagState = { hub, requestStartSeq: hub.currentSeq(), queued: new Set(), ownBatchIds: new Set() }
  context[TAGS] = state
  return state
}

/** Declare the current telefunction's result live under `tag`. Identity is server-owned
 *  (`tag:<namespace>:<tag>`) — the client never names it. Registration acquires no resource, so the
 *  source has no `dispose`; its subscription is torn down via the returned teardown. */
function liveTag(tag: string): void {
  const state = getRequestTagState()
  const { hub, requestStartSeq } = state
  const source: LiveSource = {
    identity: `tag:${hub.namespace}:${tag}`,
    subscribe(onInvalidate) {
      // A single delivery gate: skip this request's own echo (by batchId) and dedupe by seq, so a
      // publish caught by BOTH the index (register) and the scan fires exactly once.
      let lastDeliveredSeq = requestStartSeq
      const emit = (seq: number, batchId: string | undefined): void => {
        if (batchId !== undefined && state.ownBatchIds.has(batchId)) return
        if (seq <= lastDeliveredSeq) return
        lastDeliveredSeq = seq
        onInvalidate()
      }
      // Register the index listener FIRST; any receive from now flows through `emit`. Then close the
      // fence by scanning up to the current seq — a publish that already fired the index is deduped.
      const teardown = hub.registerTag(tag, emit)
      if (hub.hasOverflow(requestStartSeq)) {
        incrementCounter('live.tagHub.journalOverflow')
        lastDeliveredSeq = hub.currentSeq()
        onInvalidate()
      } else {
        const matchSeq = hub.scanSince(requestStartSeq, hub.currentSeq(), tag)
        if (matchSeq > 0) emit(matchSeq, undefined)
      }
      return teardown
    },
  }
  addLiveSource(source)
}

/** Queue an invalidation for `tag`, published as one deduped batch at settle. Inside a
 *  `runWithLiveBatch` scope it queues into that batch; inside a request, into request state;
 *  otherwise it's a usage error. */
function invalidateTag(tag: string): void {
  const batch = batchStore.getStore()
  if (batch) {
    batch.add(tag)
    return
  }
  getRequestTagState().queued.add(tag)
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

/** Publish tags outside a request (jobs/cron). Awaitable — resolves after the batch is published.
 *  The callback's own error is rethrown; a publish failure is logged/counted, never masking it. The
 *  batch scope is an AsyncLocalStorage frame, so concurrent calls never corrupt each other. */
async function runWithLiveBatch(cb: () => Promise<void> | void): Promise<void> {
  const hub = getTagHub()
  await hub.ready()
  const batch = new Set<string>()
  await batchStore.run(batch, async () => {
    try {
      await cb()
    } finally {
      await publishTags(hub, [...batch], crypto.randomUUID())
    }
  })
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
