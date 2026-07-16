export { getTagHub, configureLiveNamespace, _resetTagHubsForTesting, _setBarrierBudgetForTesting }
export type { TagHub }

import { Broadcast } from '../../../wire-protocol/server/server-broadcast.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { assertUsage, getProjectError } from '../../../utils/assert.js'

// One app-scoped Broadcast key per namespace bounds authority-DO/KV cardinality regardless of how
// many distinct tags exist. Each server keeps a bounded (seq, tags) journal of what it observed on
// that key, plus a local index of the tags it currently cares about.

const KEY_PREFIX = '__tf_tags__:'
const DEFAULT_NAMESPACE = 'default'
const JOURNAL_LIMIT = 1024
const BARRIER_BACKOFF_MS = 5
const BARRIER_MAX_BACKOFF_MS = 50
const BARRIER_MAX_ATTEMPTS = 50

// The barrier probes this many times before failing closed. Mutable only so a test can fail fast.
let barrierMaxAttempts = BARRIER_MAX_ATTEMPTS

type TagBatch = { batchId?: string; tags?: string[]; barrier?: string }
// The batchId is retained so the fence catch-up (scanSince) can suppress a request's OWN echo — the
// index path already gets it via `_notify`, but a self-publish caught by the scan would otherwise
// self-invalidate (self-write ≠ self-refetch).
type JournalEntry = { seq: number; tags: Set<string>; batchId: string | undefined }
/** Delivered with the publish's seq and originating batchId, so a subscriber can dedupe by seq and
 *  skip its own request's echo. */
type TagListener = (seq: number, batchId: string | undefined) => void

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

class TagHub {
  readonly namespace: string
  private readonly key: string
  private readonly journal: JournalEntry[] = [] // ascending seq, at most JOURNAL_LIMIT entries
  private readonly index = new Map<string, Set<TagListener>>()
  private lastSeq = 0
  private evictedUpTo = 0 // highest seq dropped from the journal — the fence overflow watermark
  private subscribed = false
  private readyPromise: Promise<void> | null = null
  // Only the in-flight barrier's token is tracked; cleared once its attempt settles, so late frames
  // from old/failed cycles are ignored and nothing accumulates across outage/recovery cycles.
  private pendingBarrier: { token: string; observed: boolean } | null = null

  constructor(namespace: string) {
    this.namespace = namespace
    this.key = KEY_PREFIX + namespace
  }

  /** Subscribe to the app-scoped key once, then publish barrier frames until we observe one of our
   *  own — proof the subscription is actually delivering (a transport's `listen()` may become active
   *  after `send()` starts acknowledging). Awaiting this before the first fence scan is what closes
   *  the async-SUBSCRIBE window. For the in-memory adapter one barrier round-trips synchronously.
   *
   *  It NEVER resolves unproven: if the subscription cannot be confirmed it rejects (the request
   *  fails closed rather than silently miss invalidations) and the cached promise is dropped so a
   *  later request re-probes — the transport may have recovered. A resolved barrier is cached and
   *  reused. */
  ready(): Promise<void> {
    if (this.readyPromise) return this.readyPromise
    if (!this.subscribed) {
      Broadcast.subscribe<TagBatch>(this.key, (batch, info) => this._receive(batch, info.seq))
      this.subscribed = true
    }
    this.readyPromise = this._establishBarrier().catch((err) => {
      this.readyPromise = null
      throw err
    })
    return this.readyPromise
  }

  /** The highest seq observed so far — stamped as a request's `requestStartSeq` fence. */
  currentSeq(): number {
    return this.lastSeq
  }

  /** Publish one batch. Awaited by the caller (settle) so a failure is detected, not silent. */
  async publish(tags: string[], batchId: string = crypto.randomUUID()): Promise<void> {
    await Broadcast.publish<TagBatch>(this.key, { batchId, tags })
  }

  /** Deliver `tags` to the local index without the Broadcast round-trip. Used when a publish fails:
   *  local subscribers still get the invalidation even though the fan-out hop was lost. */
  fireLocal(tags: string[], batchId: string): void {
    this.lastSeq += 1
    const seq = this.lastSeq
    for (const tag of tags) this._notify(tag, seq, batchId)
  }

  registerTag(tag: string, listener: TagListener): () => void {
    let listeners = this.index.get(tag)
    if (!listeners) {
      listeners = new Set()
      this.index.set(tag, listeners)
    }
    listeners.add(listener)
    return () => {
      const set = this.index.get(tag)
      if (!set) return
      set.delete(listener)
      if (set.size === 0) this.index.delete(tag)
    }
  }

  /** The window's lower part was evicted from the journal — the caller replays unconditionally. */
  hasOverflow(fromExclusive: number): boolean {
    return fromExclusive < this.evictedUpTo
  }

  /** The highest-seq publish of `tag` in `(fromExclusive, toInclusive]` as `{ seq, batchId }`, or
   *  `{ seq: 0 }` if none. The batchId lets the caller skip its own request's echo (self-write ≠
   *  self-refetch). The caller captures `toInclusive = currentSeq()` right after registering its index
   *  listener, so a publish landing after that boundary is delivered by the index and NOT re-counted. */
  scanSince(fromExclusive: number, toInclusive: number, tag: string): { seq: number; batchId: string | undefined } {
    let best = 0
    let bestBatchId: string | undefined
    for (const entry of this.journal) {
      if (entry.seq > fromExclusive && entry.seq <= toInclusive && entry.seq > best && entry.tags.has(tag)) {
        best = entry.seq
        bestBatchId = entry.batchId
      }
    }
    return { seq: best, batchId: bestBatchId }
  }

  private async _establishBarrier(): Promise<void> {
    const barrier = { token: crypto.randomUUID(), observed: false }
    this.pendingBarrier = barrier
    try {
      for (let attempt = 0; attempt < barrierMaxAttempts; attempt++) {
        try {
          await Broadcast.publish<TagBatch>(this.key, { barrier: barrier.token })
        } catch {
          // Transport not ready or momentarily down — keep probing within the attempt budget.
        }
        if (barrier.observed) return // subscription proven active
        await sleep(Math.min(BARRIER_BACKOFF_MS * (attempt + 1), BARRIER_MAX_BACKOFF_MS))
      }
      // Fail closed — resolving here would let a write during this unconfirmed window be missed.
      throw getProjectError(
        `Tag readiness could not be confirmed for namespace "${this.namespace}": the Broadcast subscription never delivered. Live queries require a working Broadcast transport.`,
      )
    } finally {
      // Drop the token once the attempt settles: a frame delivered later can no longer confirm it,
      // and nothing is retained across cycles.
      this.pendingBarrier = null
    }
  }

  private _receive(batch: TagBatch, seq: number): void {
    this.lastSeq = seq
    if (batch.barrier !== undefined) {
      if (this.pendingBarrier !== null && batch.barrier === this.pendingBarrier.token) {
        this.pendingBarrier.observed = true
      }
      return
    }
    const tags = batch.tags ?? []
    this.journal.push({ seq, tags: new Set(tags), batchId: batch.batchId })
    if (this.journal.length > JOURNAL_LIMIT) {
      const evicted = this.journal.shift()!
      this.evictedUpTo = evicted.seq
    }
    for (const tag of tags) this._notify(tag, seq, batch.batchId)
  }

  private _notify(tag: string, seq: number, batchId: string | undefined): void {
    const listeners = this.index.get(tag)
    if (!listeners) return
    for (const listener of [...listeners]) listener(seq, batchId)
  }

  /** @internal test-only — retained barrier tokens (0 or 1; never grows across fail/recover cycles). */
  _retainedBarrierTokenCountForTesting(): number {
    return this.pendingBarrier === null ? 0 : 1
  }

  /** @internal test-only — registered index listeners for `tag` (0 proves a captured-but-never-subscribed
   *  fence leaked nothing on the hub). */
  _listenerCountForTesting(tag: string): number {
    return this.index.get(tag)?.size ?? 0
  }
}

const globalObject = getGlobalObject<{ hubs: Map<string, TagHub>; namespace: string }>('tagHub.ts', () => ({
  hubs: new Map(),
  namespace: DEFAULT_NAMESPACE,
}))

/** Set the app's live namespace. Must be called before any tag activity — reconfiguring once a hub
 *  exists would silently re-key live subscriptions, so it asserts. */
function configureLiveNamespace(namespace: string): void {
  assertUsage(
    globalObject.hubs.size === 0,
    'configureLiveNamespace() must be called before any liveTag()/invalidateTag() activity.',
  )
  globalObject.namespace = namespace
}

function getTagHub(namespace: string = globalObject.namespace): TagHub {
  let hub = globalObject.hubs.get(namespace)
  if (!hub) {
    hub = new TagHub(namespace)
    globalObject.hubs.set(namespace, hub)
  }
  return hub
}

/** @internal test-only reset. */
function _resetTagHubsForTesting(): void {
  globalObject.hubs = new Map()
  globalObject.namespace = DEFAULT_NAMESPACE
  barrierMaxAttempts = BARRIER_MAX_ATTEMPTS
}

/** @internal test-only — shrink the barrier attempt budget so an unconfirmed-transport test fails fast. */
function _setBarrierBudgetForTesting(attempts: number): void {
  barrierMaxAttempts = attempts
}
