export { getTagHub, _resetTagHubsForTesting, _setBarrierBudgetForTesting }
export type { TagHub }

import { Broadcast } from '../../../wire-protocol/server/server-broadcast.js'
import { getGlobalObject } from '../../../utils/getGlobalObject.js'
import { getProjectError } from '../../../utils/assert.js'

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

type TagBatch = { tags?: string[]; barrier?: string }
type JournalEntry = { seq: number; tags: Set<string> }
/** Fired when a matching tag is published. Invalidation is idempotent, so listeners take no seq/id —
 *  a duplicate fire is harmless. */
type TagListener = () => void

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
  async publish(tags: string[]): Promise<void> {
    await Broadcast.publish<TagBatch>(this.key, { tags })
  }

  /** Deliver `tags` to the local index without the Broadcast round-trip. Used when a publish fails:
   *  local subscribers still get the invalidation even though the fan-out hop was lost. */
  fireLocal(tags: string[]): void {
    this.lastSeq += 1
    for (const tag of tags) this._notify(tag)
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

  /** Whether `tag` appears in the retained journal after `fromExclusive` — the fence catch-up for a
   *  publish that landed between the acquiring read and this subscribe. The caller replays once on a
   *  hit; invalidation is idempotent, so an overlap with the index path needs no seq coordination. */
  hasTagSince(fromExclusive: number, tag: string): boolean {
    for (const entry of this.journal) {
      if (entry.seq > fromExclusive && entry.tags.has(tag)) return true
    }
    return false
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
    this.journal.push({ seq, tags: new Set(tags) })
    if (this.journal.length > JOURNAL_LIMIT) {
      const evicted = this.journal.shift()!
      this.evictedUpTo = evicted.seq
    }
    for (const tag of tags) this._notify(tag)
  }

  private _notify(tag: string): void {
    const listeners = this.index.get(tag)
    if (!listeners) return
    for (const listener of [...listeners]) listener()
  }

  /** @internal test-only — retained barrier tokens (0 or 1; never grows across fail/recover cycles). */
  _retainedBarrierTokenCountForTesting(): number {
    return this.pendingBarrier === null ? 0 : 1
  }
}

// One app-scoped tag hub on the default namespace. (The multi-namespace generality was speculative and
// test-only, so it was removed; a single-tenant app needs exactly one hub.)
const globalObject = getGlobalObject<{ hub: TagHub | null }>('tagHub.ts', () => ({ hub: null }))

function getTagHub(): TagHub {
  if (!globalObject.hub) globalObject.hub = new TagHub(DEFAULT_NAMESPACE)
  return globalObject.hub
}

/** @internal test-only reset. */
function _resetTagHubsForTesting(): void {
  globalObject.hub = null
  barrierMaxAttempts = BARRIER_MAX_ATTEMPTS
}

/** @internal test-only — shrink the barrier attempt budget so an unconfirmed-transport test fails fast. */
function _setBarrierBudgetForTesting(attempts: number): void {
  barrierMaxAttempts = attempts
}
