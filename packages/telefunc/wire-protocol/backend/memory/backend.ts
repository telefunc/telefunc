// The memory reference realization of BackendSpi — the executable specification the Redis and
// Cloudflare backends are proved against. Single isolate, so every operation is synchronous over Maps
// and the isolate clock IS authority time; the only asynchrony is the delivery attempt, which is
// deliberately queued through a per-lane chain so a receiver can never run reentrantly inside
// commitLane.
//
// Mechanism map (spi.md §5.1):
//   head CX          one compare branch per HeadCx form, all resolved on the authority clock
//   lease minting    every CX installing 'closing' stores until = authorityNow + durationMs
//   cells            per-generation revision counter, bumped by any deliberate cell write
//   commit           ONE boolean precondition with two branches (default open / closing-control)
//   delivery         per-(inc, lane) promise chain, settlement-gated, never poisoned
//   TTL              lazy expiry on read + a sweep on the janitor path

import {
  BACKEND_SPI_VERSION,
  type BackendDriver,
  type BackendReceiver,
  type BackendSubscriptionSource,
  type BroadcastLane,
  type CellMutation,
  type CommitResult,
  type CxResult,
  type HeadCx,
  type HeadNext,
  type LaneId,
  type PublishResult,
  type RoomHead,
  type SubscriptionAttempt,
  type SubscriptionAttemptState,
  type SubscriptionDriver,
} from '../spi.js'
import { assertHeadDeleteLegal, assertHeadTransition } from '../head-transitions.js'
import { broadcastRouteKey, laneKey } from '../subscription-source.js'

const DEFAULT_MAX_RETAINED_BYTES = 16 * 1024 * 1024
const DIRECTORY_PAGE_SIZE = 100

export type MemoryBackendOptions = {
  // The authority clock. Defaults to the isolate clock, which is what authority time means in a single
  // isolate; tests inject a controlled clock so lease expiry is provable without wall-clock waits
  // — and so a skewed CALLER clock (Date.now) stays distinguishable from authority time.
  authorityNow?: () => number
  maxRetainedPayloadBytes?: number
  /** @internal Ownership injection for an embedding that preserves state across facade reconstruction. */
  state?: MemoryBackendState
}

type StoredHead = {
  rev: string
  currentInc: string | null
  state: 'open' | 'closing' | 'closed'
  config: Uint8Array
  closeLease?: { id: string; until: number }
  expiresAt: number | null
}

type Expiring = { expiresAt: number | null }
type StoredCell = Expiring & { bytes: Uint8Array }
type OrderMark = { seq: number; timestamp: number }
type RetainedEntry = { lane: LaneId; payload: Uint8Array; seq: number; timestamp: number }

type Generation = {
  revision: number
  cells: Map<string, StoredCell>
  order: Map<string, OrderMark>
  retained: Map<string, RetainedEntry>
  subs: Map<string, Set<MemorySubscriptionAttempt>>
  chains: Map<string, Promise<void>>
}

type RoomRecord = { head: StoredHead | null; gens: Map<string, Generation> }

/**
 * Storage owner for the in-process reference backend. Keeping durable maps separate from the facade
 * models backend reconstruction over preserved process state without a test-only backend method or a
 * global registry.
 *
 * @internal
 */
export class MemoryBackendState {
  readonly rooms = new Map<string, RoomRecord>()
  readonly directory = new Map<string, string>()
  readonly broadcastOrder = new Map<string, OrderMark>()
  readonly broadcastSubs = new Map<string, Set<MemorySubscriptionAttempt>>()
  revSeq = 0
}

function copyBytes(bytes: Uint8Array): Uint8Array {
  return new Uint8Array(bytes)
}

function copyLane(lane: LaneId): LaneId {
  return { ...lane }
}

function sumReceiverCounts(targets: MemorySubscriptionAttempt[]): number {
  return targets.reduce((total, target) => total + target.receiverCount(), 0)
}

function isExpired(entry: Expiring, now: number): boolean {
  return entry.expiresAt !== null && entry.expiresAt <= now
}

function newGeneration(): Generation {
  return { revision: 0, cells: new Map(), order: new Map(), retained: new Map(), subs: new Map(), chains: new Map() }
}

function publicHead(head: StoredHead): RoomHead {
  const view: RoomHead = {
    rev: head.rev,
    currentInc: head.currentInc,
    state: head.state,
    config: copyBytes(head.config),
  }
  if (head.closeLease !== undefined) view.closeLease = { ...head.closeLease }
  return view
}

class MemorySubscriptionAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  private _state: SubscriptionAttemptState = 'establishing'
  private _settle!: { resolve: () => void; reject: (err: unknown) => void }
  private readonly _listeners = new Set<(state: SubscriptionAttemptState) => void>()
  private readonly _receiver: BackendReceiver
  private readonly _localReceiverCount: () => number
  private _detach: () => void

  constructor(receiver: BackendReceiver, localReceiverCount: () => number, detach: () => void) {
    this._receiver = receiver
    this._localReceiverCount = localReceiverCount
    this._detach = detach
    this.ready = new Promise<void>((resolve, reject) => {
      this._settle = { resolve, reject }
    })
    // A fail-closed establishment rejects `ready` whether or not the caller is awaiting it yet; the
    // handler below only marks the rejection observed, it does not swallow it for the caller.
    void this.ready.catch(() => {})
  }

  get closed(): boolean {
    return this._state === 'closed'
  }

  state(): SubscriptionAttemptState {
    return this._state
  }

  onStateChange(cb: (state: SubscriptionAttemptState) => void): () => void {
    this._listeners.add(cb)
    return () => this._listeners.delete(cb)
  }

  async unsubscribe(): Promise<void> {
    if (this.closed) return
    this._detach()
    this._transition('closed')
  }

  establish(): void {
    this._transition('ready')
    this._settle.resolve()
  }

  failEstablishment(reason: string): void {
    this._transition('closed')
    this._settle.reject(new Error(reason))
  }

  async deliver(payload: Uint8Array, info: { seq: number; timestamp: number }): Promise<void> {
    // A receiver is typed `=> void`, so its completion is never a cross-backend guarantee. In memory the
    // handoff attempt IS the dispatch call, so a receiver that returns a thenable extends that attempt —
    // this is the trace that makes the ordered-chain algorithm observable here.
    await (this._receiver(payload, info) as unknown)
  }

  receiverCount(): number {
    return this.closed ? 0 : this._localReceiverCount()
  }

  private _transition(state: SubscriptionAttemptState): void {
    if (this._state === state) return
    this._state = state
    for (const cb of this._listeners) cb(state)
  }
}

export class MemoryBackend implements BackendDriver {
  readonly spiVersion = BACKEND_SPI_VERSION
  readonly capabilities: BackendDriver['capabilities']
  readonly subscriptions: SubscriptionDriver

  private readonly _now: () => number
  private readonly _state: MemoryBackendState
  private _disposed = false

  constructor(options: MemoryBackendOptions = {}) {
    this._now = options.authorityNow ?? (() => Date.now())
    this._state = options.state ?? new MemoryBackendState()
    this.subscriptions = {
      bind: (source) => ({
        partition: '',
        valid: () => true,
        open: (receiver, localReceiverCount) => this._openSubscription(source, receiver, localReceiverCount),
      }),
    }
    this.capabilities = {
      receivers: 'global',
      maxRetainedPayloadBytes: options.maxRetainedPayloadBytes ?? DEFAULT_MAX_RETAINED_BYTES,
      clusterSafe: false,
      directory: true,
    }
  }

  // ── cheap Broadcast ──

  publish(lane: BroadcastLane, payload: Uint8Array): PublishResult {
    this._assertLive()
    const previous = this._state.broadcastOrder.get(lane.key)
    if (previous?.seq === Number.MAX_SAFE_INTEGER) {
      throw new Error('publish: sequence exhausted for the ordering domain')
    }
    const mark = {
      seq: (previous?.seq ?? 0) + 1,
      timestamp: Math.max(this._now(), previous?.timestamp ?? 0),
    }
    if (!Number.isSafeInteger(mark.seq) || !Number.isSafeInteger(mark.timestamp)) {
      throw new Error('publish: sequence exhausted for the ordering domain')
    }
    this._state.broadcastOrder.set(lane.key, mark)
    const targets = [...(this._state.broadcastSubs.get(broadcastRouteKey(lane)) ?? [])]
    const frame = copyBytes(payload)
    for (const target of targets) void target.deliver(copyBytes(frame), mark).catch(console.error)
    const delivered = sumReceiverCounts(targets)
    return { ...mark, receivers: delivered, meta: { delivered, transport: 'in-memory' } }
  }

  // ── head ──

  async readHead(roomId: string): Promise<{ head: RoomHead } | null> {
    this._assertLive()
    const head = this._liveHead(this._state.rooms.get(roomId))
    return head === null ? null : { head: publicHead(head) }
  }

  async compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<
    { ok: true; head: RoomHead } | { ok: true; deleted: true } | { conflict: true; current: RoomHead | null }
  > {
    this._assertLive()
    const existing = this._state.rooms.get(roomId)
    const current = this._liveHead(existing)
    // Operation legality precedes the compare for the delete path only; every other transition is
    // validated against the head the compare actually matched, so a genuine race still conflicts.
    assertHeadDeleteLegal(next, current)
    if (!this._headCxMatches(cx, current)) {
      return { conflict: true, current: current === null ? null : publicHead(current) }
    }
    // Only a CX that actually applies materializes a room record.
    const room = this._roomFor(roomId)
    if ('delete' in next) {
      room.head = null
      return { ok: true, deleted: true }
    }
    assertHeadTransition(cx, next, current, (inc) => existing?.gens.has(inc) === true)
    return { ok: true, head: publicHead(this._storeHead(room, next)) }
  }

  private _headCxMatches(cx: HeadCx, current: StoredHead | null): boolean {
    if (cx.expect === 'absent') return current === null
    if (current === null || current.rev !== cx.expect.rev) return false
    const expect = cx.expect
    if ('closingLeaseExpired' in expect) {
      return current.state === 'closing' && current.closeLease !== undefined && current.closeLease.until < this._now()
    }
    if ('closingLease' in expect) {
      return current.state === 'closing' && current.closeLease?.id === expect.closingLease
    }
    return true
  }

  private _storeHead(room: RoomRecord, next: Extract<HeadNext, { head: unknown }>): StoredHead {
    const now = this._now()
    const stored: StoredHead = {
      rev: `rev-${++this._state.revSeq}`,
      currentInc: next.head.currentInc,
      state: next.head.state,
      config: copyBytes(next.head.config),
      expiresAt: next.ttlMs === undefined ? null : now + next.ttlMs,
    }
    // The lease deadline is minted here, inside the CX, from authority time — never supplied by a caller.
    if (next.head.closeLease !== undefined) {
      stored.closeLease = { id: next.head.closeLease.id, until: now + next.head.closeLease.durationMs }
    }
    room.head = stored
    if (stored.currentInc !== null) this._generation(room, stored.currentInc)
    return stored
  }

  // ── generation cells ──

  async readCells(
    roomId: string,
    inc: string,
    sel: { keys: string[] } | { prefix: string },
  ): Promise<{ revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }> {
    this._assertLive()
    const room = this._state.rooms.get(roomId)
    const head = this._liveHead(room)
    // Reads stay available while the head is closing — the closer's tail needs them; only writes require
    // an open head (CxResult 'stale-inc').
    if (room === undefined || head === null || head.currentInc !== inc) return { staleInc: true }
    const gen = this._generation(room, inc)
    const now = this._now()
    const keys = 'keys' in sel ? sel.keys : [...gen.cells.keys()].filter((key) => key.startsWith(sel.prefix))
    const cells = new Map<string, Uint8Array>()
    for (const key of keys) {
      const cell = gen.cells.get(key)
      if (cell === undefined || isExpired(cell, now)) continue
      cells.set(key, copyBytes(cell.bytes))
    }
    return { revision: String(gen.revision), cells }
  }

  async compareExchangeCells(
    roomId: string,
    inc: string,
    revision: string,
    mutations: CellMutation[],
  ): Promise<CxResult> {
    this._assertLive()
    const room = this._state.rooms.get(roomId)
    const head = this._liveHead(room)
    if (room === undefined || head === null || head.currentInc !== inc || head.state !== 'open') return 'stale-inc'
    const gen = this._generation(room, inc)
    if (String(gen.revision) !== revision) return 'conflict'
    const now = this._now()
    for (const mutation of mutations) {
      if (mutation.set === undefined) gen.cells.delete(mutation.key)
      else {
        const { bytes, ttlMs } = mutation.set
        gen.cells.set(mutation.key, { bytes: copyBytes(bytes), expiresAt: ttlMs === undefined ? null : now + ttlMs })
      }
    }
    gen.revision += 1
    return 'committed'
  }

  // ── lane commit ──

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitResult> {
    this._assertLive()
    const room = this._state.rooms.get(roomId)
    const head = this._liveHead(room)
    if (room === undefined || head === null || !this._commitPreconditionHolds(head, inc, lane, opts?.closingLease)) {
      return { stale: true }
    }
    const gen = this._generation(room, inc)
    const now = this._now()
    if (
      opts?.requiredCellKeys?.some((key) => {
        const cell = gen.cells.get(key)
        return cell === undefined || isExpired(cell, now)
      })
    ) {
      return { stale: true }
    }
    const key = laneKey(lane)
    const frame = copyBytes(payload)
    // Over-cap retain is rejected before anything is mutated, so a throw never half-accepts a commit.
    if (opts?.retain) this._assertRetainedCapacity(gen, key, frame)
    const mark = this._advanceOrder(gen, key)
    if (opts?.retain) {
      gen.retained.set(key, {
        lane: Object.freeze(copyLane(lane)),
        payload: frame,
        seq: mark.seq,
        timestamp: mark.timestamp,
      })
    }
    const targets = [...(gen.subs.get(key) ?? [])]
    const info = { seq: mark.seq, timestamp: mark.timestamp }
    return {
      accepted: true,
      seq: mark.seq,
      timestamp: mark.timestamp,
      receivers: sumReceiverCounts(targets),
      delivery: this._enqueueAttempt(gen, key, targets, frame, info),
    }
  }

  // The whole commit precondition: one boolean, two branches. Supplying a closing lease selects the
  // narrow closing-control branch outright, which is what makes every other lane stale while closing.
  private _commitPreconditionHolds(
    head: StoredHead,
    inc: string,
    lane: LaneId,
    closingLease: string | undefined,
  ): boolean {
    if (head.currentInc !== inc) return false
    return closingLease === undefined
      ? head.state === 'open'
      : lane.kind === 'control' &&
          head.state === 'closing' &&
          head.closeLease !== undefined &&
          head.closeLease.id === closingLease &&
          this._now() <= head.closeLease.until
  }

  private _advanceOrder(gen: Generation, domain: string): OrderMark {
    const now = this._now()
    const previous = gen.order.get(domain)
    if (previous?.seq === Number.MAX_SAFE_INTEGER) {
      throw new Error('commitLane: sequence exhausted for the ordering domain')
    }
    // seq is a standalone monotonic cursor; timestamp is independently clamped and cannot reset it.
    const mark: OrderMark = {
      seq: (previous?.seq ?? 0) + 1,
      timestamp: Math.max(now, previous?.timestamp ?? 0),
    }
    if (!Number.isSafeInteger(mark.seq) || mark.seq <= 0 || !Number.isSafeInteger(mark.timestamp)) {
      throw new Error('commitLane: sequence exhausted for the ordering domain')
    }
    gen.order.set(domain, mark)
    return mark
  }

  private _assertRetainedCapacity(gen: Generation, key: string, frame: Uint8Array): void {
    let total = frame.byteLength
    for (const [laneKeyOfEntry, entry] of gen.retained) {
      if (laneKeyOfEntry !== key) total += entry.payload.byteLength
    }
    const cap = this.capabilities.maxRetainedPayloadBytes
    if (total > cap) throw new Error(`commitLane: retained aggregate ${total} bytes exceeds the ${cap} byte cap`)
  }

  // The ordered at-most-once chain, per (incarnation, lane). The chain is gated on SETTLEMENT — success
  // or failure — so a failed frame never poisons the lane, and the returned promise rejects only on its
  // own failure.
  private _enqueueAttempt(
    gen: Generation,
    key: string,
    targets: MemorySubscriptionAttempt[],
    frame: Uint8Array,
    info: { seq: number; timestamp: number },
  ): Promise<void> {
    const previous = gen.chains.get(key) ?? Promise.resolve()
    const attempt = previous.then(async () => {
      await Promise.all(
        targets.map(async (target) => {
          if (target.closed) return
          await target.deliver(copyBytes(frame), { ...info })
        }),
      )
    })
    gen.chains.set(
      key,
      attempt.then(
        () => {},
        () => {},
      ),
    )
    return attempt
  }

  // ── retained ──

  async readRetained(
    roomId: string,
    inc: string,
    lane: LaneId,
  ): Promise<{ payload: Uint8Array; seq: number; timestamp: number } | null> {
    this._assertLive()
    const entry = this._state.rooms.get(roomId)?.gens.get(inc)?.retained.get(laneKey(lane))
    if (entry === undefined) return null
    return { payload: copyBytes(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    this._assertLive()
    const gen = this._state.rooms.get(roomId)?.gens.get(inc)
    return gen === undefined ? [] : [...gen.retained.values()].map((entry) => copyLane(entry.lane))
  }

  async deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this._assertLive()
    if (lane === undefined && opts?.ifSeq !== undefined) {
      throw new Error('deleteRetained: ifSeq requires a lane')
    }
    if (opts?.ifSeq !== undefined && (!Number.isSafeInteger(opts.ifSeq) || opts.ifSeq <= 0)) {
      throw new Error('deleteRetained: ifSeq must be a positive safe integer')
    }
    const gen = this._state.rooms.get(roomId)?.gens.get(inc)
    if (gen === undefined) return
    if (lane === undefined) gen.retained.clear()
    else {
      const key = laneKey(lane)
      const retained = gen.retained.get(key)
      if (opts?.ifSeq === undefined || retained?.seq === opts.ifSeq) gen.retained.delete(key)
    }
  }

  // ── subscriptions ──

  private _openSubscription(
    source: BackendSubscriptionSource,
    receiver: BackendReceiver,
    localReceiverCount: () => number,
  ): MemorySubscriptionAttempt {
    if (source.kind === 'broadcast') {
      const key = broadcastRouteKey(source.lane)
      let sub!: MemorySubscriptionAttempt
      sub = new MemorySubscriptionAttempt(receiver, localReceiverCount, () => {
        this._state.broadcastSubs.get(key)?.delete(sub)
      })
      const subs = this._state.broadcastSubs.get(key) ?? new Set<MemorySubscriptionAttempt>()
      subs.add(sub)
      this._state.broadcastSubs.set(key, subs)
      sub.establish()
      return sub
    }

    const { roomId, inc, lane } = source
    const room = this._state.rooms.get(roomId)
    const head = this._liveHead(room)
    const key = laneKey(lane)
    let sub!: MemorySubscriptionAttempt
    sub = new MemorySubscriptionAttempt(receiver, localReceiverCount, () => {
      this._state.rooms.get(roomId)?.gens.get(inc)?.subs.get(key)?.delete(sub)
    })
    if (room === undefined || head === null || head.currentInc !== inc || head.state !== 'open') {
      sub.failEstablishment(`subscribeLane: room '${roomId}' has no open incarnation '${inc}'`)
      return sub
    }
    // Registration is durable before `ready` resolves: a commit accepted after this point must see it.
    const gen = this._generation(room, inc)
    const subs = gen.subs.get(key) ?? new Set<MemorySubscriptionAttempt>()
    subs.add(sub)
    gen.subs.set(key, subs)
    sub.establish()
    return sub
  }

  // ── generation lifecycle ──

  async listGenerations(roomId: string): Promise<string[]> {
    this._assertLive()
    const room = this._state.rooms.get(roomId)
    if (room === undefined) return []
    this._sweep(room)
    return [...room.gens.keys()]
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    this._assertLive()
    const room = this._state.rooms.get(roomId)
    if (room === undefined) return
    if (this._liveHead(room)?.currentInc === inc) {
      throw new Error(`dropGeneration: refusing to drop the current incarnation '${inc}' of room '${roomId}'`)
    }
    const gen = room.gens.get(inc)
    if (gen === undefined) return // already dropped — the janitor is resumable
    room.gens.delete(inc)
    this._sweep(room)
  }

  // ── directory ──

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this._assertLive()
    this._state.directory.set(roomId, incTag)
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this._assertLive()
    if (this._state.directory.get(roomId) === incTag) this._state.directory.delete(roomId)
  }

  async directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> {
    this._assertLive()
    const matching = [...this._state.directory.keys()].filter((roomId) => roomId.startsWith(prefix)).sort()
    const start = cursor === undefined ? 0 : matching.findIndex((roomId) => roomId > cursor)
    if (start < 0) return { entries: [] }
    const page = matching.slice(start, start + DIRECTORY_PAGE_SIZE)
    const entries = page.map((roomId) => ({ roomId, incTag: this._state.directory.get(roomId) as string }))
    const last = page[page.length - 1]
    const more = last !== undefined && start + page.length < matching.length
    return more ? { entries, cursor: last } : { entries }
  }

  async dispose(): Promise<void> {
    if (this._disposed) return
    this._disposed = true
    this._state.rooms.clear()
    this._state.directory.clear()
    this._state.broadcastOrder.clear()
    this._state.broadcastSubs.clear()
  }

  // ── internals ──

  private _assertLive(): void {
    if (this._disposed) throw new Error('MemoryBackend: used after dispose()')
  }

  private _roomFor(roomId: string): RoomRecord {
    const existing = this._state.rooms.get(roomId)
    if (existing !== undefined) return existing
    const room: RoomRecord = { head: null, gens: new Map() }
    this._state.rooms.set(roomId, room)
    return room
  }

  private _generation(room: RoomRecord, inc: string): Generation {
    const existing = room.gens.get(inc)
    if (existing !== undefined) return existing
    const gen = newGeneration()
    room.gens.set(inc, gen)
    return gen
  }

  // Lazy TTL: a lapsed tombstone reads as absent, which is what reopens an absence epoch (I1).
  private _liveHead(room: RoomRecord | undefined): StoredHead | null {
    if (room === undefined || room.head === null) return null
    if (!isExpired(room.head, this._now())) return room.head
    room.head = null
    return null
  }

  // Reclaim expiring heads and cells. Ordering marks are generation-lifetime state and deliberately
  // have no janitor path; dropGeneration() is their cleanup boundary.
  private _sweep(room: RoomRecord): void {
    const now = this._now()
    this._liveHead(room)
    for (const gen of room.gens.values()) {
      for (const [key, cell] of gen.cells) if (isExpired(cell, now)) gen.cells.delete(key)
    }
  }
}
