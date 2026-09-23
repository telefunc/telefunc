// Reference driver: synchronous authority-time state, per-lane async settlement, CX leases/revisions, and lazy/janitor TTL.

import type { BroadcastDriver, BroadcastLane, PublishResult } from '../broadcast/contract.js'
import { broadcastRouteKey } from '../broadcast/route-key.js'
import type {
  CellMutation,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
} from '../room/contract.js'
import { encodeLaneKey } from '../room/lane-key.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import type {
  BackendReceiver,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionDriver,
} from '../subscription.js'

export type MemoryBackendOptions = {
  // Tests inject authority time to prove expiry independently of caller clock skew.
  authorityNow?: () => number
  /** @internal Ownership injection for an embedding that preserves state across facade reconstruction. */
  state?: MemoryBackendState
}

type MemorySubscriptionSource = BroadcastLane | RoomSubscriptionSource

type Expiring = { expiresAt: number | null }
type StoredHead = RoomHead & Expiring
type StoredCell = { bytes: Uint8Array }
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
const noop = () => {}

/** Durable in-process storage owner, separable from a reconstructed facade. @internal */
export class MemoryBackendState {
  readonly rooms = new Map<string, RoomRecord>()
  readonly directory = new Map<string, string>()
  readonly broadcastOrder = new Map<string, OrderMark>()
  readonly broadcastSubs = new Map<string, Set<MemorySubscriptionAttempt>>()
  revSeq = 0
}

const copyBytes = (bytes: Uint8Array): Uint8Array => new Uint8Array(bytes)
const copyLane = (lane: LaneId): LaneId => ({ ...lane })
const sumReceiverCounts = (targets: MemorySubscriptionAttempt[]): number =>
  targets.reduce((total, target) => total + target.receiverCount(), 0)
const isExpired = (entry: Expiring, now: number): boolean => entry.expiresAt !== null && entry.expiresAt <= now

function newGeneration(): Generation {
  return { revision: 0, cells: new Map(), order: new Map(), retained: new Map(), subs: new Map(), chains: new Map() }
}

function advanceOrder(
  order: Map<string, OrderMark>,
  domain: string,
  now: number,
  operation: 'publish' | 'commitLane',
): OrderMark {
  const previous = order.get(domain)
  // seq is a standalone monotonic cursor; timestamp is independently clamped and cannot reset it.
  const mark: OrderMark = {
    seq: (previous?.seq ?? 0) + 1,
    timestamp: Math.max(now, previous?.timestamp ?? 0),
  }
  if (!Number.isSafeInteger(mark.seq) || mark.seq <= 0 || !Number.isSafeInteger(mark.timestamp)) {
    throw new Error(`${operation}: sequence exhausted for the ordering domain`)
  }
  order.set(domain, mark)
  return mark
}

function publicHead(head: StoredHead): RoomHead {
  const { expiresAt: _, closeLease, ...view } = head
  return {
    ...view,
    config: copyBytes(head.config),
    ...(closeLease === undefined ? {} : { closeLease: { ...closeLease } }),
  }
}

class MemorySubscriptionAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  #state: SubscriptionAttemptState = 'establishing'
  #settle!: { resolve: () => void; reject: (err: unknown) => void }
  readonly #listeners = new Set<(state: SubscriptionAttemptState) => void>()
  readonly #receiver: BackendReceiver
  readonly #localReceiverCount: () => number
  readonly #detach?: () => void

  constructor(receiver: BackendReceiver, localReceiverCount: () => number, detach?: () => void) {
    this.#receiver = receiver
    this.#localReceiverCount = localReceiverCount
    this.#detach = detach
    this.ready = new Promise<void>((resolve, reject) => {
      this.#settle = { resolve, reject }
    })
    // Observe fail-closed rejection without swallowing it from callers.
    void this.ready.catch(noop)
  }

  get closed(): boolean {
    return this.#state === 'closed'
  }

  state(): SubscriptionAttemptState {
    return this.#state
  }

  onStateChange(cb: (state: SubscriptionAttemptState) => void): () => void {
    this.#listeners.add(cb)
    return () => this.#listeners.delete(cb)
  }

  async unsubscribe(): Promise<void> {
    if (this.closed) return
    this.#detach?.()
    this.#transition('closed')
  }

  establish(): void {
    this.#transition('ready')
    this.#settle.resolve()
  }

  failEstablishment(reason: string): void {
    this.#transition('closed')
    this.#settle.reject(new Error(reason))
  }

  async deliver(payload: Uint8Array, info: { seq: number; timestamp: number }): Promise<void> {
    // Memory dispatch awaits returned thenables for this attempt; cross-backend callback completion is not guaranteed.
    await (this.#receiver(payload, info) as unknown)
  }

  receiverCount(): number {
    return this.closed ? 0 : this.#localReceiverCount()
  }

  #transition(state: SubscriptionAttemptState): void {
    if (this.#state === state) return
    this.#state = state
    for (const cb of this.#listeners) cb(state)
  }
}

export class MemoryBackend implements BroadcastDriver, RoomDriver {
  readonly subscriptions: SubscriptionDriver<MemorySubscriptionSource>

  readonly #now: () => number
  readonly #state: MemoryBackendState
  #disposed = false

  constructor(options: MemoryBackendOptions = {}) {
    this.#now = options.authorityNow ?? (() => Date.now())
    this.#state = options.state ?? new MemoryBackendState()
    this.subscriptions = {
      bind: (source) => ({
        partition: '',
        valid: () => true,
        open: (receiver, localReceiverCount) => this.#openSubscription(source, receiver, localReceiverCount),
      }),
    }
  }

  publish(lane: BroadcastLane, payload: Uint8Array): PublishResult {
    this.#assertLive()
    const mark = advanceOrder(this.#state.broadcastOrder, lane.key, this.#now(), 'publish')
    const targets = [...(this.#state.broadcastSubs.get(broadcastRouteKey(lane)) ?? [])]
    const frame = copyBytes(payload)
    for (const target of targets) void target.deliver(copyBytes(frame), mark).catch(console.error)
    const delivered = sumReceiverCounts(targets)
    return { ...mark, receivers: delivered, meta: { delivered, transport: 'in-memory' } }
  }

  async readHead(roomId: string): Promise<{ head: RoomHead } | null> {
    this.#assertLive()
    const head = this.#readAndExpireHead(this.#state.rooms.get(roomId))
    return head === null ? null : { head: publicHead(head) }
  }

  async compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<{ ok: true; head: RoomHead } | { conflict: true; current: RoomHead | null }> {
    this.#assertLive()
    const current = this.#readAndExpireHead(this.#state.rooms.get(roomId))
    if (!this.#headCxMatches(cx, current)) {
      return { conflict: true, current: current === null ? null : publicHead(current) }
    }
    // Only a CX that actually applies materializes a room record.
    return { ok: true, head: publicHead(this.#storeHead(this.#roomFor(roomId), next)) }
  }

  #headCxMatches(cx: HeadCx, current: StoredHead | null): boolean {
    if (cx.expect === 'absent') return current === null
    if (current === null || current.rev !== cx.expect.rev) return false
    const expect = cx.expect
    if ('closingLeaseExpired' in expect) {
      return current.state === 'closing' && current.closeLease !== undefined && current.closeLease.until < this.#now()
    }
    if ('closingLease' in expect) {
      return current.state === 'closing' && current.closeLease?.id === expect.closingLease
    }
    return true
  }

  #storeHead(room: RoomRecord, next: HeadNext): StoredHead {
    const now = this.#now()
    const stored: StoredHead = {
      rev: `rev-${++this.#state.revSeq}`,
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
    if (stored.currentInc !== null) this.#generation(room, stored.currentInc)
    return stored
  }

  async readCells(
    roomId: string,
    inc: string,
    sel: { keys: string[] } | { prefix: string },
  ): Promise<{ revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }> {
    this.#assertLive()
    const room = this.#state.rooms.get(roomId)
    const head = this.#readAndExpireHead(room)
    // Closing tails may read; only writes require an open head.
    if (room === undefined || head === null || head.currentInc !== inc) return { staleInc: true }
    const gen = this.#generation(room, inc)
    const keys = 'keys' in sel ? sel.keys : [...gen.cells.keys()].filter((key) => key.startsWith(sel.prefix))
    const cells = new Map<string, Uint8Array>()
    for (const key of keys) {
      const cell = gen.cells.get(key)
      if (cell === undefined) continue
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
    this.#assertLive()
    const room = this.#state.rooms.get(roomId)
    const head = this.#readAndExpireHead(room)
    if (room === undefined || head === null || head.currentInc !== inc || head.state !== 'open') return 'stale-inc'
    const gen = this.#generation(room, inc)
    if (String(gen.revision) !== revision) return 'conflict'
    for (const mutation of mutations) {
      if (mutation.set === undefined) gen.cells.delete(mutation.key)
      else gen.cells.set(mutation.key, { bytes: copyBytes(mutation.set.bytes) })
    }
    gen.revision += 1
    return 'committed'
  }

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitResult> {
    this.#assertLive()
    const room = this.#state.rooms.get(roomId)
    const head = this.#readAndExpireHead(room)
    if (room === undefined || head === null || !this.#commitPreconditionHolds(head, inc, lane, opts?.closingLease)) {
      return { stale: 'incarnation' }
    }
    const gen = this.#generation(room, inc)
    const missing = opts?.requiredCellKeys?.find((key) => !gen.cells.has(key))
    if (missing !== undefined) return { stale: 'cell', key: missing }
    const key = encodeLaneKey(lane)
    const frame = copyBytes(payload)
    const mark = advanceOrder(gen.order, key, this.#now(), 'commitLane')
    if (opts?.retain) {
      gen.retained.set(key, {
        lane: Object.freeze(copyLane(lane)),
        payload: frame,
        ...mark,
      })
    }
    const targets = [...(gen.subs.get(key) ?? [])]
    const info = { seq: mark.seq, timestamp: mark.timestamp }
    return {
      accepted: true,
      ...mark,
      receivers: sumReceiverCounts(targets),
      delivery: this.#enqueueAttempt(gen, key, targets, frame, info),
    }
  }

  // Supplying a lease selects the narrow closing-control branch; all other closing lanes are stale.
  #commitPreconditionHolds(head: StoredHead, inc: string, lane: LaneId, closingLease: string | undefined): boolean {
    if (head.currentInc !== inc) return false
    return closingLease === undefined
      ? head.state === 'open'
      : lane.kind === 'control' &&
          head.state === 'closing' &&
          head.closeLease !== undefined &&
          head.closeLease.id === closingLease &&
          this.#now() <= head.closeLease.until
  }

  // Per-(inc,lane) at-most-once chain: settlement gates the next attempt without poisoning it.
  #enqueueAttempt(
    gen: Generation,
    key: string,
    targets: MemorySubscriptionAttempt[],
    frame: Uint8Array,
    info: { seq: number; timestamp: number },
  ): Promise<void> {
    const previous = gen.chains.get(key) ?? Promise.resolve()
    const attempt = previous.then(() =>
      Promise.all(
        targets.map((target) => (target.closed ? undefined : target.deliver(copyBytes(frame), { ...info }))),
      ).then(noop),
    )
    gen.chains.set(key, attempt.then(noop, noop))
    return attempt
  }

  async readRetained(
    roomId: string,
    inc: string,
    lane: LaneId,
  ): Promise<{ payload: Uint8Array; seq: number; timestamp: number } | null> {
    this.#assertLive()
    const entry = this.#state.rooms.get(roomId)?.gens.get(inc)?.retained.get(encodeLaneKey(lane))
    if (entry === undefined) return null
    return { payload: copyBytes(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    this.#assertLive()
    const gen = this.#state.rooms.get(roomId)?.gens.get(inc)
    return gen === undefined ? [] : [...gen.retained.values()].map((entry) => copyLane(entry.lane))
  }

  async deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this.#assertLive()
    if (lane === undefined && opts?.ifSeq !== undefined) {
      throw new Error('deleteRetained: ifSeq requires a lane')
    }
    if (opts?.ifSeq !== undefined && (!Number.isSafeInteger(opts.ifSeq) || opts.ifSeq <= 0)) {
      throw new Error('deleteRetained: ifSeq must be a positive safe integer')
    }
    const gen = this.#state.rooms.get(roomId)?.gens.get(inc)
    if (gen === undefined) return
    if (lane === undefined) gen.retained.clear()
    else {
      const key = encodeLaneKey(lane)
      const retained = gen.retained.get(key)
      if (opts?.ifSeq === undefined || retained?.seq === opts.ifSeq) gen.retained.delete(key)
    }
  }

  #openSubscription(
    source: MemorySubscriptionSource,
    receiver: BackendReceiver,
    localReceiverCount: () => number,
  ): MemorySubscriptionAttempt {
    if (!('roomId' in source)) {
      const key = broadcastRouteKey(source)
      const sub: MemorySubscriptionAttempt = new MemorySubscriptionAttempt(receiver, localReceiverCount, () =>
        removeFromSet(this.#state.broadcastSubs, key, sub),
      )
      getOrCreate(this.#state.broadcastSubs, key, () => new Set()).add(sub)
      sub.establish()
      return sub
    }

    const { roomId, inc, lane } = source
    const room = this.#state.rooms.get(roomId)
    const head = this.#readAndExpireHead(room)
    const key = encodeLaneKey(lane)
    if (room === undefined || head === null || head.currentInc !== inc || head.state !== 'open') {
      const sub = new MemorySubscriptionAttempt(receiver, localReceiverCount)
      sub.failEstablishment(`subscribeLane: room '${roomId}' has no open incarnation '${inc}'`)
      return sub
    }
    // Registration is durable before `ready` resolves: a commit accepted after this point must see it.
    const gen = this.#generation(room, inc)
    const sub: MemorySubscriptionAttempt = new MemorySubscriptionAttempt(receiver, localReceiverCount, () =>
      removeFromSet(gen.subs, key, sub),
    )
    getOrCreate(gen.subs, key, () => new Set()).add(sub)
    sub.establish()
    return sub
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    this.#assertLive()
    const room = this.#state.rooms.get(roomId)
    if (room === undefined) return
    if (this.#readAndExpireHead(room)?.currentInc === inc) {
      throw new Error(`dropGeneration: refusing to drop the current incarnation '${inc}' of room '${roomId}'`)
    }
    const gen = room.gens.get(inc)
    if (gen === undefined) return // already dropped — the janitor is resumable
    room.gens.delete(inc)
    this.#releaseWhenLapsed(roomId, room)
  }

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this.#assertLive()
    this.#state.directory.set(roomId, incTag)
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.#assertLive()
    if (this.#state.directory.get(roomId) === incTag) this.#state.directory.delete(roomId)
  }

  async directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> {
    this.#assertLive()
    const entries = [...this.#state.directory]
      .filter(([roomId]) => roomId.startsWith(prefix) && (cursor === undefined || roomId > cursor))
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([roomId, incTag]) => ({ roomId, incTag }))
    return { entries }
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return
    this.#disposed = true
    this.#state.rooms.clear()
    this.#state.directory.clear()
    this.#state.broadcastOrder.clear()
    this.#state.broadcastSubs.clear()
  }

  // ── internals ──

  #assertLive(): void {
    if (this.#disposed) throw new Error('MemoryBackend: used after dispose()')
  }

  #roomFor(roomId: string): RoomRecord {
    return getOrCreate(this.#state.rooms, roomId, () => ({ head: null, gens: new Map() }))
  }

  /** A room with no incarnation left is forgotten once its tombstone lapses (revs are process-global, never reused). */
  #releaseWhenLapsed(roomId: string, room: RoomRecord): void {
    if (this.#state.rooms.get(roomId) !== room || room.gens.size > 0) return
    const head = this.#readAndExpireHead(room)
    if (head === null) {
      this.#state.rooms.delete(roomId)
      return
    }
    if (head.expiresAt === null) return
    unrefTimer(setTimeout(() => this.#releaseWhenLapsed(roomId, room), head.expiresAt - this.#now()))
  }

  #generation(room: RoomRecord, inc: string): Generation {
    return getOrCreate(room.gens, inc, newGeneration)
  }

  // Lazy TTL: a lapsed tombstone reads as absent, which is what reopens an absence epoch.
  #readAndExpireHead(room: RoomRecord | undefined): StoredHead | null {
    if (room === undefined || room.head === null) return null
    if (!isExpired(room.head, this.#now())) return room.head
    room.head = null
    return null
  }
}

function removeFromSet<Key, Value>(map: Map<Key, Set<Value>>, key: Key, value: Value): void {
  const set = map.get(key)
  if (set?.delete(value) && set.size === 0) map.delete(key)
}

function getOrCreate<Key, Value>(map: Map<Key, Value>, key: Key, create: () => Value): Value {
  let value = map.get(key)
  if (value === undefined) map.set(key, (value = create()))
  return value
}
