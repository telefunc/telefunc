export { MemoryBackendState, MemoryBackend }

// The in-process backend, and the reference for Room SPI semantics: this process's clock is authority time.

import type { BroadcastDriver, BroadcastRoute, PublishResult } from '../broadcast/contract.js'
import { broadcastRouteKey } from '../broadcast/route-key.js'
import type {
  CellMutation,
  HeadCxResult,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
  CellSelector,
  CellsRead,
  CommitOptions,
  RetainedFrame,
  DirectoryPage,
} from '../room/contract.js'
import { encodeLaneKey } from '../room/lane-key.js'
import {
  commitPreconditionHolds,
  headCxMatches,
  isOpenIncarnation,
  materializeHead,
  nextOrderMark,
  type StoredHead,
} from '../room/semantics.js'
import type { OrderingInfo } from '../../ordering-frame.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import type { BackendReceiver, SubscriptionDriver } from '../subscription.js'
import { DriverAttempt } from '../attempt.js'

type MemoryBackendOptions = {
  /** @internal Storage to share with a reconstructed backend. */
  state?: MemoryBackendState
}

type MemorySubscriptionSource = BroadcastRoute | RoomSubscriptionSource

type Expiring = { expiresAt: number | null }
type StoredCell = { bytes: Uint8Array }
type RetainedEntry = { lane: LaneId; payload: Uint8Array; seq: number; timestamp: number }

type Generation = {
  revision: number
  cells: Map<string, StoredCell>
  order: Map<string, OrderingInfo>
  retained: Map<string, RetainedEntry>
  subs: Map<string, Set<MemorySubscriptionAttempt>>
}

type RoomRecord = { head: StoredHead | null; gens: Map<string, Generation> }

/** @internal The storage, kept apart from the backend so a reconstructed one can reuse it. */
class MemoryBackendState {
  readonly rooms = new Map<string, RoomRecord>()
  readonly directory = new Map<string, string>()
  readonly broadcastOrder = new Map<string, OrderingInfo>()
  readonly broadcastSubs = new Map<string, Set<MemorySubscriptionAttempt>>()
  revSeq = 0
}

const copyBytes = (bytes: Uint8Array): Uint8Array => new Uint8Array(bytes)
const copyLane = (lane: LaneId): LaneId => ({ ...lane })
const sumReceiverCounts = (targets: MemorySubscriptionAttempt[]): number =>
  targets.reduce((total, target) => total + target.receiverCount(), 0)
const isExpired = (entry: Expiring, now: number): boolean => entry.expiresAt !== null && entry.expiresAt <= now

// Commits assign their seq and queue their delivery in one synchronous step, so deliveries run in seq order.
function deliverAfterCommit(
  targets: MemorySubscriptionAttempt[],
  frame: Uint8Array,
  mark: OrderingInfo,
): Promise<void> {
  return Promise.resolve().then(() => {
    for (const target of targets) if (!target.ended) target.deliver(copyBytes(frame), mark)
  })
}

function newGeneration(): Generation {
  return { revision: 0, cells: new Map(), order: new Map(), retained: new Map(), subs: new Map() }
}

function advanceOrder(order: Map<string, OrderingInfo>, domain: string, now: number): OrderingInfo {
  const mark = nextOrderMark(order.get(domain), now)
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

class MemorySubscriptionAttempt extends DriverAttempt {
  readonly #receiver: BackendReceiver
  readonly #localReceiverCount: () => number
  readonly #detach: () => void

  constructor(receiver: BackendReceiver, localReceiverCount: () => number, detach: () => void) {
    super()
    this.#receiver = receiver
    this.#localReceiverCount = localReceiverCount
    this.#detach = detach
  }

  async unsubscribe(): Promise<void> {
    if (this.ended) return
    this.#detach()
    this.transition('closed')
  }

  establish(): void {
    this.transition('ready')
  }

  deliver(payload: Uint8Array, info: { seq: number; timestamp: number }): void {
    this.#receiver(payload, info)
  }

  receiverCount(): number {
    return this.#localReceiverCount()
  }
}

class MemoryBackend implements BroadcastDriver, RoomDriver {
  readonly subscriptions: SubscriptionDriver<MemorySubscriptionSource>

  readonly #state: MemoryBackendState
  constructor(options: MemoryBackendOptions = {}) {
    this.#state = options.state ?? new MemoryBackendState()
    this.subscriptions = {
      bind: (source) => ({
        partition: '',
        open: (receiver, localReceiverCount) => this.#openSubscription(source, receiver, localReceiverCount),
      }),
    }
  }

  publish(route: BroadcastRoute, payload: Uint8Array): PublishResult {
    const mark = advanceOrder(this.#state.broadcastOrder, route.key, Date.now())
    const targets = [...(this.#state.broadcastSubs.get(broadcastRouteKey(route)) ?? [])]
    // Counted before delivery, which may unsubscribe or subscribe.
    const receivers = sumReceiverCounts(targets)
    for (const target of targets) target.deliver(copyBytes(payload), mark)
    return { ...mark, receivers, meta: { transport: 'in-memory' } }
  }

  async readHead(roomId: string): Promise<RoomHead | null> {
    const head = this.#readAndExpireHead(this.#state.rooms.get(roomId))
    return head === null ? null : publicHead(head)
  }

  async compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    const current = this.#readAndExpireHead(this.#state.rooms.get(roomId))
    if (!headCxMatches(cx, current, Date.now())) {
      return { conflict: true, current: current === null ? null : publicHead(current) }
    }
    // Only a CX that actually applies materializes a room record.
    return { head: publicHead(this.#storeHead(this.#roomFor(roomId), next)) }
  }

  #storeHead(room: RoomRecord, next: HeadNext): StoredHead {
    const materialized = materializeHead(next, Date.now(), `rev-${++this.#state.revSeq}`)
    const stored = { ...materialized, config: copyBytes(materialized.config) }
    room.head = stored
    if (stored.currentInc !== null) this.#generation(room, stored.currentInc)
    return stored
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
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
    const room = this.#state.rooms.get(roomId)
    const head = this.#readAndExpireHead(room)
    if (room === undefined || !isOpenIncarnation(head, inc)) return 'stale-inc'
    const gen = this.#generation(room, inc)
    if (String(gen.revision) !== revision) return 'conflict'
    for (const mutation of mutations) {
      if (mutation.bytes === null) gen.cells.delete(mutation.key)
      else gen.cells.set(mutation.key, { bytes: copyBytes(mutation.bytes) })
    }
    gen.revision += 1
    return 'committed'
  }

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const room = this.#state.rooms.get(roomId)
    const head = this.#readAndExpireHead(room)
    if (room === undefined || !commitPreconditionHolds(head, inc, lane.kind, opts?.closingLease, Date.now())) {
      return { stale: 'incarnation' }
    }
    const gen = this.#generation(room, inc)
    const missing = opts?.requiredCellKeys?.find((key) => !gen.cells.has(key))
    if (missing !== undefined) return { stale: 'cell', key: missing }
    const key = encodeLaneKey(lane)
    const frame = copyBytes(payload)
    const mark = advanceOrder(gen.order, key, Date.now())
    if (opts?.retain) {
      gen.retained.set(key, {
        lane: copyLane(lane),
        payload: frame,
        ...mark,
      })
    }
    const targets = [...(gen.subs.get(key) ?? [])]
    return {
      accepted: true,
      ...mark,
      receivers: sumReceiverCounts(targets),
      delivery: deliverAfterCommit(targets, frame, mark),
    }
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    const entry = this.#state.rooms.get(roomId)?.gens.get(inc)?.retained.get(encodeLaneKey(lane))
    if (entry === undefined) return null
    return { payload: copyBytes(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    const gen = this.#state.rooms.get(roomId)?.gens.get(inc)
    return gen === undefined ? [] : [...gen.retained.values()].map((entry) => copyLane(entry.lane))
  }

  async deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    const retained = this.#state.rooms.get(roomId)?.gens.get(inc)?.retained
    const key = encodeLaneKey(lane)
    if (opts?.ifSeq === undefined || retained?.get(key)?.seq === opts.ifSeq) retained?.delete(key)
  }

  #openSubscription(
    source: MemorySubscriptionSource,
    receiver: BackendReceiver,
    localReceiverCount: () => number,
  ): MemorySubscriptionAttempt {
    let subs: Map<string, Set<MemorySubscriptionAttempt>>
    let key: string
    if ('roomId' in source) {
      const { roomId, inc, lane } = source
      const room = this.#state.rooms.get(roomId)
      const head = this.#readAndExpireHead(room)
      if (room === undefined || !isOpenIncarnation(head, inc))
        throw new Error(`subscribeLane: room '${roomId}' has no open incarnation '${inc}'`)
      // Registration is durable before `ready` resolves: a commit accepted after this point must see it.
      subs = this.#generation(room, inc).subs
      key = encodeLaneKey(lane)
    } else {
      subs = this.#state.broadcastSubs
      key = broadcastRouteKey(source)
    }
    const sub: MemorySubscriptionAttempt = new MemorySubscriptionAttempt(receiver, localReceiverCount, () =>
      removeFromSet(subs, key, sub),
    )
    getOrCreate(subs, key, () => new Set()).add(sub)
    sub.establish()
    return sub
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    const room = this.#state.rooms.get(roomId)
    if (room === undefined) return
    const gen = room.gens.get(inc)
    if (gen === undefined) return // already dropped (the janitor is resumable)
    room.gens.delete(inc)
    this.#releaseWhenLapsed(roomId, room)
  }

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this.#state.directory.set(roomId, incTag)
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    if (this.#state.directory.get(roomId) === incTag) this.#state.directory.delete(roomId)
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    const entries = [...this.#state.directory]
      .filter(([roomId]) => roomId.startsWith(prefix) && (cursor === undefined || roomId > cursor))
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([roomId, incTag]) => ({ roomId, incTag }))
    return { entries }
  }

  // ── internals ──

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
    unrefTimer(setTimeout(() => this.#releaseWhenLapsed(roomId, room), head.expiresAt - Date.now()))
  }

  #generation(room: RoomRecord, inc: string): Generation {
    return getOrCreate(room.gens, inc, newGeneration)
  }

  // Lazy TTL: a lapsed tombstone reads as absent, which is what reopens an absence epoch.
  #readAndExpireHead(room: RoomRecord | undefined): StoredHead | null {
    if (room === undefined || room.head === null) return null
    if (!isExpired(room.head, Date.now())) return room.head
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
