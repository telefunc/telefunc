export { MemoryBackendState, MemoryBackend }

// The in-process backend, and the reference for Room SPI semantics: this process's clock is authority time.

import type { BroadcastDriver, BroadcastPayload, BroadcastRoute, PublishResult } from '../broadcast/contract.js'
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
import type { BroadcastKind } from '../../shared-ws.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import type { BackendPayload, BackendReceiver, SubscriptionDriver } from '../subscription.js'
import { DriverAttempt } from '../attempt.js'
import { macrotaskYield } from '../../flow-control/macrotask-yield.js'

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
  subs: Subscriptions
}

type RoomRecord = { head: StoredHead | null; gens: Map<string, Generation> }

/** Subscriptions by key; a change replaces a key's array, so an array read at a publish or commit is its targets. */
type Subscriptions = Map<string, readonly MemorySubscriptionAttempt[]>

/** @internal The storage, kept apart from the backend so a reconstructed one can reuse it. */
class MemoryBackendState {
  readonly rooms = new Map<string, RoomRecord>()
  readonly directory = new Map<string, string>()
  readonly broadcastOrder = new Map<string, OrderingInfo>()
  readonly broadcastSubs: Record<BroadcastKind, Subscriptions> = {
    text: new Map(),
    binary: new Map(),
  }
  revSeq = 0
}

/** Deliveries one event-loop turn runs; the rest wait for the next, so a listener answering itself can't starve the loop. */
const DELIVERIES_PER_TURN = 1024

const copyBytes = (bytes: Uint8Array): Uint8Array => new Uint8Array(bytes)
const copyLane = (lane: LaneId): LaneId => ({ ...lane })
const sumReceiverCounts = (targets: readonly MemorySubscriptionAttempt[]): number =>
  targets.reduce((total, target) => total + target.receiverCount(), 0)
const isExpired = (entry: Expiring, now: number): boolean => entry.expiresAt !== null && entry.expiresAt <= now

/** A later macrotask; setImmediate keeps the process alive for it, where the runtime has one. */
function nextTurn(callback: () => void): void {
  if (typeof setImmediate === 'function') setImmediate(callback)
  else void macrotaskYield.yield().then(callback)
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
  private readonly _receiver: BackendReceiver<BackendPayload>
  private readonly _localReceiverCount: () => number
  private readonly _detach: () => void

  constructor(receiver: BackendReceiver<BackendPayload>, localReceiverCount: () => number, detach: () => void) {
    super()
    this._receiver = receiver
    this._localReceiverCount = localReceiverCount
    this._detach = detach
    this.transition('ready')
  }

  async unsubscribe(): Promise<void> {
    this._detach()
    this.transition('closed')
  }

  deliver(payload: BackendPayload, info: { seq: number; timestamp: number }): void {
    this._receiver(payload, info)
  }

  receiverCount(): number {
    return this._localReceiverCount()
  }
}

class MemoryBackend implements BroadcastDriver, RoomDriver {
  readonly subscriptions: SubscriptionDriver<MemorySubscriptionSource>

  private readonly _state: MemoryBackendState
  /** Deliveries in seq order: the running one stays first, so a publish made inside it is delivered after. */
  private readonly _deliveries: Array<() => void> = []
  private _deliveredThisTurn = 0
  constructor(options: MemoryBackendOptions = {}) {
    this._state = options.state ?? new MemoryBackendState()
    this.subscriptions = {
      bind: (source) => ({
        partition: '',
        open: (receiver, localReceiverCount) => this._openSubscription(source, receiver, localReceiverCount),
      }),
      partitionHere: () => '',
    }
  }

  publish(route: BroadcastRoute, payload: BroadcastPayload): PublishResult {
    const mark = advanceOrder(this._state.broadcastOrder, route.key, Date.now())
    const targets = this._state.broadcastSubs[route.kind].get(route.key) ?? []
    // Counted before delivery, which may unsubscribe or subscribe.
    const receivers = sumReceiverCounts(targets)
    this._deliver(() => {
      // A string can't change, so every subscription gets the same one; bytes are copied for each.
      for (const target of targets) target.deliver(typeof payload === 'string' ? payload : copyBytes(payload), mark)
    })
    return { seq: mark.seq, timestamp: mark.timestamp, receivers, meta: { transport: 'in-memory' } }
  }

  /** Runs `delivery` after those queued before it: now, unless one is running or this turn's deliveries ran out. */
  private _deliver(delivery: () => void): void {
    this._deliveries.push(delivery)
    if (this._deliveries.length === 1) this._drain()
  }

  private _drain(): void {
    for (; this._deliveries.length > 0; this._deliveries.shift()) {
      if (this._deliveredThisTurn === DELIVERIES_PER_TURN) return
      if (this._deliveredThisTurn++ === 0) nextTurn(() => this._nextTurn())
      this._deliveries[0]!()
    }
  }

  private _nextTurn(): void {
    this._deliveredThisTurn = 0
    if (this._deliveries.length > 0) this._drain()
  }

  async readHead(roomId: string): Promise<RoomHead | null> {
    const head = this._liveHead(this._state.rooms.get(roomId))
    return head === null ? null : publicHead(head)
  }

  async compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    const current = this._liveHead(this._state.rooms.get(roomId))
    if (!headCxMatches(cx, current, Date.now())) {
      return { conflict: true, current: current === null ? null : publicHead(current) }
    }
    // Only a CX that actually applies materializes a room record.
    return { head: publicHead(this._storeHead(this._roomFor(roomId), next)) }
  }

  private _storeHead(room: RoomRecord, next: HeadNext): StoredHead {
    const materialized = materializeHead(next, Date.now(), `rev-${++this._state.revSeq}`)
    const stored = { ...materialized, config: copyBytes(materialized.config) }
    room.head = stored
    if (stored.currentInc !== null) this._generation(room, stored.currentInc)
    return stored
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    const room = this._state.rooms.get(roomId)
    const head = this._liveHead(room)
    // Closing tails may read; only writes require an open head.
    if (room === undefined || head === null || head.currentInc !== inc) return { staleInc: true }
    const gen = this._generation(room, inc)
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
    const room = this._state.rooms.get(roomId)
    const head = this._liveHead(room)
    if (room === undefined || !isOpenIncarnation(head, inc)) return 'stale-inc'
    const gen = this._generation(room, inc)
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
    const room = this._state.rooms.get(roomId)
    const head = this._liveHead(room)
    if (room === undefined || !commitPreconditionHolds(head, inc, lane.kind, opts?.closingLease, Date.now())) {
      return { stale: 'incarnation' }
    }
    const gen = this._generation(room, inc)
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
    const targets = gen.subs.get(key) ?? []
    // Commits assign their seq and queue their delivery in one synchronous step, so deliveries run in seq order.
    const delivery = new Promise<void>((resolve) =>
      queueMicrotask(() =>
        this._deliver(() => {
          for (const target of targets) target.deliver(copyBytes(frame), mark)
          resolve()
        }),
      ),
    )
    return { accepted: true, ...mark, receivers: sumReceiverCounts(targets), delivery }
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    const entry = this._state.rooms.get(roomId)?.gens.get(inc)?.retained.get(encodeLaneKey(lane))
    if (entry === undefined) return null
    return { payload: copyBytes(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    const gen = this._state.rooms.get(roomId)?.gens.get(inc)
    return gen === undefined ? [] : [...gen.retained.values()].map((entry) => copyLane(entry.lane))
  }

  async deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    const retained = this._state.rooms.get(roomId)?.gens.get(inc)?.retained
    const key = encodeLaneKey(lane)
    if (opts?.ifSeq === undefined || retained?.get(key)?.seq === opts.ifSeq) retained?.delete(key)
  }

  private _openSubscription(
    source: MemorySubscriptionSource,
    receiver: BackendReceiver<BackendPayload>,
    localReceiverCount: () => number,
  ): MemorySubscriptionAttempt {
    let subs: Subscriptions
    let key: string
    if ('roomId' in source) {
      const { roomId, inc, lane } = source
      const room = this._state.rooms.get(roomId)
      const head = this._liveHead(room)
      if (room === undefined || !isOpenIncarnation(head, inc))
        throw new Error(`subscribeLane: room '${roomId}' has no open incarnation '${inc}'`)
      // Registration is durable before `ready` resolves: a commit accepted after this point must see it.
      subs = this._generation(room, inc).subs
      key = encodeLaneKey(lane)
    } else {
      subs = this._state.broadcastSubs[source.kind]
      key = source.key
    }
    const sub: MemorySubscriptionAttempt = new MemorySubscriptionAttempt(receiver, localReceiverCount, () =>
      removeSubscription(subs, key, sub),
    )
    subs.set(key, [...(subs.get(key) ?? []), sub])
    return sub
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    const room = this._state.rooms.get(roomId)
    if (room === undefined) return
    const gen = room.gens.get(inc)
    if (gen === undefined) return // already dropped (the janitor is resumable)
    room.gens.delete(inc)
    this._releaseWhenLapsed(roomId, room)
  }

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this._state.directory.set(roomId, incTag)
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    if (this._state.directory.get(roomId) === incTag) this._state.directory.delete(roomId)
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    const entries = [...this._state.directory]
      .filter(([roomId]) => roomId.startsWith(prefix) && (cursor === undefined || roomId > cursor))
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([roomId, incTag]) => ({ roomId, incTag }))
    return { entries }
  }

  // ── internals ──

  private _roomFor(roomId: string): RoomRecord {
    return getOrCreate(this._state.rooms, roomId, () => ({ head: null, gens: new Map() }))
  }

  /** A room with no incarnation left is forgotten once its tombstone lapses (revs are process-global, never reused). */
  private _releaseWhenLapsed(roomId: string, room: RoomRecord): void {
    if (this._state.rooms.get(roomId) !== room || room.gens.size > 0) return
    const head = this._liveHead(room)
    if (head === null) {
      this._state.rooms.delete(roomId)
      return
    }
    if (head.expiresAt === null) return
    unrefTimer(setTimeout(() => this._releaseWhenLapsed(roomId, room), head.expiresAt - Date.now()))
  }

  private _generation(room: RoomRecord, inc: string): Generation {
    return getOrCreate(room.gens, inc, newGeneration)
  }

  // Lazy TTL: a lapsed tombstone reads as absent, which is what reopens an absence epoch.
  private _liveHead(room: RoomRecord | undefined): StoredHead | null {
    const head = room?.head ?? null
    return head === null || isExpired(head, Date.now()) ? null : head
  }
}

function removeSubscription(subs: Subscriptions, key: string, sub: MemorySubscriptionAttempt): void {
  const current = subs.get(key)
  if (current === undefined || !current.includes(sub)) return
  const rest = current.filter((other) => other !== sub)
  if (rest.length === 0) subs.delete(key)
  else subs.set(key, rest)
}

function getOrCreate<Key, Value>(map: Map<Key, Value>, key: Key, create: () => Value): Value {
  let value = map.get(key)
  if (value === undefined) map.set(key, (value = create()))
  return value
}
