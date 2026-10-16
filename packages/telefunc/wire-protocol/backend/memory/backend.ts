export { MemoryBackendState, MemoryBackend }

// The in-process backend, and the reference for Room SPI semantics: its state's clock is authority time.

import type { BroadcastDriver, BroadcastPayload, BroadcastRoute, PublishResult } from '../broadcast/contract.js'
import type {
  CellCounter,
  CellsCx,
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
  RetainedOf,
  DirectoryPage,
} from '../room/contract.js'
import { CELL_PAGE_SIZE, DIRECTORY_PAGE_SIZE } from '../room/contract.js'
import { encodeLaneKey } from '../room/lane-key.js'
import {
  commitPreconditionHolds,
  countCell,
  encodeCellCounter,
  headCxMatches,
  isOpenIncarnation,
  materializeHead,
  nextOrderMark,
  publicHead,
  type StoredHead,
} from '../room/semantics.js'
import type { OrderingInfo } from '../../ordering-frame.js'
import type { BroadcastKind } from '../../shared-ws.js'
import { createDeferred } from '../../../utils/createDeferred.js'
import { getOrCreate } from '../../../utils/getOrCreate.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import type { BackendPayload, BackendReceiver, SubscriptionDriver } from '../subscription.js'
import { DriverAttempt } from '../attempt.js'
import { nextTurn } from '../../flow-control/macrotask-yield.js'

type MemoryBackendOptions = {
  /** @internal For specs: storage to inspect, or to share with another backend. */
  state?: MemoryBackendState
}

type MemorySubscriptionSource = BroadcastRoute | RoomSubscriptionSource

type Expiring = { expiresAt: number | null }
type StoredCell = { bytes: Uint8Array; version: string }
type RetainedEntry = { lane: LaneId; payload: Uint8Array; seq: number; timestamp: number }

type Generation = {
  /** The last cell version this generation issued. */
  version: number
  cells: Map<string, StoredCell>
  /** Cell keys by each of their prefixes ending in ':', the separator of cell keys, so a prefix read lists its own. */
  cellsByPrefix: Map<string, Set<string>>
  /** The room-wide lanes' order, by lane key. */
  order: Map<string, OrderingInfo>
  /** Each member's lanes' order, by lane key, so its leave drops them together. */
  memberOrders: Map<string, Map<string, OrderingInfo>>
  retained: Map<string, RetainedEntry>
  /** The keys of each member's retained binary lanes. */
  retainedOf: Map<string, Set<string>>
  /** The keys of each track's retained binary lanes. */
  retainedOfTrack: Map<string, Set<string>>
  subs: Subscriptions
  /** Every-track subscriptions by member id, so a binary commit finds its member's without building a key. */
  everyTrackSubs: Subscriptions
  /** Every-member subscriptions by track, likewise. */
  everyMemberSubs: Subscriptions
}

type RoomRecord = { head: StoredHead | null; gens: Map<string, Generation> }

type DirectoryEntry = { incTag: string; checkAt: number }
/** A directory entry's check, current while its entry is still due at `at`. */
type DirectoryCheck = { at: number; roomId: string }

/** Subscriptions by key; a change replaces a key's array, so an array read at a publish or commit is its targets. */
type Subscriptions = Map<string, readonly MemorySubscriptionAttempt[]>

/** @internal The backend's storage, kept apart from it so specs can inspect it and share it between backends. `now` is
 *  its authority time; a spec gives it a clock of its own to set the app servers' apart from it. */
class MemoryBackendState {
  readonly now: () => number
  readonly rooms = new Map<string, RoomRecord>()
  readonly directory = new Map<string, DirectoryEntry>()
  /** A min-heap by `at` of the checks each put scheduled; a check its entry no longer has is dropped when reached. */
  readonly directoryChecks: DirectoryCheck[] = []
  readonly broadcastOrder = new Map<string, OrderingInfo>()

  readonly broadcastSubs: Record<BroadcastKind, Subscriptions> = {
    text: new Map(),
    binary: new Map(),
  }

  revSeq = 0

  constructor({ now = () => Date.now() }: { now?: () => number } = {}) {
    this.now = now
  }
}

/** Deliveries one event-loop turn runs; the rest wait for the next, so a listener answering itself
 *  can't starve the loop. */
const DELIVERIES_PER_TURN = 1024

const NO_TARGETS: readonly MemorySubscriptionAttempt[] = []
const copyBytes = (bytes: Uint8Array): Uint8Array => new Uint8Array(bytes)
const copyLane = (lane: LaneId): LaneId => ({ ...lane })

const sumReceiverCounts = (targets: readonly MemorySubscriptionAttempt[]): number =>
  targets.reduce((total, target) => total + target.receiverCount(), 0)

const isExpired = (entry: Expiring, now: number): boolean => entry.expiresAt !== null && entry.expiresAt <= now

function newGeneration(): Generation {
  return {
    version: 0,
    cells: new Map(),
    cellsByPrefix: new Map(),
    order: new Map(),
    memberOrders: new Map(),
    retained: new Map(),
    retainedOf: new Map(),
    retainedOfTrack: new Map(),
    subs: new Map(),
    everyTrackSubs: new Map(),
    everyMemberSubs: new Map(),
  }
}

function* separatorPrefixes(key: string): Generator<string> {
  for (let end = key.indexOf(':'); end !== -1; end = key.indexOf(':', end + 1)) yield key.slice(0, end + 1)
}

function cellKeysWithPrefix(gen: Generation, prefix: string): Iterable<string> {
  const indexed = prefix.slice(0, prefix.lastIndexOf(':') + 1)
  const keys = indexed === '' ? gen.cells.keys() : (gen.cellsByPrefix.get(indexed) ?? [])
  return indexed === prefix ? keys : [...keys].filter((key) => key.startsWith(prefix))
}

function readCellsOf(gen: Generation, keys: Iterable<string>, now: number): CellsRead {
  const cells = new Map<string, Uint8Array>()
  const versions = new Map<string, string>()
  for (const key of keys) {
    const cell = gen.cells.get(key)
    if (cell === undefined) continue
    cells.set(key, copyBytes(cell.bytes))
    versions.set(key, cell.version)
  }
  return { cells, versions, now }
}

function setCell(gen: Generation, key: string, bytes: Uint8Array | null, version: string): void {
  if (bytes === null) {
    if (!gen.cells.delete(key)) return
    for (const prefix of separatorPrefixes(key)) unindex(gen.cellsByPrefix, prefix, key)
    return
  }
  if (!gen.cells.has(key)) {
    for (const prefix of separatorPrefixes(key)) getOrCreate(gen.cellsByPrefix, prefix, () => new Set()).add(key)
  }
  gen.cells.set(key, { bytes: copyBytes(bytes), version })
}

function advanceOrder(order: Map<string, OrderingInfo>, domain: string, now: number): OrderingInfo {
  const mark = nextOrderMark(order.get(domain), now)
  order.set(domain, mark)
  return mark
}

class MemorySubscriptionAttempt extends DriverAttempt {
  constructor(
    private readonly _receiver: BackendReceiver<BackendPayload>,
    private readonly _localReceiverCount: () => number,
    private readonly _detach: () => void,
  ) {
    super()
  }

  establish(): void {
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
  /** Deliveries in seq order from `_next`: the running one stays queued, so a publish made inside
   *  it is delivered after. */
  private readonly _deliveries: Array<() => void> = []
  private _next = 0
  private _deliveredThisTurn = 0
  /** Lane commits whose delivery is not queued yet. */
  private _committing = 0

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
    const mark = advanceOrder(this._state.broadcastOrder, route.key, this._state.now())
    const targets = this._state.broadcastSubs[route.kind].get(route.key) ?? []
    // Counted before delivery, which may unsubscribe or subscribe.
    const receivers = sumReceiverCounts(targets)
    this._deliver(() => {
      // A string can't change, so every subscription gets the same one; bytes are copied for each.
      for (const target of targets) target.deliver(typeof payload === 'string' ? payload : copyBytes(payload), mark)
    })
    return { seq: mark.seq, timestamp: mark.timestamp, receivers }
  }

  /** Runs `delivery` after those queued before it: now, unless one is running or this turn's deliveries ran out. */
  private _deliver(delivery: () => void): void {
    this._deliveries.push(delivery)
    if (this._deliveries.length - this._next === 1) this._drain()
  }

  private _drain(): void {
    for (; this._next < this._deliveries.length; this._next++) {
      if (this._deliveredThisTurn === DELIVERIES_PER_TURN) return
      if (this._deliveredThisTurn++ === 0) nextTurn(() => this._nextTurn())
      this._deliveries[this._next]!()
    }
    this._deliveries.length = 0
    this._next = 0
  }

  private _nextTurn(): void {
    this._deliveredThisTurn = 0
    this._deliveries.splice(0, this._next)
    this._next = 0
    if (this._deliveries.length > 0) this._drain()
  }

  async readHead(roomId: string): Promise<RoomHead | null> {
    const head = this._liveHead(this._state.rooms.get(roomId))
    return head === null ? null : publicHead(head, copyBytes(head.config))
  }

  async compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    const current = this._liveHead(this._state.rooms.get(roomId))
    if (!headCxMatches(cx, current, this._state.now())) {
      return { conflict: true, current: current === null ? null : publicHead(current, copyBytes(current.config)) }
    }
    // Only a CX that actually applies materializes a room record.
    const head = this._storeHead(this._roomFor(roomId), next)
    return { head: publicHead(head, copyBytes(head.config)) }
  }

  private _storeHead(room: RoomRecord, next: HeadNext): StoredHead {
    const materialized = materializeHead(next, this._state.now(), `rev-${++this._state.revSeq}`)
    const stored = { ...materialized, config: copyBytes(materialized.config) }
    room.head = stored
    if (stored.currentInc !== null) this._generation(room, stored.currentInc)
    return stored
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    const gen = this._readableGeneration(roomId, inc)
    if (gen === null) return { staleInc: true }
    return readCellsOf(gen, 'keys' in sel ? sel.keys : cellKeysWithPrefix(gen, sel.prefix), this._state.now())
  }

  /** The prefix's keys as the read starts, so a page costs its own size and a join adds nothing to the read. */
  async *readCellPages(roomId: string, inc: string, prefix: string): AsyncGenerator<CellsRead> {
    let keys: string[] | null = null
    for (let at = 0; keys === null || at < keys.length; at += CELL_PAGE_SIZE) {
      const gen = this._readableGeneration(roomId, inc)
      if (gen === null) {
        yield { staleInc: true }
        return
      }
      keys ??= [...cellKeysWithPrefix(gen, prefix)]
      yield readCellsOf(gen, keys.slice(at, at + CELL_PAGE_SIZE), this._state.now())
    }
  }

  /** Closing tails may read; only writes require an open head. */
  private _readableGeneration(roomId: string, inc: string): Generation | null {
    const room = this._state.rooms.get(roomId)
    const head = this._liveHead(room)
    if (room === undefined || head === null || head.currentInc !== inc) return null
    return this._generation(room, inc)
  }

  async compareExchangeCells(roomId: string, inc: string, cx: CellsCx): Promise<CxResult> {
    const room = this._state.rooms.get(roomId)
    const head = this._liveHead(room)
    if (room === undefined || !isOpenIncarnation(head, inc)) return 'stale-inc'
    const gen = this._generation(room, inc)
    if (cx.expect.some(({ key, version }) => (gen.cells.get(key)?.version ?? null) !== version)) return 'conflict'
    const version = String(++gen.version)
    for (const mutation of cx.mutations) setCell(gen, mutation.key, mutation.bytes, version)
    const counters = new Map<string, CellCounter>()
    for (const count of cx.counts ?? []) {
      const counted = countCell(gen.cells.get(count.key)?.bytes, count, this._state.now())
      if (counted === null) continue
      if (counted.written) setCell(gen, count.key, encodeCellCounter(counted.counter), version)
      counters.set(count.key, counted.counter)
    }
    return { committed: counters }
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
    if (room === undefined || !commitPreconditionHolds(head, inc, lane.kind, opts?.closingLease, this._state.now())) {
      return { stale: 'incarnation' }
    }
    const gen = this._generation(room, inc)
    const missing = opts?.requiredCellKeys?.find((key) => !gen.cells.has(key))
    if (missing !== undefined) return { stale: 'cell', key: missing }
    const key = encodeLaneKey(lane)
    const frame = copyBytes(payload)
    const order =
      lane.kind === 'inbox' || lane.kind === 'binary'
        ? getOrCreate(gen.memberOrders, lane.member, () => new Map())
        : gen.order
    const mark = advanceOrder(order, key, this._state.now())
    if (opts?.retain) {
      // A lane holding a retained frame is already indexed: its frame and index entries leave together.
      if (lane.kind === 'binary' && !gen.retained.has(key)) {
        getOrCreate(gen.retainedOf, lane.member, () => new Set()).add(key)
        getOrCreate(gen.retainedOfTrack, lane.track, () => new Set()).add(key)
      }
      gen.retained.set(key, { lane: copyLane(lane), payload: frame, seq: mark.seq, timestamp: mark.timestamp })
    }
    const targets = gen.subs.get(key) ?? NO_TARGETS
    // A binary commit also reaches its member's every-track lane and its track's every-member lane.
    const binary = lane.kind === 'binary'
    const everyTrack = binary ? (gen.everyTrackSubs.get(lane.member) ?? NO_TARGETS) : NO_TARGETS
    const everyMember = binary ? (gen.everyMemberSubs.get(lane.track) ?? NO_TARGETS) : NO_TARGETS
    const delivered = lane.kind === 'inbox' || lane.kind === 'control' ? createDeferred() : null
    // Commits assign their seq and queue their delivery in one synchronous step, so deliveries run in seq order.
    this._committing++
    queueMicrotask(() => {
      this._committing--
      this._deliver(() => {
        for (const target of targets) target.deliver(copyBytes(frame), mark)
        for (const target of everyTrack) target.deliver(copyBytes(frame), mark)
        for (const target of everyMember) target.deliver(copyBytes(frame), mark)
        delivered?.resolve()
      })
    })
    const receivers = sumReceiverCounts(targets) + sumReceiverCounts(everyTrack) + sumReceiverCounts(everyMember)
    // A commit past a turn's deliveries waits for the next turn, as a network write would,
    // so commits can't outrun them.
    if (this._deliveries.length - this._next >= DELIVERIES_PER_TURN) await new Promise<void>(nextTurn)
    if (delivered === null) return { accepted: true, seq: mark.seq, timestamp: mark.timestamp, receivers }
    return { accepted: true, seq: mark.seq, timestamp: mark.timestamp, receivers, delivery: delivered.promise }
  }

  async readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    const gen = this._state.rooms.get(roomId)?.gens.get(inc)
    const order = lane.kind === 'inbox' || lane.kind === 'binary' ? gen?.memberOrders.get(lane.member) : gen?.order
    const mark = order?.get(encodeLaneKey(lane))
    return mark === undefined ? null : { ...mark }
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    const entry = this._state.rooms.get(roomId)?.gens.get(inc)?.retained.get(encodeLaneKey(lane))
    if (entry === undefined) return null
    return { payload: copyBytes(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(roomId: string, inc: string, of?: RetainedOf): Promise<LaneId[]> {
    const gen = this._state.rooms.get(roomId)?.gens.get(inc)
    if (gen === undefined) return []
    if (of === undefined) return Array.from(gen.retained.values(), (entry) => copyLane(entry.lane))
    const keys = 'member' in of ? gen.retainedOf.get(of.member) : gen.retainedOfTrack.get(of.track)
    return Array.from(keys ?? [], (key) => copyLane(gen.retained.get(key)!.lane))
  }

  async deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    const gen = this._state.rooms.get(roomId)?.gens.get(inc)
    const key = encodeLaneKey(lane)
    if (gen === undefined || (opts?.ifSeq !== undefined && gen.retained.get(key)?.seq !== opts.ifSeq)) return
    gen.retained.delete(key)
    if (lane.kind !== 'binary') return
    unindex(gen.retainedOf, lane.member, key)
    unindex(gen.retainedOfTrack, lane.track, key)
  }

  async dropLaneOrders(roomId: string, inc: string, member: string): Promise<void> {
    this._state.rooms.get(roomId)?.gens.get(inc)?.memberOrders.delete(member)
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
      if (room === undefined || !isOpenIncarnation(head, inc)) {
        throw new Error(`subscribeLane: room '${roomId}' has no open incarnation '${inc}'`)
      }
      // Registration is durable before `ready` resolves: a commit accepted after this point must see it.
      const gen = this._generation(room, inc)
      if (lane.kind === 'binary-all') {
        subs = gen.everyTrackSubs
        key = lane.member
      } else if (lane.kind === 'binary-track') {
        subs = gen.everyMemberSubs
        key = lane.track
      } else {
        subs = gen.subs
        key = encodeLaneKey(lane)
      }
    } else {
      subs = this._state.broadcastSubs[source.kind]
      key = source.key
    }
    const sub: MemorySubscriptionAttempt = new MemorySubscriptionAttempt(receiver, localReceiverCount, () =>
      removeSubscription(subs, key, sub),
    )
    subs.set(key, [...(subs.get(key) ?? []), sub])
    // Ready once what was committed before it is delivered, as a network subscriber hears its confirmation after the
    // messages sent before it: a lane it replaces delivers those first.
    if (this._committing === 0 && this._deliveries.length === this._next) sub.establish()
    else queueMicrotask(() => this._deliver(() => sub.establish()))
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

  async directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    const at = Math.max(checkAt, this._state.now() + minDelayMs)
    this._state.directory.set(roomId, { incTag, checkAt: at })
    if (at !== Infinity) pushCheck(this._state.directoryChecks, { at, roomId })
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    if (this._state.directory.get(roomId)?.incTag === incTag) this._state.directory.delete(roomId)
  }

  async directoryList(prefix: string): Promise<DirectoryPage> {
    const entries = [...this._state.directory]
      .filter(([roomId]) => roomId.startsWith(prefix))
      .sort(([left], [right]) => compareCodePoints(left, right))
      .map(([roomId, { incTag }]) => ({ roomId, incTag }))
    return { entries }
  }

  async directoryDue(): Promise<DirectoryPage['entries']> {
    const { directory, directoryChecks: checks } = this._state
    const now = this._state.now()
    // A put twice at the same time scheduled two checks, both current.
    const due = new Map<string, DirectoryCheck>()
    while (due.size < DIRECTORY_PAGE_SIZE && checks.length > 0 && checks[0]!.at <= now) {
      const check = popCheck(checks)
      if (directory.get(check.roomId)?.checkAt === check.at) due.set(check.roomId, check)
    }
    // Still due until put again or deleted.
    for (const check of due.values()) pushCheck(checks, check)
    return Array.from(due.keys(), (roomId) => ({ roomId, incTag: directory.get(roomId)!.incTag }))
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
    unrefTimer(setTimeout(() => this._releaseWhenLapsed(roomId, room), head.expiresAt - this._state.now()))
  }

  private _generation(room: RoomRecord, inc: string): Generation {
    return getOrCreate(room.gens, inc, newGeneration)
  }

  /** Lazy TTL: a lapsed tombstone reads as absent, which is what reopens an absence epoch. */
  private _liveHead(room: RoomRecord | undefined): StoredHead | null {
    const head = room?.head ?? null
    return head === null || isExpired(head, this._state.now()) ? null : head
  }
}

function removeSubscription(subs: Subscriptions, key: string, sub: MemorySubscriptionAttempt): void {
  const current = subs.get(key)
  if (current === undefined || !current.includes(sub)) return
  const rest = current.filter((other) => other !== sub)
  if (rest.length === 0) subs.delete(key)
  else subs.set(key, rest)
}

/** Redis and SQLite order room ids by their UTF-8 bytes, which is code point order. */
function compareCodePoints(left: string, right: string): number {
  for (let i = 0; i < left.length && i < right.length; ) {
    const a = left.codePointAt(i)!
    const b = right.codePointAt(i)!
    if (a !== b) return a - b
    i += a > 0xffff ? 2 : 1
  }
  return left.length - right.length
}

function pushCheck(checks: DirectoryCheck[], check: DirectoryCheck): void {
  let at = checks.push(check) - 1
  while (at > 0) {
    const parent = (at - 1) >> 1
    if (checks[parent]!.at <= check.at) break
    checks[at] = checks[parent]!
    at = parent
  }
  checks[at] = check
}

function popCheck(checks: DirectoryCheck[]): DirectoryCheck {
  const top = checks[0]!
  const last = checks.pop()!
  if (checks.length === 0) return top
  let at = 0
  for (;;) {
    let child = 2 * at + 1
    if (child >= checks.length) break
    if (child + 1 < checks.length && checks[child + 1]!.at < checks[child]!.at) child++
    if (checks[child]!.at >= last.at) break
    checks[at] = checks[child]!
    at = child
  }
  checks[at] = last
  return top
}

function unindex(index: Map<string, Set<string>>, of: string, key: string): void {
  const keys = index.get(of)
  keys?.delete(key)
  if (keys?.size === 0) index.delete(of)
}
