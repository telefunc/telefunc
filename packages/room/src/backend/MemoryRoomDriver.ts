export { MemoryRoomDriver }

// The in-process Room driver, and the reference for Room SPI semantics: its clock is authority time.

import type {
  CellCount,
  CellCounter,
  CellMutation,
  CellsExchange,
  CellsExchangeResult,
  CommitResult,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  LaneId,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
  CellSelector,
  CellsRead,
  CommitAccepted,
  CommitOptions,
  CommitTarget,
  OperationResult,
  RetainedFrame,
  RetainedOf,
  RoomOperation,
  DirectoryPage,
} from './contract.js'
import { CELL_PAGE_SIZE, DIRECTORY_PAGE_SIZE } from './contract.js'
import { encodeLaneKey } from './laneKey.js'
import { OPERATION_COMMIT_LANE, recordOperation } from './operation.js'
import {
  commitPreconditionHolds,
  countCell,
  encodeCellCounter,
  headExpectationHolds,
  isOpenIncarnation,
  materializeHead,
  nextOrdering,
  publicHead,
  type StoredHead,
} from './semantics.js'
import {
  type BackendReceiver,
  type DriverAttempt,
  type OrderingInfo,
  MemoryPubSub,
  type Subscribers,
  createDeferred,
  getOrCreate,
} from 'telefunc/__internal'
import { SortedIds, compareCodePoints } from '../utils/SortedIds.js'
import { unrefTimer } from '../utils/unrefTimer.js'

type Expiring = { expiresAt: number | null }
type StoredCell = { bytes: Uint8Array; version: string }
type RetainedEntry = { lane: LaneId; payload: Uint8Array; seq: number; timestamp: number; owner?: string }

type Incarnation = {
  /** The last cell version this incarnation issued. */
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
  subs: Subscribers
  /** Every-track subscriptions by member id, so a binary commit finds its member's without building a key. */
  everyTrackSubs: Subscribers
  /** Every-member subscriptions by track, likewise. */
  everyMemberSubs: Subscribers
}

type RoomRecord = { head: StoredHead | null; incarnations: Map<string, Incarnation> }

type DirectoryEntry = { incTag: string; checkAt: number }
/** A directory entry's check, current while its entry is still due at `at`. */
type DirectoryCheck = { at: number; roomId: string }

const NO_TARGETS = [] as const
const SEMANTIC_LANE: LaneId = { kind: 'semantic' }
const SEMANTIC_LANE_KEY = encodeLaneKey(SEMANTIC_LANE)

function copyBytes(bytes: Uint8Array): Uint8Array {
  return new Uint8Array(bytes)
}

function copyLane(lane: LaneId): LaneId {
  return { ...lane }
}

function isExpired(entry: Expiring, now: number): boolean {
  return entry.expiresAt !== null && entry.expiresAt <= now
}

function newIncarnation(): Incarnation {
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

/** The collection `prefix`'s keys are among: its longest separator prefix's index entry, or every cell. */
function cellKeyIndex(incarnation: Incarnation, prefix: string): Map<string, StoredCell> | Set<string> | undefined {
  const indexed = prefix.slice(0, prefix.lastIndexOf(':') + 1)
  return indexed === '' ? incarnation.cells : incarnation.cellsByPrefix.get(indexed)
}

function cellKeysWithPrefix(incarnation: Incarnation, prefix: string): Iterable<string> {
  const keys = cellKeyIndex(incarnation, prefix)?.keys() ?? []
  return prefix.endsWith(':') ? keys : [...keys].filter((key) => key.startsWith(prefix))
}

/** The keys under `prefix`, live, so a key added during the iteration comes, those sharing the prefix's next
 *  separator-ended segment as one run, as key order keeps them on Redis and Cloudflare. */
function* liveKeysWithPrefix(incarnation: Incarnation, prefix: string): Generator<string> {
  const runs = new Set<string>()
  for (const key of liveEntryKeys(() => cellKeyIndex(incarnation, prefix))) {
    if (!key.startsWith(prefix)) continue
    const end = key.indexOf(':', prefix.length)
    if (end === -1) {
      yield key
      continue
    }
    const run = key.slice(0, end + 1)
    if (runs.has(run)) continue
    runs.add(run)
    yield* liveEntryKeys(() => incarnation.cellsByPrefix.get(run))
  }
}

/** The keys of the index entry `entry()` returns, live; once they run out, those of an entry made since, as removing
 *  an entry's last key drops it. */
function* liveEntryKeys(entry: () => Map<string, StoredCell> | Set<string> | undefined): Generator<string> {
  let last: ReturnType<typeof entry>
  for (let at = entry(); at !== last; at = entry()) {
    last = at
    yield* at?.keys() ?? []
  }
}

/** Up to a page of `keys` not yet `seen`, which it adds to `seen`. */
function takePage(keys: Iterator<string>, seen: Set<string>): string[] {
  const page: string[] = []
  for (let next = keys.next(); !next.done; next = keys.next()) {
    if (seen.has(next.value)) continue
    seen.add(next.value)
    if (page.push(next.value) === CELL_PAGE_SIZE) break
  }
  return page
}

function cellsRead(incarnation: Incarnation, keys: Iterable<string>, now: number): CellsRead {
  const cells = new Map<string, Uint8Array>()
  const versions = new Map<string, string>()
  for (const key of keys) {
    const cell = incarnation.cells.get(key)
    if (cell === undefined) continue
    cells.set(key, copyBytes(cell.bytes))
    versions.set(key, cell.version)
  }
  return { kind: 'read', cells, versions, now }
}

function setCell(incarnation: Incarnation, key: string, bytes: Uint8Array | null, version: string): void {
  if (bytes === null) {
    if (!incarnation.cells.delete(key)) return
    for (const prefix of separatorPrefixes(key)) unindex(incarnation.cellsByPrefix, prefix, key)
    return
  }
  if (!incarnation.cells.has(key)) {
    for (const prefix of separatorPrefixes(key))
      getOrCreate(incarnation.cellsByPrefix, prefix, () => new Set()).add(key)
  }
  incarnation.cells.set(key, { bytes: copyBytes(bytes), version })
}

/** Applies the mutations and counts as one write, at the incarnation's next version; returns each counted counter. */
function applyCells(
  incarnation: Incarnation,
  mutations: readonly CellMutation[],
  counts: readonly CellCount[],
): Map<string, CellCounter> {
  const version = String(++incarnation.version)
  for (const mutation of mutations) setCell(incarnation, mutation.key, mutation.bytes, version)
  const counters = new Map<string, CellCounter>()
  for (const count of counts) {
    const counted = countCell(incarnation.cells.get(count.key)?.bytes, count, Date.now())
    if (counted === null) continue
    if (counted.written) setCell(incarnation, count.key, encodeCellCounter(counted.counter), version)
    counters.set(count.key, counted.counter)
  }
  return counters
}

function deleteRetainedEntry(incarnation: Incarnation, lane: LaneId): void {
  const key = encodeLaneKey(lane)
  incarnation.retained.delete(key)
  if (lane.kind !== 'binary') return
  unindex(incarnation.retainedOf, lane.member, key)
  unindex(incarnation.retainedOfTrack, lane.track, key)
}

/** Forgets `member`'s lanes' order, the frames its binary lanes retained, and the semantic frame while it owns it. */
function dropMemberLanes(incarnation: Incarnation, member: string): void {
  incarnation.memberOrders.delete(member)
  for (const key of [...(incarnation.retainedOf.get(member) ?? [])]) {
    deleteRetainedEntry(incarnation, incarnation.retained.get(key)!.lane)
  }
  if (incarnation.retained.get(SEMANTIC_LANE_KEY)?.owner === member) deleteRetainedEntry(incarnation, SEMANTIC_LANE)
}

function advanceOrder(order: Map<string, OrderingInfo>, domain: string, now: number): OrderingInfo {
  const mark = nextOrdering(order.get(domain), now)
  order.set(domain, mark)
  return mark
}

class MemoryRoomDriver implements RoomDriver {
  private readonly rooms = new Map<string, RoomRecord>()
  private readonly directory = new Map<string, DirectoryEntry>()
  /** The directory's room ids in code point order, so a page reads only its own ids. The first list sorts them, and
   *  each list merges in the puts and deletes since the one before: a server that never lists pays nothing per put. */
  private sortedIds: SortedIds | null = null
  /** A min-heap by `at` of the checks each put scheduled; a check its entry no longer has is dropped when reached. */
  private readonly directoryChecks: DirectoryCheck[] = []
  private revSeq = 0
  private readonly pubSub = new MemoryPubSub()

  async readHead(roomId: string): Promise<RoomHead | null> {
    const head = this.liveHead(this.rooms.get(roomId))
    return head === null ? null : publicHead(head, copyBytes(head.config))
  }

  async compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    const current = this.liveHead(this.rooms.get(roomId))
    if (!headExpectationHolds(expected, current, Date.now())) {
      const currentView = current === null ? null : publicHead(current, copyBytes(current.config))
      return { kind: 'conflict', current: currentView }
    }
    // Only an exchange that actually applies materializes a room record.
    const head = this.storeHead(this.roomFor(roomId), next)
    return { kind: 'committed', head: publicHead(head, copyBytes(head.config)) }
  }

  private storeHead(room: RoomRecord, next: HeadNext): StoredHead {
    const materialized = materializeHead(next, Date.now(), `rev-${++this.revSeq}`)
    const stored = { ...materialized, config: copyBytes(materialized.config) }
    room.head = stored
    if (stored.currentInc !== null) {
      const incarnation = this.incarnation(room, stored.currentInc)
      if (next.counts !== undefined) applyCells(incarnation, [], next.counts)
    }
    return stored
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    const incarnation = this.readableIncarnation(roomId, inc)
    if (incarnation === null) return { kind: 'stale-incarnation' }
    return cellsRead(incarnation, 'keys' in sel ? sel.keys : cellKeysWithPrefix(incarnation, sel.prefix), Date.now())
  }

  /** Skips a key it returned that was removed and added again (see `liveKeysWithPrefix`). */
  async *readCellPages(roomId: string, inc: string, prefix: string): AsyncGenerator<CellsRead> {
    let keys: Generator<string> | null = null
    const seen = new Set<string>()
    let page: string[]
    do {
      const incarnation = this.readableIncarnation(roomId, inc)
      if (incarnation === null) {
        yield { kind: 'stale-incarnation' }
        return
      }
      keys ??= liveKeysWithPrefix(incarnation, prefix)
      page = takePage(keys, seen)
      yield cellsRead(incarnation, page, Date.now())
    } while (page.length === CELL_PAGE_SIZE)
  }

  /** Closing tails may read; only writes require an open head. */
  private readableIncarnation(roomId: string, inc: string): Incarnation | null {
    const room = this.rooms.get(roomId)
    const head = this.liveHead(room)
    if (room === undefined || head === null || head.currentInc !== inc) return null
    return this.incarnation(room, inc)
  }

  async compareExchangeCells(roomId: string, inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    const room = this.rooms.get(roomId)
    const head = this.liveHead(room)
    if (room === undefined || !isOpenIncarnation(head, inc)) return { kind: 'stale-incarnation' }
    const incarnation = this.incarnation(room, inc)
    if (exchange.expect.some(({ key, version }) => (incarnation.cells.get(key)?.version ?? null) !== version))
      return { kind: 'conflict' }
    return { kind: 'committed', counters: applyCells(incarnation, exchange.mutations, exchange.counts ?? []) }
  }

  /** Synchronous from its read to its commit's seq, so nothing runs between. */
  async runOperation<Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
  ): Promise<OperationResult<Value>> {
    const room = this.rooms.get(roomId)
    if (room === undefined || !isOpenIncarnation(this.liveHead(room), inc)) return { kind: 'stale-incarnation' }
    const incarnation = this.incarnation(room, inc)
    const { value, writes } = recordOperation(operation, args, {
      now: Date.now(),
      cell: (key) => {
        const cell = incarnation.cells.get(key)
        return cell === undefined ? undefined : copyBytes(cell.bytes)
      },
    })
    for (const prefix of writes.prefixes) {
      for (const key of [...cellKeysWithPrefix(incarnation, prefix)]) setCell(incarnation, key, null, '')
    }
    const counters = applyCells(incarnation, writes.mutations, writes.counts)
    for (const member of writes.members) dropMemberLanes(incarnation, member)
    if (writes.commit === null) return { kind: 'done', value, counters }
    return {
      kind: 'done',
      value,
      counters,
      commit: await this.commitOn(incarnation, OPERATION_COMMIT_LANE, writes.commit),
    }
  }

  async commitLane(
    { roomId, inc, lane }: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const room = this.rooms.get(roomId)
    const head = this.liveHead(room)
    if (room === undefined || !commitPreconditionHolds(head, inc, lane.kind, opts?.closingLease, Date.now())) {
      return { kind: 'stale-incarnation' }
    }
    const incarnation = this.incarnation(room, inc)
    const missing = opts?.requiredCellKeys?.find((key) => !incarnation.cells.has(key))
    if (missing !== undefined) return { kind: 'stale-cell', key: missing }
    return await this.commitOn(incarnation, lane, payload, opts)
  }

  /** Assigns the commit's seq and queues its delivery in one synchronous step, so deliveries run in seq order. */
  private async commitOn(
    incarnation: Incarnation,
    lane: LaneId,
    payload: Uint8Array,
    { retain, owner }: CommitOptions = {},
  ): Promise<CommitAccepted> {
    const key = encodeLaneKey(lane)
    const frame = copyBytes(payload)
    const order =
      lane.kind === 'inbox' || lane.kind === 'binary'
        ? getOrCreate(incarnation.memberOrders, lane.member, () => new Map())
        : incarnation.order
    const mark = advanceOrder(order, key, Date.now())
    if (retain) {
      // A lane holding a retained frame is already indexed: its frame and index entries leave together.
      if (lane.kind === 'binary' && !incarnation.retained.has(key)) {
        getOrCreate(incarnation.retainedOf, lane.member, () => new Set()).add(key)
        getOrCreate(incarnation.retainedOfTrack, lane.track, () => new Set()).add(key)
      }
      const entry: RetainedEntry = { lane: copyLane(lane), payload: frame, seq: mark.seq, timestamp: mark.timestamp }
      if (owner !== undefined) entry.owner = owner
      incarnation.retained.set(key, entry)
    }
    const targets = incarnation.subs.get(key) ?? NO_TARGETS
    // A binary commit also reaches its member's every-track lane and its track's every-member lane.
    const binary = lane.kind === 'binary'
    const everyTrack = binary ? (incarnation.everyTrackSubs.get(lane.member) ?? NO_TARGETS) : NO_TARGETS
    const everyMember = binary ? (incarnation.everyMemberSubs.get(lane.track) ?? NO_TARGETS) : NO_TARGETS
    const delivered = lane.kind === 'inbox' || lane.kind === 'control' ? createDeferred() : null
    const nextTurn = this.pubSub.deliverSoon(() => {
      // A subscriber may write into the bytes it is given, which a retained frame must not share.
      const shared = retain ? copyBytes(frame) : frame
      for (const target of targets) target.deliver(shared, mark)
      for (const target of everyTrack) target.deliver(shared, mark)
      for (const target of everyMember) target.deliver(shared, mark)
      delivered?.resolve()
    })
    if (nextTurn !== null) await nextTurn
    if (delivered === null) return { kind: 'accepted', seq: mark.seq, timestamp: mark.timestamp }
    return { kind: 'accepted', seq: mark.seq, timestamp: mark.timestamp, delivery: delivered.promise }
  }

  async readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    const incarnation = this.rooms.get(roomId)?.incarnations.get(inc)
    const order =
      lane.kind === 'inbox' || lane.kind === 'binary' ? incarnation?.memberOrders.get(lane.member) : incarnation?.order
    const mark = order?.get(encodeLaneKey(lane))
    return mark === undefined ? null : { ...mark }
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    const entry = this.rooms.get(roomId)?.incarnations.get(inc)?.retained.get(encodeLaneKey(lane))
    if (entry === undefined) return null
    return { payload: copyBytes(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(roomId: string, inc: string, of?: RetainedOf): Promise<LaneId[]> {
    const incarnation = this.rooms.get(roomId)?.incarnations.get(inc)
    if (incarnation === undefined) return []
    if (of === undefined) return Array.from(incarnation.retained.values(), (entry) => copyLane(entry.lane))
    const keys = 'member' in of ? incarnation.retainedOf.get(of.member) : incarnation.retainedOfTrack.get(of.track)
    return Array.from(keys ?? [], (key) => copyLane(incarnation.retained.get(key)!.lane))
  }

  subscribe({ roomId, inc, lane }: RoomSubscriptionSource, receiver: BackendReceiver): DriverAttempt {
    const room = this.rooms.get(roomId)
    if (room === undefined || !isOpenIncarnation(this.liveHead(room), inc)) {
      throw new Error(`subscribeLane: room '${roomId}' has no open incarnation '${inc}'`)
    }
    // Registration is durable before `ready` resolves: a commit accepted after this point must see it.
    const incarnation = this.incarnation(room, inc)
    let subscribers: Subscribers
    let key: string
    if (lane.kind === 'binary-all') {
      subscribers = incarnation.everyTrackSubs
      key = lane.member
    } else if (lane.kind === 'binary-track') {
      subscribers = incarnation.everyMemberSubs
      key = lane.track
    } else {
      subscribers = incarnation.subs
      key = encodeLaneKey(lane)
    }
    // Ready once what was committed before it is delivered: a lane it replaces delivers those first.
    return this.pubSub.subscribe({ subscribers, key, receiver })
  }

  async dropIncarnation(roomId: string, inc: string): Promise<void> {
    const room = this.rooms.get(roomId)
    if (room === undefined) return
    const incarnation = room.incarnations.get(inc)
    if (incarnation === undefined) return // already dropped (the janitor is resumable)
    room.incarnations.delete(inc)
    this.releaseWhenLapsed(roomId, room)
  }

  async directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    const at = Math.max(checkAt, Date.now() + minDelayMs)
    if (!this.directory.has(roomId)) this.sortedIds?.add(roomId)
    this.directory.set(roomId, { incTag, checkAt: at })
    if (at !== Infinity) pushCheck(this.directoryChecks, { at, roomId })
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    if (this.directory.get(roomId)?.incTag !== incTag) return
    this.directory.delete(roomId)
    this.sortedIds?.delete(roomId)
  }

  async directoryGet(roomId: string): Promise<string | undefined> {
    return this.directory.get(roomId)?.incTag
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    // The ids under `prefix` sit together in code point order; a page starts past the cursor.
    const from = cursor !== undefined && compareCodePoints(cursor, prefix) > 0 ? cursor : prefix
    const entries: DirectoryPage['entries'] = []
    this.sortedIds ??= new SortedIds(this.directory.keys())
    for (const roomId of this.sortedIds.from(from)) {
      if (!roomId.startsWith(prefix)) break
      if (roomId === cursor) continue
      if (entries.length === DIRECTORY_PAGE_SIZE) return { entries, cursor: entries.at(-1)!.roomId }
      entries.push({ roomId, incTag: this.directory.get(roomId)!.incTag })
    }
    return { entries }
  }

  async directoryDue(): Promise<DirectoryPage['entries']> {
    const now = Date.now()
    // A put twice at the same time scheduled two checks, both current.
    const due = new Map<string, DirectoryCheck>()
    while (due.size < DIRECTORY_PAGE_SIZE && this.directoryChecks.length > 0 && this.directoryChecks[0]!.at <= now) {
      const check = popCheck(this.directoryChecks)
      if (this.directory.get(check.roomId)?.checkAt === check.at) due.set(check.roomId, check)
    }
    // Still due until put again or deleted.
    for (const check of due.values()) pushCheck(this.directoryChecks, check)
    return Array.from(due.keys(), (roomId) => ({ roomId, incTag: this.directory.get(roomId)!.incTag }))
  }

  // ── internals ──

  private roomFor(roomId: string): RoomRecord {
    return getOrCreate(this.rooms, roomId, () => ({ head: null, incarnations: new Map() }))
  }

  /** A room with no incarnation left is forgotten once its tombstone lapses (this backend's revs are never reused). */
  private releaseWhenLapsed(roomId: string, room: RoomRecord): void {
    if (this.rooms.get(roomId) !== room || room.incarnations.size > 0) return
    const head = this.liveHead(room)
    if (head === null) {
      this.rooms.delete(roomId)
      return
    }
    if (head.expiresAt === null) return
    unrefTimer(setTimeout(() => this.releaseWhenLapsed(roomId, room), head.expiresAt - Date.now()))
  }

  private incarnation(room: RoomRecord, inc: string): Incarnation {
    return getOrCreate(room.incarnations, inc, newIncarnation)
  }

  /** Lazy TTL: a lapsed tombstone reads as absent, which is what reopens an absence epoch. */
  private liveHead(room: RoomRecord | undefined): StoredHead | null {
    const head = room?.head ?? null
    return head === null || isExpired(head, Date.now()) ? null : head
  }
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
