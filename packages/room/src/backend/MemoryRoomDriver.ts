export { MemoryRoomDriver }

// The in-process Room driver, and the reference for Room SPI semantics: its clock is authority time.

import type {
  CellCount,
  CellCounter,
  CellMutation,
  ExpiringCell,
  ExpiringCellsRead,
  CellsExchange,
  CellsExchangeResult,
  CommitResult,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  CommitKept,
  LaneId,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
  CellSelector,
  CellsBeside,
  CellsRead,
  CommitAccepted,
  CommitOptions,
  CommitTarget,
  OperationOptions,
  OperationResult,
  RetainedFrame,
  RetainedOf,
  RoomOperation,
  DirectoryPage,
} from './contract.js'
import { CELL_PAGE_SIZE, DIRECTORY_PAGE_SIZE } from './contract.js'
import { encodeLaneKey } from './laneKey.js'
import { OPERATION_COMMIT_LANE, beforeOperation, recordOperation } from './operation.js'
import {
  commitPreconditionHolds,
  countCell,
  encodeCellCounter,
  hasLapsed,
  headExpectationHolds,
  isOpenIncarnation,
  materializeHead,
  nextOrdering,
  publicHead,
  type StoredHead,
  unheldRequiredCell,
} from './semantics.js'
import {
  type BackendReceiver,
  DriverAttempt,
  type OrderingInfo,
  type Subscribers,
  createDeferred,
  deliverTo,
  getOrCreate,
  unrefTimer,
} from 'telefunc/__internal'
import { SortedIds, compareCodePoints } from '../utils/SortedIds.js'
import { TIMER_DELAY_MAX_MS } from '../shared/constants.js'
import { MemoryCommitQueue, type MemoryAttempt } from './MemoryCommitQueue.js'

type Expiring = { expiresAt: number | null }
/** `lapsesAt`: the authority time the cell lapses at, for one written with `lapsesInMs`. */
type StoredCell = { bytes: Uint8Array; version: string; lapsesAt?: number }
type RetainedEntry = { lane: LaneId; payload: Uint8Array; seq: number; timestamp: number; owner?: string }

type Incarnation = {
  /** The last cell version this incarnation issued. */
  version: number
  cells: Map<string, StoredCell>
  /** Each expiring cell, with when it expires, which a read checks, and the timer that deletes it. */
  expiring: Map<string, { bytes: Uint8Array; expiresAt: number; timer: ReturnType<typeof setTimeout> }>
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
  /** Its lanes' subscriptions, which end with it. */
  attempts: Set<MemoryAttempt>
}

/** A room's one lapse timer, and when it fires: at its head's lapse, or a timer's range before it. */
type LapseTimer = { firesAt: number; timer: ReturnType<typeof setTimeout> }

/** When room `roomId` is due under `incTag`, and where it is in the due heap (-1: not in it). */
type DueCheck = { at: number; roomId: string; incTag: string; place: number }

type RoomRecord = {
  head: StoredHead | null
  incarnations: Map<string, Incarnation>
  lapse: LapseTimer | null
  /** Its due checks by tag: the tag its head names, or, while it names none, those of the creates waiting for it. */
  due: Map<string, DueCheck>
}

/** About 10 ms of commits. */
const COMMITS_BETWEEN_TURNS = 1024
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
    expiring: new Map(),
    cellsByPrefix: new Map(),
    order: new Map(),
    memberOrders: new Map(),
    retained: new Map(),
    retainedOf: new Map(),
    retainedOfTrack: new Map(),
    subs: new Map(),
    everyTrackSubs: new Map(),
    everyMemberSubs: new Map(),
    attempts: new Set(),
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

const referenceDecoder = new TextDecoder()

function cellsRead(incarnation: Incarnation, keys: Iterable<string>, now: number, beside?: CellsBeside): CellsRead {
  const cells = new Map<string, Uint8Array>()
  const versions = new Map<string, string>()
  const lapsed = new Set<string>()
  const besides = beside === undefined ? undefined : new Map<string, Uint8Array | null>()
  for (const key of keys) {
    const cell = incarnation.cells.get(key)
    if (cell === undefined) continue
    cells.set(key, copyBytes(cell.bytes))
    versions.set(key, cell.version)
    if (hasLapsed(cell.lapsesAt, now)) lapsed.add(key)
    if (beside === undefined || besides === undefined || !key.startsWith(beside.of)) continue
    const reference = incarnation.cells.get(beside.reference + key.slice(beside.of.length))
    if (reference === undefined) continue
    const target = incarnation.cells.get(beside.target + referenceDecoder.decode(reference.bytes))
    const live = target !== undefined && !hasLapsed(target.lapsesAt, now)
    besides.set(key, live ? copyBytes(target.bytes) : null)
  }
  return besides === undefined
    ? { kind: 'read', cells, versions, now, lapsed }
    : { kind: 'read', cells, versions, now, lapsed, beside: besides }
}

function setCell(
  incarnation: Incarnation,
  key: string,
  bytes: Uint8Array | null,
  version: string,
  lapsesAt?: number,
): void {
  if (bytes === null) {
    if (!incarnation.cells.delete(key)) return
    for (const prefix of separatorPrefixes(key)) unindex(incarnation.cellsByPrefix, prefix, key)
    return
  }
  if (!incarnation.cells.has(key)) {
    for (const prefix of separatorPrefixes(key))
      getOrCreate(incarnation.cellsByPrefix, prefix, () => new Set()).add(key)
  }
  const cell: StoredCell = { bytes: copyBytes(bytes), version }
  if (lapsesAt !== undefined) cell.lapsesAt = lapsesAt
  incarnation.cells.set(key, cell)
}

/** Writes an expiring cell, replacing one at its key, and the timer that deletes it, which may run late. */
function setExpiring(incarnation: Incarnation, { key, bytes, ttlMs }: ExpiringCell): void {
  clearTimeout(incarnation.expiring.get(key)?.timer)
  const timer = unrefTimer(
    setTimeout(() => {
      if (incarnation.expiring.get(key)?.timer === timer) incarnation.expiring.delete(key)
    }, ttlMs),
  )
  incarnation.expiring.set(key, { bytes: copyBytes(bytes), expiresAt: Date.now() + ttlMs, timer })
}

/** Applies the mutations and counts as one write, at the incarnation's next version; returns each counted counter. */
function applyCells(
  incarnation: Incarnation,
  mutations: readonly CellMutation[],
  counts: readonly CellCount[],
): Map<string, CellCounter> {
  // A revision only for what is written: an operation that writes nothing writes no revision, as on Redis.
  let version: string | undefined
  const written = () => (version ??= String(++incarnation.version))
  const now = Date.now()
  for (const { key, bytes, lapsesInMs } of mutations) {
    setCell(incarnation, key, bytes, written(), lapsesInMs === undefined ? undefined : now + lapsesInMs)
  }
  const counters = new Map<string, CellCounter>()
  for (const count of counts) {
    const counted = countCell(incarnation.cells.get(count.key)?.bytes, count, now)
    if (counted === null) continue
    if (counted.written) setCell(incarnation, count.key, encodeCellCounter(counted.counter), written())
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

/** A lane of an incarnation that isn't open: over at once and on purpose, as a dropped incarnation's is on every backend. */
class EndedAttempt extends DriverAttempt {
  constructor() {
    super()
    this.transition('closed')
  }

  async unsubscribe(): Promise<void> {}
}

class MemoryRoomDriver implements RoomDriver {
  private readonly rooms = new Map<string, RoomRecord>()
  /** The ids of the rooms whose head names an incarnation, which are the listing, in code point order, so a page reads
   *  only its own ids. The first list sorts them, and each list merges in the head turns since the one before: a
   *  server that never lists pays nothing per head turn. */
  private sortedIds: SortedIds | null = null
  private readonly dueChecks = new DueChecks()
  private readonly commits = new MemoryCommitQueue({ commitsBetweenTurns: COMMITS_BETWEEN_TURNS })

  async readHead(roomId: string): Promise<RoomHead | null> {
    const head = this.liveHead(roomId, this.rooms.get(roomId))
    return head === null ? null : publicHead(head, copyBytes(head.config))
  }

  async compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    const current = this.liveHead(roomId, this.rooms.get(roomId))
    if (!headExpectationHolds(expected, current, Date.now())) {
      const currentView = current === null ? null : publicHead(current, copyBytes(current.config))
      return { kind: 'conflict', current: currentView }
    }
    // Only an exchange that actually applies materializes a room record.
    const head = this.storeHead(roomId, this.roomFor(roomId), next)
    return { kind: 'committed', head: publicHead(head, copyBytes(head.config)) }
  }

  /** The head's turn: an incarnation it no longer names ends in it, whatever call about it comes later. */
  private storeHead(roomId: string, room: RoomRecord, next: HeadNext): StoredHead {
    const materialized = materializeHead(next, Date.now())
    const stored = { ...materialized, config: copyBytes(materialized.config) }
    const named = room.head?.currentInc ?? null
    room.head = stored
    this.followHead(roomId, room, named)
    for (const inc of [...room.incarnations.keys()]) if (inc !== stored.currentInc) this.endIncarnation(room, inc)
    if (stored.currentInc !== null) {
      const incarnation = this.incarnation(room, stored.currentInc)
      if (next.counts !== undefined) applyCells(incarnation, [], next.counts)
    }
    this.scheduleLapse(roomId, room)
    return stored
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    const incarnation = this.readableIncarnation(roomId, inc)
    if (incarnation === null) return { kind: 'stale-incarnation' }
    const keys = 'keys' in sel ? sel.keys : cellKeysWithPrefix(incarnation, sel.prefix)
    return cellsRead(incarnation, keys, Date.now(), sel.beside)
  }

  /** Skips a key it returned that was removed and added again (see `liveKeysWithPrefix`). */
  async *readCellPages(roomId: string, inc: string, prefix: string, beside?: CellsBeside): AsyncGenerator<CellsRead> {
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
      yield cellsRead(incarnation, page, Date.now(), beside)
    } while (page.length === CELL_PAGE_SIZE)
  }

  /** Closing tails may read; only writes require an open head. */
  private readableIncarnation(roomId: string, inc: string): Incarnation | null {
    const room = this.rooms.get(roomId)
    const head = this.liveHead(roomId, room)
    if (room === undefined || head === null || head.currentInc !== inc) return null
    return this.incarnation(room, inc)
  }

  async readExpiringCells(roomId: string, inc: string, keys: readonly string[]): Promise<ExpiringCellsRead> {
    const incarnation = this.readableIncarnation(roomId, inc)
    if (incarnation === null) return { kind: 'stale-incarnation' }
    const cells = new Map<string, Uint8Array>()
    const now = Date.now()
    for (const key of keys) {
      const cell = incarnation.expiring.get(key)
      if (cell !== undefined && cell.expiresAt > now) cells.set(key, copyBytes(cell.bytes))
    }
    return { kind: 'read', cells }
  }

  async compareExchangeCells(roomId: string, inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    const room = this.rooms.get(roomId)
    const head = this.liveHead(roomId, room)
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
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    await beforeOperation(options)
    const room = this.rooms.get(roomId)
    if (room === undefined || !isOpenIncarnation(this.liveHead(roomId, room), inc)) return { kind: 'stale-incarnation' }
    const incarnation = this.incarnation(room, inc)
    const now = Date.now()
    const { value, writes } = recordOperation(operation, args, {
      now,
      cell: (key) => {
        const cell = incarnation.cells.get(key)
        return cell === undefined ? undefined : copyBytes(cell.bytes)
      },
      lapsed: (key) => {
        const cell = incarnation.cells.get(key)
        return cell !== undefined && hasLapsed(cell.lapsesAt, now)
      },
    })
    for (const prefix of writes.prefixes) {
      for (const key of [...cellKeysWithPrefix(incarnation, prefix)]) setCell(incarnation, key, null, '')
    }
    const counters = applyCells(incarnation, writes.mutations, writes.counts)
    for (const cell of writes.expiring) setExpiring(incarnation, cell)
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
    const head = this.liveHead(roomId, room)
    const now = Date.now()
    if (room === undefined || !commitPreconditionHolds(head, inc, lane.kind, opts?.closingLease, now)) {
      return { kind: 'stale-incarnation' }
    }
    const incarnation = this.incarnation(room, inc)
    const unheld =
      opts?.required &&
      unheldRequiredCell(opts.required, (key) => {
        const cell = incarnation.cells.get(key)
        return cell && { bytes: cell.bytes, lapsed: hasLapsed(cell.lapsesAt, now) }
      })
    if (unheld !== undefined) return { kind: 'stale-cell', key: unheld }
    return await this.commitOn(incarnation, lane, payload, opts)
  }

  /** The memory store has no size rule: it keeps a payload at any size, delivered or stored. */
  assertCommittable(_lane: LaneId, _payload: Uint8Array, _kept: CommitKept): void {}

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
    const nextTurn = this.commits.deliverSoon(() => {
      // A subscriber may write into the bytes it is given, which a retained frame must not share.
      const shared = retain ? copyBytes(frame) : frame
      deliverTo(targets, shared, mark)
      deliverTo(everyTrack, shared, mark)
      deliverTo(everyMember, shared, mark)
      delivered?.resolve()
    })
    if (nextTurn !== null) await nextTurn
    if (delivered === null) return { kind: 'accepted', seq: mark.seq, timestamp: mark.timestamp }
    return { kind: 'accepted', seq: mark.seq, timestamp: mark.timestamp, delivery: delivered.promise }
  }

  async readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    const incarnation = this.storedIncarnation(roomId, inc)
    const order =
      lane.kind === 'inbox' || lane.kind === 'binary' ? incarnation?.memberOrders.get(lane.member) : incarnation?.order
    const mark = order?.get(encodeLaneKey(lane))
    return mark === undefined ? null : { ...mark }
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    const entry = this.storedIncarnation(roomId, inc)?.retained.get(encodeLaneKey(lane))
    if (entry === undefined) return null
    return { payload: copyBytes(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(roomId: string, inc: string, of?: RetainedOf): Promise<LaneId[]> {
    const incarnation = this.storedIncarnation(roomId, inc)
    if (incarnation === undefined) return []
    if (of === undefined) return Array.from(incarnation.retained.values(), (entry) => copyLane(entry.lane))
    const keys = 'member' in of ? incarnation.retainedOf.get(of.member) : incarnation.retainedOfTrack.get(of.track)
    return Array.from(keys ?? [], (key) => copyLane(incarnation.retained.get(key)!.lane))
  }

  subscribe({ roomId, inc, lane }: RoomSubscriptionSource, receiver: BackendReceiver): DriverAttempt {
    const room = this.rooms.get(roomId)
    if (room === undefined || !isOpenIncarnation(this.liveHead(roomId, room), inc)) return new EndedAttempt()
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
    return this.commits.subscribe(subscribers, key, receiver, incarnation.attempts)
  }

  /** Drops nothing a live head names: that incarnation ends in the turn its head stops naming it. */
  async dropIncarnation(roomId: string, inc: string): Promise<void> {
    const room = this.rooms.get(roomId)
    if (room === undefined || this.liveHead(roomId, room)?.currentInc === inc) return
    this.endIncarnation(room, inc)
  }

  /** The heads are the listing, so this only schedules: the check of the tag the head names, or, while it names
   *  none, of a create's tag, which the next head turn keeps if it names it. */
  async directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    const at = Math.max(checkAt, Date.now() + minDelayMs)
    const named = this.liveHead(roomId, this.rooms.get(roomId))?.currentInc ?? null
    if (named !== null && named !== incTag) return
    const room = at === Infinity ? this.rooms.get(roomId) : this.roomFor(roomId)
    if (room === undefined) return
    if (at === Infinity) this.unschedule(roomId, room, incTag)
    else this.schedule(room, roomId, incTag, at)
  }

  /** Only a check no live head names: a repair acting on an earlier read can't unschedule what a head took since. */
  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    const room = this.rooms.get(roomId)
    if (room === undefined || this.liveHead(roomId, room)?.currentInc === incTag) return
    this.unschedule(roomId, room, incTag)
  }

  async directoryGet(roomId: string): Promise<string | undefined> {
    return this.liveHead(roomId, this.rooms.get(roomId))?.currentInc ?? undefined
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    // The ids under `prefix` sit together in code point order; a page starts past the cursor.
    const from = cursor !== undefined && compareCodePoints(cursor, prefix) > 0 ? cursor : prefix
    const entries: DirectoryPage['entries'] = []
    const now = Date.now()
    this.sortedIds ??= new SortedIds(this.listedIds())
    for (const roomId of this.sortedIds.from(from)) {
      if (!roomId.startsWith(prefix)) break
      if (roomId === cursor) continue
      // A lapsed head goes here, before it counts toward the page.
      const incTag = this.liveHead(roomId, this.rooms.get(roomId), now)?.currentInc ?? null
      if (incTag === null) continue
      if (entries.length === DIRECTORY_PAGE_SIZE) return { entries, cursor: entries.at(-1)!.roomId }
      entries.push({ roomId, incTag })
    }
    return { entries }
  }

  /** Still due until put again, or retired with the tag its head stops naming. */
  async directoryDue(): Promise<DirectoryPage['entries']> {
    return this.dueChecks.due(Date.now(), DIRECTORY_PAGE_SIZE).map(({ roomId, incTag }) => ({ roomId, incTag }))
  }

  // ── internals ──

  private roomFor(roomId: string): RoomRecord {
    return getOrCreate(this.rooms, roomId, () => ({ head: null, incarnations: new Map(), lapse: null, due: new Map() }))
  }

  private *listedIds(): Generator<string> {
    for (const [roomId, room] of this.rooms) if ((room.head?.currentInc ?? null) !== null) yield roomId
  }

  /** The listing and the due checks follow the head, in its turn. It is listed while it names an incarnation, whose
   *  check stays while every other goes; the turn that first names it schedules its first check from the head's own
   *  lapse, or the create's put if that is sooner. */
  private followHead(roomId: string, room: RoomRecord, before: string | null): void {
    const named = room.head?.currentInc ?? null
    if (before === null && named !== null) this.sortedIds?.add(roomId)
    if (before !== null && named === null) this.sortedIds?.delete(roomId)
    for (const tag of [...room.due.keys()]) if (tag !== named) this.unschedule(roomId, room, tag)
    const lapse = room.head?.expiresAt ?? null
    if (named === null || named === before || lapse === null) return
    this.schedule(room, roomId, named, Math.min(room.due.get(named)?.at ?? Infinity, lapse))
  }

  private schedule(room: RoomRecord, roomId: string, incTag: string, at: number): void {
    this.dueChecks.set(
      getOrCreate(room.due, incTag, () => ({ at, roomId, incTag, place: -1 })),
      at,
    )
  }

  /** A record left with nothing goes too: a create's check that no head took. */
  private unschedule(roomId: string, room: RoomRecord, incTag: string): void {
    const check = room.due.get(incTag)
    if (check === undefined) return
    room.due.delete(incTag)
    this.dueChecks.remove(check)
    const empty = room.head === null && room.due.size === 0 && room.incarnations.size === 0
    if (empty && this.rooms.get(roomId) === room) this.rooms.delete(roomId)
  }

  /** What incarnation `inc` stored, as a read sees it: none once its room's head lapsed. Closing tails and history
   *  reads may read an incarnation the head no longer names. */
  private storedIncarnation(roomId: string, inc: string): Incarnation | undefined {
    const room = this.rooms.get(roomId)
    this.liveHead(roomId, room)
    return room?.incarnations.get(inc)
  }

  /** One timer per room, firing at its head's lapse: armed when a lapse appears or moves earlier than it fires,
   *  cleared when the lapse goes. A lapse pushed out keeps it; it re-arms when it fires. */
  private scheduleLapse(roomId: string, room: RoomRecord): void {
    const at = room.head?.expiresAt ?? null
    if (at !== null && room.lapse !== null && room.lapse.firesAt <= at) return
    if (room.lapse !== null) clearTimeout(room.lapse.timer)
    room.lapse = null
    if (at !== null) this.armLapse(roomId, room, at)
  }

  /** At most a timer's range ahead, so a lapse past it fires early and re-arms for the rest. */
  private armLapse(roomId: string, room: RoomRecord, at: number): void {
    const delay = Math.min(Math.max(at - Date.now(), 0), TIMER_DELAY_MAX_MS)
    const lapse: LapseTimer = {
      firesAt: Date.now() + delay,
      timer: unrefTimer(setTimeout(() => this.lapseFired(roomId, room, lapse), delay)),
    }
    room.lapse = lapse
  }

  /** Only the room's armed timer acts, and only on the head's lapse as the store's clock has it now. */
  private lapseFired(roomId: string, room: RoomRecord, lapse: LapseTimer): void {
    if (room.lapse !== lapse || this.rooms.get(roomId) !== room) return
    room.lapse = null
    const at = room.head?.expiresAt ?? null
    if (at === null) return
    if (at > Date.now()) return this.armLapse(roomId, room, at)
    this.collect(roomId, room)
  }

  /** The room's head lapsed: the head goes with every incarnation it held, in this turn, for good. */
  private collect(roomId: string, room: RoomRecord): void {
    if ((room.head?.currentInc ?? null) !== null) this.sortedIds?.delete(roomId)
    for (const tag of [...room.due.keys()]) this.unschedule(roomId, room, tag)
    for (const inc of [...room.incarnations.keys()]) this.endIncarnation(room, inc)
    if (room.lapse !== null) clearTimeout(room.lapse.timer)
    room.lapse = null
    room.head = null
    if (this.rooms.get(roomId) === room) this.rooms.delete(roomId)
  }

  /** Its data goes now, and its lanes end once what was committed on them is delivered. */
  private endIncarnation(room: RoomRecord, inc: string): void {
    const incarnation = room.incarnations.get(inc)
    if (incarnation === undefined) return // already ended (the janitor is resumable)
    for (const { timer } of incarnation.expiring.values()) clearTimeout(timer)
    room.incarnations.delete(inc)
    this.commits.endAfterDeliveries(incarnation.attempts)
  }

  private incarnation(room: RoomRecord, inc: string): Incarnation {
    return getOrCreate(room.incarnations, inc, newIncarnation)
  }

  /** A head the store's clock finds lapsed is collected here, before its timer, and reads as absent from then on,
   *  which is what reopens an absence epoch. */
  private liveHead(roomId: string, room: RoomRecord | undefined, now?: number): StoredHead | null {
    const head = room?.head ?? null
    if (room === undefined || head === null || !isExpired(head, now ?? Date.now())) return head
    this.collect(roomId, room)
    return null
  }
}

/** The due checks in a min-heap by `at`. Each knows its place, so a put moves it and a retire removes it: every node
 *  is a live check. */
class DueChecks {
  private readonly heap: DueCheck[] = []

  get size(): number {
    return this.heap.length
  }

  /** Adds `check` at `at`, or moves it there. */
  set(check: DueCheck, at: number): void {
    check.at = at
    if (check.place === -1) this.place(check, this.heap.length)
    this.up(check.place)
    this.down(check.place)
  }

  remove(check: DueCheck): void {
    const at = check.place
    if (at === -1) return
    check.place = -1
    const last = this.heap.pop()
    if (last === undefined || last === check) return
    this.place(last, at)
    this.up(at)
    this.down(at)
  }

  /** Up to `limit` checks due at `now`, earliest first, which stay due. */
  due(now: number, limit: number): DueCheck[] {
    const taken: DueCheck[] = []
    for (let top = this.heap[0]; top !== undefined && top.at <= now && taken.length < limit; top = this.heap[0]) {
      this.remove(top)
      taken.push(top)
    }
    for (const check of taken) this.set(check, check.at)
    return taken
  }

  private place(check: DueCheck, at: number): void {
    this.heap[at] = check
    check.place = at
  }

  private up(from: number): void {
    const check = this.heap[from]
    if (check === undefined) return
    let at = from
    while (at > 0) {
      const parentAt = (at - 1) >> 1
      const parent = this.heap[parentAt]
      if (parent === undefined || parent.at <= check.at) break
      this.place(parent, at)
      at = parentAt
    }
    this.place(check, at)
  }

  private down(from: number): void {
    const check = this.heap[from]
    if (check === undefined) return
    let at = from
    for (;;) {
      const leftAt = 2 * at + 1
      const left = this.heap[leftAt]
      if (left === undefined) break
      const right = this.heap[leftAt + 1]
      const [child, childAt] = right !== undefined && right.at < left.at ? [right, leftAt + 1] : [left, leftAt]
      if (child.at >= check.at) break
      this.place(child, at)
      at = childAt
    }
    this.place(check, at)
  }
}

function unindex(index: Map<string, Set<string>>, of: string, key: string): void {
  const keys = index.get(of)
  keys?.delete(key)
  if (keys?.size === 0) index.delete(of)
}
