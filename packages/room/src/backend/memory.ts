export { MemoryRoomDriver }

// The in-process Room driver, and the reference for Room SPI semantics: its clock is authority time.

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
  CommitTarget,
  RetainedFrame,
  RetainedOf,
  DirectoryPage,
} from './contract.js'
import { CELL_PAGE_SIZE, DIRECTORY_PAGE_SIZE } from './contract.js'
import { encodeLaneKey } from './laneKey.js'
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
} from './semantics.js'
import {
  type BackendPayload,
  type BackendReceiver,
  type OrderingInfo,
  type SubscriptionAttempt,
  type SubscriptionDriver,
  MemoryPubSub,
  type Subscriptions,
} from 'telefunc/__internal'
import { createDeferred } from '../utils/createDeferred.js'
import { getOrCreate } from '../utils/getOrCreate.js'
import { SortedIds, compareCodePoints } from '../utils/SortedIds.js'
import { unrefTimer } from '../utils/unrefTimer.js'

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

const NO_TARGETS = [] as const

function copyBytes(bytes: Uint8Array): Uint8Array {
  return new Uint8Array(bytes)
}

function copyLane(lane: LaneId): LaneId {
  return { ...lane }
}

function isExpired(entry: Expiring, now: number): boolean {
  return entry.expiresAt !== null && entry.expiresAt <= now
}

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

/** The collection `prefix`'s keys are among: its longest separator prefix's index entry, or every cell. */
function cellKeyIndex(gen: Generation, prefix: string): Map<string, StoredCell> | Set<string> | undefined {
  const indexed = prefix.slice(0, prefix.lastIndexOf(':') + 1)
  return indexed === '' ? gen.cells : gen.cellsByPrefix.get(indexed)
}

function cellKeysWithPrefix(gen: Generation, prefix: string): Iterable<string> {
  const keys = cellKeyIndex(gen, prefix)?.keys() ?? []
  return prefix.endsWith(':') ? keys : [...keys].filter((key) => key.startsWith(prefix))
}

/** The keys under `prefix`, live, so a key added during the iteration comes, those sharing the prefix's next
 *  separator-ended segment as one run, as key order keeps them on Redis and Cloudflare. */
function* liveKeysWithPrefix(gen: Generation, prefix: string): Generator<string> {
  const runs = new Set<string>()
  for (const key of liveEntryKeys(() => cellKeyIndex(gen, prefix))) {
    if (!key.startsWith(prefix)) continue
    const end = key.indexOf(':', prefix.length)
    if (end === -1) {
      yield key
      continue
    }
    const run = key.slice(0, end + 1)
    if (runs.has(run)) continue
    runs.add(run)
    yield* liveEntryKeys(() => gen.cellsByPrefix.get(run))
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

class MemoryRoomDriver implements RoomDriver {
  readonly subscriptions: SubscriptionDriver<RoomSubscriptionSource>

  private readonly rooms = new Map<string, RoomRecord>()
  private readonly directory = new Map<string, DirectoryEntry>()
  /** The directory's room ids in code point order, so a page reads only its own ids. The first list sorts them, and puts
   *  and deletes keep them in order from then on: a server that never lists pays nothing per put. */
  private sortedIds: SortedIds | null = null
  /** A min-heap by `at` of the checks each put scheduled; a check its entry no longer has is dropped when reached. */
  private readonly directoryChecks: DirectoryCheck[] = []
  private revSeq = 0
  private readonly pubSub = new MemoryPubSub()

  constructor() {
    this.subscriptions = {
      bind: (source) => ({
        partition: '',
        open: (receiver, localReceiverCount) => this.openSubscription(source, receiver, localReceiverCount),
      }),
      partitionHere: () => '',
    }
  }

  async readHead(roomId: string): Promise<RoomHead | null> {
    const head = this.liveHead(this.rooms.get(roomId))
    return head === null ? null : publicHead(head, copyBytes(head.config))
  }

  async compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    const current = this.liveHead(this.rooms.get(roomId))
    if (!headCxMatches(cx, current, Date.now())) {
      return { conflict: true, current: current === null ? null : publicHead(current, copyBytes(current.config)) }
    }
    // Only a CX that actually applies materializes a room record.
    const head = this.storeHead(this.roomFor(roomId), next)
    return { head: publicHead(head, copyBytes(head.config)) }
  }

  private storeHead(room: RoomRecord, next: HeadNext): StoredHead {
    const materialized = materializeHead(next, Date.now(), `rev-${++this.revSeq}`)
    const stored = { ...materialized, config: copyBytes(materialized.config) }
    room.head = stored
    if (stored.currentInc !== null) this.generation(room, stored.currentInc)
    return stored
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    const gen = this.readableGeneration(roomId, inc)
    if (gen === null) return { staleInc: true }
    return readCellsOf(gen, 'keys' in sel ? sel.keys : cellKeysWithPrefix(gen, sel.prefix), Date.now())
  }

  /** Skips a key it returned that was removed and added again (see `liveKeysWithPrefix`). */
  async *readCellPages(roomId: string, inc: string, prefix: string): AsyncGenerator<CellsRead> {
    let keys: Generator<string> | null = null
    const seen = new Set<string>()
    let page: string[]
    do {
      const gen = this.readableGeneration(roomId, inc)
      if (gen === null) {
        yield { staleInc: true }
        return
      }
      keys ??= liveKeysWithPrefix(gen, prefix)
      page = takePage(keys, seen)
      yield readCellsOf(gen, page, Date.now())
    } while (page.length === CELL_PAGE_SIZE)
  }

  /** Closing tails may read; only writes require an open head. */
  private readableGeneration(roomId: string, inc: string): Generation | null {
    const room = this.rooms.get(roomId)
    const head = this.liveHead(room)
    if (room === undefined || head === null || head.currentInc !== inc) return null
    return this.generation(room, inc)
  }

  async compareExchangeCells(roomId: string, inc: string, cx: CellsCx): Promise<CxResult> {
    const room = this.rooms.get(roomId)
    const head = this.liveHead(room)
    if (room === undefined || !isOpenIncarnation(head, inc)) return 'stale-inc'
    const gen = this.generation(room, inc)
    if (cx.expect.some(({ key, version }) => (gen.cells.get(key)?.version ?? null) !== version)) return 'conflict'
    const version = String(++gen.version)
    for (const mutation of cx.mutations) setCell(gen, mutation.key, mutation.bytes, version)
    const counters = new Map<string, CellCounter>()
    for (const count of cx.counts ?? []) {
      const counted = countCell(gen.cells.get(count.key)?.bytes, count, Date.now())
      if (counted === null) continue
      if (counted.written) setCell(gen, count.key, encodeCellCounter(counted.counter), version)
      counters.set(count.key, counted.counter)
    }
    return { committed: counters }
  }

  async commitLane(
    { roomId, inc, lane }: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const room = this.rooms.get(roomId)
    const head = this.liveHead(room)
    if (room === undefined || !commitPreconditionHolds(head, inc, lane.kind, opts?.closingLease, Date.now())) {
      return { stale: 'incarnation' }
    }
    const gen = this.generation(room, inc)
    const missing = opts?.requiredCellKeys?.find((key) => !gen.cells.has(key))
    if (missing !== undefined) return { stale: 'cell', key: missing }
    const key = encodeLaneKey(lane)
    const frame = copyBytes(payload)
    const order =
      lane.kind === 'inbox' || lane.kind === 'binary'
        ? getOrCreate(gen.memberOrders, lane.member, () => new Map())
        : gen.order
    const mark = advanceOrder(order, key, Date.now())
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
    const nextTurn = this.pubSub.deliverSoon(() => {
      // A subscriber may write into the bytes it is given, which a retained frame must not share.
      const shared = opts?.retain ? copyBytes(frame) : frame
      for (const target of targets) target.deliver(shared, mark)
      for (const target of everyTrack) target.deliver(shared, mark)
      for (const target of everyMember) target.deliver(shared, mark)
      delivered?.resolve()
    })
    const receivers = this.pubSub.receivers(targets, everyTrack, everyMember)
    if (nextTurn !== null) await nextTurn
    if (delivered === null) return { accepted: true, seq: mark.seq, timestamp: mark.timestamp, receivers }
    return { accepted: true, seq: mark.seq, timestamp: mark.timestamp, receivers, delivery: delivered.promise }
  }

  async readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    const gen = this.rooms.get(roomId)?.gens.get(inc)
    const order = lane.kind === 'inbox' || lane.kind === 'binary' ? gen?.memberOrders.get(lane.member) : gen?.order
    const mark = order?.get(encodeLaneKey(lane))
    return mark === undefined ? null : { ...mark }
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    const entry = this.rooms.get(roomId)?.gens.get(inc)?.retained.get(encodeLaneKey(lane))
    if (entry === undefined) return null
    return { payload: copyBytes(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(roomId: string, inc: string, of?: RetainedOf): Promise<LaneId[]> {
    const gen = this.rooms.get(roomId)?.gens.get(inc)
    if (gen === undefined) return []
    if (of === undefined) return Array.from(gen.retained.values(), (entry) => copyLane(entry.lane))
    const keys = 'member' in of ? gen.retainedOf.get(of.member) : gen.retainedOfTrack.get(of.track)
    return Array.from(keys ?? [], (key) => copyLane(gen.retained.get(key)!.lane))
  }

  async deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    const gen = this.rooms.get(roomId)?.gens.get(inc)
    const key = encodeLaneKey(lane)
    if (gen === undefined || (opts?.ifSeq !== undefined && gen.retained.get(key)?.seq !== opts.ifSeq)) return
    gen.retained.delete(key)
    if (lane.kind !== 'binary') return
    unindex(gen.retainedOf, lane.member, key)
    unindex(gen.retainedOfTrack, lane.track, key)
  }

  async dropLaneOrders(roomId: string, inc: string, member: string): Promise<void> {
    this.rooms.get(roomId)?.gens.get(inc)?.memberOrders.delete(member)
  }

  private openSubscription(
    { roomId, inc, lane }: RoomSubscriptionSource,
    receiver: BackendReceiver<BackendPayload>,
    localReceiverCount: () => number,
  ): SubscriptionAttempt {
    const room = this.rooms.get(roomId)
    const head = this.liveHead(room)
    if (room === undefined || !isOpenIncarnation(head, inc)) {
      throw new Error(`subscribeLane: room '${roomId}' has no open incarnation '${inc}'`)
    }
    // Registration is durable before `ready` resolves: a commit accepted after this point must see it.
    const gen = this.generation(room, inc)
    let subs: Subscriptions
    let key: string
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
    // Ready once what was committed before it is delivered: a lane it replaces delivers those first.
    return this.pubSub.subscribe({ subs, key, receiver, localReceiverCount })
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    const room = this.rooms.get(roomId)
    if (room === undefined) return
    const gen = room.gens.get(inc)
    if (gen === undefined) return // already dropped (the janitor is resumable)
    room.gens.delete(inc)
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
    return getOrCreate(this.rooms, roomId, () => ({ head: null, gens: new Map() }))
  }

  /** A room with no incarnation left is forgotten once its tombstone lapses (this backend's revs are never reused). */
  private releaseWhenLapsed(roomId: string, room: RoomRecord): void {
    if (this.rooms.get(roomId) !== room || room.gens.size > 0) return
    const head = this.liveHead(room)
    if (head === null) {
      this.rooms.delete(roomId)
      return
    }
    if (head.expiresAt === null) return
    unrefTimer(setTimeout(() => this.releaseWhenLapsed(roomId, room), head.expiresAt - Date.now()))
  }

  private generation(room: RoomRecord, inc: string): Generation {
    return getOrCreate(room.gens, inc, newGeneration)
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
