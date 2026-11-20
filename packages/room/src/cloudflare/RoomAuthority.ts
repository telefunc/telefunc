/// <reference types="@cloudflare/workers-types" />
export { RoomAuthority }
export type { AcceptedCommitWire, CommitWire, InboxLease, OperationWire, RegisterWire, RenewWire }

// A room's authority owns its time, state and fanout.

import type {
  CellCounter,
  CellsExchange,
  CellsExchangeResult,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  LaneId,
  RoomHead,
  StaleCommit,
  CellSelector,
  CellsRead,
  CommitOptions,
  RetainedFrame,
  RetainedOf,
  DirectoryPage,
  ExpiringCellsRead,
} from '../backend/contract.js'
import {
  OrderedStubs,
  grantHolder,
  leaseAnswer,
  liveLeases,
  registerLease,
  removeLease,
  renewLease,
  setLeaseAnswer,
  sweepLeases,
  type CloudflareExtensionKit,
  type LeaseOrder,
  type LeaseRegistered,
  type LeaseRemoved,
  type LeaseWritten,
  type OrderingInfo,
  type TelefuncExtensionNamespace,
} from 'telefunc/__internal'
import { assert } from '../utils/assert.js'
import { OPERATION_COMMIT_LANE, type OperationWrites } from '../backend/operation.js'
import { ROOM_OPERATIONS, recordRequest, type RoomOperationRequest } from '../server/store/roomOperations.js'
import { decodeRoomRecord, encodeRoomRecord, lostReason } from '../server/lanes/lanes.js'
import { reportLaneLost } from '../server/config.js'
import { encodeLaneKey, everyMemberLaneKey, everyTrackLaneKey } from '../backend/laneKey.js'
import { commitPreconditionHolds, isOpenIncarnation, publicHead } from '../backend/semantics.js'
import { Fanout } from './Fanout.js'
import { openRegionDirectory, type RegionDirectoryStub } from './objects.js'
import { deleteRetained, installRetained, listRetained, readRetained, retainedOwner } from './retained.js'
import { routeLease, routeScope, type RouteInstallation } from './routes.js'
import {
  advanceOrder,
  claimListingBucket,
  compareExchangeCells,
  compareExchangeHead,
  dropCellPrefix,
  deleteListing,
  dropIncarnationRows,
  dropLaneOrderRows,
  initSchema,
  deleteLapsedHead,
  listOrphanIncarnations,
  hasIncarnation,
  nextMaintenanceDeadline,
  readCells,
  readCellPage,
  readLiveHead,
  readListing,
  readListingBucket,
  readOrder,
  settleDueListing,
  writeListing,
  type CellPage,
  readExpiringCells,
  writeExpiringCell,
  deleteExpiredCells,
  storeHandoff,
  listHandoffs,
  deleteHandoff,
  type Handoff,
} from './storage.js'

/** An inbox or control commit replies once its delivery was handed off, with why it was lost, if it was. */
type AcceptedCommitWire = {
  kind: 'accepted'
  seq: number
  timestamp: number
  lost?: string
}
/** A session DO subscribed to a lane, and the bucket it is in. */
type RouteHolder = { holder: string; bucket: string }
/** A commit the transaction accepted: its order, the lanes it reaches, and the session DOs subscribed to them then. */
type AcceptedCommit = { kind: 'accepted'; mark: OrderingInfo; laneKeys: string[]; routes: RouteHolder[] }
type CommitWire = AcceptedCommitWire | StaleCommit
/** An operation that ran, with its commit's receipt, or its incarnation stale, or the inbox lease it carried overtaken
 *  by a later write of its session's on that inbox. */
type OperationWire =
  | {
      kind: 'done'
      value: unknown
      counters: Map<string, CellCounter>
      commit?: AcceptedCommitWire
      /** Set where it registered the inbox lease it carried, now or when it came before. */
      registered?: true
    }
  | { kind: 'stale-incarnation' }
  | { kind: 'overtaken' }
/** A member's exact inbox lease, which an operation registers in its own transaction, under the session's `order`. */
type InboxLease = { route: RouteInstallation; order: LeaseOrder }
/** What an operation that registered its inbox lease answered, kept with that lease for a copy of it that comes
 *  again. */
type StoredAnswer = { value: unknown; counters: Map<string, CellCounter>; commit?: { seq: number; timestamp: number } }

/** A route registered, refused because its incarnation is dropped or closing, or not, as `registerLease()` says. */
type RegisterWire = LeaseRegistered | 'dropped'
/** A route renewed, lapsed on its own, or deleted with its dropped incarnation. */
type RenewWire = 'renewed' | 'lapsed' | 'dropped'

const ROOM_MAINTENANCE_RETRY_MS = 30_000
/** When the alarm hands on a stored commit whose hand-on this run of the object hasn't settled. */
const ROOM_HANDOFF_RESUME_MS = 10_000
const SEMANTIC_LANE: LaneId = { kind: 'semantic' }

/** Room's part of a Durable Object that is a room's authority, fanning out through Broadcast's `regions`. Its tables are
 *  created on first use. */
class RoomAuthority {
  private readonly ctx: DurableObjectState
  private readonly getNamespace: () => TelefuncExtensionNamespace
  private readonly fanout: Fanout
  /** Its calls to its listing's region directory, which reach it in the order the listing changed. */
  private readonly directoryCalls = new OrderedStubs<RegionDirectoryStub>()
  /** The stored commits this run of the object is handing on, each to why its hand-on lost it, or null. */
  private readonly handingOn = new Map<string, Promise<string | null>>()
  /** Whether this run handed on what an earlier one stored and didn't. */
  private resumed = false
  private schema = false

  constructor(ctx: DurableObjectState, { regions, getNamespace }: CloudflareExtensionKit) {
    this.ctx = ctx
    this.getNamespace = getNamespace
    this.fanout = new Fanout({ regions })
  }

  async readHead(): Promise<RoomHead | null> {
    const head = readLiveHead(this.sql(), Date.now())
    return head === null ? null : publicHead(head, head.config)
  }

  async compareExchangeHead(expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    const now = Date.now()
    const outcome = this.transaction((sql) => compareExchangeHead(sql, expected, next, now))
    await this.scheduleMaintenanceIfNeeded()
    if (outcome.kind === 'committed') return { kind: 'committed', head: publicHead(outcome.head, outcome.head.config) }
    const { current } = outcome
    return { kind: 'conflict', current: current === null ? null : publicHead(current, current.config) }
  }

  async readCells(inc: string, sel: CellSelector): Promise<CellsRead> {
    return readCells(this.sql(), inc, sel, Date.now())
  }

  async readExpiringCells(inc: string, keys: readonly string[]): Promise<ExpiringCellsRead> {
    return readExpiringCells(this.sql(), inc, keys, Date.now())
  }

  async readCellPage(inc: string, prefix: string, cursor?: string): Promise<CellPage> {
    return readCellPage(this.sql(), inc, prefix, cursor, Date.now())
  }

  async compareExchangeCells(inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    const now = Date.now()
    return this.transaction((sql) => compareExchangeCells(sql, inc, exchange, now))
  }

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitWire> {
    const now = Date.now()
    const outcome = this.transaction((sql): StaleCommit | AcceptedCommit => {
      if (!commitPreconditionHolds(readLiveHead(sql, now), inc, lane.kind, opts?.closingLease, now)) {
        return { kind: 'stale-incarnation' }
      }
      if (opts?.requiredCellKeys !== undefined) {
        const required = readCells(sql, inc, { keys: opts.requiredCellKeys }, now)
        assert(required.kind === 'read') // the precondition just found this incarnation open
        const missing = opts.requiredCellKeys.find((cell) => !required.cells.has(cell))
        if (missing !== undefined) return { kind: 'stale-cell', key: missing }
      }
      return commitIn(sql, inc, lane, payload, opts ?? {}, now)
    })
    if (outcome.kind !== 'accepted') return outcome
    return await this.deliver(roomId, inc, lane, payload, outcome)
  }

  /** Runs the operation `request` names in one transaction, which a plan that throws rolls back, then hands its commit
   *  on. */
  async runOperation(inc: string, request: RoomOperationRequest): Promise<OperationWire> {
    const wire = await this.operate(inc, request, undefined)
    assert(wire !== 'regrant') // only an inbox lease's write is under a grant
    return wire
  }

  /** As `runOperation()`, and the same transaction registers the member's exact inbox lease, and keeps the answer
   *  with it: a copy of the call that comes again answers it and changes nothing. A plan that writes nothing registers
   *  nothing. */
  async runOperationWithInbox(
    inc: string,
    request: RoomOperationRequest,
    inbox: InboxLease,
  ): Promise<OperationWire | 'regrant'> {
    return this.operate(inc, request, inbox)
  }

  private async operate(
    inc: string,
    request: RoomOperationRequest,
    inbox: InboxLease | undefined,
  ): Promise<OperationWire | 'regrant'> {
    assert(Object.hasOwn(ROOM_OPERATIONS, request.name))
    const now = Date.now()
    const lease = inbox === undefined ? null : routeLease(inbox.route)
    let outcome: OperationOutcome
    try {
      outcome = this.transaction((sql): OperationOutcome => {
        if (!isOpenIncarnation(readLiveHead(sql, now), inc)) return { kind: 'stale-incarnation' }
        if (inbox !== undefined && lease !== null) {
          const written: LeaseWritten = registerLease(sql, lease, inbox.order, now)
          if (written === 'regrant') return { kind: 'regrant' }
          if (written === 'stale') return { kind: 'overtaken' }
          if (written === 'repeated') {
            const answer = leaseAnswer(sql, lease.scope, lease.holder)
            assert(answer !== null) // only an operation registers under its own order, and stores its answer with it
            return { kind: 'repeated', answer: decodeRoomRecord<StoredAnswer>(answer) }
          }
        }
        const { value, writes } = recordRequest(request, { now, cell: (key) => selectCell(sql, inc, key, now) })
        if (lease !== null && writesNothing(writes)) throw new NothingWritten(value)
        const done = applyOperation(sql, inc, value, writes, now)
        if (inbox !== undefined && lease !== null) {
          const answer: StoredAnswer = { value, counters: done.counters }
          if (done.handoff !== null) answer.commit = { seq: done.handoff.seq, timestamp: done.handoff.timestamp }
          setLeaseAnswer(sql, lease.scope, lease.holder, inbox.order, encodeRoomRecord(answer))
        }
        return done
      })
    } catch (error) {
      if (!(error instanceof NothingWritten)) throw error
      return { kind: 'done', value: error.value, counters: new Map() }
    }
    if (outcome.kind === 'regrant') return 'regrant'
    if (outcome.kind !== 'done' && outcome.kind !== 'repeated') return outcome
    if (outcome.kind === 'repeated') {
      const { value, counters, commit } = outcome.answer
      if (commit === undefined) return { kind: 'done', value, counters, registered: true }
      this.resumeHandoffs()
      const handing = this.handingOn.get(
        handoffKey({ inc, laneKey: encodeLaneKey(OPERATION_COMMIT_LANE), seq: commit.seq }),
      )
      // One this run isn't handing on was handed on before.
      const receipt = await receiptOf(commit, handing ?? Promise.resolve(null))
      return { kind: 'done', value, counters, commit: receipt, registered: true }
    }
    const { value, counters, handoff } = outcome
    // In this turn, so it leaves before anything committed after it.
    const handing = handoff === null ? null : this.handOn(handoff)
    // The alarm deletes the expiring cells, as no server may be on the room by then, and hands on a commit whose
    // hand-on didn't settle.
    if (outcome.expiring || handoff !== null) await this.scheduleMaintenanceIfNeeded()
    const done: OperationWire = { kind: 'done', value, counters }
    if (inbox !== undefined) done.registered = true
    if (handoff !== null && handing !== null) done.commit = await receiptOf(handoff, handing)
    return done
  }

  /** Hands every commit an earlier run of the object stored and didn't hand on, in commit order, before anything this
   *  run commits; once per run, and again on the alarm. */
  private resumeHandoffs(again = false): void {
    if (this.resumed && !again) return
    this.resumed = true
    for (const handoff of listHandoffs(this.sql())) this.handOn(handoff)
  }

  /** Hands a stored commit on to the session DOs subscribed to its lane now, once per run of the object; its row goes
   *  once the hand-on settled, lost or not. */
  private handOn(handoff: Handoff): Promise<string | null> {
    this.resumeHandoffs()
    const key = handoffKey(handoff)
    const handing = this.handingOn.get(key)
    if (handing !== undefined) return handing
    const { inc, laneKey, seq, timestamp, payload } = handoff
    const routes = liveLeases(this.sql(), [routeScope(inc, laneKey)], Date.now())
    const delivery = this.fanout.send(inc, [laneKey], routes, payload, { seq, timestamp })
    const handed = lostReason(delivery).then((lost) => {
      this.transaction((sql) => deleteHandoff(sql, handoff))
      this.handingOn.delete(key)
      return lost
    })
    this.handingOn.set(key, handed)
    return handed
  }

  /** Fans a commit out; an inbox or control commit's receipt waits for its handoff. */
  private async deliver(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    { mark, laneKeys, routes }: AcceptedCommit,
  ): Promise<AcceptedCommitWire> {
    this.resumeHandoffs()
    const delivery = this.fanout.send(inc, laneKeys, routes, payload, mark)
    const receipt: AcceptedCommitWire = { kind: 'accepted', seq: mark.seq, timestamp: mark.timestamp }
    if (lane.kind !== 'inbox' && lane.kind !== 'control') {
      // No committer awaits a data or binary frame's handoff, so a lost one is reported here.
      void reportIfLost(delivery, roomId, lane)
      return receipt
    }
    const lost = await lostReason(delivery)
    if (lost !== null) receipt.lost = lost
    return receipt
  }

  async readLaneOrder(inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    return readOrder(this.sql(), inc, encodeLaneKey(lane))
  }

  async readRetained(inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    return readRetained(this.sql(), inc, lane)
  }

  async listRetained(inc: string, of?: RetainedOf): Promise<LaneId[]> {
    return listRetained(this.sql(), inc, of)
  }

  /** A grant for a session DO's writes of routes here (`leases.ts` of core). */
  async grantHolder(sessionDoId: string): Promise<number> {
    const grant = this.transaction((sql) => grantHolder(sql, sessionDoId, Date.now()))
    await this.scheduleMaintenanceIfNeeded()
    return grant
  }

  async registerRoute(route: RouteInstallation, order: LeaseOrder): Promise<RegisterWire> {
    const now = Date.now()
    const registration = this.transaction((sql): RegisterWire => {
      if (!isOpenIncarnation(readLiveHead(sql, now), route.inc)) return 'dropped'
      const written = registerLease(sql, routeLease(route), order, now)
      // A registration that came before is answered as it was.
      return written === 'repeated' ? 'registered' : written
    })
    if (registration === 'registered') await this.scheduleMaintenanceIfNeeded()
    return registration
  }

  /** Renews each route, or says it lapsed or went with its dropped incarnation; its session then ends its
   *  subscription. */
  async renewRoutes(routes: RouteInstallation[]): Promise<RenewWire[]> {
    const now = Date.now()
    const renewals = this.transaction((sql) =>
      routes.map((route): RenewWire => {
        const { scope, holder, lease } = routeLease(route)
        if (renewLease(sql, scope, holder, lease, now) === 'renewed') return 'renewed'
        return hasIncarnation(sql, route.inc) ? 'lapsed' : 'dropped'
      }),
    )
    await this.scheduleMaintenanceIfNeeded()
    return renewals
  }

  async unsubscribeRoute(route: RouteInstallation, order: LeaseOrder): Promise<LeaseRemoved> {
    const { scope, holder, lease } = routeLease(route)
    const removal = this.transaction((sql) => removeLease(sql, scope, holder, lease, order, Date.now()))
    await this.scheduleMaintenanceIfNeeded()
    return removal
  }

  async dropIncarnation(inc: string): Promise<void> {
    this.transaction((sql) => dropIncarnationRows(sql, inc))
    await this.scheduleMaintenanceIfNeeded()
  }

  /** Lists the room, and tells its region's directory when `incTag` is new here: `bucket`'s, if it is the room's first
   *  listing. */
  async directoryPut(
    roomId: string,
    incTag: string,
    checkAt: number,
    minDelayMs: number,
    bucket: string,
  ): Promise<void> {
    const at = Math.max(checkAt, Date.now() + minDelayMs)
    const newlyListedIn = this.transaction((sql) => {
      const listed = readListing(sql, roomId) === incTag
      writeListing(sql, roomId, incTag, at)
      const listedIn = claimListingBucket(sql, bucket)
      return listed ? null : listedIn
    })
    await Promise.all([
      newlyListedIn !== null && this.callDirectory(newlyListedIn, (directory) => directory.add(roomId, incTag)),
      this.scheduleMaintenanceIfNeeded(),
    ])
  }

  /** Its region directory forgets `incTag` even when the listing here is gone or newer, as after an alarm that couldn't
   *  reach it. */
  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.transaction((sql) => deleteListing(sql, roomId, incTag))
    await this.unlistInItsRegion(roomId, incTag)
  }

  async directoryGet(roomId: string): Promise<string | undefined> {
    return readListing(this.sql(), roomId)
  }

  async alarm(): Promise<void> {
    try {
      this.resumeHandoffs(true)
      const unlisted = this.runSweep(Date.now())
      if (unlisted) await this.unlistInItsRegion(unlisted.roomId, unlisted.incTag)
    } finally {
      await this.scheduleMaintenanceIfNeeded()
    }
  }

  /** Storage only: a session whose route lapsed or whose incarnation went learns it at its next renewal. Returns the
   *  listing it deleted, for its region directory to forget too. */
  private runSweep(now: number): DirectoryPage['entries'][number] | null {
    return this.transaction((sql) => {
      const unlisted = settleDueListing(sql, now)
      const currentInc = readLiveHead(sql, now)?.currentInc ?? null
      deleteLapsedHead(sql, now)
      sweepLeases(sql, now)
      deleteExpiredCells(sql, now)
      for (const inc of listOrphanIncarnations(sql, currentInc)) dropIncarnationRows(sql, inc)
      return unlisted
    })
  }

  private async unlistInItsRegion(roomId: string, incTag: string): Promise<void> {
    const listedIn = readListingBucket(this.sql())
    if (listedIn !== null) await this.callDirectory(listedIn, (directory) => directory.remove(roomId, incTag))
  }

  private callDirectory(bucket: string, invoke: (directory: RegionDirectoryStub) => Promise<void>): Promise<void> {
    return this.directoryCalls.call(bucket, () => openRegionDirectory(this.getNamespace(), bucket), invoke)
  }

  /** The schema is created outside the transaction, so a refused first write can't roll it back. */
  private transaction<T>(fn: (sql: SqlStorage) => T): T {
    const sql = this.sql()
    return this.ctx.storage.transactionSync(() => fn(sql))
  }

  private sql(): SqlStorage {
    if (!this.schema) {
      initSchema(this.ctx.storage.sql)
      this.schema = true
    }
    return this.ctx.storage.sql
  }

  private async scheduleMaintenanceIfNeeded(): Promise<void> {
    const now = Date.now()
    const deadline = nextMaintenanceDeadline(this.sql(), now)
    const currentAlarm = await this.ctx.storage.getAlarm()
    if (deadline === null) {
      if (currentAlarm !== null) await this.ctx.storage.deleteAlarm()
      return
    }
    const nextAlarm = deadline <= now ? now + ROOM_MAINTENANCE_RETRY_MS : deadline
    if (currentAlarm === nextAlarm) return
    // Never postpone an already-scheduled retry for work that is due now.
    if (deadline <= now && currentAlarm !== null && currentAlarm <= nextAlarm) return
    await this.ctx.storage.setAlarm(nextAlarm)
  }
}

/** Orders a commit on `lane` in a transaction that found the incarnation open, and retains it with `retain`. */
function commitIn(
  sql: SqlStorage,
  inc: string,
  lane: LaneId,
  payload: Uint8Array,
  { retain, owner }: Pick<CommitOptions, 'retain' | 'owner'>,
  now: number,
): AcceptedCommit {
  const key = encodeLaneKey(lane)
  const mark = advanceOrder(sql, inc, key, now)
  if (retain === true) installRetained(sql, inc, lane, payload, mark, owner)
  // A binary commit also reaches its member's every-track lane and its track's every-member lane.
  const laneKeys =
    lane.kind === 'binary' ? [key, everyTrackLaneKey(lane.member), everyMemberLaneKey(lane.track)] : [key]
  const routes = liveLeases(
    sql,
    laneKeys.map((laneKey) => routeScope(inc, laneKey)),
    now,
  )
  return { kind: 'accepted', mark, laneKeys, routes }
}

/** An operation's outcome in its transaction. */
type OperationOutcome =
  | OperationApplied
  | { kind: 'stale-incarnation' }
  | { kind: 'overtaken' }
  | { kind: 'regrant' }
  | { kind: 'repeated'; answer: StoredAnswer }
type OperationApplied = {
  kind: 'done'
  value: unknown
  counters: Map<string, CellCounter>
  /** Its commit, stored until handed on, or null where it committed nothing. */
  handoff: Handoff | null
  expiring: boolean
}

/** A plan that wrote nothing, whose transaction rolls back the inbox lease it registered. */
class NothingWritten {
  readonly value: unknown
  constructor(value: unknown) {
    this.value = value
  }
}

function writesNothing({ prefixes, members, mutations, counts, expiring, commit }: OperationWrites): boolean {
  return [prefixes, members, mutations, counts, expiring].every((writes) => writes.length === 0) && commit === null
}

/** Applies a plan's writes in a transaction that found `inc` open; its commit is stored until handed on. */
function applyOperation(sql: SqlStorage, inc: string, value: unknown, writes: OperationWrites, now: number) {
  for (const prefix of writes.prefixes) dropCellPrefix(sql, inc, prefix)
  const exchange = { expect: [], mutations: writes.mutations, counts: writes.counts }
  const exchanged = compareExchangeCells(sql, inc, exchange, now)
  assert(exchanged.kind === 'committed') // nothing expected, in an incarnation found open
  for (const member of writes.members) {
    dropLaneOrderRows(sql, inc, member)
    for (const lane of listRetained(sql, inc, { member })) deleteRetained(sql, inc, lane)
    if (retainedOwner(sql, inc, SEMANTIC_LANE) === member) deleteRetained(sql, inc, SEMANTIC_LANE)
  }
  for (const cell of writes.expiring) writeExpiringCell(sql, inc, cell, now)
  let handoff: Handoff | null = null
  if (writes.commit !== null) {
    const laneKey = encodeLaneKey(OPERATION_COMMIT_LANE)
    const { seq, timestamp } = advanceOrder(sql, inc, laneKey, now)
    handoff = { inc, laneKey, seq, timestamp, payload: writes.commit }
    storeHandoff(sql, handoff, now + ROOM_HANDOFF_RESUME_MS)
  }
  const applied: OperationApplied = {
    kind: 'done',
    value,
    counters: exchanged.counters,
    handoff,
    expiring: writes.expiring.length > 0,
  }
  return applied
}

/** A stored commit's receipt, once its hand-on settled. */
async function receiptOf(
  { seq, timestamp }: { seq: number; timestamp: number },
  handing: Promise<string | null>,
): Promise<AcceptedCommitWire> {
  const receipt: AcceptedCommitWire = { kind: 'accepted', seq, timestamp }
  const lost = await handing
  if (lost !== null) receipt.lost = lost
  return receipt
}

function handoffKey({ inc, laneKey, seq }: Pick<Handoff, 'inc' | 'laneKey' | 'seq'>): string {
  return JSON.stringify([inc, laneKey, seq])
}

function selectCell(sql: SqlStorage, inc: string, key: string, now: number): Uint8Array | undefined {
  const read = readCells(sql, inc, { keys: [key] }, now)
  assert(read.kind === 'read') // the operation found this incarnation open
  return read.cells.get(key)
}

/** A data or binary frame a subscribed session DO didn't take is reported as lost on its room's lane. */
async function reportIfLost(delivery: Promise<void>, roomId: string, lane: LaneId): Promise<void> {
  try {
    await delivery
  } catch (error) {
    reportLaneLost(roomId, lane, String(error))
  }
}
