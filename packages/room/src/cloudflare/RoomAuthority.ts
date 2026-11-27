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
  CellsBeside,
  CellsRead,
  CommitOptions,
  RetainedFrame,
  RetainedOf,
  ExpiringCellsRead,
  DirectoryEntry,
} from '../backend/contract.js'
import {
  LEASE_CALL_DEADLINE_MS,
  OrderedStubs,
  grantHolder,
  leaseRegistered,
  liveLeases,
  registerLease,
  removeLease,
  renewLease,
  sweepLeases,
  type CloudflareExtensionKit,
  type DurableStorage,
  type SqlStore,
  type LeaseOrder,
  type LeaseRegistered,
  type LeaseRemoved,
  type OrderingInfo,
  type TelefuncExtensionNamespace,
} from 'telefunc/__internal'
import { assert } from '../utils/assert.js'
import { OPERATION_COMMIT_LANE, type OperationWrites } from '../backend/operation.js'
import {
  ROOM_OPERATIONS,
  recordRequest,
  type OperationArgs,
  type OperationName,
  type OperationValue,
  type RoomOperationRequest,
} from '../server/store/roomOperations.js'
import { decodeRoomRecord, encodeRoomRecord, lostReason } from '../server/lanes/lanes.js'
import { ROOM_TOMBSTONE_TTL_MS } from '../server/constants.js'
import { fieldsOf, numberOf, textOf } from '../shared/readers.js'
import { reportLaneLost } from '../server/config.js'
import { encodeLaneKey, everyMemberLaneKey, everyTrackLaneKey } from '../backend/laneKey.js'
import { commitPreconditionHolds, isOpenIncarnation, publicHead, unheldRequiredCell } from '../backend/semantics.js'
import { Fanout } from './Fanout.js'
import { ROOM_JOIN_DEADLINE_MS, openRegionDirectory, type RegionDirectoryStub } from './objects.js'
import { deleteRetained, installRetained, listRetained, readRetained, retainedOwner } from './retained.js'
import { routeLease, routeScope, type RouteInstallation } from './routes.js'
import {
  advanceOrder,
  compareExchangeCells,
  heldCell,
  compareExchangeHead,
  dropCellPrefix,
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
  readListedEntry,
  readUnsentListing,
  readOrder,
  listWithHead,
  relistOver,
  resendListing,
  settleListing,
  type CellPage,
  type ListingPlace,
  readExpiringCells,
  writeExpiringCell,
  deleteExpiredCells,
  storeHandoff,
  listHandoffs,
  deleteHandoff,
  type Handoff,
  storeAnswer,
  readStoredAnswer,
  recordAnswerLoss,
  deleteExpiredAnswers,
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
const routeHolder = (holder: string, bucket: string): RouteHolder => ({ holder, bucket })
/** A commit the transaction accepted: its order, the lanes it reaches, and the session DOs subscribed to them then. */
type AcceptedCommit = { kind: 'accepted'; mark: OrderingInfo; laneKeys: string[]; routes: RouteHolder[] }
type CommitWire = AcceptedCommitWire | StaleCommit
/** An operation that ran, with its commit's receipt, or its incarnation stale, or the inbox lease it carried overtaken
 *  by a later write of its session's on that inbox. */
type OperationWire<Value> =
  | {
      kind: 'done'
      value: Value
      counters: Map<string, CellCounter>
      commit?: AcceptedCommitWire
      /** Set where it registered the inbox lease it carried, now or when it came before. */
      registered?: true
    }
  | { kind: 'stale-incarnation' }
  | { kind: 'overtaken' }
/** A member's exact inbox lease, which a join registers in its own transaction, under the session's `order`. */
type InboxLease = { route: RouteInstallation; order: LeaseOrder }
/** What a join that registered its inbox lease answered, kept by the lease's order for a copy of it that comes
 *  again. */
type StoredAnswer<Value> = {
  value: Value
  counters: Map<string, CellCounter>
  commit?: { seq: number; timestamp: number }
}
/** The inbox lease an operation registers, and how its stored answer's value reads back: a join's, the one operation
 *  that carries an inbox. */
type CarriedInbox<Value> = { lease: InboxLease; valueOf(stored: unknown): Value }

/** A join's value as `encodeRoomRecord()` stored it. */
function joinValueOf(value: unknown): OperationValue['join'] {
  assert(value === 'joined' || value === 'lease-lost', "A stored join's value is neither 'joined' nor 'lease-lost'")
  return value
}

/** A `StoredAnswer` as `encodeRoomRecord()` wrote it, its counters rebuilt key by key and its value by `valueOf`. */
function storedAnswerOf<Value>(bytes: Uint8Array, valueOf: (stored: unknown) => Value): StoredAnswer<Value> {
  const fields = fieldsOf(decodeRoomRecord(bytes), 'A stored answer')
  const stored = fields.counters
  assert(stored instanceof Map, "A stored answer's counters are not a map")
  const counters = new Map<string, CellCounter>()
  for (const [key, counter] of stored) {
    const { value, changedAt } = fieldsOf(counter, 'A stored counter')
    counters.set(textOf(key, "A stored counter's key"), {
      value: numberOf(value, "A stored counter's value"),
      changedAt: numberOf(changedAt, "A stored counter's change time"),
    })
  }
  const answer: StoredAnswer<Value> = { value: valueOf(fields.value), counters }
  if (fields.commit !== undefined) {
    const { seq, timestamp } = fieldsOf(fields.commit, "A stored answer's commit")
    answer.commit = {
      seq: numberOf(seq, "A stored answer's commit seq"),
      timestamp: numberOf(timestamp, "A stored answer's commit time"),
    }
  }
  return answer
}

/** A route registered, refused because its incarnation is dropped or closing, or not, as `registerLease()` says. */
type RegisterWire = LeaseRegistered | 'dropped'
/** A route renewed, lapsed on its own, or deleted with its dropped incarnation. */
type RenewWire = 'renewed' | 'lapsed' | 'dropped'

const ROOM_MAINTENANCE_RETRY_MS = 30_000
/** How long a join's answer is kept for a copy of it: a copy that comes past the join's deadline has no caller waiting
 *  for it. */
const ANSWER_KEPT_MS = ROOM_JOIN_DEADLINE_MS
/** When the alarm hands on a stored commit whose hand-on this run of the object hasn't settled. */
const ROOM_HANDOFF_RESUME_MS = 10_000
const SEMANTIC_LANE: LaneId = { kind: 'semantic' }
const CONTROL_LANE: LaneId = { kind: 'control' }

/** Room's part of a Durable Object that is a room's authority, fanning out through Broadcast's `regions`. Its tables are
 *  created on first use. */
class RoomAuthority {
  private readonly ctx: { storage: DurableStorage }
  private readonly getNamespace: () => TelefuncExtensionNamespace
  private readonly fanout: Fanout
  /** Its calls to its listing's region directory, which reach it in the order the listing changed. */
  private readonly directoryCalls = new OrderedStubs<RegionDirectoryStub>()
  /** The listing's versions this run is sending, one call at a time, until none is unsent. */
  private showing: Promise<void> | null = null
  /** The alarm as this run set it, or undefined until it sets one: the first schedule of a run sets it, whatever an
   *  earlier run had set. */
  private alarmAt: number | null | undefined = undefined
  /** The stored commits this run of the object is handing on, each to why its hand-on lost it, or null. */
  private readonly handingOn = new Map<string, Promise<string | null>>()
  /** Whether this run handed on what an earlier one stored and didn't. */
  private resumed = false
  private schema = false

  constructor(ctx: { storage: DurableStorage }, { regions, getNamespace }: CloudflareExtensionKit) {
    this.ctx = ctx
    this.getNamespace = getNamespace
    this.fanout = new Fanout({ regions })
  }

  async readHead(): Promise<RoomHead | null> {
    const head = readLiveHead(this.sql(), Date.now())
    return head === null ? null : publicHead(head, head.config)
  }

  /** Lists what the head it stores lists, at `place`, in the same transaction: no call but a head's change lists a
   *  room or unlists it. */
  async compareExchangeHead(
    expected: HeadExpectation,
    next: HeadNext,
    place: ListingPlace,
  ): Promise<HeadExchangeResult> {
    const now = Date.now()
    const outcome = this.transaction((sql) => {
      const exchanged = compareExchangeHead(sql, expected, next, now)
      if (exchanged.kind === 'committed') listWithHead(sql, exchanged.head, now, place)
      return exchanged
    })
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

  async readCellPage(inc: string, prefix: string, cursor?: string, beside?: CellsBeside): Promise<CellPage> {
    return readCellPage(this.sql(), inc, prefix, cursor, Date.now(), beside)
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
      const unheld = opts?.required && unheldRequiredCell(opts.required, (key) => heldCell(sql, inc, key, now))
      if (unheld !== undefined) return { kind: 'stale-cell', key: unheld }
      return commitIn(sql, inc, lane, payload, opts ?? {}, now)
    })
    if (outcome.kind !== 'accepted') return outcome
    return await this.deliver(roomId, inc, lane, payload, outcome)
  }

  /** Runs the operation `request` names in one transaction, which a plan that throws rolls back, then hands its commit
   *  on. */
  async runOperation<N extends OperationName>(
    inc: string,
    request: RoomOperationRequest<N>,
  ): Promise<OperationWire<OperationValue[N]>> {
    const wire = await this.operate(inc, request, undefined)
    assert(wire !== 'regrant') // only an inbox lease's write is under a grant
    return wire
  }

  /** Runs a join as `runOperation()` does, and the same transaction registers the member's exact inbox lease, and
   *  keeps the answer by the lease's order: a copy of the call that comes within the call's deadline is answered as
   *  the call was, its commit's loss included, and changes nothing. A join that writes nothing registers nothing. */
  async runJoinWithInbox(
    inc: string,
    args: OperationArgs['join'],
    inbox: InboxLease,
  ): Promise<OperationWire<OperationValue['join']> | 'regrant'> {
    return this.operate(inc, { name: 'join', args }, { lease: inbox, valueOf: joinValueOf })
  }

  private async operate<N extends OperationName>(
    inc: string,
    request: RoomOperationRequest<N>,
    inbox: CarriedInbox<OperationValue[N]> | undefined,
  ): Promise<OperationWire<OperationValue[N]> | 'regrant'> {
    assert(Object.hasOwn(ROOM_OPERATIONS, request.name))
    const now = Date.now()
    // The inbox lease it registers, and the order the session sent it under, by which its answer is kept.
    const lease = inbox === undefined ? null : { ...routeLease(inbox.lease.route), order: inbox.lease.order }
    // The value of a plan that wrote nothing, whose transaction then rolls back the inbox lease it registered.
    const unwritten: { plan: { value: OperationValue[N] } | null } = { plan: null }
    let outcome: OperationOutcome<OperationValue[N]>
    try {
      outcome = this.transaction((sql): OperationOutcome<OperationValue[N]> => {
        // A copy is answered as the operation was, whatever was written since.
        const kept = lease === null ? null : readStoredAnswer(sql, lease)
        if (kept !== null && inbox !== undefined) {
          return { kind: 'repeated', answer: storedAnswerOf(kept.answer, inbox.valueOf), lost: kept.lost }
        }
        if (!isOpenIncarnation(readLiveHead(sql, now), inc)) return { kind: 'stale-incarnation' }
        if (lease !== null) {
          const written = registerLease(sql, lease, lease.order, now)
          if (written === 'regrant') return { kind: 'regrant' }
          // A later write of the session's on the inbox came first, or this copy came once its answer's time was over.
          if (written !== 'registered') return { kind: 'overtaken' }
        }
        const { value, writes } = recordRequest(request, {
          now,
          cell: (key) => heldCell(sql, inc, key, now)?.bytes,
          lapsed: (key) => heldCell(sql, inc, key, now)?.lapsed === true,
        })
        if (lease !== null && writesNothing(writes)) {
          unwritten.plan = { value }
          throw new NothingWritten()
        }
        const done = applyOperation(sql, inc, value, writes, now)
        if (lease !== null) {
          const answer: StoredAnswer<OperationValue[N]> = { value, counters: done.counters }
          if (done.handoff !== null) answer.commit = { seq: done.handoff.seq, timestamp: done.handoff.timestamp }
          const stored = { answer: encodeRoomRecord(answer), commitSeq: done.handoff?.seq ?? null }
          storeAnswer(sql, lease, inc, stored, now + ANSWER_KEPT_MS)
        }
        return done
      })
    } catch (error) {
      if (!(error instanceof NothingWritten) || unwritten.plan === null) throw error
      return { kind: 'done', value: unwritten.plan.value, counters: new Map() }
    }
    if (outcome.kind === 'regrant') return 'regrant'
    if (outcome.kind === 'repeated') return this.answerAgain(inc, outcome.answer, outcome.lost)
    if (outcome.kind !== 'done') return outcome
    const { value, counters, handoff } = outcome
    // In this turn, so it leaves before anything committed after it.
    const handing = handoff === null ? null : this.handOn(handoff)
    // The alarm deletes the expiring cells and the stored answer, as no server may be on the room by then, and hands on
    // a commit whose hand-on didn't settle.
    if (outcome.expiring || handoff !== null || lease !== null) await this.scheduleMaintenanceIfNeeded()
    const done: OperationWire<OperationValue[N]> = { kind: 'done', value, counters }
    if (inbox !== undefined) done.registered = true
    if (handoff !== null && handing !== null) done.commit = await receiptOf(handoff, handing)
    return done
  }

  /** A copy of an operation that registered its inbox lease: answered as the operation was, its commit's loss
   *  included, once its hand-on settled. */
  private async answerAgain<Value>(
    inc: string,
    { value, counters, commit }: StoredAnswer<Value>,
    lost: string | null,
  ): Promise<OperationWire<Value>> {
    if (commit === undefined) return { kind: 'done', value, counters, registered: true }
    this.resumeHandoffs()
    const handing = this.handingOn.get(
      handoffKey({ inc, laneKey: encodeLaneKey(OPERATION_COMMIT_LANE), seq: commit.seq }),
    )
    // One this run isn't handing on was handed on before, as its answer says.
    const receipt = await receiptOf(commit, handing ?? Promise.resolve(lost))
    return { kind: 'done', value, counters, commit: receipt, registered: true }
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
    const routes = liveLeases(this.sql(), [routeScope(inc, laneKey)], Date.now(), routeHolder)
    const delivery = this.fanout.send(inc, [laneKey], routes, payload, { seq, timestamp }, true)
    const handed = this.settleHandOn(handoff, delivery)
    this.handingOn.set(key, handed)
    return handed
  }

  /** Once `handoff`'s hand-on settled, its row goes, and the answer of the operation that committed it keeps why it was
   *  lost, if it was, in the same transaction. */
  private async settleHandOn(handoff: Handoff, delivery: Promise<void>): Promise<string | null> {
    const lost = await lostReason(delivery)
    this.transaction((sql) => {
      deleteHandoff(sql, handoff)
      if (lost !== null) recordAnswerLoss(sql, handoff.inc, handoff.seq, lost)
    })
    this.handingOn.delete(handoffKey(handoff))
    return lost
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
    const owed = lane.kind === 'inbox' || lane.kind === 'control'
    const delivery = this.fanout.send(inc, laneKeys, routes, payload, mark, owed)
    const receipt: AcceptedCommitWire = { kind: 'accepted', seq: mark.seq, timestamp: mark.timestamp }
    if (!owed) {
      // No committer awaits a data or binary frame's handoff, so a lost one is reported here.
      void reportIfLost(delivery, roomId, lane)
      return receipt
    }
    const lost =
      lane.kind === 'inbox' && routes.length === 0
        ? `no session holds the inbox of '${lane.member}'`
        : await lostReason(delivery)
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
      return leaseRegistered(registerLease(sql, routeLease(route), order, now))
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
    this.transaction((sql) => {
      if (readLiveHead(sql, Date.now())?.currentInc !== inc) dropIncarnationRows(sql, inc)
    })
    await this.scheduleMaintenanceIfNeeded()
  }

  /** Sends the listing to its region's directory, waiting for the directory's answer; a call that fails leaves it due
   *  for the alarm. With `readVersion`, the version a repair found the directory showing, it sends it also when the
   *  directory has it, unless the listing is still at that version. */
  async directoryShow(readVersion?: string): Promise<void> {
    const now = Date.now()
    this.transaction((sql) => {
      settleListing(sql, now)
      if (readVersion !== undefined) relistOver(sql, Number(readVersion), now)
    })
    await this.scheduleMaintenanceIfNeeded()
    // The listing is stored: what the directory shows of it follows, here or from the alarm.
    await this.showInDirectory().catch(() => {})
  }

  async directoryGet(): Promise<Omit<DirectoryEntry, 'roomId'> | undefined> {
    return readListedEntry(this.sql())
  }

  async alarm(): Promise<void> {
    // Cloudflare unsets the alarm that runs this.
    this.alarmAt = null
    try {
      this.resumeHandoffs(true)
      const now = Date.now()
      this.finishStoppedClose(now)
      this.runSweep(now)
      await this.scheduleMaintenanceIfNeeded()
      await this.showInDirectory().catch(() => {})
    } finally {
      await this.scheduleMaintenanceIfNeeded()
    }
  }

  /** Finishes the close of a closing head whose close lease ran out, as its closer would have (`finishClose()`): in one
   *  transaction its `closed` frame, stored until handed on, and its closed tombstone, which unlists the room. The
   *  frame is handed on to the routes of its incarnation, which the sweep then drops as orphaned. */
  private finishStoppedClose(now: number): void {
    const handoff = this.transaction((sql): Handoff | null => {
      const head = readLiveHead(sql, now)
      if (head?.state !== 'closing' || head.closeLease === undefined || head.closeLease.until >= now) return null
      const inc = head.currentInc
      assert(inc !== null) // a closing head names its incarnation
      const laneKey = encodeLaneKey(CONTROL_LANE)
      const { seq, timestamp } = advanceOrder(sql, inc, laneKey, now)
      const handoff = { inc, laneKey, seq, timestamp, payload: encodeRoomRecord({ __r: 'closed' }) }
      storeHandoff(sql, handoff, now + ROOM_HANDOFF_RESUME_MS)
      const closed = compareExchangeHead(
        sql,
        { form: 'finalize', rev: head.rev, lease: head.closeLease.id },
        {
          head: { rev: crypto.randomUUID(), currentInc: null, state: 'closed', config: head.config },
          ttlMs: ROOM_TOMBSTONE_TTL_MS,
        },
        now,
      )
      assert(closed.kind === 'committed') // the closing head just read, under its own lease
      listWithHead(sql, closed.head, now)
      return handoff
    })
    if (handoff !== null) this.handOn(handoff)
  }

  /** Storage only: a session whose route lapsed or whose incarnation went learns it at its next renewal, and the
   *  listing of a lapsed head goes. */
  private runSweep(now: number): void {
    this.transaction((sql) => {
      settleListing(sql, now)
      const currentInc = readLiveHead(sql, now)?.currentInc ?? null
      deleteLapsedHead(sql, now)
      sweepLeases(sql, now)
      deleteExpiredCells(sql, now)
      deleteExpiredAnswers(sql, now)
      for (const inc of listOrphanIncarnations(sql, currentInc)) dropIncarnationRows(sql, inc)
    })
  }

  /** The listing's unsent version to its region's directory, and each newer one that comes meanwhile; one whose call
   *  fails is due again a retry from now, and rejects this. */
  private showInDirectory(): Promise<void> {
    this.showing ??= this.sendListing().finally(() => {
      this.showing = null
    })
    return this.showing
  }

  private async sendListing(): Promise<void> {
    for (let unsent = readUnsentListing(this.sql()); unsent !== null; unsent = readUnsentListing(this.sql())) {
      const { roomId, incTag, bucket, version } = unsent
      const open = () => openRegionDirectory(this.getNamespace(), bucket)
      try {
        const show = (directory: RegionDirectoryStub) => directory.show(roomId, incTag, version)
        await this.directoryCalls.call(bucket, open, show, { deadline: Date.now() + LEASE_CALL_DEADLINE_MS })
      } catch (error) {
        this.transaction((sql) => resendListing(sql, version, Date.now() + ROOM_MAINTENANCE_RETRY_MS))
        await this.scheduleMaintenanceIfNeeded()
        throw error
      }
      this.transaction((sql) => resendListing(sql, version, null))
      await this.scheduleMaintenanceIfNeeded()
    }
  }

  /** The schema is created outside the transaction, so a refused first write can't roll it back. */
  private transaction<T>(fn: (sql: SqlStore) => T): T {
    const sql = this.sql()
    return this.ctx.storage.transactionSync(() => fn(sql))
  }

  private sql(): SqlStore {
    if (!this.schema) {
      initSchema(this.ctx.storage.sql)
      this.schema = true
    }
    return this.ctx.storage.sql
  }

  /** Sets the alarm to the next deadline in the turn of the write that stored it, with no `await` between them, so
   *  both are stored or neither. */
  private scheduleMaintenanceIfNeeded(): Promise<void> {
    const now = Date.now()
    const deadline = nextMaintenanceDeadline(this.sql(), now)
    const current = this.alarmAt
    if (deadline === null) {
      if (current === null) return Promise.resolve()
      this.alarmAt = null
      return this.ctx.storage.deleteAlarm()
    }
    const nextAlarm = deadline <= now ? now + ROOM_MAINTENANCE_RETRY_MS : deadline
    if (current === nextAlarm) return Promise.resolve()
    // Never postpone an already-scheduled retry for work that is due now.
    if (deadline <= now && current !== undefined && current !== null && current <= nextAlarm) return Promise.resolve()
    this.alarmAt = nextAlarm
    return this.ctx.storage.setAlarm(nextAlarm)
  }
}

/** Orders a commit on `lane` in a transaction that found the incarnation open, and retains it with `retain`. */
function commitIn(
  sql: SqlStore,
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
    routeHolder,
  )
  return { kind: 'accepted', mark, laneKeys, routes }
}

/** An operation's outcome in its transaction. */
type OperationOutcome<Value> =
  | OperationApplied<Value>
  | { kind: 'stale-incarnation' }
  | { kind: 'overtaken' }
  | { kind: 'regrant' }
  | { kind: 'repeated'; answer: StoredAnswer<Value>; lost: string | null }
type OperationApplied<Value> = {
  kind: 'done'
  value: Value
  counters: Map<string, CellCounter>
  /** Its commit, stored until handed on, or null where it committed nothing. */
  handoff: Handoff | null
  expiring: boolean
}

/** A plan that wrote nothing, whose transaction rolls back the inbox lease it registered. */
class NothingWritten {}

function writesNothing({ prefixes, members, mutations, counts, expiring, commit }: OperationWrites): boolean {
  return [prefixes, members, mutations, counts, expiring].every((writes) => writes.length === 0) && commit === null
}

/** Applies a plan's writes in a transaction that found `inc` open; its commit is stored until handed on. */
function applyOperation<Value>(
  sql: SqlStore,
  inc: string,
  value: Value,
  writes: OperationWrites,
  now: number,
): OperationApplied<Value> {
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
  return { kind: 'done', value, counters: exchanged.counters, handoff, expiring: writes.expiring.length > 0 }
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

/** A data or binary frame a subscribed session DO didn't take is reported as lost on its room's lane. */
async function reportIfLost(delivery: Promise<void>, roomId: string, lane: LaneId): Promise<void> {
  try {
    await delivery
  } catch (error) {
    reportLaneLost(roomId, lane, String(error))
  }
}
