/// <reference types="@cloudflare/workers-types" />
export { RoomAuthority }
export type { AcceptedCommitWire, CloudflareRoomAuthorityStub, CommitWire, OperationWire, RegisterWire, RenewWire }

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
} from '../backend/contract.js'
import {
  OrderedStubs,
  type CloudflareExtensionKit,
  type CloudflareRegions,
  type OrderingInfo,
  type TelefuncExtensionNamespace,
} from 'telefunc/__internal'
import { assert } from '../utils/assert.js'
import { OPERATION_COMMIT_LANE, recordOperation } from '../backend/operation.js'
import { ROOM_OPERATIONS } from '../server/store/roomOperations.js'
import { raceTimeout } from '../utils/raceTimeout.js'
import { ROOM_HORIZON_MS } from '../server/constants.js'
import { encodeLaneKey, everyMemberLaneKey, everyTrackLaneKey } from '../backend/laneKey.js'
import { commitPreconditionHolds, isOpenIncarnation, publicHead } from '../backend/semantics.js'
import { Fanout } from './Fanout.js'
import { openRegionDirectory, type RegionDirectoryStub } from './objects.js'
import { deleteRetained, installRetained, listRetained, readRetained, retainedOwner } from './retained.js'
import {
  deleteExpiredRoutes,
  deleteRoute,
  renewRoute,
  snapshotRoutes,
  type RouteInstallation,
  upsertRoute,
} from './routes.js'
import {
  advanceOrder,
  claimListingBucket,
  compareExchangeCells,
  compareExchangeHead,
  dropCellPrefix,
  directoryDelete,
  directoryGet,
  directoryList,
  directoryPut,
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
  readListingBucket,
  readOrder,
  settleDueListing,
  type CellPage,
} from './storage.js'

/** An inbox or control commit replies once its delivery was handed off, with why it was lost, if it was. */
type AcceptedCommitWire = {
  kind: 'accepted'
  seq: number
  timestamp: number
  lost?: string
}
/** A commit the transaction accepted: its order, the lanes it reaches, and the routes subscribed to them then. */
type AcceptedCommit = { kind: 'accepted'; mark: OrderingInfo; laneKeys: string[]; routes: RouteInstallation[] }
type CommitWire = AcceptedCommitWire | StaleCommit
/** An operation that ran, with its commit's receipt, or its incarnation stale. */
type OperationWire =
  | { kind: 'done'; value: unknown; counters: Map<string, CellCounter>; commit?: AcceptedCommitWire }
  | { kind: 'stale-incarnation' }

/** A route registered, or refused because its incarnation is dropped or closing. */
type RegisterWire = 'registered' | 'dropped'
/** A route renewed, lapsed on its own, or deleted with its dropped incarnation. */
type RenewWire = 'renewed' | 'lapsed' | 'dropped'

/** The room authority's methods, which its Durable Object serves over RPC. */
type CloudflareRoomAuthorityStub = Omit<RoomAuthority, 'alarm'>

const ROOM_MAINTENANCE_RETRY_MS = 30_000
const SEMANTIC_LANE: LaneId = { kind: 'semantic' }

/** Room's part of a Durable Object that is a room authority, fanning out through Broadcast's `regions`, or a region's
 *  directory. Its tables are created on first use. */
class RoomAuthority {
  private readonly ctx: DurableObjectState
  private readonly regions: CloudflareRegions
  private readonly getNamespace: () => TelefuncExtensionNamespace
  private readonly fanout: Fanout
  /** Its calls to its listing's region directory, which reach it in the order the listing changed. */
  private readonly directoryCalls = new OrderedStubs<RegionDirectoryStub>()
  private schema = false

  constructor(ctx: DurableObjectState, { regions, getNamespace }: CloudflareExtensionKit) {
    this.ctx = ctx
    this.regions = regions
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

  async readCellPage(inc: string, prefix: string, cursor?: string): Promise<CellPage> {
    return readCellPage(this.sql(), inc, prefix, cursor, Date.now())
  }

  async compareExchangeCells(inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    const now = Date.now()
    return this.transaction((sql) => compareExchangeCells(sql, inc, exchange, now))
  }

  async commitLane(inc: string, lane: LaneId, payload: Uint8Array, opts?: CommitOptions): Promise<CommitWire> {
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
    return await this.deliver(inc, lane, payload, outcome)
  }

  /** Runs the operation `name` names in one transaction, which a plan that throws rolls back, then fans its commit
   *  out. */
  async runOperation(inc: string, name: string, args: unknown): Promise<OperationWire> {
    const operation = ROOM_OPERATIONS.get(name)
    assert(operation !== undefined)
    const now = Date.now()
    const outcome = this.transaction((sql) => {
      if (!isOpenIncarnation(readLiveHead(sql, now), inc)) return { kind: 'stale-incarnation' } as const
      const { value, writes } = recordOperation(operation, args as never, {
        now,
        cell: (key) => selectCell(sql, inc, key, now),
      })
      for (const prefix of writes.prefixes) dropCellPrefix(sql, inc, prefix)
      const exchange = { expect: [], mutations: writes.mutations, counts: writes.counts }
      const exchanged = compareExchangeCells(sql, inc, exchange, now)
      assert(exchanged.kind === 'committed') // nothing expected, in an incarnation found open
      for (const member of writes.members) {
        dropLaneOrderRows(sql, inc, member)
        for (const lane of listRetained(sql, inc, { member })) deleteRetained(sql, inc, lane)
        if (retainedOwner(sql, inc, SEMANTIC_LANE) === member) deleteRetained(sql, inc, SEMANTIC_LANE)
      }
      const commit = writes.commit === null ? null : commitIn(sql, inc, OPERATION_COMMIT_LANE, writes.commit, {}, now)
      return { kind: 'done', value, counters: exchanged.counters, commit, payload: writes.commit } as const
    })
    if (outcome.kind !== 'done') return outcome
    const { value, counters, commit, payload } = outcome
    if (commit === null || payload === null) return { kind: 'done', value, counters }
    return { kind: 'done', value, counters, commit: await this.deliver(inc, OPERATION_COMMIT_LANE, payload, commit) }
  }

  /** Fans a commit out; an inbox or control commit's receipt waits for its handoff. */
  private async deliver(
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    { mark, laneKeys, routes }: AcceptedCommit,
  ): Promise<AcceptedCommitWire> {
    const delivery = this.fanout.send(inc, laneKeys, routes, payload, mark)
    const receipt: AcceptedCommitWire = { kind: 'accepted', seq: mark.seq, timestamp: mark.timestamp }
    if (lane.kind !== 'inbox' && lane.kind !== 'control') return receipt
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

  async registerRoute(route: RouteInstallation): Promise<RegisterWire> {
    const now = Date.now()
    const registration = this.transaction((sql): RegisterWire => {
      if (!isOpenIncarnation(readLiveHead(sql, now), route.inc)) return 'dropped'
      upsertRoute(sql, route, now)
      return 'registered'
    })
    if (registration === 'registered') await this.scheduleMaintenanceIfNeeded()
    return registration
  }

  /** Renews the route, or says it lapsed or went with its dropped incarnation; the session then ends its
   *  subscription. */
  async renewRoute(route: RouteInstallation): Promise<RenewWire> {
    const now = Date.now()
    const renewal = this.transaction((sql): RenewWire => {
      if (renewRoute(sql, route, now)) return 'renewed'
      return hasIncarnation(sql, route.inc) ? 'lapsed' : 'dropped'
    })
    await this.scheduleMaintenanceIfNeeded()
    return renewal
  }

  async unsubscribeRoute(route: RouteInstallation): Promise<void> {
    this.transaction((sql) => deleteRoute(sql, route))
    await this.scheduleMaintenanceIfNeeded()
  }

  async dropIncarnation(inc: string): Promise<void> {
    this.transaction((sql) => dropIncarnationRows(sql, inc))
    await this.scheduleMaintenanceIfNeeded()
  }

  /** At a room's authority: lists the room, in the region directory of `bucket` if it is the room's first listing. */
  async directoryPut(
    roomId: string,
    incTag: string,
    checkAt: number,
    minDelayMs: number,
    bucket: string,
  ): Promise<void> {
    const at = Math.max(checkAt, Date.now() + minDelayMs)
    const listedIn = this.transaction((sql) => {
      directoryPut(sql, roomId, incTag, at)
      return claimListingBucket(sql, bucket)
    })
    await this.callDirectory(listedIn, (directory) => directory.listInRegion(roomId, incTag))
    await this.scheduleMaintenanceIfNeeded()
  }

  /** At a room's authority. Its region directory forgets `incTag` even when the listing here is gone or newer, as after
   *  an alarm that couldn't reach it. */
  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.transaction((sql) => directoryDelete(sql, roomId, incTag))
    await this.unlistInItsRegion(roomId, incTag)
  }

  async directoryGet(roomId: string): Promise<string | undefined> {
    return directoryGet(this.sql(), roomId)
  }

  /** At a region's directory. */
  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    return directoryList(this.sql(), prefix, cursor)
  }

  /** At a region's directory. */
  async listInRegion(roomId: string, incTag: string): Promise<void> {
    this.transaction((sql) => directoryPut(sql, roomId, incTag, Infinity))
  }

  /** At a region's directory. */
  async unlistInRegion(roomId: string, incTag: string): Promise<void> {
    this.transaction((sql) => directoryDelete(sql, roomId, incTag))
  }

  /** Only a room authority schedules alarms. */
  async alarm(): Promise<void> {
    try {
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
      deleteExpiredRoutes(sql, now)
      for (const inc of listOrphanIncarnations(sql, currentInc)) dropIncarnationRows(sql, inc)
      return unlisted
    })
  }

  private async unlistInItsRegion(roomId: string, incTag: string): Promise<void> {
    const listedIn = readListingBucket(this.sql())
    if (listedIn !== null) await this.callDirectory(listedIn, (directory) => directory.unlistInRegion(roomId, incTag))
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
  return { kind: 'accepted', mark, laneKeys, routes: snapshotRoutes(sql, inc, laneKeys, now) }
}

function selectCell(sql: SqlStorage, inc: string, key: string, now: number): Uint8Array | undefined {
  const read = readCells(sql, inc, { keys: [key] }, now)
  assert(read.kind === 'read') // the operation found this incarnation open
  return read.cells.get(key)
}

/** Null once each session DO took the delivery, else why it was lost: a handoff unconfirmed within the horizon is. */
function lostReason(delivery: Promise<void>): Promise<string | null> {
  const settled = delivery.then(
    () => null,
    (reason: unknown) => String(reason),
  )
  return raceTimeout(settled, ROOM_HORIZON_MS, () => 'unconfirmed within the horizon')
}
