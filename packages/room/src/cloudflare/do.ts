/// <reference types="@cloudflare/workers-types" />
export { RoomAuthority }
export type { AcceptedCommitWire, CloudflareRoomAuthorityStub, CommitWire, RegisterWire }

// A room's authority owns its time, state and fanout.

import type {
  CellsCx,
  CxResult,
  HeadCx,
  HeadCxResult,
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
import type { OrderingInfo, TelefuncExtensionNamespace } from 'telefunc/__internal'
import { assert } from '../utils/assert.js'
import { encodeLaneKey, everyMemberLaneKey, everyTrackLaneKey } from '../backend/laneKey.js'
import { commitPreconditionHolds, isOpenIncarnation, publicHead } from '../backend/semantics.js'
import { Fanout } from './Fanout.js'
import { deleteRetained, installRetained, listRetained, readRetained } from './retained.js'
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
  compareExchangeCells,
  compareExchangeHead,
  directoryDelete,
  directoryDue,
  directoryGet,
  directoryList,
  directoryPut,
  dropGenerationRows,
  dropLaneOrderRows,
  initSchema,
  deleteLapsedHead,
  listOrphanGenerations,
  nextMaintenanceDeadline,
  readCells,
  readCellPage,
  readLiveHead,
  readOrder,
  type CellPage,
} from './storage.js'

type AcceptedCommitWire = { accepted: true; seq: number; timestamp: number; receivers: number; deliveryToken?: string }
type CommitWire = AcceptedCommitWire | StaleCommit

type RegisterWire = { ok: true } | { rejected: true; reason: string }

/** The room authority's methods, which its Durable Object serves over RPC. */
type CloudflareRoomAuthorityStub = Omit<RoomAuthority, 'alarm'>

const ROOM_MAINTENANCE_RETRY_MS = 30_000

/** The role of a Durable Object that is a room authority, or the room directory, fanning out to the session DOs of
 *  `sessions`. Its tables are created on first use. */
class RoomAuthority {
  readonly #ctx: DurableObjectState
  readonly #fanout: Fanout
  #schema = false

  constructor(ctx: DurableObjectState, sessions: TelefuncExtensionNamespace) {
    this.#ctx = ctx
    this.#fanout = new Fanout(sessions)
  }

  async readHead(): Promise<RoomHead | null> {
    const head = readLiveHead(this.#sql(), Date.now())
    return head === null ? null : publicHead(head, head.config)
  }

  async compareExchangeHead(cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    const now = Date.now()
    const outcome = this.#transaction((sql) => compareExchangeHead(sql, cx, next, now))
    await this.#scheduleMaintenanceIfNeeded()
    if ('head' in outcome) return { head: publicHead(outcome.head, outcome.head.config) }
    const { current } = outcome
    return { conflict: true, current: current === null ? null : publicHead(current, current.config) }
  }

  async readCells(inc: string, sel: CellSelector): Promise<CellsRead> {
    return readCells(this.#sql(), inc, sel, Date.now())
  }

  async readCellPage(inc: string, prefix: string, cursor?: string): Promise<CellPage> {
    return readCellPage(this.#sql(), inc, prefix, cursor, Date.now())
  }

  async compareExchangeCells(inc: string, cx: CellsCx): Promise<CxResult> {
    const now = Date.now()
    return this.#transaction((sql) => compareExchangeCells(sql, inc, cx, now))
  }

  async commitLane(inc: string, lane: LaneId, payload: Uint8Array, opts?: CommitOptions): Promise<CommitWire> {
    const now = Date.now()
    const key = encodeLaneKey(lane)
    const outcome = this.#transaction((sql): StaleCommit | { mark: OrderingInfo; routes: RouteInstallation[] } => {
      if (!commitPreconditionHolds(readLiveHead(sql, now), inc, lane.kind, opts?.closingLease, now)) {
        return { stale: 'incarnation' }
      }
      if (opts?.requiredCellKeys !== undefined) {
        const required = readCells(sql, inc, { keys: opts.requiredCellKeys }, now)
        assert(!('staleInc' in required)) // the precondition just found this incarnation open
        const missing = opts.requiredCellKeys.find((cell) => !required.cells.has(cell))
        if (missing !== undefined) return { stale: 'cell', key: missing }
      }
      const mark = advanceOrder(sql, inc, key, now)
      if (opts?.retain === true) installRetained(sql, inc, lane, payload, mark)
      // A binary commit also reaches its member's every-track lane and its track's every-member lane.
      const laneKeys =
        lane.kind === 'binary' ? [key, everyTrackLaneKey(lane.member), everyMemberLaneKey(lane.track)] : [key]
      return { mark, routes: snapshotRoutes(sql, inc, laneKeys, now) }
    })
    if ('stale' in outcome) return outcome
    const { mark, routes } = outcome
    const delivery = this.#fanout.send(routes, payload, mark)
    const receipt: AcceptedCommitWire = {
      accepted: true,
      seq: mark.seq,
      timestamp: mark.timestamp,
      receivers: routes.length,
    }
    if (lane.kind === 'inbox' || lane.kind === 'control') receipt.deliveryToken = this.#fanout.tokenOf(delivery)
    return receipt
  }

  async awaitDelivery(token: string): Promise<void> {
    await this.#fanout.awaitDelivery(token)
  }

  async readLaneOrder(inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    return readOrder(this.#sql(), inc, encodeLaneKey(lane))
  }

  async readRetained(inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    return readRetained(this.#sql(), inc, lane)
  }

  async listRetained(inc: string, of?: RetainedOf): Promise<LaneId[]> {
    return listRetained(this.#sql(), inc, of)
  }

  async deleteRetained(inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this.#transaction((sql) => deleteRetained(sql, inc, lane, opts))
  }

  async registerRoute(route: RouteInstallation): Promise<RegisterWire> {
    const now = Date.now()
    const result = this.#transaction((sql): RegisterWire => {
      if (!isOpenIncarnation(readLiveHead(sql, now), route.inc)) {
        return { rejected: true, reason: `room has no open incarnation '${route.inc}'` }
      }
      upsertRoute(sql, route, now)
      return { ok: true }
    })
    if ('ok' in result) await this.#scheduleMaintenanceIfNeeded()
    return result
  }

  /** Fails once the route lapsed or its generation was dropped; the session then ends the attempt. */
  async renewRoute(route: RouteInstallation): Promise<boolean> {
    const now = Date.now()
    const renewed = this.#transaction((sql) => renewRoute(sql, route, now))
    await this.#scheduleMaintenanceIfNeeded()
    return renewed
  }

  async unsubscribeRoute(route: RouteInstallation): Promise<void> {
    this.#transaction((sql) => deleteRoute(sql, route))
    await this.#scheduleMaintenanceIfNeeded()
  }

  async dropLaneOrders(inc: string, member: string): Promise<void> {
    this.#transaction((sql) => dropLaneOrderRows(sql, inc, member))
  }

  async dropGeneration(inc: string): Promise<void> {
    this.#transaction((sql) => dropGenerationRows(sql, inc))
    await this.#scheduleMaintenanceIfNeeded()
  }

  async directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    const at = Math.max(checkAt, Date.now() + minDelayMs)
    this.#transaction((sql) => directoryPut(sql, roomId, incTag, at))
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.#transaction((sql) => directoryDelete(sql, roomId, incTag))
  }

  async directoryGet(roomId: string): Promise<string | undefined> {
    return directoryGet(this.#sql(), roomId)
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    return directoryList(this.#sql(), prefix, cursor)
  }

  async directoryDue(): Promise<DirectoryPage['entries']> {
    return directoryDue(this.#sql(), Date.now())
  }

  /** Only a room authority schedules alarms. */
  async alarm(): Promise<void> {
    try {
      this.#runSweep(Date.now())
    } finally {
      await this.#scheduleMaintenanceIfNeeded()
    }
  }

  /** Storage only: a session whose route lapsed or whose generation went learns it at its next renewal. */
  #runSweep(now: number): void {
    this.#transaction((sql) => {
      const currentInc = readLiveHead(sql, now)?.currentInc ?? null
      deleteLapsedHead(sql, now)
      deleteExpiredRoutes(sql, now)
      for (const inc of listOrphanGenerations(sql, currentInc)) dropGenerationRows(sql, inc)
    })
  }

  /** The schema is created outside the transaction, so a refused first write can't roll it back. */
  #transaction<T>(fn: (sql: SqlStorage) => T): T {
    const sql = this.#sql()
    return this.#ctx.storage.transactionSync(() => fn(sql))
  }

  #sql(): SqlStorage {
    if (!this.#schema) {
      initSchema(this.#ctx.storage.sql)
      this.#schema = true
    }
    return this.#ctx.storage.sql
  }

  async #scheduleMaintenanceIfNeeded(): Promise<void> {
    const now = Date.now()
    const deadline = nextMaintenanceDeadline(this.#sql(), now)
    const currentAlarm = await this.#ctx.storage.getAlarm()
    if (deadline === null) {
      if (currentAlarm !== null) await this.#ctx.storage.deleteAlarm()
      return
    }
    const nextAlarm = deadline <= now ? now + ROOM_MAINTENANCE_RETRY_MS : deadline
    if (currentAlarm === nextAlarm) return
    // Never postpone an already-scheduled retry for work that is due now.
    if (deadline <= now && currentAlarm !== null && currentAlarm <= nextAlarm) return
    await this.#ctx.storage.setAlarm(nextAlarm)
  }
}
