/// <reference types="@cloudflare/workers-types" />
export { RoomAuthority }
export type { CommitWire, RegisterWire }

// A room's authority owns its time, state and fanout; transaction rollback and RPC structured clone preserve SPI outcomes.

import { DurableObject } from 'cloudflare:workers'
import type {
  CellMutation,
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
  DirectoryPage,
} from '../../../../backend/room/contract.js'
import { assert } from '../../../../../utils/assert.js'
import { encodeLaneKey } from '../../../../backend/room/lane-key.js'
import { commitPreconditionHolds, isOpenIncarnation, type StoredHead } from '../../../../backend/room/semantics.js'
import { Fanout, type RoomSessionNamespace } from './fanout.js'
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
  directoryList,
  directoryPut,
  dropGenerationRows,
  initSchema,
  deleteLapsedTombstone,
  hasOrphanGeneration,
  listOrphanGenerations,
  readCells,
  readLiveHead,
} from './storage.js'

type CommitWire =
  | { accepted: true; seq: number; timestamp: number; receivers: number; deliveryToken: string }
  | StaleCommit
type RegisterWire = { ok: true } | { rejected: true; reason: string }

const ROOM_MAINTENANCE_RETRY_MS = 30_000

function headForRpc(head: StoredHead): RoomHead {
  return {
    rev: head.rev,
    currentInc: head.currentInc,
    state: head.state,
    config: head.config,
    ...(head.closeLease === undefined ? {} : { closeLease: { ...head.closeLease } }),
  }
}

/** A Durable Object that can act as a room authority, fanning out to the session DOs of `sessions`. Its tables are
 *  created on first use, so a DO in another role never has them. */
class RoomAuthority<Env = unknown> extends DurableObject<Env> {
  readonly #fanout: Fanout
  #schema = false

  constructor(ctx: DurableObjectState, env: Env, sessions: RoomSessionNamespace) {
    super(ctx, env)
    this.#fanout = new Fanout(sessions)
  }

  async readHead(): Promise<RoomHead | null> {
    const head = readLiveHead(this.#sql(), Date.now())
    return head === null ? null : headForRpc(head)
  }

  async compareExchangeHead(cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    const now = Date.now()
    const outcome = this.#transaction((sql) => compareExchangeHead(sql, cx, next, now, () => crypto.randomUUID()))
    await this.#scheduleMaintenanceIfNeeded()
    if ('conflict' in outcome)
      return { conflict: true, current: outcome.current === null ? null : headForRpc(outcome.current) }
    return { head: headForRpc(outcome.head) }
  }

  async readCells(inc: string, sel: CellSelector): Promise<CellsRead> {
    return readCells(this.#sql(), inc, sel, Date.now())
  }

  async compareExchangeCells(inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult> {
    const now = Date.now()
    return this.#transaction((sql) => compareExchangeCells(sql, inc, revision, mutations, now))
  }

  async commitLane(inc: string, lane: LaneId, payload: Uint8Array, opts?: CommitOptions): Promise<CommitWire> {
    const now = Date.now()
    const key = encodeLaneKey(lane)
    const outcome = this.#transaction(
      (sql): StaleCommit | { seq: number; timestamp: number; routes: RouteInstallation[] } => {
        if (!commitPreconditionHolds(readLiveHead(sql, now), inc, lane.kind, opts?.closingLease, now))
          return { stale: 'incarnation' }
        if (opts?.requiredCellKeys !== undefined) {
          const required = readCells(sql, inc, { keys: opts.requiredCellKeys }, now)
          assert(!('staleInc' in required)) // the precondition just found this incarnation open
          const missing = opts.requiredCellKeys.find((cell) => !required.cells.has(cell))
          if (missing !== undefined) return { stale: 'cell', key: missing }
        }
        const mark = advanceOrder(sql, inc, key, now)
        if (opts?.retain === true) installRetained(sql, inc, key, payload, mark)
        return { ...mark, routes: snapshotRoutes(sql, inc, key, now) }
      },
    )
    if ('stale' in outcome) return outcome
    const { seq, timestamp, routes } = outcome
    const deliveryToken = this.#fanout.send(routes, payload, seq, timestamp)
    return { accepted: true, seq, timestamp, receivers: routes.length, deliveryToken }
  }

  async awaitDelivery(token: string): Promise<void> {
    await this.#fanout.await(token)
  }

  async readRetained(inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    return readRetained(this.#sql(), inc, lane)
  }

  async listRetained(inc: string): Promise<LaneId[]> {
    return listRetained(this.#sql(), inc)
  }

  async deleteRetained(inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this.#transaction((sql) => deleteRetained(sql, inc, lane, opts))
  }

  async registerRoute(route: RouteInstallation): Promise<RegisterWire> {
    const result = this.#transaction((sql): RegisterWire => {
      const now = Date.now()
      if (!isOpenIncarnation(readLiveHead(sql, now), route.inc))
        return { rejected: true, reason: `room has no open incarnation '${route.inc}'` }
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

  async dropGeneration(inc: string): Promise<void> {
    this.#transaction((sql) => dropGenerationRows(sql, inc))
    await this.#scheduleMaintenanceIfNeeded()
  }

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this.#transaction((sql) => directoryPut(sql, roomId, incTag))
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.#transaction((sql) => directoryDelete(sql, roomId, incTag))
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    return directoryList(this.#sql(), prefix, cursor)
  }

  // Only a room authority schedules alarms.
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
      deleteLapsedTombstone(sql, now)
      deleteExpiredRoutes(sql, now)
      for (const inc of listOrphanGenerations(sql, currentInc)) dropGenerationRows(sql, inc)
    })
  }

  /** The schema is created outside the transaction, so a refused first write can't roll it back. */
  #transaction<T>(fn: (sql: SqlStorage) => T): T {
    const sql = this.#sql()
    return this.ctx.storage.transactionSync(() => fn(sql))
  }

  #sql(): SqlStorage {
    if (!this.#schema) {
      initSchema(this.ctx.storage.sql)
      this.#schema = true
    }
    return this.ctx.storage.sql
  }

  async #scheduleMaintenanceIfNeeded(): Promise<void> {
    const now = Date.now()
    const deadline = nextMaintenanceDeadline(this.#sql(), now)
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

function nextMaintenanceDeadline(sql: SqlStorage, now: number): number | null {
  const deadlines = [
    sql.exec<{ deadline: number | null }>('SELECT MIN(expires_at) AS deadline FROM head').toArray()[0]?.deadline,
    sql.exec<{ deadline: number | null }>('SELECT MIN(expires_at) AS deadline FROM route').toArray()[0]?.deadline,
  ].filter((deadline): deadline is number => deadline !== null && deadline !== undefined)
  if (hasOrphanGeneration(sql, readLiveHead(sql, now)?.currentInc ?? null)) deadlines.push(now)
  return deadlines.length === 0 ? null : Math.min(...deadlines)
}
