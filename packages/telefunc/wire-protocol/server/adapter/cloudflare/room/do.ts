/// <reference types="@cloudflare/workers-types" />
export { RoomAuthority, RoomAuthorityHost }
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
import { dispatchRoomFanout, Fanout, type RoomFanoutNamespace, type RoomFanoutOutcome } from './fanout.js'
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

// Delivery is at-most-once: a failed target is loss, not the publisher's error; its route lapses with its lease.
function reportLostDeliveries(outcomes: RoomFanoutOutcome[]): void {
  const failed = outcomes.filter((outcome) => outcome.error !== undefined)
  if (failed.length > 0)
    console.error(`Cloudflare Room delivery lost to ${failed.length}/${outcomes.length} routes: ${failed[0]!.error}`)
}

/** The room authority role of a Telefunc Durable Object: `sessions` is the namespace its fanout delivers to. */
class RoomAuthority {
  readonly #ctx: DurableObjectState
  readonly #sql: SqlStorage
  readonly #fanout: Fanout
  readonly #sessions: RoomFanoutNamespace

  constructor(ctx: DurableObjectState, sessions: RoomFanoutNamespace) {
    this.#ctx = ctx
    this.#sessions = sessions
    this.#sql = ctx.storage.sql
    initSchema(this.#sql)
    this.#fanout = new Fanout(async (routes, payload, { seq, timestamp }) => {
      const request = { path: 'root', routes, payload, seq, timestamp }
      reportLostDeliveries(await dispatchRoomFanout(this.#sessions, request))
    })
  }

  async readHead(): Promise<RoomHead | null> {
    const head = readLiveHead(this.#sql, Date.now())
    return head === null ? null : headForRpc(head)
  }

  async compareExchangeHead(cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    const now = Date.now()
    const outcome = this.#ctx.storage.transactionSync(() =>
      compareExchangeHead(this.#sql, cx, next, now, () => crypto.randomUUID()),
    )
    await this.#scheduleMaintenanceIfNeeded()
    if ('conflict' in outcome)
      return { conflict: true, current: outcome.current === null ? null : headForRpc(outcome.current) }
    return { head: headForRpc(outcome.head) }
  }

  async readCells(inc: string, sel: CellSelector): Promise<CellsRead> {
    return readCells(this.#sql, inc, sel, Date.now())
  }

  async compareExchangeCells(inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult> {
    const now = Date.now()
    return this.#ctx.storage.transactionSync(() => compareExchangeCells(this.#sql, inc, revision, mutations, now))
  }

  async commitLane(inc: string, lane: LaneId, payload: Uint8Array, opts?: CommitOptions): Promise<CommitWire> {
    const now = Date.now()
    const key = encodeLaneKey(lane)
    const outcome = this.#ctx.storage.transactionSync(
      (): StaleCommit | { seq: number; timestamp: number; routes: RouteInstallation[] } => {
        if (!commitPreconditionHolds(readLiveHead(this.#sql, now), inc, lane.kind, opts?.closingLease, now))
          return { stale: 'incarnation' }
        if (opts?.requiredCellKeys !== undefined) {
          const required = readCells(this.#sql, inc, { keys: opts.requiredCellKeys }, now)
          assert(!('staleInc' in required)) // the precondition just found this incarnation open
          const missing = opts.requiredCellKeys.find((cell) => !required.cells.has(cell))
          if (missing !== undefined) return { stale: 'cell', key: missing }
        }
        const mark = advanceOrder(this.#sql, inc, key, now)
        if (opts?.retain === true) installRetained(this.#sql, inc, key, payload, mark)
        return { ...mark, routes: snapshotRoutes(this.#sql, inc, key, now) }
      },
    )
    if ('stale' in outcome) return outcome
    const { seq, timestamp, routes } = outcome
    const deliveryToken = this.#fanout.enqueue(routes, payload, { inc, laneKey: key, seq, timestamp })
    return { accepted: true, seq, timestamp, receivers: routes.length, deliveryToken }
  }

  async awaitDelivery(token: string): Promise<void> {
    await this.#fanout.await(token)
  }

  async readRetained(inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    return readRetained(this.#sql, inc, lane)
  }

  async listRetained(inc: string): Promise<LaneId[]> {
    return listRetained(this.#sql, inc)
  }

  async deleteRetained(inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this.#ctx.storage.transactionSync(() => deleteRetained(this.#sql, inc, lane, opts))
  }

  async registerRoute(route: RouteInstallation): Promise<RegisterWire> {
    const result = this.#ctx.storage.transactionSync((): RegisterWire => {
      const now = Date.now()
      const head = readLiveHead(this.#sql, now)
      if (!isOpenIncarnation(head, route.inc))
        return { rejected: true, reason: `room has no open incarnation '${route.inc}'` }
      upsertRoute(this.#sql, route, now)
      return { ok: true }
    })
    if ('ok' in result) await this.#scheduleMaintenanceIfNeeded()
    return result
  }

  /** Fails once the route lapsed or its generation was dropped; the session then ends the attempt. */
  async renewRoute(route: RouteInstallation): Promise<boolean> {
    const now = Date.now()
    const renewed = this.#ctx.storage.transactionSync(() => renewRoute(this.#sql, route, now))
    await this.#scheduleMaintenanceIfNeeded()
    return renewed
  }

  async unsubscribeRoute(route: RouteInstallation): Promise<void> {
    this.#ctx.storage.transactionSync(() => deleteRoute(this.#sql, route))
    await this.#scheduleMaintenanceIfNeeded()
  }

  async dropGeneration(inc: string): Promise<void> {
    this.#dropGenerationNow(inc)
    await this.#scheduleMaintenanceIfNeeded()
  }

  #dropGenerationNow(inc: string): void {
    this.#ctx.storage.transactionSync(() => dropGenerationRows(this.#sql, inc))
    this.#fanout.clearIncarnation(inc)
  }

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this.#ctx.storage.transactionSync(() => directoryPut(this.#sql, roomId, incTag))
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.#ctx.storage.transactionSync(() => directoryDelete(this.#sql, roomId, incTag))
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    return directoryList(this.#sql, prefix, cursor)
  }

  async alarm(): Promise<void> {
    try {
      this.#runSweep(Date.now())
    } finally {
      await this.#scheduleMaintenanceIfNeeded()
    }
  }

  /** Storage only: a session whose route lapsed or whose generation went learns it at its next renewal. */
  #runSweep(now: number): void {
    const orphanIncs = this.#ctx.storage.transactionSync(() => {
      const currentInc = readLiveHead(this.#sql, now)?.currentInc ?? null
      deleteLapsedTombstone(this.#sql, now)
      deleteExpiredRoutes(this.#sql, now)
      return listOrphanGenerations(this.#sql, currentInc)
    })
    for (const inc of orphanIncs) this.#dropGenerationNow(inc)
  }

  async #scheduleMaintenanceIfNeeded(): Promise<void> {
    const now = Date.now()
    const deadline = nextMaintenanceDeadline(this.#sql, now)
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

function nextMaintenanceDeadline(sql: SqlStorage, now: number): number | null {
  const deadlines = [
    sql.exec<{ deadline: number | null }>('SELECT MIN(expires_at) AS deadline FROM head').toArray()[0]?.deadline,
    sql.exec<{ deadline: number | null }>('SELECT MIN(expires_at) AS deadline FROM route').toArray()[0]?.deadline,
  ].filter((deadline): deadline is number => deadline !== null && deadline !== undefined)
  if (hasOrphanGeneration(sql, readLiveHead(sql, now)?.currentInc ?? null)) deadlines.push(now)
  return deadlines.length === 0 ? null : Math.min(...deadlines)
}

/** A Durable Object that can act as a room authority: the role, built on first use, fans out through `sessions`. */
class RoomAuthorityHost<Env = unknown> extends DurableObject<Env> {
  readonly #sessions: RoomFanoutNamespace
  #role: RoomAuthority | null = null

  constructor(ctx: DurableObjectState, env: Env, sessions: RoomFanoutNamespace) {
    super(ctx, env)
    this.#sessions = sessions
  }

  readHead(...args: Parameters<RoomAuthority['readHead']>) {
    return this.#authority().readHead(...args)
  }

  compareExchangeHead(...args: Parameters<RoomAuthority['compareExchangeHead']>) {
    return this.#authority().compareExchangeHead(...args)
  }

  readCells(...args: Parameters<RoomAuthority['readCells']>) {
    return this.#authority().readCells(...args)
  }

  compareExchangeCells(...args: Parameters<RoomAuthority['compareExchangeCells']>) {
    return this.#authority().compareExchangeCells(...args)
  }

  commitLane(...args: Parameters<RoomAuthority['commitLane']>) {
    return this.#authority().commitLane(...args)
  }

  awaitDelivery(...args: Parameters<RoomAuthority['awaitDelivery']>) {
    return this.#authority().awaitDelivery(...args)
  }

  readRetained(...args: Parameters<RoomAuthority['readRetained']>) {
    return this.#authority().readRetained(...args)
  }

  listRetained(...args: Parameters<RoomAuthority['listRetained']>) {
    return this.#authority().listRetained(...args)
  }

  deleteRetained(...args: Parameters<RoomAuthority['deleteRetained']>) {
    return this.#authority().deleteRetained(...args)
  }

  registerRoute(...args: Parameters<RoomAuthority['registerRoute']>) {
    return this.#authority().registerRoute(...args)
  }

  renewRoute(...args: Parameters<RoomAuthority['renewRoute']>) {
    return this.#authority().renewRoute(...args)
  }

  unsubscribeRoute(...args: Parameters<RoomAuthority['unsubscribeRoute']>) {
    return this.#authority().unsubscribeRoute(...args)
  }

  dropGeneration(...args: Parameters<RoomAuthority['dropGeneration']>) {
    return this.#authority().dropGeneration(...args)
  }

  directoryPut(...args: Parameters<RoomAuthority['directoryPut']>) {
    return this.#authority().directoryPut(...args)
  }

  directoryDelete(...args: Parameters<RoomAuthority['directoryDelete']>) {
    return this.#authority().directoryDelete(...args)
  }

  directoryList(...args: Parameters<RoomAuthority['directoryList']>) {
    return this.#authority().directoryList(...args)
  }

  // Only a room authority schedules alarms.
  alarm() {
    return this.#authority().alarm()
  }

  #authority(): RoomAuthority {
    return (this.#role ??= new RoomAuthority(this.ctx, this.#sessions))
  }
}
