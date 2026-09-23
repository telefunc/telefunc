/// <reference types="@cloudflare/workers-types" />
// One Room DO owns authority time/state/fanout; transaction rollback and RPC structured clone preserve SPI outcomes.

import { DurableObject } from 'cloudflare:workers'
import type {
  CellMutation,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  RoomHead,
  StaleCommit,
} from '../../../../backend/room/contract.js'
import { assert } from '../../../../../utils/assert.js'
import { encodeLaneKey } from '../../../../backend/room/lane-key.js'
import { commitPreconditionHolds } from '../../../../backend/room/semantics.js'
import { dispatchRoomFanout, Fanout, type RoomFanoutNamespace, type RoomFanoutOutcome } from './fanout.js'
import { deleteRetained, installRetained, listRetained, readRetained } from './retained.js'
import {
  deleteRoute,
  listExpiredRouteInstallations,
  listRouteInstallations,
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
  listOrphanGenerations,
  readCells,
  hasGeneration,
  readLiveHead,
  type StoredHead,
} from './storage.js'

export type HeadCxResult = { ok: true; head: RoomHead } | { conflict: true; current: RoomHead | null }
export type CellsResult = { revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }
export type CommitWire =
  | { accepted: true; seq: number; timestamp: number; receivers: number; deliveryToken: string }
  | StaleCommit
export type RetainedResult = { payload: Uint8Array; seq: number; timestamp: number }
export type RegisterWire = { ok: true } | { rejected: true; reason: string; terminal?: boolean }
/** The session Durable Object namespace fan-out delivers to, as the adapter scopes it. */
export type SessionNamespaceResolver = (env: unknown) => DurableObjectNamespace

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

export class TelefuncRoomDurableObject extends DurableObject {
  readonly #sql: SqlStorage
  readonly #fanout: Fanout
  readonly #sessions: RoomFanoutNamespace

  constructor(ctx: DurableObjectState, env: unknown, sessionNamespace: SessionNamespaceResolver) {
    super(ctx, env as never)
    this.#sessions = sessionNamespace(env) as unknown as RoomFanoutNamespace
    this.#sql = ctx.storage.sql
    initSchema(this.#sql)
    this.#fanout = new Fanout(
      async (routes, payload, { seq, timestamp }) => {
        const request = { operation: 'deliver' as const, path: 'root', routes, payload, seq, timestamp }
        reportLostDeliveries(await dispatchRoomFanout(this.#sessions, request))
      },
      // A macrotask, so a commit's RPC reply is sent before its fanout starts.
      (resume) => setTimeout(resume, 0),
    )
    this.ctx.blockConcurrencyWhile(async () => {
      await this.#scheduleMaintenanceIfNeeded()
    })
  }

  async readHead(): Promise<RoomHead | null> {
    const head = readLiveHead(this.#sql, Date.now())
    return head === null ? null : headForRpc(head)
  }

  async compareExchangeHead(cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    const now = Date.now()
    const outcome = this.ctx.storage.transactionSync(() =>
      compareExchangeHead(this.#sql, cx, next, now, () => crypto.randomUUID()),
    )
    await this.#scheduleMaintenanceIfNeeded()
    if ('conflict' in outcome)
      return { conflict: true, current: outcome.current === null ? null : headForRpc(outcome.current) }
    return { ok: true, head: headForRpc(outcome.head) }
  }

  async readCells(inc: string, sel: { keys: string[] } | { prefix: string }): Promise<CellsResult> {
    return readCells(this.#sql, inc, sel, Date.now())
  }

  async compareExchangeCells(inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult> {
    const now = Date.now()
    return this.ctx.storage.transactionSync(() => compareExchangeCells(this.#sql, inc, revision, mutations, now))
  }

  async commitLane(
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitWire> {
    const now = Date.now()
    const key = encodeLaneKey(lane)
    const outcome = this.ctx.storage.transactionSync(
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

  async readRetained(inc: string, lane: LaneId): Promise<RetainedResult | null> {
    return readRetained(this.#sql, inc, lane)
  }

  async listRetained(inc: string): Promise<LaneId[]> {
    return listRetained(this.#sql, inc)
  }

  async deleteRetained(inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRetained(this.#sql, inc, lane, opts))
  }

  async registerRoute(route: RouteInstallation): Promise<RegisterWire> {
    try {
      this.#sessions.idFromString(route.sessionDoId)
    } catch {
      return {
        rejected: true,
        reason: `session Durable Object id '${route.sessionDoId}' is invalid`,
        terminal: true,
      }
    }
    const result = this.ctx.storage.transactionSync((): RegisterWire => {
      const now = Date.now()
      const head = readLiveHead(this.#sql, now)
      if (head === null || head.currentInc !== route.inc || head.state !== 'open')
        return { rejected: true, reason: `room has no open incarnation '${route.inc}'`, terminal: true }
      upsertRoute(this.#sql, route, now)
      return { ok: true }
    })
    if ('ok' in result) await this.#scheduleMaintenanceIfNeeded()
    return result
  }

  async renewRoute(route: RouteInstallation): Promise<{ ok: boolean; terminal?: boolean }> {
    const now = Date.now()
    // Missing exact routes recover with a fresh lease; only a dropped generation is terminal.
    const result = this.ctx.storage.transactionSync(() =>
      hasGeneration(this.#sql, route.inc) ? { ok: renewRoute(this.#sql, route, now) } : { ok: false, terminal: true },
    )
    await this.#scheduleMaintenanceIfNeeded()
    return result
  }

  async unsubscribeRoute(route: RouteInstallation): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRoute(this.#sql, route))
    await this.#scheduleMaintenanceIfNeeded()
  }

  async dropGeneration(inc: string): Promise<void> {
    await this.#dropGenerationNow(inc)
    await this.#scheduleMaintenanceIfNeeded()
  }

  /** Routes and rows stay durable until every exact-lease uninstall succeeds, so a failed drop is retried by the sweep. */
  async #dropGenerationNow(inc: string): Promise<void> {
    await this.#terminateInstallations(listRouteInstallations(this.#sql, inc))
    this.ctx.storage.transactionSync(() => dropGenerationRows(this.#sql, inc))
    this.#fanout.clearIncarnation(inc)
  }

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this.ctx.storage.transactionSync(() => directoryPut(this.#sql, roomId, incTag))
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.ctx.storage.transactionSync(() => directoryDelete(this.#sql, roomId, incTag))
  }

  async directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> {
    return directoryList(this.#sql, prefix, cursor)
  }

  async alarm(): Promise<void> {
    try {
      await this.#runSweep(Date.now())
    } finally {
      await this.#scheduleMaintenanceIfNeeded()
    }
  }

  async #runSweep(now: number): Promise<void> {
    const orphanIncs = this.ctx.storage.transactionSync(() => {
      const currentInc = readLiveHead(this.#sql, now)?.currentInc ?? null
      // A lapsed tombstone is reclaimed through the delete path (this backend has no native head TTL).
      this.#sql.exec(
        "DELETE FROM head WHERE id = 1 AND state = 'closed' AND expires_at IS NOT NULL AND expires_at <= ?",
        now,
      )
      return listOrphanGenerations(this.#sql, currentInc)
    })

    const failedOrphans = new Set<string>()
    for (const inc of orphanIncs) {
      try {
        await this.#dropGenerationNow(inc)
      } catch {
        failedOrphans.add(inc)
      }
    }

    for (const installation of listExpiredRouteInstallations(this.#sql, now)) {
      if (failedOrphans.has(installation.inc)) continue
      try {
        await this.#invalidateInstallation(installation)
      } catch {
        // Preserve this exact route row as the next sweep's retry source.
        continue
      }
      this.ctx.storage.transactionSync(() => deleteRoute(this.#sql, installation))
    }
  }

  async #invalidateInstallation(installation: RouteInstallation): Promise<void> {
    const session = this.#sessions
    await session.get(session.idFromString(installation.sessionDoId)).telefuncRoomInvalidate(installation)
  }

  async #terminateInstallations(installations: RouteInstallation[]): Promise<void> {
    const outcomes = await dispatchRoomFanout(this.#sessions, {
      operation: 'invalidate',
      path: 'root',
      routes: installations,
      terminal: true,
    })
    const failed = outcomes.find((outcome) => outcome.error !== undefined)
    if (failed?.error !== undefined) throw new Error(failed.error)
  }

  async #scheduleMaintenanceIfNeeded(): Promise<void> {
    const now = Date.now()
    const deadline = nextMaintenanceDeadline(this.#sql, now)
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
  const currentInc = readLiveHead(sql, now)?.currentInc ?? null
  const hasOrphan =
    currentInc === null
      ? sql.exec('SELECT 1 FROM gen LIMIT 1').toArray().length > 0
      : sql.exec('SELECT 1 FROM gen WHERE inc <> ? LIMIT 1', currentInc).toArray().length > 0
  if (hasOrphan) deadlines.push(now)
  return deadlines.length === 0 ? null : Math.min(...deadlines)
}

// Closing accepts only its live authority-time lease through the closing-control branch; all other lanes are stale.
export function createTelefuncRoomDurableObjectClass(
  sessionNamespace: SessionNamespaceResolver,
): new (
  ctx: DurableObjectState,
  env: unknown,
) => TelefuncRoomDurableObject {
  const BaseTelefuncRoomDurableObject = TelefuncRoomDurableObject
  return class TelefuncRoomDurableObject extends BaseTelefuncRoomDurableObject {
    constructor(ctx: DurableObjectState, env: unknown) {
      super(ctx, env, sessionNamespace)
    }
  }
}
