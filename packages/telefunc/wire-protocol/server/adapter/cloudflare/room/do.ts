/// <reference types="@cloudflare/workers-types" />
// One Room DO owns authority time/state/fanout; transaction rollback and RPC structured clone preserve SPI outcomes.

import { DurableObject } from 'cloudflare:workers'
import type { CellMutation, CxResult, HeadCx, HeadNext, LaneId, RoomHead } from '../../../../backend/spi.js'
import { laneKey as laneKeyOf } from './codec.js'
import {
  dispatchRoomShardFanout,
  dispatchRoomShardFanoutViaCoordinator,
  Fanout,
  type RoomShardFanoutNamespace,
  type RoomShardFanoutOutcome,
  type RoomShardFanoutRequest,
} from './fanout.js'
import { deleteRetained, installRetained, listRetained, readRetained } from './retained.js'
import {
  deleteRoute,
  listExpiredRouteInstallations,
  listRouteInstallations,
  recordRouteDeliveryFailure,
  recordRouteDeliverySuccess,
  renewRoute,
  snapshotRoutes,
  type RouteInstallation,
  type RouteTarget,
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
  listGenerations,
  readCells,
  readGenerationToken,
  readLiveHead,
  type StoredHead,
} from './storage.js'

export type HeadCxResult =
  | { ok: true; head: RoomHead }
  | { ok: true; deleted: true }
  | { conflict: true; current: RoomHead | null }
export type CellsResult = { revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }
export type CommitWire =
  | { accepted: true; seq: number; timestamp: number; receivers: number; deliveryToken: string }
  | { stale: true }
export type RetainedResult = { payload: Uint8Array; seq: number; timestamp: number }
export type RegisterWire =
  | { ok: true; generationToken: string }
  | { rejected: true; reason: string; terminal?: boolean }

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

export class TelefuncRoomDurableObject extends DurableObject {
  readonly #sql: SqlStorage
  readonly #fanout: Fanout
  readonly #sessionNamespaceValue: RoomShardFanoutNamespace

  constructor(ctx: DurableObjectState, env: unknown, sessionBindingName: string = 'TelefuncDurableObject') {
    super(ctx, env as never)
    const sessionNamespace = (env as Record<string, RoomShardFanoutNamespace | undefined>)[sessionBindingName]
    if (sessionNamespace === undefined) {
      throw new Error(
        `Missing Cloudflare session Durable Object binding "${sessionBindingName}" in TelefuncRoomDurableObject constructor.`,
      )
    }
    this.#sessionNamespaceValue = sessionNamespace
    this.#sql = ctx.storage.sql
    initSchema(this.#sql)
    this.#fanout = new Fanout(
      async (targets, frame, info, coordinatorIndex) => {
        const request: RoomShardFanoutRequest = {
          operation: 'deliver',
          ...info,
          targets,
          frame,
          path: String(coordinatorIndex ?? 0),
        }
        const outcomes = await this.#dispatchFanout(request, coordinatorIndex)
        await this.#settleDeliveryOutcomes(outcomes, info, coordinatorIndex)
      },
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
    let outcome!: ReturnType<typeof compareExchangeHead>
    this.ctx.storage.transactionSync(() => {
      outcome = compareExchangeHead(this.#sql, cx, next, now, () => crypto.randomUUID())
    })
    await this.#scheduleMaintenanceIfNeeded()
    if ('conflict' in outcome)
      return { conflict: true, current: outcome.current === null ? null : headForRpc(outcome.current) }
    if ('deleted' in outcome) return { ok: true, deleted: true }
    return { ok: true, head: headForRpc(outcome.head) }
  }

  async readCells(inc: string, sel: { keys: string[] } | { prefix: string }): Promise<CellsResult> {
    return readCells(this.#sql, inc, sel, Date.now())
  }

  async compareExchangeCells(inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult> {
    const now = Date.now()
    let result!: CxResult
    this.ctx.storage.transactionSync(() => {
      result = compareExchangeCells(this.#sql, inc, revision, mutations, now)
    })
    if (result === 'committed') await this.#scheduleMaintenanceIfNeeded()
    return result
  }

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitWire> {
    const now = Date.now()
    const key = laneKeyOf(lane)
    const frame = payload instanceof Uint8Array ? payload : new Uint8Array(payload)
    let accepted: { seq: number; timestamp: number; targets: RouteTarget[] } | null = null
    this.ctx.storage.transactionSync(() => {
      if (!commitPreconditionHolds(this.#sql, inc, lane, opts?.closingLease, now)) return
      if (opts?.requiredCellKeys !== undefined) {
        const required = readCells(this.#sql, inc, { keys: opts.requiredCellKeys }, now)
        if ('staleInc' in required || opts.requiredCellKeys.some((cell) => !required.cells.has(cell))) return
      }
      const mark = advanceOrder(this.#sql, inc, key, now)
      if (opts?.retain === true) installRetained(this.#sql, inc, lane, frame, mark)
      const targets = snapshotRoutes(this.#sql, inc, key, now)
      accepted = { seq: mark.seq, timestamp: mark.timestamp, targets }
    })
    if (accepted === null) return { stale: true }
    const { seq, timestamp, targets } = accepted as { seq: number; timestamp: number; targets: RouteTarget[] }
    const deliveryToken = this.#fanout.enqueue(inc, key, targets, frame, {
      roomId,
      inc,
      laneKey: key,
      seq,
      timestamp,
    })
    return { accepted: true, seq, timestamp, receivers: targets.length, deliveryToken }
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

  async deleteRetainedLane(inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRetained(this.#sql, inc, lane, opts))
  }

  async registerRoute(
    roomId: string,
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
  ): Promise<RegisterWire> {
    const sessionNamespace = this.#sessionNamespaceValue
    try {
      sessionNamespace.idFromString(subscriberDoId)
    } catch {
      return { rejected: true, reason: `subscriber Durable Object id '${subscriberDoId}' is invalid`, terminal: true }
    }
    let result: RegisterWire = {
      rejected: true,
      reason: `room has no open incarnation '${inc}'`,
      terminal: true,
    }
    this.ctx.storage.transactionSync(() => {
      const now = Date.now()
      const head = readLiveHead(this.#sql, now)
      if (head === null || head.currentInc !== inc || head.state !== 'open') return
      const generationToken = readGenerationToken(this.#sql, inc)
      if (generationToken === null) return
      upsertRoute(this.#sql, roomId, inc, laneKey, subscriberDoId, leaseId, generationToken, now)
      result = { ok: true, generationToken }
    })
    if ('ok' in result) await this.#scheduleMaintenanceIfNeeded()
    return result
  }

  async renewRoute(
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
    expectedGenerationToken: string,
  ): Promise<{ ok: boolean; terminal?: boolean }> {
    const now = Date.now()
    let renewed = false
    let generationInvalid = false
    this.ctx.storage.transactionSync(() => {
      const currentGenerationToken = readGenerationToken(this.#sql, inc)
      if (currentGenerationToken === null || currentGenerationToken !== expectedGenerationToken) {
        generationInvalid = true
        return
      }
      renewed = renewRoute(this.#sql, inc, laneKey, subscriberDoId, leaseId, now)
    })
    await this.#scheduleMaintenanceIfNeeded()
    if (generationInvalid) return { ok: false, terminal: true }
    // Missing exact routes recover with a fresh lease; only generation identity loss is terminal.
    return { ok: renewed }
  }

  async unsubscribeRoute(inc: string, laneKey: string, subscriberDoId: string, leaseId: string): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRoute(this.#sql, inc, laneKey, subscriberDoId, leaseId))
    await this.#scheduleMaintenanceIfNeeded()
  }

  async listGenerations(): Promise<string[]> {
    return listGenerations(this.#sql)
  }

  async dropGeneration(inc: string): Promise<void> {
    const now = Date.now()
    const head = readLiveHead(this.#sql, now)
    if (head?.currentInc === inc) {
      throw new Error(`dropGeneration: refusing to drop the current incarnation '${inc}'`)
    }
    const installations = listRouteInstallations(this.#sql, inc)
    // Routes/generation stay durable until exact-lease uninstall succeeds; orphan sweeps retry independently.
    await this.#terminateInstallations(inc, installations)
    this.ctx.storage.transactionSync(() => dropGenerationRows(this.#sql, inc))
    this.#fanout.clearIncarnation(inc)
    await this.#scheduleMaintenanceIfNeeded()
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

  async #runSweep(now: number): Promise<number> {
    let orphanIncs: string[] = []
    this.ctx.storage.transactionSync(() => {
      this.#sql.exec('DELETE FROM cell WHERE expires_at IS NOT NULL AND expires_at <= ?', now)
      const currentInc = readLiveHead(this.#sql, now)?.currentInc ?? null
      orphanIncs = listGenerations(this.#sql).filter((inc) => inc !== currentInc)
      // A lapsed tombstone is reclaimed through the delete path (this backend has no native head TTL).
      this.#sql.exec(
        "DELETE FROM head WHERE id = 1 AND state = 'closed' AND expires_at IS NOT NULL AND expires_at <= ?",
        now,
      )
    })

    const failedOrphans = new Set<string>()
    for (const inc of orphanIncs) {
      const installations = listRouteInstallations(this.#sql, inc)
      try {
        await this.#terminateInstallations(inc, installations)
      } catch {
        failedOrphans.add(inc)
        continue
      }
      this.ctx.storage.transactionSync(() => dropGenerationRows(this.#sql, inc))
      this.#fanout.clearIncarnation(inc)
    }

    let prunedRoutes = 0
    for (const installation of listExpiredRouteInstallations(this.#sql, now)) {
      if (failedOrphans.has(installation.inc)) continue
      try {
        await this.#invalidateInstallation(installation)
      } catch {
        // Preserve this exact route row as the next sweep's retry source.
        continue
      }
      this.ctx.storage.transactionSync(() => {
        deleteRoute(
          this.#sql,
          installation.inc,
          installation.laneKey,
          installation.subscriberDoId,
          installation.leaseId,
        )
      })
      prunedRoutes += 1
    }
    return prunedRoutes
  }

  async #invalidateInstallation(installation: RouteInstallation, terminal: boolean = false): Promise<void> {
    const session = this.#sessionNamespaceValue
    await session.get(session.idFromString(installation.subscriberDoId)).telefuncRoomInvalidate({
      ...installation,
      ...(terminal ? { terminal: true as const } : {}),
    })
  }

  async #terminateInstallations(inc: string, installations: RouteInstallation[]): Promise<void> {
    const outcomes = await this.#dispatchFanout({
      operation: 'invalidate',
      roomId: installations[0]?.roomId ?? '',
      inc,
      laneKey: installations[0]?.laneKey ?? '',
      targets: installations,
      path: 'terminate',
      terminal: true,
    })
    const failed = outcomes.find((outcome) => outcome.error !== undefined)
    if (failed?.error !== undefined) throw new Error(failed.error)
  }

  #dispatchFanout(request: RoomShardFanoutRequest, coordinatorIndex?: number): Promise<RoomShardFanoutOutcome[]> {
    return coordinatorIndex === undefined
      ? dispatchRoomShardFanout(this.#sessionNamespaceValue, request)
      : dispatchRoomShardFanoutViaCoordinator(this.#sessionNamespaceValue, request)
  }

  async #settleDeliveryOutcomes(
    outcomes: RoomShardFanoutOutcome[],
    info: { roomId: string; inc: string; laneKey: string; seq: number; timestamp: number },
    coordinatorIndex?: number,
  ): Promise<void> {
    const deliveryFailures: Error[] = []
    const evicted: RouteTarget[] = []
    this.ctx.storage.transactionSync(() => {
      for (const outcome of outcomes) {
        const target = outcome.target
        if (outcome.error === undefined) {
          recordRouteDeliverySuccess(this.#sql, info.inc, info.laneKey, target.subscriberDoId, target.leaseId)
          continue
        }
        deliveryFailures.push(new Error(outcome.error))
        if (recordRouteDeliveryFailure(this.#sql, info.inc, info.laneKey, target.subscriberDoId, target.leaseId)) {
          evicted.push(target)
        }
      }
    })
    if (evicted.length > 0) await this.#scheduleMaintenanceIfNeeded()

    const invalidationFailures: Error[] = []
    if (evicted.length > 0) {
      // K=3 invalidation stays exact-lease scoped and uses the delivery coordinator bucket.
      const request: RoomShardFanoutRequest = {
        operation: 'invalidate',
        roomId: info.roomId,
        inc: info.inc,
        laneKey: info.laneKey,
        targets: evicted,
        path: `${coordinatorIndex ?? 0}.invalidate`,
      }
      const invalidations = await this.#dispatchFanout(request, coordinatorIndex)
      for (const outcome of invalidations) {
        if (outcome.error !== undefined) invalidationFailures.push(new Error(outcome.error))
      }
    }

    if (deliveryFailures.length === 1 && invalidationFailures.length === 0) throw deliveryFailures[0]
    const failures = [...deliveryFailures, ...invalidationFailures]
    if (failures.length > 0) {
      const message =
        deliveryFailures.length === 1 && invalidationFailures.length === 1
          ? 'Cloudflare Room delivery and exact-route invalidation both failed'
          : 'Cloudflare Room fanout failed'
      throw new AggregateError(failures, message)
    }
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
    sql
      .exec<{ deadline: number | null }>('SELECT MIN(expires_at) AS deadline FROM cell WHERE expires_at IS NOT NULL')
      .toArray()[0]?.deadline,
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
function commitPreconditionHolds(
  sql: SqlStorage,
  inc: string,
  lane: LaneId,
  closingLease: string | undefined,
  now: number,
): boolean {
  const hasClosingLease = closingLease === undefined ? 0 : 1
  return (
    sql
      .exec(
        `SELECT 1 FROM head WHERE id = 1 AND inc = ? AND (
          (? = 0 AND state = 'open') OR
          (? = 1 AND ? = 'control' AND state = 'closing' AND lease_id = ? AND lease_until IS NOT NULL AND ? <= lease_until)
        )`,
        inc,
        hasClosingLease,
        hasClosingLease,
        lane.kind,
        closingLease ?? '',
        now,
      )
      .toArray().length === 1
  )
}

export function createTelefuncRoomDurableObjectClass(
  sessionBindingName: string = 'TelefuncDurableObject',
): typeof TelefuncRoomDurableObject {
  const BaseTelefuncRoomDurableObject = TelefuncRoomDurableObject
  return class TelefuncRoomDurableObject extends BaseTelefuncRoomDurableObject {
    constructor(ctx: DurableObjectState, env: unknown) {
      super(ctx, env, sessionBindingName)
    }
  }
}
