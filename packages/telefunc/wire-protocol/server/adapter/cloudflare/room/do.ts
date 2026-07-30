/// <reference types="@cloudflare/workers-types" />
// The class shell of the Cloudflare Room backend: `TelefuncRoomDurableObject` (one DO per room). It owns
// the RPC surface the Room backend seam calls, the room DO's own authority clock (used for lease minting,
// commit preconditions and TTLs — never a caller clock), acceptance-time route snapshots, and the alarm
// janitor. Storage, retained chunking, routes and fanout are the invariant modules alongside it. The
// per-lane fanout chains live here at the single room authority, including across facade instances.
//
// The Cloudflare entrypoint publishes a configured subclass as `TelefuncRoomDurableObject`; public Room
// traffic and the conformance lane drive this same authority implementation.

import { DurableObject } from 'cloudflare:workers'
import type { CellMutation, CxResult, HeadCx, HeadNext, LaneId, RoomHead } from '../../../../backend/spi.js'
import { laneKey as laneKeyOf } from './codec.js'
import { Fanout } from './fanout.js'
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
import type { RoomShardDeliveryRequest, RoomShardInvalidationRequest } from './backend.js'
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

// RPC preserves the SPI's structured-cloneable maps, typed arrays, and records directly.

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

type SubscriberStub = {
  telefuncRoomDeliver(request: RoomShardDeliveryRequest): Promise<void>
  telefuncRoomInvalidate(request: RoomShardInvalidationRequest): Promise<void>
}

type SubscriberNamespace = {
  idFromString(id: string): unknown
  get(id: unknown): SubscriberStub
}

const ROOM_ALARM_INTERVAL_MS = 30_000

function headForRpc(head: StoredHead): RoomHead {
  const result: RoomHead = {
    rev: head.rev,
    currentInc: head.currentInc,
    state: head.state,
    config: head.config,
  }
  if (head.closeLease !== undefined) result.closeLease = { ...head.closeLease }
  return result
}

// Extends the `cloudflare:workers` DurableObject base so the Room backend seam can call its methods over
// RPC (a plain class would only expose `fetch`). One DO per room. The room DO owns all durable state
// (head, cells, order, retained, routes, directory), acceptance transaction, and ephemeral delivery
// chains. Fanout dispatches to the existing session-shard DO stubs derived from persisted namespace IDs.
export class TelefuncRoomDurableObject extends DurableObject {
  readonly #sql: SqlStorage
  readonly #fanout: Fanout
  readonly #sessionNamespaceValue: SubscriberNamespace

  constructor(ctx: DurableObjectState, env: unknown, sessionBindingName: string = 'TelefuncDurableObject') {
    super(ctx, env as never)
    const sessionNamespace = (env as Record<string, SubscriberNamespace | undefined>)[sessionBindingName]
    if (sessionNamespace === undefined) {
      throw new Error(
        `Missing Cloudflare session Durable Object binding "${sessionBindingName}" in TelefuncRoomDurableObject constructor.`,
      )
    }
    this.#sessionNamespaceValue = sessionNamespace
    this.#sql = ctx.storage.sql
    initSchema(this.#sql)
    this.#fanout = new Fanout(
      async (target, frame, info) => {
        const session = this.#sessionNamespaceValue.get(this.#sessionNamespaceValue.idFromString(target.subscriberDoId))
        try {
          await session.telefuncRoomDeliver({
            roomId: info.roomId,
            inc: info.inc,
            laneKey: info.laneKey,
            subscriberDoId: target.subscriberDoId,
            leaseId: target.leaseId,
            generationToken: target.generationToken,
            frame,
            seq: info.seq,
            timestamp: info.timestamp,
          })
          this.ctx.storage.transactionSync(() => {
            recordRouteDeliverySuccess(this.#sql, info.inc, info.laneKey, target.subscriberDoId, target.leaseId)
          })
        } catch (error) {
          let evicted = false
          this.ctx.storage.transactionSync(() => {
            evicted = recordRouteDeliveryFailure(
              this.#sql,
              info.inc,
              info.laneKey,
              target.subscriberDoId,
              target.leaseId,
            )
          })
          // Core cannot infer an authority-side K=3 eviction from a later local attachment: it already
          // owns the ready slot. Tell the exact live attempt now; its ordinary `closed` state lets Room
          // apply its recovery policy, while the retained route row remains the janitor's retry source
          // if this best-effort invalidation RPC is lost.
          if (evicted) {
            try {
              const recoverySession = this.#sessionNamespaceValue.get(
                this.#sessionNamespaceValue.idFromString(target.subscriberDoId),
              )
              await recoverySession.telefuncRoomInvalidate({
                roomId: info.roomId,
                inc: info.inc,
                laneKey: info.laneKey,
                subscriberDoId: target.subscriberDoId,
                leaseId: target.leaseId,
                generationToken: target.generationToken,
              })
            } catch (invalidationError) {
              throw new AggregateError(
                [error, invalidationError],
                'Cloudflare Room delivery and exact-route invalidation both failed',
              )
            }
          }
          throw error
        }
      },
      (resume) => setTimeout(resume, 0),
    )
    this.ctx.blockConcurrencyWhile(async () => {
      if ((await this.ctx.storage.getAlarm()) === null) await this.#armAlarm()
    })
  }

  // ── head ──

  async readHead(): Promise<RoomHead | null> {
    const head = readLiveHead(this.#sql, Date.now())
    return head === null ? null : headForRpc(head)
  }

  async compareExchangeHead(cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    const now = Date.now()
    let outcome!: ReturnType<typeof compareExchangeHead>
    // One SQL transaction: single-object serialization gives head linearizability (I1). A validation
    // throw rolls the transaction back and crosses the RPC boundary as a normal rejection.
    this.ctx.storage.transactionSync(() => {
      outcome = compareExchangeHead(this.#sql, cx, next, now, () => crypto.randomUUID())
    })
    if ('conflict' in outcome)
      return { conflict: true, current: outcome.current === null ? null : headForRpc(outcome.current) }
    if ('deleted' in outcome) return { ok: true, deleted: true }
    return { ok: true, head: headForRpc(outcome.head) }
  }

  // ── cells ──

  async readCells(inc: string, sel: { keys: string[] } | { prefix: string }): Promise<CellsResult> {
    return readCells(this.#sql, inc, sel, Date.now())
  }

  async compareExchangeCells(inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult> {
    const now = Date.now()
    let result!: CxResult
    this.ctx.storage.transactionSync(() => {
      result = compareExchangeCells(this.#sql, inc, revision, mutations, now)
    })
    return result
  }

  // ── commit ──

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
    // The acceptance transaction encodes the SAME precondition branch as Redis/memory. Zero-row match ⇒
    // stale. A storage or validation failure rejects the RPC and rolls back the transaction.
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
    const settled: { seq: number; timestamp: number; targets: RouteTarget[] } = accepted
    const deliveryToken = this.#fanout.enqueue(inc, key, settled.targets, frame, {
      roomId,
      inc,
      laneKey: key,
      seq: settled.seq,
      timestamp: settled.timestamp,
    })
    return {
      accepted: true,
      seq: settled.seq,
      timestamp: settled.timestamp,
      receivers: settled.targets.length,
      deliveryToken,
    }
  }

  async awaitDelivery(token: string): Promise<void> {
    await this.#fanout.await(token)
  }

  // ── retained ──

  async readRetained(inc: string, lane: LaneId): Promise<RetainedResult | null> {
    return readRetained(this.#sql, inc, lane)
  }

  async listRetained(inc: string): Promise<LaneId[]> {
    return listRetained(this.#sql, inc)
  }

  async deleteRetainedLane(inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRetained(this.#sql, inc, lane, opts))
  }

  // ── routes / readiness ──

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
    if (generationInvalid) return { ok: false, terminal: true }
    // A missing/non-live exact route inside the SAME generation is recoverable: the subscription
    // lifecycle enters lost and establishes a fresh lease. Only generation identity loss is terminal.
    return { ok: renewed }
  }

  async unsubscribeRoute(inc: string, laneKey: string, subscriberDoId: string, leaseId: string): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRoute(this.#sql, inc, laneKey, subscriberDoId, leaseId))
  }

  // ── generation lifecycle ──

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
    // Subscriber uninstalls are fallible. Keep their durable route rows and generation entry intact
    // until every exact-lease uninstall succeeds, so a retry can replay the same invalidations after a
    // transport failure or crash. This is the generation analogue of data-first / gens-entry-last.
    await Promise.all(installations.map((installation) => this.#invalidateInstallation(installation, true)))
    this.ctx.storage.transactionSync(() => dropGenerationRows(this.#sql, inc))
    this.#fanout.clearIncarnation(inc)
  }

  // ── directory (this DO, addressed as a singleton, is the best-effort projection store) ──

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

  // ── alarm janitor ──

  async alarm(): Promise<void> {
    try {
      await this.#runSweep(Date.now())
    } finally {
      await this.#armAlarm()
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

    // Each orphan is an independent retry unit. One failed subscriber cannot block another orphan or
    // unrelated expiry/tombstone hygiene; its own generation and route rows remain durable for retry.
    const failedOrphans = new Set<string>()
    for (const inc of orphanIncs) {
      const installations = listRouteInstallations(this.#sql, inc)
      const outcomes = await Promise.allSettled(installations.map((entry) => this.#invalidateInstallation(entry, true)))
      if (outcomes.some((outcome) => outcome.status === 'rejected')) {
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
      roomId: installation.roomId,
      inc: installation.inc,
      laneKey: installation.laneKey,
      subscriberDoId: installation.subscriberDoId,
      leaseId: installation.leaseId,
      generationToken: installation.generationToken,
      ...(terminal ? { terminal: true as const } : {}),
    })
  }

  async #armAlarm(): Promise<void> {
    await this.ctx.storage.setAlarm(Date.now() + ROOM_ALARM_INTERVAL_MS)
  }
}

// The commit precondition: one boolean, two branches. Supplying a closing lease selects the narrow
// closing-control branch outright, which is what makes every other lane stale while closing. `now` is
// authority time, so an expired lease is stale even with the correct id.
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
        `SELECT 1 FROM head
         WHERE id = 1 AND inc = ?
           AND (
             (? = 0 AND state = 'open')
             OR
             (? = 1 AND ? = 'control' AND state = 'closing'
               AND lease_id = ? AND lease_until IS NOT NULL AND ? <= lease_until)
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

// Factory glue for the developer-facing Cloudflare entrypoint. The exact returned class name is also the
// Wrangler binding class name, while the closure captures the configured existing session namespace.
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
