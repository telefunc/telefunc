/// <reference types="@cloudflare/workers-types" />
// The class shell of the Cloudflare Room backend: `TelefuncRoomDurableObject` (one DO per room). It owns
// the RPC surface the Room backend seam calls, the room DO's own authority clock (used for lease minting,
// commit preconditions and TTLs — never a caller clock), acceptance-time route snapshots, and the alarm
// janitor. Storage, retained chunking, routes and fanout are the invariant modules alongside it. The
// per-lane fanout chains live here at the single room authority, including across facade instances.
//
// The Cloudflare entrypoint publishes a configured subclass as `TelefuncRoomDurableObject`; it is not yet
// selected by Room policy; W5-C owns that switch. Conformance drives the same authority implementation.

import { DurableObject } from 'cloudflare:workers'
import type { CellMutation, CxResult, HeadCx, HeadNext, LaneId } from '../../../../backend/spi.js'
import { base64ToBytes, bytesToBase64, laneKey as laneKeyOf } from './codec.js'
import { Fanout, type RouteTarget } from './fanout.js'
import { assertRetainedCapacity, deleteRetained, installRetained, listRetained, readRetained } from './retained.js'
import {
  deleteRoute,
  listExpiredRouteInstallations,
  listRouteInstallations,
  recordRouteDeliveryFailure,
  recordRouteDeliverySuccess,
  renewRoute,
  snapshotRoutes,
  type RouteInstallation,
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
  GEN_ORPHAN_GRACE_MS,
  initSchema,
  listGenerations,
  observeAndListGraceAgedOrphans,
  readCells,
  readGenerationToken,
  readLiveHead,
  type StoredHead,
} from './storage.js'

// ── wire shapes (binary as base64 across the Node↔workerd RPC seam) ──

export type HeadWire = {
  rev: string
  currentInc: string | null
  state: 'open' | 'closing' | 'closed'
  configB64: string
  closeLease?: { id: string; until: number }
}
export type HeadNextWire =
  | {
      head: {
        currentInc: string | null
        state: 'open' | 'closing' | 'closed'
        configB64: string
        closeLease?: { id: string; durationMs: number }
      }
      ttlMs?: number
    }
  | { delete: true }
export type HeadCxWire =
  | { ok: true; head: HeadWire }
  | { ok: true; deleted: true }
  | { conflict: true; current: HeadWire | null }
  | { error: string }
export type CellsWire = { revision: string; cells: Array<[string, string]> } | { staleInc: true }
export type CellMutationWire = { key: string; set?: { bytesB64: string; ttlMs?: number } }
export type CommitWire =
  | { accepted: true; seq: number; timestamp: number; receivers: number; deliveryToken: string }
  | { stale: true }
  | { error: string }
export type RetainedWire = { payloadB64: string; seq: number; timestamp: number }
export type RegisterWire =
  | { ok: true; expiresAt: number; generationToken: string }
  | { rejected: true; reason: string; terminal?: boolean }
export type DropWire =
  | { droppedSubscribers: Array<{ laneKey: string; subscriberDoId: string; leaseId: string; generationToken: string }> }
  | { error: string }

type SubscriberStub = {
  telefuncRoomDeliver(request: RoomShardDeliveryRequest): Promise<void>
  telefuncRoomInvalidate(request: RoomShardInvalidationRequest): Promise<void>
}

type SubscriberNamespace = {
  idFromString(id: string): unknown
  get(id: unknown): SubscriberStub
}

const ROOM_ALARM_INTERVAL_MS = 30_000

function headToWire(head: StoredHead): HeadWire {
  const wire: HeadWire = {
    rev: head.rev,
    currentInc: head.currentInc,
    state: head.state,
    configB64: bytesToBase64(head.config),
  }
  if (head.closeLease !== undefined) wire.closeLease = { ...head.closeLease }
  return wire
}

function nextFromWire(next: HeadNextWire): HeadNext {
  if ('delete' in next) return { delete: true }
  const head: Extract<HeadNext, { head: unknown }>['head'] = {
    currentInc: next.head.currentInc,
    state: next.head.state,
    config: base64ToBytes(next.head.configB64),
  }
  if (next.head.closeLease !== undefined) head.closeLease = { ...next.head.closeLease }
  return next.ttlMs === undefined ? { head } : { head, ttlMs: next.ttlMs }
}

// Extends the `cloudflare:workers` DurableObject base so the Room backend seam can call its methods over
// RPC (a plain class would only expose `fetch`). One DO per room. The room DO owns all durable state
// (head, cells, order, retained, routes, directory), acceptance transaction, and ephemeral delivery
// chains. Fanout dispatches to the existing session-shard DO stubs derived from persisted namespace IDs.
export class TelefuncRoomDurableObject extends DurableObject {
  private readonly _sql: SqlStorage
  private readonly _fanout: Fanout
  private readonly _sessionNamespaceValue: SubscriberNamespace

  constructor(ctx: DurableObjectState, env: unknown, sessionBindingName: string = 'TelefuncDurableObject') {
    super(ctx, env as never)
    const sessionNamespace = (env as Record<string, SubscriberNamespace | undefined>)[sessionBindingName]
    if (sessionNamespace === undefined) {
      throw new Error(
        `Missing Cloudflare session Durable Object binding "${sessionBindingName}" in TelefuncRoomDurableObject constructor.`,
      )
    }
    this._sessionNamespaceValue = sessionNamespace
    this._sql = ctx.storage.sql
    initSchema(this._sql)
    this._fanout = new Fanout(
      async (target, frame, info) => {
        const session = this._sessionNamespaceValue.get(this._sessionNamespaceValue.idFromString(target.subscriberDoId))
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
            recordRouteDeliverySuccess(this._sql, info.inc, info.laneKey, target.subscriberDoId, target.leaseId)
          })
        } catch (error) {
          let evicted = false
          this.ctx.storage.transactionSync(() => {
            evicted = recordRouteDeliveryFailure(
              this._sql,
              info.inc,
              info.laneKey,
              target.subscriberDoId,
              target.leaseId,
            ).evicted
          })
          // Core cannot infer an authority-side K=3 eviction from a later local attachment: it already
          // owns the ready slot. Tell the exact live attempt now; its ordinary `closed` state lets the
          // shared supervisor replan, while the retained route row remains the janitor's retry source
          // if this best-effort invalidation RPC is lost.
          if (evicted) {
            try {
              await session.telefuncRoomInvalidate({
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
      if ((await this.ctx.storage.getAlarm()) === null) await this._armAlarm()
    })
  }

  // ── head ──

  async readHead(): Promise<HeadWire | null> {
    const head = readLiveHead(this._sql, Date.now())
    return head === null ? null : headToWire(head)
  }

  async compareExchangeHead(cx: HeadCx, nextWire: HeadNextWire): Promise<HeadCxWire> {
    const next = nextFromWire(nextWire)
    const now = Date.now()
    let outcome!: ReturnType<typeof compareExchangeHead>
    // One SQL transaction: single-object serialization gives head linearizability (I1). A validation
    // throw rolls the tx back and is surfaced as a structured error the facade rethrows verbatim so
    // callers can identify the failure by its stable message, never as a conflict.
    try {
      this.ctx.storage.transactionSync(() => {
        outcome = compareExchangeHead(this._sql, cx, next, now, () => crypto.randomUUID())
      })
    } catch (error) {
      return { error: (error as Error).message }
    }
    if ('conflict' in outcome)
      return { conflict: true, current: outcome.current === null ? null : headToWire(outcome.current) }
    if ('deleted' in outcome) return { ok: true, deleted: true }
    return { ok: true, head: headToWire(outcome.head) }
  }

  // ── cells ──

  async readCells(inc: string, sel: { keys: string[] } | { prefix: string }): Promise<CellsWire> {
    const result = readCells(this._sql, inc, sel, Date.now())
    if ('staleInc' in result) return { staleInc: true }
    return { revision: result.revision, cells: [...result.cells].map(([key, bytes]) => [key, bytesToBase64(bytes)]) }
  }

  async compareExchangeCells(inc: string, revision: string, mutationsWire: CellMutationWire[]): Promise<CxResult> {
    const mutations: CellMutation[] = mutationsWire.map((mutation) =>
      mutation.set === undefined
        ? { key: mutation.key }
        : { key: mutation.key, set: { bytes: base64ToBytes(mutation.set.bytesB64), ttlMs: mutation.set.ttlMs } },
    )
    const now = Date.now()
    let result!: CxResult
    this.ctx.storage.transactionSync(() => {
      result = compareExchangeCells(this._sql, inc, revision, mutations, now)
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
    // stale. Over-cap retain throws BEFORE the order advances (the tx rolls back), surfaced as a
    // structured error the facade rethrows.
    try {
      this.ctx.storage.transactionSync(() => {
        if (!commitPreconditionHolds(this._sql, inc, lane, opts?.closingLease, now)) return
        if (opts?.requiredCellKeys !== undefined) {
          const required = readCells(this._sql, inc, { keys: opts.requiredCellKeys }, now)
          if ('staleInc' in required || opts.requiredCellKeys.some((cell) => !required.cells.has(cell))) return
        }
        if (opts?.retain === true) assertRetainedCapacity(this._sql, inc, key, frame.byteLength)
        const mark = advanceOrder(this._sql, inc, key, now)
        if (opts?.retain === true) installRetained(this._sql, inc, lane, frame, mark)
        const targets = snapshotRoutes(this._sql, inc, key, now)
        accepted = { seq: mark.seq, timestamp: mark.timestamp, targets }
      })
    } catch (error) {
      return { error: (error as Error).message }
    }
    if (accepted === null) return { stale: true }
    const settled: { seq: number; timestamp: number; targets: RouteTarget[] } = accepted
    const deliveryToken = this._fanout.enqueue(inc, key, settled.targets, frame, {
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
    await this._fanout.await(token)
  }

  // ── retained ──

  async readRetained(inc: string, lane: LaneId): Promise<RetainedWire | null> {
    const entry = readRetained(this._sql, inc, lane)
    return entry === null
      ? null
      : { payloadB64: bytesToBase64(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(inc: string): Promise<LaneId[]> {
    return listRetained(this._sql, inc)
  }

  async deleteRetainedLane(inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRetained(this._sql, inc, lane, opts))
  }

  // ── routes / readiness ──

  async registerRoute(
    roomId: string,
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
  ): Promise<RegisterWire> {
    const sessionNamespace = this._sessionNamespaceValue
    if (!/^[0-9a-f]{64}$/.test(subscriberDoId)) {
      return { rejected: true, reason: `subscriber Durable Object id '${subscriberDoId}' is invalid`, terminal: true }
    }
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
      const head = readLiveHead(this._sql, now)
      if (head === null || head.currentInc !== inc || head.state !== 'open') return
      const generationToken = readGenerationToken(this._sql, inc)
      if (generationToken === null) return
      const expiresAt = upsertRoute(this._sql, roomId, inc, laneKey, subscriberDoId, leaseId, generationToken, now)
      result = { ok: true, expiresAt, generationToken }
    })
    return result
  }

  async renewRoute(
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
    expectedGenerationToken: string,
  ): Promise<{ ok: boolean; expiresAt?: number; terminal?: boolean }> {
    const now = Date.now()
    let result!: ReturnType<typeof renewRoute>
    let generationInvalid = false
    this.ctx.storage.transactionSync(() => {
      const currentGenerationToken = readGenerationToken(this._sql, inc)
      if (currentGenerationToken === null || currentGenerationToken !== expectedGenerationToken) {
        generationInvalid = true
        return
      }
      result = renewRoute(this._sql, inc, laneKey, subscriberDoId, leaseId, now)
    })
    if (generationInvalid) return { ok: false, terminal: true }
    // A missing/non-live exact route inside the SAME generation is recoverable: the subscription
    // lifecycle enters lost and establishes a fresh lease. Only generation identity loss is terminal.
    return result.ok ? { ok: true, expiresAt: result.expiresAt } : { ok: false }
  }

  async unsubscribeRoute(inc: string, laneKey: string, subscriberDoId: string, leaseId: string): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRoute(this._sql, inc, laneKey, subscriberDoId, leaseId))
  }

  // ── generation lifecycle ──

  async listGenerations(): Promise<string[]> {
    return listGenerations(this._sql)
  }

  async dropGeneration(inc: string): Promise<DropWire> {
    const now = Date.now()
    const head = readLiveHead(this._sql, now)
    if (head?.currentInc === inc) {
      return { error: `dropGeneration: refusing to drop the current incarnation '${inc}'` }
    }
    const installations = listRouteInstallations(this._sql, inc)
    // Subscriber uninstalls are fallible. Keep their durable route rows and generation entry intact
    // until every exact-lease uninstall succeeds, so a retry can replay the same invalidations after a
    // transport failure or crash. This is the generation analogue of data-first / gens-entry-last.
    await Promise.all(installations.map((installation) => this._invalidateInstallation(installation, true)))
    this.ctx.storage.transactionSync(() => dropGenerationRows(this._sql, inc))
    this._fanout.clearIncarnation(inc)
    // Report the routes that were on the dropped generation so the facade can close their local
    // attachments (the channel no longer exists — the subscription is terminal, not merely lost).
    const droppedSubscribers = installations.map((installation) => ({
      laneKey: installation.laneKey,
      subscriberDoId: installation.subscriberDoId,
      leaseId: installation.leaseId,
      generationToken: installation.generationToken,
    }))
    return { droppedSubscribers }
  }

  // ── directory (this DO, addressed as a singleton, is the best-effort projection store) ──

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this.ctx.storage.transactionSync(() => directoryPut(this._sql, roomId, incTag))
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.ctx.storage.transactionSync(() => directoryDelete(this._sql, roomId, incTag))
  }

  async directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> {
    return directoryList(this._sql, prefix, cursor)
  }

  // ── alarm janitor ──

  async alarm(): Promise<void> {
    try {
      await this._runSweep(Date.now())
    } finally {
      await this._armAlarm()
    }
  }

  private async _runSweep(now: number): Promise<number> {
    let orphanIncs: string[] = []
    this.ctx.storage.transactionSync(() => {
      this._sql.exec('DELETE FROM cell WHERE expires_at IS NOT NULL AND expires_at <= ?', now)
      const currentInc = readLiveHead(this._sql, now)?.currentInc ?? null
      orphanIncs = observeAndListGraceAgedOrphans(this._sql, currentInc, now, GEN_ORPHAN_GRACE_MS)
      // A lapsed tombstone is reclaimed through the delete path (this backend has no native head TTL).
      this._sql.exec(
        "DELETE FROM head WHERE id = 1 AND state = 'closed' AND expires_at IS NOT NULL AND expires_at <= ?",
        now,
      )
    })

    // Each orphan is an independent retry unit. One failed subscriber cannot block another orphan or
    // unrelated expiry/tombstone hygiene; its own generation and route rows remain durable for retry.
    const failedOrphans = new Set<string>()
    for (const inc of orphanIncs) {
      const installations = listRouteInstallations(this._sql, inc)
      const outcomes = await Promise.allSettled(installations.map((entry) => this._invalidateInstallation(entry, true)))
      if (outcomes.some((outcome) => outcome.status === 'rejected')) {
        failedOrphans.add(inc)
        continue
      }
      this.ctx.storage.transactionSync(() => dropGenerationRows(this._sql, inc))
      this._fanout.clearIncarnation(inc)
    }

    let prunedRoutes = 0
    for (const installation of listExpiredRouteInstallations(this._sql, now)) {
      if (failedOrphans.has(installation.inc)) continue
      try {
        await this._invalidateInstallation(installation)
      } catch {
        // Preserve this exact route row as the next sweep's retry source.
        continue
      }
      this.ctx.storage.transactionSync(() => {
        deleteRoute(
          this._sql,
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

  private async _invalidateInstallation(installation: RouteInstallation, terminal: boolean = false): Promise<void> {
    const session = this._sessionNamespaceValue
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

  private async _armAlarm(): Promise<void> {
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
