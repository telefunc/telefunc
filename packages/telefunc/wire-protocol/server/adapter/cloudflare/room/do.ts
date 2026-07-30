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
  ROUTE_CAPTURE_TTL_MS,
  snapshotRoutes,
  type RouteInstallation,
  upsertRoute,
} from './routes.js'
import type { RoomShardDeliveryRequest, RoomShardInvalidationRequest } from './backend.js'
import {
  advanceOrder,
  compareExchangeCells,
  compareExchangeHead,
  countRouteGenerationCaptures,
  deleteExpiredRouteGenerationCaptures,
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
  readRouteGenerationCapture,
  insertRouteGenerationCapture,
  touchRouteGenerationCapture,
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
export type GenerationWire = { ok: true; generationToken: string } | { rejected: true; reason: string; terminal: true }
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

type RoomEnv = {
  TELEFUNC_ROOM_ALARM_INTERVAL_MS?: string
}

export const ROOM_ALARM_INTERVAL_MS = 30_000

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
  readonly #sql: SqlStorage
  readonly #fanout: Fanout
  readonly #maxRetainedBytes: number
  readonly #alarmIntervalMs: number
  readonly #sessionNamespaceValue: SubscriberNamespace
  readonly #authorityNow: () => number

  constructor(
    ctx: DurableObjectState,
    env: unknown,
    sessionBindingName: string = 'TelefuncDurableObject',
    authorityNow: () => number = Date.now,
  ) {
    super(ctx, env as never)
    const sessionNamespace = (env as Record<string, SubscriberNamespace | undefined>)[sessionBindingName]
    if (sessionNamespace === undefined) {
      throw new Error(
        `Missing Cloudflare session Durable Object binding "${sessionBindingName}" in TelefuncRoomDurableObject constructor.`,
      )
    }
    this.#sessionNamespaceValue = sessionNamespace
    this.#authorityNow = authorityNow
    this.#sql = ctx.storage.sql
    initSchema(this.#sql)
    this.#maxRetainedBytes = 16 * 1024 * 1024
    const configuredInterval = Number((env as RoomEnv).TELEFUNC_ROOM_ALARM_INTERVAL_MS)
    this.#alarmIntervalMs =
      Number.isFinite(configuredInterval) && configuredInterval > 0 ? configuredInterval : ROOM_ALARM_INTERVAL_MS
    this.#fanout = new Fanout(
      async (target, frame, info) => {
        const session = this.#sessionNamespace().get(this.#sessionNamespace().idFromString(target.subscriberDoId))
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
    // Schedule on the runtime clock; sweep predicates use the room authority clock. The test binding only
    // shortens this cadence and never replaces the production alarm path.
    this.ctx.blockConcurrencyWhile(async () => {
      if ((await this.ctx.storage.getAlarm()) === null) await this.#armAlarm()
    })
  }

  // ── head ──

  async readHead(): Promise<HeadWire | null> {
    const head = readLiveHead(this.#sql, this.#authorityNow())
    return head === null ? null : headToWire(head)
  }

  async compareExchangeHead(cx: HeadCx, nextWire: HeadNextWire): Promise<HeadCxWire> {
    const next = nextFromWire(nextWire)
    const now = this.#authorityNow()
    let outcome!: ReturnType<typeof compareExchangeHead>
    // One SQL transaction: single-object serialization gives head linearizability (I1). A validation
    // throw rolls the tx back and is surfaced as a structured error the facade rethrows verbatim so
    // callers can identify the failure by its stable message, never as a conflict.
    try {
      this.ctx.storage.transactionSync(() => {
        outcome = compareExchangeHead(this.#sql, cx, next, now, () => crypto.randomUUID())
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
    const result = readCells(this.#sql, inc, sel, this.#authorityNow())
    if ('staleInc' in result) return { staleInc: true }
    return { revision: result.revision, cells: [...result.cells].map(([key, bytes]) => [key, bytesToBase64(bytes)]) }
  }

  async compareExchangeCells(inc: string, revision: string, mutationsWire: CellMutationWire[]): Promise<CxResult> {
    const mutations: CellMutation[] = mutationsWire.map((mutation) =>
      mutation.set === undefined
        ? { key: mutation.key }
        : { key: mutation.key, set: { bytes: base64ToBytes(mutation.set.bytesB64), ttlMs: mutation.set.ttlMs } },
    )
    const now = this.#authorityNow()
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
    const now = this.#authorityNow()
    const key = laneKeyOf(lane)
    const frame = payload instanceof Uint8Array ? payload : new Uint8Array(payload)
    let accepted: { seq: number; timestamp: number; targets: RouteTarget[] } | null = null
    // The acceptance transaction encodes the SAME precondition branch as Redis/memory. Zero-row match ⇒
    // stale. Over-cap retain throws BEFORE the order advances (the tx rolls back), surfaced as a
    // structured error the facade rethrows.
    try {
      this.ctx.storage.transactionSync(() => {
        if (!commitPreconditionHolds(this.#sql, inc, lane, opts?.closingLease, now)) return
        if (opts?.requiredCellKeys !== undefined) {
          const required = readCells(this.#sql, inc, { keys: opts.requiredCellKeys }, now)
          if ('staleInc' in required || opts.requiredCellKeys.some((cell) => !required.cells.has(cell))) return
        }
        if (opts?.retain === true) assertRetainedCapacity(this.#sql, inc, key, frame.byteLength, this.#maxRetainedBytes)
        const mark = advanceOrder(this.#sql, inc, key, now)
        if (opts?.retain === true) installRetained(this.#sql, inc, lane, frame, mark)
        const targets = snapshotRoutes(this.#sql, inc, key, now)
        accepted = { seq: mark.seq, timestamp: mark.timestamp, targets }
      })
    } catch (error) {
      return { error: (error as Error).message }
    }
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

  async readRetained(inc: string, lane: LaneId): Promise<RetainedWire | null> {
    const entry = readRetained(this.#sql, inc, lane)
    return entry === null
      ? null
      : { payloadB64: bytesToBase64(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(inc: string): Promise<LaneId[]> {
    return listRetained(this.#sql, inc)
  }

  async deleteRetainedLane(inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRetained(this.#sql, inc, lane, opts))
  }

  // ── routes / readiness ──

  async captureRouteGeneration(
    inc: string,
    attemptId: string | null = null,
    attemptCreatedAt: number | null = null,
  ): Promise<GenerationWire> {
    let generationToken: string | null = null
    let invalidAttempt = false
    this.ctx.storage.transactionSync(() => {
      const now = this.#authorityNow()
      if (attemptId !== null) {
        const prior = readRouteGenerationCapture(this.#sql, attemptId)
        if (prior !== null) {
          if (
            attemptCreatedAt !== null &&
            prior.inc === inc &&
            prior.createdAt === attemptCreatedAt &&
            touchRouteGenerationCapture(
              this.#sql,
              attemptId,
              inc,
              prior.token,
              attemptCreatedAt,
              now,
              ROUTE_CAPTURE_TTL_MS,
            )
          ) {
            generationToken = prior.token
          } else {
            invalidAttempt = true
          }
          return
        }
        // This is the critical absent-row distinction. A genuinely fresh internal attempt carries an
        // authority-aligned creation epoch inside the bounded capture window. Once its durable row has
        // expired and been swept, the same old epoch can never be reinterpreted as a new first capture.
        if (
          attemptCreatedAt === null ||
          !Number.isSafeInteger(attemptCreatedAt) ||
          attemptCreatedAt > now ||
          attemptCreatedAt + ROUTE_CAPTURE_TTL_MS <= now
        ) {
          invalidAttempt = true
          return
        }
      }
      const head = readLiveHead(this.#sql, now)
      if (head?.currentInc === inc && head.state === 'open') {
        generationToken = readGenerationToken(this.#sql, inc)
        if (generationToken !== null && attemptId !== null) {
          insertRouteGenerationCapture(
            this.#sql,
            attemptId,
            inc,
            generationToken,
            attemptCreatedAt!,
            now + ROUTE_CAPTURE_TTL_MS,
          )
          const pinned = readRouteGenerationCapture(this.#sql, attemptId)
          if (
            pinned === null ||
            pinned.inc !== inc ||
            pinned.token !== generationToken ||
            pinned.createdAt !== attemptCreatedAt
          ) {
            generationToken = null
            invalidAttempt = true
          }
        }
      }
    })
    if (invalidAttempt) {
      return { rejected: true, reason: 'generation capture attempt is absent, expired, or invalid', terminal: true }
    }
    return generationToken === null
      ? { rejected: true, reason: `room has no open incarnation '${inc}'`, terminal: true }
      : { ok: true, generationToken }
  }

  async releaseRouteGenerationCapture(attemptId: string): Promise<void> {
    this.ctx.storage.transactionSync(() => {
      this.#sql.exec('DELETE FROM route_capture WHERE attempt_id = ?', attemptId)
    })
  }

  async countRouteGenerationCaptures(): Promise<number> {
    return countRouteGenerationCaptures(this.#sql)
  }

  async registerRoute(
    roomId: string,
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
    expectedGenerationToken: string,
    captureAttemptId: string | null = null,
    captureCreatedAt: number | null = null,
  ): Promise<RegisterWire> {
    // The shard id is canonical only inside the configured session namespace. Parse it before touching
    // durable state; an authority never probes back into the caller during registration (self-fanout).
    const sessionNamespace = this.#sessionNamespace()
    if (!/^[0-9a-f]{64}$/.test(subscriberDoId)) {
      return { rejected: true, reason: `subscriber Durable Object id '${subscriberDoId}' is invalid`, terminal: true }
    }
    try {
      sessionNamespace.idFromString(subscriberDoId)
    } catch {
      return { rejected: true, reason: `subscriber Durable Object id '${subscriberDoId}' is invalid`, terminal: true }
    }
    let observedGenerationToken: string | null = null
    this.ctx.storage.transactionSync(() => {
      const head = readLiveHead(this.#sql, this.#authorityNow())
      if (head?.currentInc === inc && head.state === 'open') {
        observedGenerationToken = readGenerationToken(this.#sql, inc)
      }
    })
    if (observedGenerationToken === null) {
      return { rejected: true, reason: `room has no open incarnation '${inc}'`, terminal: true }
    }
    if (expectedGenerationToken !== observedGenerationToken) {
      return { rejected: true, reason: `generation '${inc}' was invalidated`, terminal: true }
    }
    const probedGenerationToken = observedGenerationToken
    let expiresAt = 0
    let registered = false
    let generationChanged = false
    let captureInvalid = false
    this.ctx.storage.transactionSync(() => {
      // Mint the full TTL from durable registration, never from before the awaited addressability probe.
      const now = this.#authorityNow()
      const head = readLiveHead(this.#sql, now)
      if (head === null || head.currentInc !== inc || head.state !== 'open') return
      if (readGenerationToken(this.#sql, inc) !== probedGenerationToken) {
        generationChanged = true
        return
      }
      if (
        captureAttemptId !== null &&
        (captureCreatedAt === null ||
          !touchRouteGenerationCapture(
            this.#sql,
            captureAttemptId,
            inc,
            probedGenerationToken,
            captureCreatedAt,
            now,
            ROUTE_CAPTURE_TTL_MS,
          ))
      ) {
        captureInvalid = true
        return
      }
      expiresAt = upsertRoute(this.#sql, roomId, inc, laneKey, subscriberDoId, leaseId, probedGenerationToken, now)
      registered = true
    })
    if (registered) return { ok: true, expiresAt, generationToken: probedGenerationToken }
    return generationChanged || captureInvalid
      ? { rejected: true, reason: `generation '${inc}' was invalidated`, terminal: true }
      : { rejected: true, reason: `room has no open incarnation '${inc}'`, terminal: true }
  }

  async renewRoute(
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
    expectedGenerationToken: string | null = null,
    captureAttemptId: string | null = null,
    captureCreatedAt: number | null = null,
  ): Promise<{ ok: boolean; expiresAt?: number; terminal?: boolean }> {
    const now = this.#authorityNow()
    let result!: ReturnType<typeof renewRoute>
    let generationInvalid = false
    this.ctx.storage.transactionSync(() => {
      const currentGenerationToken = readGenerationToken(this.#sql, inc)
      if (
        currentGenerationToken === null ||
        (expectedGenerationToken !== null && currentGenerationToken !== expectedGenerationToken)
      ) {
        generationInvalid = true
        return
      }
      if (captureAttemptId !== null) {
        const capture = readRouteGenerationCapture(this.#sql, captureAttemptId)
        if (
          captureCreatedAt === null ||
          capture === null ||
          capture.inc !== inc ||
          capture.token !== currentGenerationToken ||
          capture.createdAt !== captureCreatedAt ||
          capture.expiresAt <= now
        ) {
          generationInvalid = true
          return
        }
      }
      result = renewRoute(this.#sql, inc, laneKey, subscriberDoId, leaseId, now)
      if (
        result.ok &&
        captureAttemptId !== null &&
        !touchRouteGenerationCapture(
          this.#sql,
          captureAttemptId,
          inc,
          currentGenerationToken,
          captureCreatedAt!,
          now,
          ROUTE_CAPTURE_TTL_MS,
        )
      ) {
        // The read-only capture validation and exact route renewal share this synchronous transaction;
        // an exact capture cannot disappear between them. Throwing rolls back the route renewal if that
        // invariant is ever violated instead of committing a live route without its generation fence.
        throw new Error('route capture changed during exact renewal')
      }
    })
    if (generationInvalid) return { ok: false, terminal: true }
    // A missing/non-live exact route inside the SAME generation is recoverable: the subscription
    // lifecycle enters lost and establishes a fresh lease. Only generation identity loss is terminal.
    return result.ok ? { ok: true, expiresAt: result.expiresAt } : { ok: false }
  }

  async unsubscribeRoute(inc: string, laneKey: string, subscriberDoId: string, leaseId: string): Promise<void> {
    const installation = listRouteInstallations(this.#sql, inc).find(
      (entry) => entry.laneKey === laneKey && entry.subscriberDoId === subscriberDoId && entry.leaseId === leaseId,
    )
    if (installation === undefined) return
    await this.#invalidateInstallation(installation)
    this.ctx.storage.transactionSync(() => deleteRoute(this.#sql, inc, laneKey, subscriberDoId, leaseId))
  }

  // ── generation lifecycle ──

  async listGenerations(): Promise<string[]> {
    return listGenerations(this.#sql)
  }

  async dropGeneration(inc: string): Promise<DropWire> {
    const now = this.#authorityNow()
    const head = readLiveHead(this.#sql, now)
    if (head?.currentInc === inc) {
      return { error: `dropGeneration: refusing to drop the current incarnation '${inc}'` }
    }
    const installations = listRouteInstallations(this.#sql, inc)
    // Subscriber uninstalls are fallible. Keep their durable route rows and generation entry intact
    // until every exact-lease uninstall succeeds, so a retry can replay the same invalidations after a
    // transport failure or crash. This is the generation analogue of data-first / gens-entry-last.
    await this.#invalidateInstallations(installations)
    this.ctx.storage.transactionSync(() => dropGenerationRows(this.#sql, inc))
    this.#fanout.clearIncarnation(inc)
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
      await this.#runSweep(this.#authorityNow())
    } finally {
      await this.#armAlarm()
    }
  }

  // Operational maintenance RPC: the alarm uses the same sweep, while this explicit form lets an owner
  // request immediate convergence after lifecycle work without changing any data-path semantics.
  async telefuncRoomRunMaintenance(): Promise<{ prunedRoutes: number }> {
    return { prunedRoutes: await this.#runSweep(this.#authorityNow()) }
  }

  async #runSweep(now: number): Promise<number> {
    let orphanIncs: string[] = []
    this.ctx.storage.transactionSync(() => {
      deleteExpiredRouteGenerationCaptures(this.#sql, now)
      this.#sql.exec('DELETE FROM cell WHERE expires_at IS NOT NULL AND expires_at <= ?', now)
      const currentInc = readLiveHead(this.#sql, now)?.currentInc ?? null
      orphanIncs = observeAndListGraceAgedOrphans(this.#sql, currentInc, now, GEN_ORPHAN_GRACE_MS)
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
      const outcomes = await Promise.allSettled(installations.map((entry) => this.#invalidateInstallation(entry)))
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

  async #invalidateInstallations(installations: RouteInstallation[]): Promise<void> {
    await Promise.all(installations.map((installation) => this.#invalidateInstallation(installation)))
  }

  async #invalidateInstallation(installation: RouteInstallation): Promise<void> {
    const session = this.#sessionNamespace()
    await session.get(session.idFromString(installation.subscriberDoId)).telefuncRoomInvalidate({
      roomId: installation.roomId,
      inc: installation.inc,
      laneKey: installation.laneKey,
      subscriberDoId: installation.subscriberDoId,
      leaseId: installation.leaseId,
      generationToken: installation.generationToken,
    })
  }

  #sessionNamespace(): SubscriberNamespace {
    return this.#sessionNamespaceValue
  }

  async #armAlarm(): Promise<void> {
    await this.ctx.storage.setAlarm(Date.now() + this.#alarmIntervalMs)
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
