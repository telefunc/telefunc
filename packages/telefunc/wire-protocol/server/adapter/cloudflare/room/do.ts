/// <reference types="@cloudflare/workers-types" />
// The class shell of the Cloudflare Room backend: `TelefuncRoomDurableObject` (one DO per room). It owns
// the RPC surface the Room backend seam calls, the room DO's own authority clock (used for lease minting,
// commit preconditions and TTLs — never a caller clock), the ephemeral delivery chains, and the alarm
// janitor. Storage, retained chunking, routes and fanout are the four invariant modules alongside it.
//
// DARK: this class is not published as a binding, not exported from any barrel, and not wired to any Room
// call site — that is W3-C. W2c proves it passes the conformance suite in local workerd.

import { DurableObject } from 'cloudflare:workers'
import type { CellMutation, CxResult, HeadCx, HeadNext, LaneId } from '../../../../backend/spi.js'
import { base64ToBytes, bytesToBase64, laneKey as laneKeyOf } from './codec.js'
import { assertRetainedCapacity, deleteRetained, installRetained, listRetained, readRetained } from './retained.js'
import { deleteRoute, pruneExpiredRoutes, renewRoute, snapshotRoutes, upsertRoute } from './routes.js'
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
  readLiveHead,
  type StoredHead,
} from './storage.js'

// The authority clock is injectable at the module seam the DO always reads. Production leaves the hook
// null, so `authorityNow()` IS `Date.now()` inside the DO; the parity fixture installs a controlled clock
// through the SAME seam so lease expiry is provable without wall-clock waits (and a skewed caller clock
// stays distinguishable). This is the one code path the DO uses for `:now`.
let authorityNowHook: (() => number) | null = null
export function __setRoomAuthorityNowHook(hook: (() => number) | null): void {
  authorityNowHook = hook
}
function authorityNow(): number {
  return authorityNowHook !== null ? authorityNowHook() : Date.now()
}

// ── wire shapes (binary as base64 across the Node↔workerd RPC seam) ──

export type HeadWire = {
  rev: string
  currentInc: string | null
  state: 'open' | 'closing' | 'closed'
  configB64: string
  closeLease?: { id: string; until: number }
}
export type HeadNextWire =
  | { head: { currentInc: string | null; state: 'open' | 'closing' | 'closed'; configB64: string; closeLease?: { id: string; durationMs: number } }; ttlMs?: number }
  | { delete: true }
export type HeadCxWire =
  | { ok: true; head: HeadWire }
  | { ok: true; deleted: true }
  | { conflict: true; current: HeadWire | null }
  | { error: string }
export type CellsWire = { revision: string; cells: Array<[string, string]> } | { staleInc: true }
export type CellMutationWire = { key: string; set?: { bytesB64: string; ttlMs?: number } }
export type CommitWire =
  // `targets` is the acceptance-time route snapshot; the subscriber-isolate (the facade in the
  // conformance lane) drives the ordered delivery chain over it. `receivers` is its size.
  | { accepted: true; seq: number; timestamp: number; receivers: number; targets: string[] }
  | { stale: true }
  | { error: string }
export type RetainedWire = { payloadB64: string; seq: number; timestamp: number }
export type RegisterWire = { ok: true; expiresAt: number } | { rejected: true; reason: string }
export type DropWire = { droppedSubscribers: Array<[string, string]> } | { error: string }

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
// (head, cells, order, retained, routes, directory) and the acceptance transaction; the ephemeral
// delivery chain (fanout.ts) is driven by the subscriber isolate over the acceptance-time route snapshot
// this DO returns — in production the room DO would RPC each subscriber DO, wiring gated to W3-C.
export class TelefuncRoomDurableObject extends DurableObject {
  readonly #sql: SqlStorage
  readonly #maxRetainedBytes: number

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env as never)
    this.#sql = ctx.storage.sql
    initSchema(this.#sql)
    this.#maxRetainedBytes = 16 * 1024 * 1024
  }

  // ── head ──

  async readHead(): Promise<HeadWire | null> {
    const head = readLiveHead(this.#sql, authorityNow())
    return head === null ? null : headToWire(head)
  }

  async compareExchangeHead(cx: HeadCx, nextWire: HeadNextWire): Promise<HeadCxWire> {
    const next = nextFromWire(nextWire)
    const now = authorityNow()
    let outcome!: ReturnType<typeof compareExchangeHead>
    // One SQL transaction: single-object serialization gives head linearizability (I1). A validation
    // throw rolls the tx back and is surfaced as a structured error the facade rethrows verbatim (the
    // conformance suite matches on the message), never as a conflict.
    try {
      this.ctx.storage.transactionSync(() => {
        outcome = compareExchangeHead(this.#sql, cx, next, now, () => crypto.randomUUID())
      })
    } catch (error) {
      return { error: (error as Error).message }
    }
    if ('conflict' in outcome) return { conflict: true, current: outcome.current === null ? null : headToWire(outcome.current) }
    if ('deleted' in outcome) return { ok: true, deleted: true }
    return { ok: true, head: headToWire(outcome.head) }
  }

  // ── cells ──

  async readCells(inc: string, sel: { keys: string[] } | { prefix: string }): Promise<CellsWire> {
    const result = readCells(this.#sql, inc, sel, authorityNow())
    if ('staleInc' in result) return { staleInc: true }
    return { revision: result.revision, cells: [...result.cells].map(([key, bytes]) => [key, bytesToBase64(bytes)]) }
  }

  async compareExchangeCells(inc: string, revision: string, mutationsWire: CellMutationWire[]): Promise<CxResult> {
    const mutations: CellMutation[] = mutationsWire.map((mutation) =>
      mutation.set === undefined
        ? { key: mutation.key }
        : { key: mutation.key, set: { bytes: base64ToBytes(mutation.set.bytesB64), ttlMs: mutation.set.ttlMs } },
    )
    const now = authorityNow()
    let result!: CxResult
    this.ctx.storage.transactionSync(() => {
      result = compareExchangeCells(this.#sql, inc, revision, mutations, now)
    })
    return result
  }

  // ── commit ──

  async commitLane(
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; orderTtlMs?: number; closingLease?: string },
  ): Promise<CommitWire> {
    const now = authorityNow()
    const key = laneKeyOf(lane)
    const frame = payload instanceof Uint8Array ? payload : new Uint8Array(payload)
    let accepted: { seq: number; timestamp: number; targets: string[] } | null = null
    // The acceptance transaction encodes the SAME precondition branch as Redis/memory. Zero-row match ⇒
    // stale. Over-cap retain throws BEFORE the order advances (the tx rolls back), surfaced as a
    // structured error the facade rethrows.
    try {
      this.ctx.storage.transactionSync(() => {
        const head = readLiveHead(this.#sql, now)
        if (head === null || !commitPreconditionHolds(head, inc, lane, opts?.closingLease, now)) return
        if (opts?.retain === true) assertRetainedCapacity(this.#sql, inc, key, frame.byteLength, this.#maxRetainedBytes)
        const mark = advanceOrder(this.#sql, inc, key, now, opts?.orderTtlMs)
        if (opts?.retain === true) installRetained(this.#sql, inc, lane, frame, mark)
        const targets = snapshotRoutes(this.#sql, inc, key, now)
        accepted = { seq: mark.seq, timestamp: mark.timestamp, targets }
      })
    } catch (error) {
      return { error: (error as Error).message }
    }
    if (accepted === null) return { stale: true }
    const settled: { seq: number; timestamp: number; targets: string[] } = accepted
    return { accepted: true, seq: settled.seq, timestamp: settled.timestamp, receivers: settled.targets.length, targets: settled.targets }
  }

  // ── retained ──

  async readRetained(inc: string, lane: LaneId): Promise<RetainedWire | null> {
    const entry = readRetained(this.#sql, inc, lane)
    return entry === null ? null : { payloadB64: bytesToBase64(entry.payload), seq: entry.seq, timestamp: entry.timestamp }
  }

  async listRetained(inc: string): Promise<LaneId[]> {
    return listRetained(this.#sql, inc)
  }

  async deleteRetainedLane(inc: string, lane?: LaneId): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRetained(this.#sql, inc, lane))
  }

  // ── routes / readiness ──

  async registerRoute(inc: string, laneKey: string, subscriber: string, leaseId: string, bucket: string | null): Promise<RegisterWire> {
    const now = authorityNow()
    const head = readLiveHead(this.#sql, now)
    // Establishment open-head check: a mismatch fails registration (ready rejects, fail-closed). The
    // addressability probe (readiness-ordering §2.3) lives in the subscriber isolate — the facade only
    // registers a route it has a live receiver for.
    if (head === null || head.currentInc !== inc || head.state !== 'open') {
      return { rejected: true, reason: `room has no open incarnation '${inc}'` }
    }
    let expiresAt = 0
    this.ctx.storage.transactionSync(() => {
      expiresAt = upsertRoute(this.#sql, inc, laneKey, subscriber, leaseId, bucket, now)
    })
    return { ok: true, expiresAt }
  }

  async renewRoute(inc: string, laneKey: string, subscriber: string, leaseId: string): Promise<{ ok: boolean; expiresAt?: number }> {
    const now = authorityNow()
    let result: { ok: true; expiresAt: number } | { ok: false } = { ok: false }
    this.ctx.storage.transactionSync(() => {
      result = renewRoute(this.#sql, inc, laneKey, subscriber, leaseId, now)
    })
    return result.ok ? { ok: true, expiresAt: result.expiresAt } : { ok: false }
  }

  async unsubscribeRoute(inc: string, laneKey: string, subscriber: string, leaseId: string): Promise<void> {
    this.ctx.storage.transactionSync(() => deleteRoute(this.#sql, inc, laneKey, subscriber, leaseId))
  }

  // ── generation lifecycle ──

  async listGenerations(): Promise<string[]> {
    // Reclaim lapsed TTL data opportunistically; reads already filter it, so this only frees rows.
    this.ctx.storage.transactionSync(() => this.#sweep(authorityNow()))
    return listGenerations(this.#sql)
  }

  async dropGeneration(inc: string): Promise<DropWire> {
    const now = authorityNow()
    const head = readLiveHead(this.#sql, now)
    if (head?.currentInc === inc) {
      return { error: `dropGeneration: refusing to drop the current incarnation '${inc}'` }
    }
    // Report the routes that were on the dropped generation so the facade can close their local
    // subscriptions (the channel no longer exists — the subscription is terminal, not merely lost).
    const droppedSubscribers = this.#sql
      .exec<{ lane_key: string; subscriber: string }>('SELECT lane_key, subscriber FROM route WHERE inc = ?', inc)
      .toArray()
      .map((row): [string, string] => [row.lane_key, row.subscriber])
    this.ctx.storage.transactionSync(() => {
      dropGenerationRows(this.#sql, inc)
    })
    return { droppedSubscribers }
  }

  // ── directory (this DO, addressed as a singleton, is the best-effort projection store) ──

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this.ctx.storage.transactionSync(() => directoryPut(this.#sql, roomId, incTag))
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.ctx.storage.transactionSync(() => directoryDelete(this.#sql, roomId, incTag))
  }

  async directoryList(prefix: string, cursor?: string): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> {
    return directoryList(this.#sql, prefix, cursor)
  }

  // ── alarm janitor ──

  async alarm(): Promise<void> {
    this.ctx.storage.transactionSync(() => this.#sweep(authorityNow()))
  }

  // Test/janitor hook: run the sweep on demand (expired cells/routes/order rows + lapsed tombstone).
  async runJanitor(): Promise<{ prunedRoutes: number }> {
    const now = authorityNow()
    let prunedRoutes = 0
    this.ctx.storage.transactionSync(() => {
      prunedRoutes = pruneExpiredRoutes(this.#sql, now)
      this.#sweep(now)
    })
    return { prunedRoutes }
  }

  #sweep(now: number): void {
    this.#sql.exec('DELETE FROM cell WHERE expires_at IS NOT NULL AND expires_at <= ?', now)
    this.#sql.exec('DELETE FROM ord WHERE expires_at IS NOT NULL AND expires_at <= ?', now)
    this.#sql.exec('DELETE FROM route WHERE expires_at <= ?', now)
    // A lapsed tombstone is reclaimed through the delete path (this backend has no native head TTL).
    this.#sql.exec("DELETE FROM head WHERE id = 1 AND state = 'closed' AND expires_at IS NOT NULL AND expires_at <= ?", now)
  }
}

// The commit precondition: one boolean, two branches. Supplying a closing lease selects the narrow
// closing-control branch outright, which is what makes every other lane stale while closing. `now` is
// authority time, so an expired lease is stale even with the correct id.
function commitPreconditionHolds(
  head: StoredHead,
  inc: string,
  lane: LaneId,
  closingLease: string | undefined,
  now: number,
): boolean {
  if (head.currentInc !== inc) return false
  if (closingLease === undefined) return head.state === 'open'
  return (
    lane.kind === 'control' &&
    head.state === 'closing' &&
    head.closeLease !== undefined &&
    head.closeLease.id === closingLease &&
    now <= head.closeLease.until
  )
}

// Factory glue (dark; publication is W3-C). Returns the DO class so a host can register it under a
// binding without importing the class name directly.
export function createTelefuncRoomDurableObjectClass(): typeof TelefuncRoomDurableObject {
  return TelefuncRoomDurableObject
}
