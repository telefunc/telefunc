/// <reference types="@cloudflare/workers-types" />

// The Cloudflare Room facade is deliberately split at the session shard boundary. The authority proxy has
// no callback map: callback ownership belongs to the TelefuncDurableObject instance that installed it.

import { getRawContext, isAsyncMode, restoreContext, type Context } from '../../../../../node/server/context/context.js'
import type {
  CellMutation,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  LaneReceiver,
  LaneSubscription,
  RoomBackendSpi,
  RoomHead,
} from '../../../../backend/spi.js'
import { ROOM_SPI_VERSION } from '../../../../backend/spi.js'
import { base64ToBytes, bytesToBase64, laneKey as laneKeyOf } from './codec.js'
import {
  CloudflareLaneSubscription,
  CloudflareLaneSubscriptionMultiplexer,
  type SubscriptionScheduler,
} from './subscription.js'
import type {
  CellsWire,
  CommitWire,
  DropWire,
  GenerationWire,
  HeadCxWire,
  HeadNextWire,
  HeadWire,
  RegisterWire,
  RetainedWire,
} from './do.js'

const DIRECTORY_DO_NAME = '__telefunc_room_directory__'
const MAX_RETAINED_BYTES = 16 * 1024 * 1024
const ROOM_MANAGER = Symbol('telefunc.cloudflare.room-manager')

function assertOrderingPosition(seq: number, timestamp: number, context: string): void {
  if (!Number.isSafeInteger(seq) || seq <= 0 || !Number.isSafeInteger(timestamp) || timestamp < 0) {
    throw new Error(`${context}: invalid Room ordering position`)
  }
}

export const CLOUDFLARE_ROOM_CONTEXT_ERROR =
  // spellcheck-ignore  nodejs_als is a real Cloudflare compatibility flag (AsyncLocalStorage), not a typo
  'Cloudflare Room requires await-safe context. Import "telefunc/async_hooks" and enable the Cloudflare "nodejs_als" or "nodejs_compat" compatibility flag.'

export type RoomShardDeliveryRequest = {
  roomId: string
  inc: string
  laneKey: string
  subscriberDoId: string
  leaseId: string
  generationToken: string
  frame: Uint8Array
  seq: number
  timestamp: number
}

export type RoomShardInvalidationRequest = Omit<RoomShardDeliveryRequest, 'frame' | 'seq' | 'timestamp'>

export type CloudflareRoomAuthorityStub = {
  readHead(): Promise<HeadWire | null>
  compareExchangeHead(cx: HeadCx, next: HeadNextWire): Promise<HeadCxWire>
  readCells(inc: string, sel: { keys: string[] } | { prefix: string }): Promise<CellsWire>
  compareExchangeCells(
    inc: string,
    revision: string,
    mutations: Array<{ key: string; set?: { bytesB64: string; ttlMs?: number } }>,
  ): Promise<CxResult>
  commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string },
  ): Promise<CommitWire>
  awaitDelivery(token: string): Promise<void>
  readRetained(inc: string, lane: LaneId): Promise<RetainedWire | null>
  listRetained(inc: string): Promise<LaneId[]>
  deleteRetainedLane(inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void>
  captureRouteGeneration(
    inc: string,
    attemptId?: string | null,
    attemptCreatedAt?: number | null,
  ): Promise<GenerationWire>
  releaseRouteGenerationCapture(attemptId: string): Promise<void>
  registerRoute(
    roomId: string,
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
    expectedGenerationToken: string,
    captureAttemptId?: string | null,
    captureCreatedAt?: number | null,
  ): Promise<RegisterWire>
  renewRoute(
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
    expectedGenerationToken?: string | null,
    captureAttemptId?: string | null,
    captureCreatedAt?: number | null,
  ): Promise<{ ok: boolean; terminal?: boolean }>
  unsubscribeRoute(inc: string, laneKey: string, subscriberDoId: string, leaseId: string): Promise<void>
  listGenerations(): Promise<string[]>
  dropGeneration(inc: string): Promise<DropWire>
  directoryPut(roomId: string, incTag: string): Promise<void>
  directoryDelete(roomId: string, incTag: string): Promise<void>
  directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }>
}

export type CloudflareRoomNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): CloudflareRoomAuthorityStub
}

export function requireCloudflareRoomNamespace(
  env: unknown,
  bindingName: string = 'TelefuncRoomDurableObject',
): CloudflareRoomNamespace {
  const binding = (env as Record<string, CloudflareRoomNamespace | undefined>)[bindingName]
  if (binding === undefined) {
    throw new Error(`Missing Cloudflare Room Durable Object binding "${bindingName}". Add it to your wrangler.jsonc.`)
  }
  return binding
}

type LocalRoute = {
  leaseId: string
  generationToken: string
  callbacks: Map<symbol, LaneReceiver>
  mux: CloudflareLaneSubscriptionMultiplexer
}

type ManagerEntry = { identity: Omit<RoomShardInvalidationRequest, 'leaseId' | 'generationToken'>; route: LocalRoute }

export class CloudflareRoomSessionManager {
  readonly #id: string
  readonly #getRoomNamespace: () => CloudflareRoomNamespace
  readonly #entries = new Map<string, ManagerEntry>()
  readonly #deliverySettlements = new Map<string, Promise<void>>()
  readonly #scheduler: SubscriptionScheduler | undefined
  readonly #now: () => number
  readonly #authorityOverride: ((roomId: string) => CloudflareRoomAuthorityStub) | undefined
  readonly #jitter: (() => number) | undefined

  constructor(
    sessionId: string,
    getRoomNamespace: () => CloudflareRoomNamespace,
    options: {
      scheduler?: SubscriptionScheduler
      now?: () => number
      authority?: (roomId: string) => CloudflareRoomAuthorityStub
      jitter?: () => number
    } = {},
  ) {
    this.#id = sessionId
    this.#getRoomNamespace = getRoomNamespace
    this.#scheduler = options.scheduler
    this.#now = options.now ?? Date.now
    this.#authorityOverride = options.authority
    this.#jitter = options.jitter
  }

  subscribeLane(roomId: string, inc: string, lane: LaneId, receiver: LaneReceiver): LaneSubscription {
    // Resolve the binding before installing even provisional local state. The per-isolate backend calls
    // this method only after resolving this exact manager from raw async context.
    const authority = this.authority(roomId)
    const laneKey = laneKeyOf(lane)
    const key = JSON.stringify([roomId, inc, laneKey])
    const existing = this.#entries.get(key)
    if (existing !== undefined) {
      return this.#attach(existing.route, receiver)
    }
    const identity = { roomId, inc, laneKey, subscriberDoId: this.#id }
    let leaseId = ''
    const captureAttemptId = crypto.randomUUID()
    const captureCreatedAt = this.#now()
    let generationToken: string | null = null
    let mux!: CloudflareLaneSubscriptionMultiplexer
    let route!: LocalRoute
    const subscription = new CloudflareLaneSubscription(
      {
        establish: async (newOperation) => {
          // A single establishment operation owns one stable lease through every bounded retry. Only
          // the lifecycle state machine can begin a new operation (initial or lost->re-establish).
          if (newOperation) {
            leaseId = crypto.randomUUID()
            route.leaseId = leaseId
          }
          const capture = await authority.captureRouteGeneration(inc, captureAttemptId, captureCreatedAt)
          if ('rejected' in capture) return { ready: false, retryable: false, reason: capture.reason }
          if (generationToken !== null && generationToken !== capture.generationToken) {
            return { ready: false, retryable: false, reason: `generation '${inc}' was invalidated` }
          }
          generationToken = capture.generationToken
          route.generationToken = generationToken
          const result = await authority.registerRoute(
            roomId,
            inc,
            laneKey,
            this.#id,
            leaseId,
            generationToken,
            captureAttemptId,
            captureCreatedAt,
          )
          if ('ok' in result) {
            return { ready: true }
          }
          return { ready: false, retryable: result.terminal !== true, reason: result.reason }
        },
        renew: async () => {
          const result = await authority.renewRoute(
            inc,
            laneKey,
            this.#id,
            leaseId,
            generationToken,
            captureAttemptId,
            captureCreatedAt,
          )
          return {
            renewed: result.ok,
            terminal: result.terminal,
            reason: result.terminal ? `generation '${inc}' was invalidated` : undefined,
          }
        },
        validate: async () => {
          const result = await authority.renewRoute(
            inc,
            laneKey,
            this.#id,
            leaseId,
            generationToken,
            captureAttemptId,
            captureCreatedAt,
          )
          return {
            renewed: result.ok,
            terminal: result.terminal,
            reason: result.terminal ? `generation '${inc}' was invalidated` : undefined,
          }
        },
        unsubscribe: async () => {
          const retiringLease = leaseId
          this.#removeExact({ ...identity, leaseId: retiringLease, generationToken: generationToken ?? '' })
          const outcomes = await Promise.allSettled([
            Promise.resolve().then(() => authority.unsubscribeRoute(inc, laneKey, this.#id, retiringLease)),
            Promise.resolve().then(() => authority.releaseRouteGenerationCapture(captureAttemptId)),
          ])
          const failures = outcomes
            .filter((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected')
            .map((outcome) =>
              outcome.reason instanceof Error ? outcome.reason : new Error('unknown teardown failure'),
            )
          if (failures.length === 1) throw new Error(failures[0]!.message)
          if (failures.length > 1) throw new AggregateError(failures, 'Cloudflare Room route teardown failed')
        },
        closed: () => {
          this.#removeExact({ ...identity, leaseId, generationToken: generationToken ?? '' })
        },
      },
      {
        ...(this.#scheduler === undefined ? {} : { scheduler: this.#scheduler }),
        ...(this.#jitter === undefined ? {} : { jitter: this.#jitter }),
      },
    )
    mux = new CloudflareLaneSubscriptionMultiplexer(subscription, () => this.#entries.delete(key))
    route = { leaseId, generationToken: generationToken ?? '', callbacks: new Map(), mux }
    this.#entries.set(key, { identity, route })
    const attachment = this.#attach(route, receiver)
    subscription.start()
    return attachment
  }

  async deliver(request: RoomShardDeliveryRequest): Promise<void> {
    assertOrderingPosition(request.seq, request.timestamp, 'Cloudflare Room delivery')
    if (request.subscriberDoId !== this.#id)
      throw new Error('Cloudflare Room delivery addressed the wrong session shard')
    const entry = this.#entries.get(JSON.stringify([request.roomId, request.inc, request.laneKey]))
    if (
      entry === undefined ||
      entry.route.leaseId !== request.leaseId ||
      entry.route.generationToken !== request.generationToken
    ) {
      throw new Error('Cloudflare Room delivery lease is not installed')
    }
    await Promise.all(
      [...entry.route.callbacks.values()].map(async (callback) => {
        await (callback(new Uint8Array(request.frame), {
          seq: request.seq,
          timestamp: request.timestamp,
        }) as unknown as Promise<void> | void)
      }),
    )
  }

  invalidate(request: RoomShardInvalidationRequest): void {
    this.#removeExact(request)
  }

  dispose(): void {
    for (const entry of this.#entries.values()) entry.route.mux.backendDisposed()
    this.#entries.clear()
    this.#deliverySettlements.clear()
  }

  #removeExact(request: RoomShardInvalidationRequest): void {
    const key = JSON.stringify([request.roomId, request.inc, request.laneKey])
    const entry = this.#entries.get(key)
    if (
      entry === undefined ||
      entry.route.leaseId !== request.leaseId ||
      entry.route.generationToken !== request.generationToken
    )
      return
    entry.route.mux.lifecycleClosed(new Error('Cloudflare Room route invalidated'))
    this.#entries.delete(key)
  }

  authority(roomId: string): CloudflareRoomAuthorityStub {
    if (this.#authorityOverride !== undefined) return this.#authorityOverride(roomId)
    const namespace = this.#getRoomNamespace()
    return namespace.get(namespace.idFromName(roomId))
  }

  settleDelivery(roomId: string, inc: string, lane: LaneId, attempt: Promise<void>): Promise<void> {
    // `awaitDelivery()` is a second DO RPC so workerd may return two already ordered authority attempts
    // in either RPC-response order. Keep the caller-visible fence on this exact event-local manager;
    // the stateless backend proxy never owns settlement state across session shards.
    const key = JSON.stringify([roomId, inc, laneKeyOf(lane)])
    const previous = this.#deliverySettlements.get(key) ?? Promise.resolve()
    const delivery = previous.then(() => attempt)
    const tail = delivery.then(
      () => undefined,
      () => undefined,
    )
    this.#deliverySettlements.set(key, tail)
    void tail.then(() => {
      if (this.#deliverySettlements.get(key) === tail) this.#deliverySettlements.delete(key)
    })
    void attempt.catch(() => {})
    void delivery.catch(() => {})
    return delivery
  }

  dropGenerationSettlements(roomId: string, inc: string): void {
    for (const key of this.#deliverySettlements.keys()) {
      const [candidateRoomId, candidateInc] = JSON.parse(key) as [string, string, string]
      if (candidateRoomId === roomId && candidateInc === inc) this.#deliverySettlements.delete(key)
    }
  }

  #attach(route: LocalRoute, receiver: LaneReceiver): LaneSubscription {
    const attachmentId = Symbol('cloudflare-room-attachment')
    route.callbacks.set(attachmentId, receiver)
    return route.mux.attach(() => route.callbacks.delete(attachmentId))
  }
}

export function withCloudflareRoomSessionManager<T>(manager: CloudflareRoomSessionManager, fn: () => T): T {
  if (!isAsyncMode()) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
  const raw: Context = { ...(getRawContext() ?? {}), [ROOM_MANAGER]: manager }
  return restoreContext(raw, fn)
}

export function getCloudflareRoomSessionManager(): CloudflareRoomSessionManager {
  if (!isAsyncMode()) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
  const manager = getRawContext()?.[ROOM_MANAGER]
  if (!(manager instanceof CloudflareRoomSessionManager)) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
  return manager
}

export class CloudflareRoomBackend implements RoomBackendSpi {
  readonly spiVersion = ROOM_SPI_VERSION
  readonly capabilities = {
    receivers: 'global' as const,
    maxRetainedPayloadBytes: MAX_RETAINED_BYTES,
    clusterSafe: false,
    directory: true,
  }
  async readHead(roomId: string): Promise<{ head: RoomHead } | null> {
    const wire = await this.#stub(roomId).readHead()
    return wire === null ? null : { head: headFromWire(wire) }
  }
  async compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<
    { ok: true; head: RoomHead } | { ok: true; deleted: true } | { conflict: true; current: RoomHead | null }
  > {
    const wire = await this.#stub(roomId).compareExchangeHead(cx, nextToWire(next))
    if ('error' in wire) throw new Error(wire.error)
    if ('conflict' in wire)
      return { conflict: true, current: wire.current === null ? null : headFromWire(wire.current) }
    return 'deleted' in wire ? { ok: true, deleted: true } : { ok: true, head: headFromWire(wire.head) }
  }
  async readCells(
    roomId: string,
    inc: string,
    sel: { keys: string[] } | { prefix: string },
  ): Promise<{ revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }> {
    const wire = await this.#stub(roomId).readCells(inc, sel)
    if ('staleInc' in wire) return wire
    return { revision: wire.revision, cells: new Map(wire.cells.map(([key, value]) => [key, base64ToBytes(value)])) }
  }
  async compareExchangeCells(
    roomId: string,
    inc: string,
    revision: string,
    mutations: CellMutation[],
  ): Promise<CxResult> {
    return this.#stub(roomId).compareExchangeCells(
      inc,
      revision,
      mutations.map((mutation) =>
        mutation.set === undefined
          ? { key: mutation.key }
          : { key: mutation.key, set: { bytesB64: bytesToBase64(mutation.set.bytes), ttlMs: mutation.set.ttlMs } },
      ),
    )
  }
  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string },
  ): Promise<CommitResult> {
    const manager = getCloudflareRoomSessionManager()
    const stub = manager.authority(roomId)
    const wire = await stub.commitLane(roomId, inc, lane, payload, opts)
    try {
      if ('error' in wire) throw new Error(wire.error)
      if ('stale' in wire) return { stale: true }
      assertOrderingPosition(wire.seq, wire.timestamp, 'CloudflareRoomBackend.commitLane')
      const deliveryToken = wire.deliveryToken
      const attempt = new Promise<void>((resolve, reject) => {
        setTimeout(() => void stub.awaitDelivery(deliveryToken).then(resolve, reject), 0)
      })
      const delivery = manager.settleDelivery(roomId, inc, lane, attempt)
      return { accepted: true, seq: wire.seq, timestamp: wire.timestamp, receivers: wire.receivers, delivery }
    } finally {
      disposeRpcResult(wire)
    }
  }
  async readRetained(roomId: string, inc: string, lane: LaneId) {
    const wire = await this.#stub(roomId).readRetained(inc, lane)
    if (wire === null) return null
    assertOrderingPosition(wire.seq, wire.timestamp, 'CloudflareRoomBackend.readRetained')
    return { payload: base64ToBytes(wire.payloadB64), seq: wire.seq, timestamp: wire.timestamp }
  }
  async listRetained(roomId: string, inc: string) {
    return this.#stub(roomId).listRetained(inc)
  }
  async deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }) {
    await this.#stub(roomId).deleteRetainedLane(inc, lane, opts)
  }
  subscribeLane(roomId: string, inc: string, lane: LaneId, receiver: LaneReceiver): LaneSubscription {
    return getCloudflareRoomSessionManager().subscribeLane(roomId, inc, lane, receiver)
  }
  async listGenerations(roomId: string) {
    return this.#stub(roomId).listGenerations()
  }
  async dropGeneration(roomId: string, inc: string) {
    const manager = getCloudflareRoomSessionManager()
    const wire = await manager.authority(roomId).dropGeneration(inc)
    if ('error' in wire) throw new Error(wire.error)
    manager.dropGenerationSettlements(roomId, inc)
    for (const dropped of wire.droppedSubscribers) {
      manager.invalidate({ roomId, inc, ...dropped })
    }
  }
  async directoryPut(roomId: string, incTag: string) {
    await this.#directory().directoryPut(roomId, incTag)
  }
  async directoryDelete(roomId: string, incTag: string) {
    await this.#directory().directoryDelete(roomId, incTag)
  }
  async directoryList(prefix: string, cursor?: string) {
    return this.#directory().directoryList(prefix, cursor)
  }
  async dispose() {
    /* The stateless proxy owns no callbacks, authority stubs, or caller-settlement state. */
  }
  #stub(roomId: string): CloudflareRoomAuthorityStub {
    return getCloudflareRoomSessionManager().authority(roomId)
  }
  #directory(): CloudflareRoomAuthorityStub {
    return this.#stub(DIRECTORY_DO_NAME)
  }
}

function disposeRpcResult(value: unknown): void {
  if (typeof value !== 'object' || value === null || !(Symbol.dispose in value)) return
  const dispose = (value as { [Symbol.dispose]?: unknown })[Symbol.dispose]
  if (typeof dispose === 'function') dispose.call(value)
}

function headFromWire(wire: HeadWire): RoomHead {
  const head: RoomHead = {
    rev: wire.rev,
    currentInc: wire.currentInc,
    state: wire.state,
    config: base64ToBytes(wire.configB64),
  }
  if (wire.closeLease !== undefined) head.closeLease = wire.closeLease
  return head
}
function nextToWire(next: HeadNext): HeadNextWire {
  if ('delete' in next) return next
  const head: Extract<HeadNextWire, { head: unknown }>['head'] = {
    currentInc: next.head.currentInc,
    state: next.head.state,
    configB64: bytesToBase64(next.head.config),
  }
  if (next.head.closeLease !== undefined) head.closeLease = next.head.closeLease
  return next.ttlMs === undefined ? { head } : { head, ttlMs: next.ttlMs }
}
