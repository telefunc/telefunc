/// <reference types="@cloudflare/workers-types" />

// The Cloudflare Room facade is deliberately split at the session shard boundary. The authority proxy has
// no callback map: callback ownership belongs to the TelefuncDurableObject instance that installed it.

import { getRawContext, isAsyncMode, restoreContext, type Context } from '../../../../../node/server/context/context.js'
import type {
  BackendDriver,
  BackendReceiver,
  BackendSubscriptionSource,
  BroadcastLane,
  CellMutation,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  PublishResult,
  RoomHead,
  SubscriptionBinding,
  SubscriptionDriver,
} from '../../../../backend/spi.js'
import { BACKEND_SPI_VERSION } from '../../../../backend/spi.js'
import { CloudflareBroadcastTransport } from '../broadcast.js'
import { base64ToBytes, bytesToBase64, laneKey as laneKeyOf } from './codec.js'
import { CloudflareRoomSubscriptionAttempt, type CloudflareRoomSubscriptionSource } from './subscription.js'
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
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
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

type ManagerEntry = {
  source: CloudflareRoomSubscriptionSource
  attempt: CloudflareRoomSubscriptionAttempt
}

export class CloudflareRoomSessionManager {
  private readonly _id: string
  private readonly _subscriptionPartition = crypto.randomUUID()
  private readonly _getRoomNamespace: () => CloudflareRoomNamespace
  private readonly _entries = new Map<string, ManagerEntry>()
  private readonly _deliverySettlements = new Map<string, Promise<void>>()
  private _disposed = false

  constructor(sessionId: string, getRoomNamespace: () => CloudflareRoomNamespace) {
    this._id = sessionId
    this._getRoomNamespace = getRoomNamespace
  }

  openSubscription(
    roomId: string,
    inc: string,
    lane: LaneId,
    receiver: BackendReceiver,
  ): CloudflareRoomSubscriptionAttempt {
    if (this._disposed) throw new Error('Cloudflare Room session manager is disposed')
    // Resolve the binding before installing even provisional local state. The per-isolate backend calls
    // this method only after resolving this exact manager from raw async context.
    const authority = this.authority(roomId)
    const laneKey = laneKeyOf(lane)
    const source: CloudflareRoomSubscriptionSource = {
      roomId,
      inc,
      laneKey,
      subscriberDoId: this._id,
      authority,
    }
    return this._openSubscription(source, receiver)
  }

  async deliver(request: RoomShardDeliveryRequest): Promise<void> {
    assertOrderingPosition(request.seq, request.timestamp, 'Cloudflare Room delivery')
    if (request.subscriberDoId !== this._id)
      throw new Error('Cloudflare Room delivery addressed the wrong session shard')
    const entry = this._entries.get(JSON.stringify([request.roomId, request.inc, request.laneKey]))
    if (entry === undefined || !entry.attempt.matches(request)) {
      throw new Error('Cloudflare Room delivery lease is not installed')
    }
    await entry.attempt.deliver(request.frame, request.seq, request.timestamp)
  }

  invalidate(request: RoomShardInvalidationRequest): void {
    const entry = this._entries.get(JSON.stringify([request.roomId, request.inc, request.laneKey]))
    if (entry?.attempt.matches(request)) entry.attempt.invalidate()
  }

  dispose(): void {
    if (this._disposed) return
    this._disposed = true
    for (const entry of this._entries.values()) entry.attempt.terminate()
    this._entries.clear()
    this._deliverySettlements.clear()
  }

  get subscriptionPartition(): string {
    return this._subscriptionPartition
  }

  valid(): boolean {
    return !this._disposed
  }

  authority(roomId: string): CloudflareRoomAuthorityStub {
    const namespace = this._getRoomNamespace()
    return namespace.get(namespace.idFromName(roomId))
  }

  settleDelivery(roomId: string, inc: string, lane: LaneId, attempt: Promise<void>): Promise<void> {
    // `awaitDelivery()` is a second DO RPC so workerd may return two already ordered authority attempts
    // in either RPC-response order. Keep the caller-visible fence on this exact event-local manager;
    // the stateless backend proxy never owns settlement state across session shards.
    const key = JSON.stringify([roomId, inc, laneKeyOf(lane)])
    const previous = this._deliverySettlements.get(key) ?? Promise.resolve()
    const delivery = previous.then(() => attempt)
    const tail = delivery.then(
      () => undefined,
      () => undefined,
    )
    this._deliverySettlements.set(key, tail)
    void tail.then(() => {
      if (this._deliverySettlements.get(key) === tail) this._deliverySettlements.delete(key)
    })
    void attempt.catch(() => {})
    void delivery.catch(() => {})
    return delivery
  }

  dropGenerationSettlements(roomId: string, inc: string): void {
    for (const key of this._deliverySettlements.keys()) {
      const [candidateRoomId, candidateInc] = JSON.parse(key) as [string, string, string]
      if (candidateRoomId === roomId && candidateInc === inc) this._deliverySettlements.delete(key)
    }
  }

  private _openSubscription(
    source: CloudflareRoomSubscriptionSource,
    receiver: BackendReceiver,
  ): CloudflareRoomSubscriptionAttempt {
    const key = JSON.stringify([source.roomId, source.inc, source.laneKey])
    let attempt!: CloudflareRoomSubscriptionAttempt
    attempt = new CloudflareRoomSubscriptionAttempt(source, receiver, {
      onClosed: () => {
        if (this._entries.get(key)?.attempt === attempt) this._entries.delete(key)
      },
    })
    this._entries.set(key, { source, attempt })
    attempt.start()
    return attempt
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

export class CloudflareRoomBackend implements BackendDriver {
  readonly spiVersion = BACKEND_SPI_VERSION
  readonly capabilities = {
    receivers: 'global' as const,
    maxRetainedPayloadBytes: MAX_RETAINED_BYTES,
    clusterSafe: false,
    directory: true,
  }
  readonly broadcast: CloudflareBroadcastTransport
  readonly subscriptions: SubscriptionDriver
  private _disposed = false

  constructor(broadcast = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc' })) {
    this.broadcast = broadcast
    this.subscriptions = {
      bind: (source) => this._bindSubscription(source),
    }
  }

  publish(lane: BroadcastLane, payload: Uint8Array): Promise<PublishResult> {
    return this.broadcast.publish(lane, payload)
  }

  async readHead(roomId: string): Promise<{ head: RoomHead } | null> {
    const wire = await this._stub(roomId).readHead()
    return wire === null ? null : { head: headFromWire(wire) }
  }
  async compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<
    { ok: true; head: RoomHead } | { ok: true; deleted: true } | { conflict: true; current: RoomHead | null }
  > {
    const wire = await this._stub(roomId).compareExchangeHead(cx, nextToWire(next))
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
    const wire = await this._stub(roomId).readCells(inc, sel)
    if ('staleInc' in wire) return wire
    return { revision: wire.revision, cells: new Map(wire.cells.map(([key, value]) => [key, base64ToBytes(value)])) }
  }
  async compareExchangeCells(
    roomId: string,
    inc: string,
    revision: string,
    mutations: CellMutation[],
  ): Promise<CxResult> {
    return this._stub(roomId).compareExchangeCells(
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
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
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
    const wire = await this._stub(roomId).readRetained(inc, lane)
    if (wire === null) return null
    assertOrderingPosition(wire.seq, wire.timestamp, 'CloudflareRoomBackend.readRetained')
    return { payload: base64ToBytes(wire.payloadB64), seq: wire.seq, timestamp: wire.timestamp }
  }
  async listRetained(roomId: string, inc: string) {
    return this._stub(roomId).listRetained(inc)
  }
  async deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }) {
    await this._stub(roomId).deleteRetainedLane(inc, lane, opts)
  }
  async listGenerations(roomId: string) {
    return this._stub(roomId).listGenerations()
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
    await this._directory().directoryPut(roomId, incTag)
  }
  async directoryDelete(roomId: string, incTag: string) {
    await this._directory().directoryDelete(roomId, incTag)
  }
  async directoryList(prefix: string, cursor?: string) {
    return this._directory().directoryList(prefix, cursor)
  }
  async dispose() {
    if (this._disposed) return
    this._disposed = true
    await this.broadcast.dispose()
  }
  get disposed(): boolean {
    return this._disposed
  }
  private _bindSubscription(source: BackendSubscriptionSource): SubscriptionBinding {
    if (source.kind === 'broadcast') {
      return {
        partition: '',
        valid: () => !this._disposed,
        open: (receiver) => this.broadcast.openSubscription(source.lane, receiver),
      }
    }
    const manager = getCloudflareRoomSessionManager()
    return {
      partition: manager.subscriptionPartition,
      valid: () => manager.valid(),
      open: (receiver) => manager.openSubscription(source.roomId, source.inc, source.lane, receiver),
    }
  }
  private _stub(roomId: string): CloudflareRoomAuthorityStub {
    return getCloudflareRoomSessionManager().authority(roomId)
  }
  private _directory(): CloudflareRoomAuthorityStub {
    return this._stub(DIRECTORY_DO_NAME)
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
