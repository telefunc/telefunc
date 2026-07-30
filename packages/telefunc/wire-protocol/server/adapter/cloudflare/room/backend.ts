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
import { laneKey as laneKeyOf } from './codec.js'
import { CloudflareRoomSubscriptionAttempt, type CloudflareRoomSubscriptionSource } from './subscription.js'
import type {
  CellsResult,
  CommitWire,
  HeadCxResult,
  RegisterWire,
  RetainedResult,
} from './do.js'

const DIRECTORY_DO_NAME = '__telefunc_room_directory__'
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

export type RoomShardInvalidationRequest = Omit<RoomShardDeliveryRequest, 'frame' | 'seq' | 'timestamp'> & {
  terminal?: true
}

export type CloudflareRoomAuthorityStub = {
  readHead(): Promise<RoomHead | null>
  compareExchangeHead(cx: HeadCx, next: HeadNext): Promise<HeadCxResult>
  readCells(inc: string, sel: { keys: string[] } | { prefix: string }): Promise<CellsResult>
  compareExchangeCells(inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult>
  commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitWire>
  awaitDelivery(token: string): Promise<void>
  readRetained(inc: string, lane: LaneId): Promise<RetainedResult | null>
  listRetained(inc: string): Promise<LaneId[]>
  deleteRetainedLane(inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void>
  registerRoute(
    roomId: string,
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
  ): Promise<RegisterWire>
  renewRoute(
    inc: string,
    laneKey: string,
    subscriberDoId: string,
    leaseId: string,
    expectedGenerationToken: string,
  ): Promise<{ ok: boolean; terminal?: boolean }>
  unsubscribeRoute(inc: string, laneKey: string, subscriberDoId: string, leaseId: string): Promise<void>
  listGenerations(): Promise<string[]>
  dropGeneration(inc: string): Promise<void>
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

export class CloudflareRoomSessionManager {
  readonly #id: string
  readonly #subscriptionPartition = crypto.randomUUID()
  readonly #getRoomNamespace: () => CloudflareRoomNamespace
  readonly #entries = new Map<string, CloudflareRoomSubscriptionAttempt>()
  #disposed = false

  constructor(sessionId: string, getRoomNamespace: () => CloudflareRoomNamespace) {
    this.#id = sessionId
    this.#getRoomNamespace = getRoomNamespace
  }

  openSubscription(
    roomId: string,
    inc: string,
    lane: LaneId,
    receiver: BackendReceiver,
  ): CloudflareRoomSubscriptionAttempt {
    if (this.#disposed) throw new Error('Cloudflare Room session manager is disposed')
    // Resolve the binding before installing even provisional local state. The per-isolate backend calls
    // this method only after resolving this exact manager from raw async context.
    const authority = this.authority(roomId)
    const laneKey = laneKeyOf(lane)
    const source: CloudflareRoomSubscriptionSource = {
      roomId,
      inc,
      laneKey,
      subscriberDoId: this.#id,
      authority,
    }
    return this.#openSubscription(source, receiver)
  }

  async deliver(request: RoomShardDeliveryRequest): Promise<void> {
    assertOrderingPosition(request.seq, request.timestamp, 'Cloudflare Room delivery')
    if (request.subscriberDoId !== this.#id)
      throw new Error('Cloudflare Room delivery addressed the wrong session shard')
    const entry = this.#entries.get(JSON.stringify([request.roomId, request.inc, request.laneKey]))
    if (entry === undefined || !entry.matches(request)) {
      throw new Error('Cloudflare Room delivery lease is not installed')
    }
    await entry.deliver(request.frame, request.seq, request.timestamp)
  }

  invalidate(request: RoomShardInvalidationRequest): void {
    const entry = this.#entries.get(JSON.stringify([request.roomId, request.inc, request.laneKey]))
    if (entry?.matches(request)) {
      if (request.terminal === true) entry.terminate()
      else entry.invalidate()
    }
  }

  dispose(): void {
    if (this.#disposed) return
    this.#disposed = true
    for (const attempt of this.#entries.values()) attempt.terminate()
    this.#entries.clear()
  }

  get subscriptionPartition(): string {
    return this.#subscriptionPartition
  }

  valid(): boolean {
    return !this.#disposed
  }

  authority(roomId: string): CloudflareRoomAuthorityStub {
    const namespace = this.#getRoomNamespace()
    return namespace.get(namespace.idFromName(roomId))
  }

  #openSubscription(
    source: CloudflareRoomSubscriptionSource,
    receiver: BackendReceiver,
  ): CloudflareRoomSubscriptionAttempt {
    const key = JSON.stringify([source.roomId, source.inc, source.laneKey])
    let attempt!: CloudflareRoomSubscriptionAttempt
    attempt = new CloudflareRoomSubscriptionAttempt(source, receiver, {
      onClosed: () => {
        if (this.#entries.get(key) === attempt) this.#entries.delete(key)
      },
    })
    this.#entries.set(key, attempt)
    attempt.start()
    return attempt
  }
}

export function withCloudflareRoomSessionManager<T>(
  manager: CloudflareRoomSessionManager | (() => CloudflareRoomSessionManager),
  fn: () => T,
): T {
  if (!isAsyncMode()) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
  const raw: Context = { ...(getRawContext() ?? {}), [ROOM_MANAGER]: manager }
  return restoreContext(raw, fn)
}

export function getCloudflareRoomSessionManager(): CloudflareRoomSessionManager {
  if (!isAsyncMode()) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
  const managerOrFactory = getRawContext()?.[ROOM_MANAGER]
  const manager = typeof managerOrFactory === 'function' ? managerOrFactory() : managerOrFactory
  if (!(manager instanceof CloudflareRoomSessionManager)) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
  return manager
}

export class CloudflareRoomBackend implements BackendDriver {
  readonly spiVersion = BACKEND_SPI_VERSION
  readonly broadcast: CloudflareBroadcastTransport
  readonly subscriptions: SubscriptionDriver
  #disposed = false

  constructor(broadcast = new CloudflareBroadcastTransport({ baseInstanceName: 'telefunc' })) {
    this.broadcast = broadcast
    this.subscriptions = {
      bind: (source) => this.#bindSubscription(source),
    }
  }

  publish(lane: BroadcastLane, payload: Uint8Array): Promise<PublishResult> {
    return this.broadcast.publish(lane, payload)
  }

  async readHead(roomId: string): Promise<{ head: RoomHead } | null> {
    const head = await this.#stub(roomId).readHead()
    return head === null ? null : { head }
  }
  async compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<
    { ok: true; head: RoomHead } | { ok: true; deleted: true } | { conflict: true; current: RoomHead | null }
  > {
    return this.#stub(roomId).compareExchangeHead(cx, next)
  }
  async readCells(
    roomId: string,
    inc: string,
    sel: { keys: string[] } | { prefix: string },
  ): Promise<{ revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }> {
    return this.#stub(roomId).readCells(inc, sel)
  }
  async compareExchangeCells(
    roomId: string,
    inc: string,
    revision: string,
    mutations: CellMutation[],
  ): Promise<CxResult> {
    return this.#stub(roomId).compareExchangeCells(inc, revision, mutations)
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
    if ('stale' in wire) return { stale: true }
    assertOrderingPosition(wire.seq, wire.timestamp, 'CloudflareRoomBackend.commitLane')
    const deliveryToken = wire.deliveryToken
    const delivery = stub.awaitDelivery(deliveryToken)
    return { accepted: true, seq: wire.seq, timestamp: wire.timestamp, receivers: wire.receivers, delivery }
  }
  async readRetained(roomId: string, inc: string, lane: LaneId) {
    const wire = await this.#stub(roomId).readRetained(inc, lane)
    if (wire === null) return null
    assertOrderingPosition(wire.seq, wire.timestamp, 'CloudflareRoomBackend.readRetained')
    return wire
  }
  async listRetained(roomId: string, inc: string) {
    return this.#stub(roomId).listRetained(inc)
  }
  async deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }) {
    await this.#stub(roomId).deleteRetainedLane(inc, lane, opts)
  }
  async listGenerations(roomId: string) {
    return this.#stub(roomId).listGenerations()
  }
  async dropGeneration(roomId: string, inc: string) {
    await this.#stub(roomId).dropGeneration(inc)
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
    if (this.#disposed) return
    this.#disposed = true
    await this.broadcast.dispose()
  }
  get disposed(): boolean {
    return this.#disposed
  }
  #bindSubscription(source: BackendSubscriptionSource): SubscriptionBinding {
    if (source.kind === 'broadcast') {
      return {
        partition: '',
        valid: () => !this.#disposed,
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
  #stub(roomId: string): CloudflareRoomAuthorityStub {
    return getCloudflareRoomSessionManager().authority(roomId)
  }
  #directory(): CloudflareRoomAuthorityStub {
    return this.#stub(DIRECTORY_DO_NAME)
  }
}
