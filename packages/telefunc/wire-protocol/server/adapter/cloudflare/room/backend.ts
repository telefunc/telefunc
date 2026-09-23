/// <reference types="@cloudflare/workers-types" />

import { getRawContext, isAsyncMode, restoreContext } from '../../../../../node/server/context/context.js'
import type { BroadcastDriver, BroadcastLane, PublishResult } from '../../../../backend/broadcast/contract.js'
import type {
  CellMutation,
  CommitResult,
  HeadCx,
  HeadNext,
  LaneId,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
} from '../../../../backend/room/contract.js'
import type { BackendReceiver, SubscriptionBinding, SubscriptionDriver } from '../../../../backend/subscription.js'
import { CloudflareBroadcastTransport } from '../broadcast.js'
import { encodeLaneKey } from '../../../../backend/room/lane-key.js'
import { CloudflareRoomSubscriptionAttempt } from './subscription.js'
import type { TelefuncRoomDurableObject } from './do.js'
import type { RouteInstallation } from './routes.js'

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
export const CLOUDFLARE_ROOM_SESSION_ERROR =
  'A Cloudflare Room subscription delivers to a Telefunc session: subscribe from a telefunction or a channel handler, not from outside a request.'

export type RoomShardDeliveryRequest = RouteInstallation & {
  frame: Uint8Array
  seq: number
  timestamp: number
}

export type RoomShardInvalidationRequest = Omit<RoomShardDeliveryRequest, 'frame' | 'seq' | 'timestamp'> & {
  terminal?: true
}

export type CloudflareRoomAuthorityStub = Omit<TelefuncRoomDurableObject, 'alarm'>

export type CloudflareRoomNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): CloudflareRoomAuthorityStub
}

function roomAuthority(namespace: CloudflareRoomNamespace, roomId: string): CloudflareRoomAuthorityStub {
  return namespace.get(namespace.idFromName(roomId))
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
    // Resolve the binding before installing provisional local state.
    const authority = this.authority(roomId)
    const laneKey = encodeLaneKey(lane)
    const source = {
      roomId,
      inc,
      laneKey,
      subscriberDoId: this.#id,
      authority,
    }
    const key = JSON.stringify([roomId, inc, laneKey])
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
    return roomAuthority(this.#getRoomNamespace(), roomId)
  }
}

export function withCloudflareRoomSessionManager<T>(
  manager: CloudflareRoomSessionManager | (() => CloudflareRoomSessionManager),
  fn: () => T,
): T {
  if (!isAsyncMode()) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
  return restoreContext({ [ROOM_MANAGER]: manager }, fn)
}

export function materializeCloudflareRoomSessionManager(): CloudflareRoomSessionManager {
  if (!isAsyncMode()) throw new Error(CLOUDFLARE_ROOM_CONTEXT_ERROR)
  const managerOrFactory = getRawContext()?.[ROOM_MANAGER]
  const manager = typeof managerOrFactory === 'function' ? managerOrFactory() : managerOrFactory
  if (!(manager instanceof CloudflareRoomSessionManager)) throw new Error(CLOUDFLARE_ROOM_SESSION_ERROR)
  return manager
}

type CloudflareSubscriptionSource = BroadcastLane | RoomSubscriptionSource

/** Room reads and commits address the authority straight from the bindings; only a subscription needs its session, from context. */
export class CloudflareRoomBackend implements BroadcastDriver, RoomDriver {
  readonly broadcast: CloudflareBroadcastTransport
  readonly subscriptions: SubscriptionDriver<CloudflareSubscriptionSource>
  readonly #rooms: () => CloudflareRoomNamespace
  #disposed = false

  constructor({ rooms, broadcast }: { rooms: () => CloudflareRoomNamespace; broadcast: CloudflareBroadcastTransport }) {
    this.#rooms = rooms
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
  async compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext) {
    return this.#stub(roomId).compareExchangeHead(cx, next)
  }
  async readCells(roomId: string, inc: string, sel: { keys: string[] } | { prefix: string }) {
    return this.#stub(roomId).readCells(inc, sel)
  }
  async compareExchangeCells(roomId: string, inc: string, revision: string, mutations: CellMutation[]) {
    return this.#stub(roomId).compareExchangeCells(inc, revision, mutations)
  }
  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitResult> {
    const stub = this.#stub(roomId)
    const wire = await stub.commitLane(roomId, inc, lane, payload, opts)
    if ('stale' in wire) return wire
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
  async deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }) {
    await this.#stub(roomId).deleteRetained(inc, lane, opts)
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
  #bindSubscription(source: CloudflareSubscriptionSource): SubscriptionBinding {
    if (!('roomId' in source)) {
      return {
        partition: '',
        valid: () => !this.#disposed,
        open: (receiver) => this.broadcast.openSubscription(source, receiver),
      }
    }
    const manager = materializeCloudflareRoomSessionManager()
    return {
      partition: manager.subscriptionPartition,
      valid: () => manager.valid(),
      open: (receiver) => manager.openSubscription(source.roomId, source.inc, source.lane, receiver),
    }
  }
  #stub(roomId: string): CloudflareRoomAuthorityStub {
    return roomAuthority(this.#rooms(), roomId)
  }
  #directory(): CloudflareRoomAuthorityStub {
    return this.#stub(DIRECTORY_DO_NAME)
  }
}
