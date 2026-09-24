/// <reference types="@cloudflare/workers-types" />

import type { BroadcastDriver, BroadcastLane, PublishResult } from '../../../../backend/broadcast/contract.js'
import type {
  CellMutation,
  CellSelector,
  CellsRead,
  CommitOptions,
  CommitResult,
  CxResult,
  DirectoryPage,
  HeadCx,
  HeadCxResult,
  HeadNext,
  LaneId,
  RetainedFrame,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
} from '../../../../backend/room/contract.js'
import type { BackendReceiver, SubscriptionBinding, SubscriptionDriver } from '../../../../backend/subscription.js'
import { CloudflareBroadcastTransport } from '../broadcast.js'
import { encodeLaneKey } from '../../../../backend/room/lane-key.js'
import { CloudflareRoomSubscriptionAttempt } from './subscription.js'
import type { RoomAuthority } from './do.js'
import type { RouteInstallation } from './routes.js'
import { materializeCloudflareSession } from '../session.js'

// Room authorities share the Telefunc namespace with sessions and Broadcast, so a room id is always prefixed.
const ROOM_AUTHORITY_PREFIX = '__telefunc_room__:'
const DIRECTORY_DO_NAME = '__telefunc_room_directory__'

export type RoomSessionDeliveryRequest = RouteInstallation & {
  payload: Uint8Array
  seq: number
  timestamp: number
}

export type RoomSessionInvalidationRequest = RouteInstallation & { terminal?: true }

export type CloudflareRoomAuthorityStub = Omit<RoomAuthority, 'alarm'>

export type CloudflareRoomNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): CloudflareRoomAuthorityStub
}

const entryKey = (route: Pick<RouteInstallation, 'roomId' | 'inc' | 'laneKey'>) =>
  JSON.stringify([route.roomId, route.inc, route.laneKey])

export class CloudflareRoomSessionManager {
  readonly #id: string
  readonly #subscriptionPartition = crypto.randomUUID()
  readonly #entries = new Map<string, CloudflareRoomSubscriptionAttempt>()
  #disposed = false

  constructor(sessionId: string) {
    this.#id = sessionId
  }

  openSubscription(
    { roomId, inc, lane }: RoomSubscriptionSource,
    authority: CloudflareRoomAuthorityStub,
    receiver: BackendReceiver,
  ): CloudflareRoomSubscriptionAttempt {
    if (this.#disposed) throw new Error('Cloudflare Room session manager is disposed')
    const source = { roomId, inc, laneKey: encodeLaneKey(lane), sessionDoId: this.#id, authority }
    const key = entryKey(source)
    const attempt: CloudflareRoomSubscriptionAttempt = new CloudflareRoomSubscriptionAttempt(source, receiver, {
      onClosed: () => {
        if (this.#entries.get(key) === attempt) this.#entries.delete(key)
      },
    })
    this.#entries.set(key, attempt)
    attempt.start()
    return attempt
  }

  /** A delivery to a lease this session no longer holds, as after a restart, is dropped: delivery is at-most-once, and
   *  the authority's route lapses with the lease. */
  async deliver(request: RoomSessionDeliveryRequest): Promise<void> {
    const entry = this.#entries.get(entryKey(request))
    if (entry?.leaseId !== request.leaseId) return
    await entry.deliver(request.payload, request.seq, request.timestamp)
  }

  invalidate(request: RoomSessionInvalidationRequest): void {
    const entry = this.#entries.get(entryKey(request))
    if (entry?.leaseId === request.leaseId) {
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

  async readHead(roomId: string): Promise<RoomHead | null> {
    return this.#stub(roomId).readHead()
  }

  async compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    return this.#stub(roomId).compareExchangeHead(cx, next)
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
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
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const stub = this.#stub(roomId)
    const wire = await stub.commitLane(inc, lane, payload, opts)
    if ('stale' in wire) return wire
    const delivery = stub.awaitDelivery(wire.deliveryToken)
    return { accepted: true, seq: wire.seq, timestamp: wire.timestamp, receivers: wire.receivers, delivery }
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    return this.#stub(roomId).readRetained(inc, lane)
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    return this.#stub(roomId).listRetained(inc)
  }

  async deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    return this.#stub(roomId).deleteRetained(inc, lane, opts)
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    return this.#stub(roomId).dropGeneration(inc)
  }

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    return this.#directory().directoryPut(roomId, incTag)
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this.#directory().directoryDelete(roomId, incTag)
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    return this.#directory().directoryList(prefix, cursor)
  }

  async dispose(): Promise<void> {
    this.#disposed = true
  }

  #bindSubscription(source: CloudflareSubscriptionSource): SubscriptionBinding {
    if (!('roomId' in source)) {
      const member = materializeCloudflareSession().broadcast()
      return {
        partition: member.partition,
        valid: () => !this.#disposed,
        open: (receiver) => member.openSubscription(source, receiver),
      }
    }
    const manager = materializeCloudflareSession().room()
    return {
      partition: manager.subscriptionPartition,
      valid: () => manager.valid(),
      // The authority stub resolves before the manager installs any local state.
      open: (receiver) => manager.openSubscription(source, this.#stub(source.roomId), receiver),
    }
  }

  #stub(roomId: string): CloudflareRoomAuthorityStub {
    return this.#object(ROOM_AUTHORITY_PREFIX + roomId)
  }

  #directory(): CloudflareRoomAuthorityStub {
    return this.#object(DIRECTORY_DO_NAME)
  }

  #object(name: string): CloudflareRoomAuthorityStub {
    const namespace = this.#rooms()
    return namespace.get(namespace.idFromName(name))
  }
}
