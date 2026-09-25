/// <reference types="@cloudflare/workers-types" />
export { CloudflareBackend }
export type { CloudflareRoomAuthorityStub, CloudflareRoomNamespace }

import type { BroadcastDriver, BroadcastRoute, PublishResult } from '../../../../backend/broadcast/contract.js'
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
import type { SubscriptionBinding, SubscriptionDriver } from '../../../../backend/subscription.js'
import { CloudflareBroadcastTransport } from '../broadcast.js'
import type { RoomAuthority } from './do.js'
import type { DurableObject } from 'cloudflare:workers'
import { currentCloudflareSession, requireCloudflareSession } from '../session.js'

// Room authorities share the Telefunc namespace with sessions and Broadcast, so a room id is always prefixed.
const ROOM_AUTHORITY_PREFIX = '__telefunc_room__:'
const DIRECTORY_DO_NAME = '__telefunc_room_directory__'

/** The room authority's own methods, which its Durable Object serves over RPC. */
type CloudflareRoomAuthorityStub = Omit<RoomAuthority, keyof DurableObject>

type CloudflareRoomNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): CloudflareRoomAuthorityStub
}

type CloudflareSubscriptionSource = BroadcastRoute | RoomSubscriptionSource

/** Room reads and commits address the authority straight from the bindings; only a subscription needs its session, from context. */
class CloudflareBackend implements BroadcastDriver, RoomDriver {
  readonly broadcast: CloudflareBroadcastTransport
  readonly subscriptions: SubscriptionDriver<CloudflareSubscriptionSource>
  readonly #rooms: () => CloudflareRoomNamespace

  constructor({ rooms, broadcast }: { rooms: () => CloudflareRoomNamespace; broadcast: CloudflareBroadcastTransport }) {
    this.#rooms = rooms
    this.broadcast = broadcast
    this.subscriptions = {
      bind: (source) => this.#bindSubscription(source),
      partitionHere: (source) => {
        const session = currentCloudflareSession()
        if (session === undefined) return null
        return 'roomId' in source ? session.room.partition : session.broadcast.partition
      },
    }
  }

  publish(route: BroadcastRoute, payload: Uint8Array): Promise<PublishResult> {
    return this.broadcast.publish(route, payload)
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
    const commit = (stub: CloudflareRoomAuthorityStub) => stub.commitLane(inc, lane, payload, opts)
    // From a session DO, through its ordered stub, so its commits to a room keep the order Room sent them in.
    const session = currentCloudflareSession()
    const wire = await (session
      ? session.room.callAuthority(roomId, () => this.#stub(roomId), commit)
      : commit(this.#stub(roomId)))
    if ('stale' in wire) return wire
    const delivery = this.#stub(roomId).awaitDelivery(wire.deliveryToken)
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

  #bindSubscription(source: CloudflareSubscriptionSource): SubscriptionBinding {
    if (!('roomId' in source)) {
      const member = requireCloudflareSession().broadcast
      return {
        partition: member.partition,
        open: (receiver) => member.openSubscription(source, receiver),
      }
    }
    const manager = requireCloudflareSession().room
    return {
      partition: manager.partition,
      // A route call line opens a fresh stub, like a commit's: a stub that rejected may be broken.
      open: (receiver) => manager.openSubscription(source, () => this.#stub(source.roomId), receiver),
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
