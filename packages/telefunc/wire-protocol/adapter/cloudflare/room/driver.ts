/// <reference types="@cloudflare/workers-types" />
export { CloudflareRoomDriver }
export type { CloudflareRoomNamespace }

import type {
  CellsCx,
  CellSelector,
  CellsRead,
  CommitAccepted,
  CommitOptions,
  CommitTarget,
  CommitResult,
  CxResult,
  DirectoryPage,
  HeadCx,
  HeadCxResult,
  HeadNext,
  LaneId,
  RetainedFrame,
  RetainedOf,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
} from '../../../backend/room/contract.js'
import type { OrderingInfo, SubscriptionDriver } from '../../../../node/server/__internal.js'
import type { CloudflareRoomAuthorityStub } from './do.js'
import { getRoomSessionManager } from './subscription.js'
import { currentCloudflareSession, requireCloudflareSession } from '../session.js'

// Room authorities share the Telefunc namespace with sessions and Broadcast, so a room id is always prefixed.
const ROOM_AUTHORITY_PREFIX = '__telefunc_room__:'
const DIRECTORY_DO_NAME = '__telefunc_room_directory__'

type CloudflareRoomNamespace = {
  idFromName(name: string): unknown
  get(id: unknown): CloudflareRoomAuthorityStub
}

/** A subscription requires its session DO's context; a lane commit made in one goes through that session's ordered
 *  stub. */
class CloudflareRoomDriver implements RoomDriver {
  readonly subscriptions: SubscriptionDriver<RoomSubscriptionSource>
  readonly #rooms: () => CloudflareRoomNamespace

  constructor({ rooms }: { rooms: () => CloudflareRoomNamespace }) {
    this.#rooms = rooms
    this.subscriptions = {
      bind: (source) => {
        requireCloudflareSession()
        const room = getRoomSessionManager()
        return {
          partition: room.partition,
          // A route call line opens a fresh stub, like a commit's: a stub that rejected may be broken.
          open: (receiver) => room.openSubscription(source, () => this.#stub(source.roomId), receiver),
        }
      },
      partitionHere: () => (currentCloudflareSession() === undefined ? null : getRoomSessionManager().partition),
    }
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

  async *readCellPages(roomId: string, inc: string, prefix: string): AsyncGenerator<CellsRead> {
    let cursor: string | undefined
    do {
      const page = await this.#stub(roomId).readCellPage(inc, prefix, cursor)
      yield page
      if ('staleInc' in page) return
      cursor = page.cursor
    } while (cursor !== undefined)
  }

  async compareExchangeCells(roomId: string, inc: string, cx: CellsCx): Promise<CxResult> {
    return this.#stub(roomId).compareExchangeCells(inc, cx)
  }

  async commitLane(
    { roomId, inc, lane }: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const commit = (stub: CloudflareRoomAuthorityStub) => stub.commitLane(inc, lane, payload, opts)
    // From a session DO, through its ordered stub, so its commits to a room keep the order Room sent them in.
    const wire = await (currentCloudflareSession()
      ? getRoomSessionManager().callAuthority(roomId, () => this.#stub(roomId), commit)
      : commit(this.#stub(roomId)))
    if ('stale' in wire) return wire
    const receipt: CommitAccepted = {
      accepted: true,
      seq: wire.seq,
      timestamp: wire.timestamp,
      receivers: wire.receivers,
    }
    if (wire.deliveryToken !== undefined) receipt.delivery = this.#stub(roomId).awaitDelivery(wire.deliveryToken)
    return receipt
  }

  async readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    return this.#stub(roomId).readLaneOrder(inc, lane)
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    return this.#stub(roomId).readRetained(inc, lane)
  }

  async listRetained(roomId: string, inc: string, of?: RetainedOf): Promise<LaneId[]> {
    return this.#stub(roomId).listRetained(inc, of)
  }

  async deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    return this.#stub(roomId).deleteRetained(inc, lane, opts)
  }

  async dropLaneOrders(roomId: string, inc: string, member: string): Promise<void> {
    return this.#stub(roomId).dropLaneOrders(inc, member)
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    return this.#stub(roomId).dropGeneration(inc)
  }

  async directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    return this.#directory().directoryPut(roomId, incTag, checkAt, minDelayMs)
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this.#directory().directoryDelete(roomId, incTag)
  }

  async directoryGet(roomId: string): Promise<string | undefined> {
    return this.#directory().directoryGet(roomId)
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    return this.#directory().directoryList(prefix, cursor)
  }

  async directoryDue(): Promise<DirectoryPage['entries']> {
    return this.#directory().directoryDue()
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
