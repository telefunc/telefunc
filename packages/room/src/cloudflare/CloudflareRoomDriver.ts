/// <reference types="@cloudflare/workers-types" />
export { CloudflareRoomDriver }

import type {
  CellsExchange,
  CellsExchangeResult,
  CellSelector,
  CellsRead,
  CommitAccepted,
  CommitOptions,
  CommitTarget,
  CommitResult,
  DirectoryPage,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  LaneId,
  RetainedFrame,
  RetainedOf,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
} from '../backend/contract.js'
import type { BackendReceiver, CloudflarePlaneKit, DriverAttempt, OrderingInfo } from 'telefunc/__internal'
import { CLOUDFLARE_BUCKETS, currentCloudflareMember } from 'telefunc/__internal'
import type { CloudflareRoomAuthorityStub } from './RoomAuthority.js'
import { currentRoomSessionSubscriptions } from './RoomSessionSubscriptions.js'
import { openRegionDirectory, openRoomAuthority } from './objects.js'
import { compareCodePoints } from '../utils/SortedIds.js'
import { assert } from '../utils/assert.js'

/** A subscription requires its session DO's context; a lane commit made in one goes through that session's ordered
 *  stub. */
class CloudflareRoomDriver implements RoomDriver {
  private readonly getNamespace: CloudflarePlaneKit['getNamespace']
  private readonly locationFallback: string

  constructor({ getNamespace, locationFallback }: CloudflarePlaneKit) {
    this.getNamespace = getNamespace
    this.locationFallback = locationFallback
  }

  hostKey(): string | null {
    return currentCloudflareMember()?.hostKey ?? null
  }

  subscribe(source: RoomSubscriptionSource, receiver: BackendReceiver): DriverAttempt {
    const subscriptions = currentRoomSessionSubscriptions(this)
    assert(subscriptions !== null) // core refuses a subscribe outside a session
    return subscriptions.subscribe(source, receiver)
  }

  async readHead(roomId: string): Promise<RoomHead | null> {
    return this.stub(roomId).readHead()
  }

  async compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    return this.stub(roomId).compareExchangeHead(expected, next)
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    return this.stub(roomId).readCells(inc, sel)
  }

  async *readCellPages(roomId: string, inc: string, prefix: string): AsyncGenerator<CellsRead> {
    let cursor: string | undefined
    do {
      const page = await this.stub(roomId).readCellPage(inc, prefix, cursor)
      yield page
      if (page.kind === 'stale-incarnation') return
      cursor = page.cursor
    } while (cursor !== undefined)
  }

  async compareExchangeCells(roomId: string, inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    return this.stub(roomId).compareExchangeCells(inc, exchange)
  }

  async commitLane(
    { roomId, inc, lane }: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const commit = (stub: CloudflareRoomAuthorityStub) => stub.commitLane(inc, lane, payload, opts)
    // From a session DO, through its ordered stub, so its commits to a room keep the order Room sent them in.
    const session = currentRoomSessionSubscriptions(this)
    const wire = await (session ? session.callAuthority(roomId, commit) : commit(this.stub(roomId)))
    if (wire.kind !== 'accepted') return wire
    const receipt: CommitAccepted = { kind: 'accepted', seq: wire.seq, timestamp: wire.timestamp }
    if (wire.deliveryToken !== undefined) receipt.delivery = this.stub(roomId).awaitDelivery(wire.deliveryToken)
    return receipt
  }

  async readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    return this.stub(roomId).readLaneOrder(inc, lane)
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    return this.stub(roomId).readRetained(inc, lane)
  }

  async listRetained(roomId: string, inc: string, of?: RetainedOf): Promise<LaneId[]> {
    return this.stub(roomId).listRetained(inc, of)
  }

  async deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    return this.stub(roomId).deleteRetained(inc, lane, opts)
  }

  async dropLaneOrders(roomId: string, inc: string, member: string): Promise<void> {
    return this.stub(roomId).dropLaneOrders(inc, member)
  }

  async dropIncarnation(roomId: string, inc: string): Promise<void> {
    return this.stub(roomId).dropIncarnation(inc)
  }

  // A room's listing is kept by its own authority, which also writes it to its region's directory for `Room.list()`.

  async directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    return this.stub(roomId).directoryPut(roomId, incTag, checkAt, minDelayMs, this.bucketHere())
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this.stub(roomId).directoryDelete(roomId, incTag)
  }

  async directoryGet(roomId: string): Promise<string | undefined> {
    return this.stub(roomId).directoryGet(roomId)
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    const namespace = this.getNamespace()
    const pages = await Promise.all(
      CLOUDFLARE_BUCKETS.map((bucket) => openRegionDirectory(namespace, bucket).directoryList(prefix, cursor)),
    )
    return mergeRegionPages(pages)
  }

  /** Never any: each room's authority settles its own listing on its alarm. */
  async directoryDue(): Promise<DirectoryPage['entries']> {
    return []
  }

  /** A room's authority, which the call that creates it places in `bucketHere()`. */
  stub(roomId: string): CloudflareRoomAuthorityStub {
    return openRoomAuthority(this.getNamespace(), roomId, this.bucketHere())
  }

  /** The bucket of the session this call runs in, else `locationFallback`: a cron or queue handler has none. */
  private bucketHere(): string {
    return currentCloudflareMember()?.bucket ?? this.locationFallback
  }
}

/** One page of the regions' pages, in code point order. A region that has more past its page may list a room before
 *  another region's later entries, so the page ends where the first such region's does. */
function mergeRegionPages(pages: DirectoryPage[]): DirectoryPage {
  const cursor = pages.flatMap((page) => (page.cursor === undefined ? [] : [page.cursor])).sort(compareCodePoints)[0]
  const entries = pages
    .flatMap((page) => page.entries)
    .filter(({ roomId }) => cursor === undefined || compareCodePoints(roomId, cursor) <= 0)
    .sort((a, b) => compareCodePoints(a.roomId, b.roomId))
  return cursor === undefined ? { entries } : { entries, cursor }
}
