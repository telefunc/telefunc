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
  OperationOptions,
  OperationResult,
  RetainedFrame,
  RetainedOf,
  RoomDriver,
  RoomHead,
  RoomOperation,
  RoomSubscriptionSource,
  ExpiringCellsRead,
} from '../backend/contract.js'
import type { BackendReceiver, CloudflarePlaneKit, DriverAttempt, OrderingInfo } from 'telefunc/__internal'
import {
  CLOUDFLARE_BUCKETS,
  ChannelOverflowError,
  LEASE_CALL_DEADLINE_MS,
  currentCloudflareHostKey,
  currentCloudflareMember,
  markHandled,
} from 'telefunc/__internal'
import type { AcceptedCommitWire, OperationWire } from './RoomAuthority.js'
import { currentRoomSessionSubscriptions } from './RoomSessionSubscriptions.js'
import { openRegionDirectory, openRoomAuthority, type CloudflareRoomAuthorityStub } from './objects.js'
import { operationRequest } from '../server/store/roomOperations.js'
import { compareCodePoints } from '../utils/SortedIds.js'
import { assert } from '../utils/assert.js'
import { OPERATION_COMMIT_LANE, beforeOperation } from '../backend/operation.js'

/** A subscription requires its session DO's context; a lane commit made in one goes through that session's ordered
 *  stub. */
class CloudflareRoomDriver implements RoomDriver {
  private readonly getNamespace: CloudflarePlaneKit['getNamespace']
  private readonly locationFallback: string

  constructor({ getNamespace, locationFallback }: CloudflarePlaneKit) {
    this.getNamespace = getNamespace
    this.locationFallback = locationFallback
  }

  hostKey(): string {
    return currentCloudflareHostKey()
  }

  subscribe(source: RoomSubscriptionSource, receiver: BackendReceiver): DriverAttempt {
    const subscriptions = currentRoomSessionSubscriptions(this)
    assert(subscriptions !== null) // `hostKey()` refused a subscribe outside a session
    return subscriptions.subscribe(source, receiver)
  }

  async readHead(roomId: string): Promise<RoomHead | null> {
    return this.stub(roomId).readHead()
  }

  async compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    return rowLimited(this.stub(roomId).compareExchangeHead(expected, next, { roomId, bucket: this.bucketHere() }))
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    return this.stub(roomId).readCells(inc, sel)
  }

  async readExpiringCells(roomId: string, inc: string, keys: readonly string[]): Promise<ExpiringCellsRead> {
    return this.stub(roomId).readExpiringCells(inc, keys)
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
    return rowLimited(this.stub(roomId).compareExchangeCells(inc, exchange))
  }

  async commitLane(
    { roomId, inc, lane }: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const commit = (stub: CloudflareRoomAuthorityStub) => stub.commitLane(roomId, inc, lane, payload, opts)
    // From a session DO, through its ordered stub, so its commits to a room keep the order Room sent them in.
    const session = currentRoomSessionSubscriptions(this)
    const wire = await rowLimited(session ? session.callAuthority(roomId, commit) : commit(this.stub(roomId)))
    return wire.kind === 'accepted' ? acceptedCommit(lane, wire) : wire
  }

  /** Refuses a payload that the rows its two callers store it in would hold over the authority's SQLite row limit: a
   *  DM reply's cell, and a leave's handoff row and expiring cell. A commit on its own keeps no row of its payload, or
   *  payload chunks of up to 1.5 MB where it is retained. Any other row over the limit is refused inside its write's
   *  transaction (`rowLimited()`). */
  assertCommittable(_lane: LaneId, payload: Uint8Array): void {
    if (payload.byteLength <= CLOUDFLARE_PAYLOAD_MAX_BYTES) return
    throw new ChannelOverflowError(
      `Telefunc's Cloudflare backend takes a Room value of ${CLOUDFLARE_PAYLOAD_MAX_BYTES} bytes at most, and this one has ${payload.byteLength}`,
    )
  }

  /** In the room's authority, by the operation's name, through the session's ordered stub as a commit goes. An
   *  operation that waits for this session's inbox of a member carries the inbox's exact route instead, which the
   *  authority registers in the operation's own transaction: sent right behind the inbox's own registration, it needs
   *  no answer of it, and both or neither hold. */
  async runOperation<Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    const request = operationRequest(operation, args)
    const session = currentRoomSessionSubscriptions(this)
    const inbox =
      options?.inbox === undefined
        ? undefined
        : session?.attemptOf(roomId, inc, { kind: 'inbox', member: options.inbox.member })
    let wire: OperationWire
    if (session !== null && inbox !== undefined) {
      wire = await rowLimited(
        session.runCarrying(inbox, request, { deadline: Date.now() + LEASE_CALL_DEADLINE_MS, wanted: options?.wanted }),
      )
    } else {
      await beforeOperation(options)
      const run = (stub: CloudflareRoomAuthorityStub) => stub.runOperation(inc, request)
      wire = await rowLimited(session ? session.callAuthority(roomId, run) : run(this.stub(roomId)))
    }
    // A later write of this session's on the member's inbox, as the removal that rolls a failed join back, came first.
    if (wire.kind === 'overtaken') throw new Error(`Cloudflare Room operation '${request.name}' was overtaken`)
    if (wire.kind !== 'done') return wire
    const { value, counters, commit } = wire
    const done: OperationResult<Value> = { kind: 'done', value: value as Value, counters }
    if (commit !== undefined) done.commit = acceptedCommit(OPERATION_COMMIT_LANE, commit)
    return done
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

  async dropIncarnation(roomId: string, inc: string): Promise<void> {
    return this.stub(roomId).dropIncarnation(inc)
  }

  // A room's listing is kept by its own authority, which lists what its head lists, with the head's exchange, and sends
  // it to its region's directory for `Room.list()`: a put or a delete sends it and waits for the directory.

  async directoryPut(roomId: string): Promise<void> {
    return this.stub(roomId).directoryShow()
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this.stub(roomId).directoryShow(incTag)
  }

  async directoryGet(roomId: string): Promise<string | undefined> {
    return this.stub(roomId).directoryGet()
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    const namespace = this.getNamespace()
    const pages = await Promise.all(
      CLOUDFLARE_BUCKETS.map((bucket) => openRegionDirectory(namespace, bucket).list(prefix, cursor)),
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

/** The row limit of a Durable Object's SQLite storage, workerd's `MAX_ROW_LENGTH`
 *  (https://github.com/cloudflare/workerd/blob/v1.20261110.1/src/workerd/util/sqlite.h#L78): the length of a row's
 *  whole record, its header and every column. */
const CLOUDFLARE_ROW_MAX_BYTES = 8 * 1024 * 1024 + 34
/** The most a row `assertCommittable()` guards holds besides its payload, as SQLite records it: a DM reply's cell, of
 *  a 9-byte header, the incarnation (a UUID, 36 bytes), its key (`ack:<member>:<ack>`, two UUIDs, 77 bytes) and its
 *  version (8 bytes at most). A leave's handoff row holds 77 bytes besides its record, and its expiring cell 98 besides
 *  the leave's cause, which the record holds. */
const CLOUDFLARE_ROW_BESIDES_PAYLOAD_MAX_BYTES = 130
const CLOUDFLARE_PAYLOAD_MAX_BYTES = CLOUDFLARE_ROW_MAX_BYTES - CLOUDFLARE_ROW_BESIDES_PAYLOAD_MAX_BYTES

/** `call`, with the authority's refusal of a row over the limit as `ChannelOverflowError`: its transaction rolled
 *  back, so the call wrote nothing. */
async function rowLimited<T>(call: Promise<T>): Promise<T> {
  try {
    return await call
  } catch (error) {
    if (!(error instanceof Error && error.message.endsWith(': SQLITE_TOOBIG'))) throw error
    throw new ChannelOverflowError(
      `Telefunc's Cloudflare backend takes a stored row of ${CLOUDFLARE_ROW_MAX_BYTES} bytes at most, with its key, and this call's is longer`,
    )
  }
}

/** An inbox or control commit's receipt comes once its delivery was handed off: its delivery rejects with why it was
 *  lost, if it was, for its committer to await. */
function acceptedCommit(lane: LaneId, { seq, timestamp, lost }: AcceptedCommitWire): CommitAccepted {
  const receipt: CommitAccepted = { kind: 'accepted', seq, timestamp }
  if (lane.kind !== 'inbox' && lane.kind !== 'control') return receipt
  receipt.delivery = lost === undefined ? Promise.resolve() : markHandled(Promise.reject(new Error(lost)))
  return receipt
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
