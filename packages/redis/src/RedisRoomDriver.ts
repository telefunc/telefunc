export { RedisRoomDriver }

import type { BackendReceiver, DriverAttempt, OrderingInfo } from 'telefunc/__internal'
import type {
  CellsExchange,
  CellsExchangeResult,
  CommitAccepted,
  CommitResult,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  LaneId,
  RoomDriver,
  RoomHead,
  CellSelector,
  CellsRead,
  CommitOptions,
  CommitTarget,
  RetainedFrame,
  RetainedOf,
  DirectoryPage,
  RoomPlaneKit,
  RoomSubscriptionSource,
} from '@telefunc/room/backend'
import { decodeOrderingFrame } from 'telefunc/__internal'
import { isCluster } from './ioredis.js'
import {
  channelKey,
  directoryIndexKey,
  directoryTagsKey,
  incarnationInvalidationChannel,
  incarnationKeysKey,
  memberOrderKey,
  orderKey,
  retainedKey,
  retainedLanesKey,
  retainedOfKey,
  retainedOfTrackKey,
  commitChannels,
  type FenceTokens,
} from './keys.js'
import { LANE_PAGE_SIZE, REDIS_COMMANDS, parseOrdering } from './commands.js'
import type { RedisConnection } from './RedisConnection.js'
import type { RedisPlane } from './RedisSubscriptions.js'

const NO_FENCE_TOKENS: FenceTokens = { lane: '', everyTrack: '', everyMember: '' }

type RedisRoomDriverOptions = { connection: RedisConnection; kit: RoomPlaneKit }

/** Room's plane on a Redis backend: its store and lanes on `connection`, keyed and paged by Room's `kit`. */
class RedisRoomDriver implements RoomDriver, RedisPlane {
  private readonly connection: RedisConnection
  private readonly kit: RoomPlaneKit
  /** A Cluster's last commit per lane. */
  private readonly laneTurns = new Map<string, Promise<void>>()

  constructor({ connection, kit }: RedisRoomDriverOptions) {
    this.connection = connection
    this.kit = kit
  }

  readHead(roomId: string): Promise<RoomHead | null> {
    return this.connection.run(REDIS_COMMANDS.readHead, roomId)
  }

  compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    return this.connection.run(REDIS_COMMANDS.headExchange, { roomId, expected, next })
  }

  readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    return this.connection.run(REDIS_COMMANDS.readCells, { roomId, inc, sel })
  }

  async *readCellPages(roomId: string, inc: string, cellPrefix: string): AsyncGenerator<CellsRead> {
    let after: string | undefined
    do {
      const page = await this.connection.run(REDIS_COMMANDS.readCellPage, {
        roomId,
        inc,
        cellPrefix,
        after,
        pageSize: this.kit.CELL_PAGE_SIZE,
      })
      yield page
      if (page.kind === 'stale-incarnation') return
      after = page.after
    } while (after !== undefined)
  }

  compareExchangeCells(roomId: string, inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    return this.connection.run(REDIS_COMMANDS.cellsExchange, { roomId, inc, exchange })
  }

  commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    // A close commits under its lease, which would lapse before the hold ends.
    if (!this.connection.hold.active || opts?.closingLease !== undefined) return this.commit(target, payload, opts)
    return this.commitOnceReady(target, payload, opts)
  }

  subscribe(source: RoomSubscriptionSource, receiver: BackendReceiver): DriverAttempt {
    const { prefix } = this.connection
    return this.connection.subscriptions.subscribe({
      source,
      laneChannel: channelKey(prefix, source.roomId, source.inc, this.kit.encodeLaneKey(source.lane)),
      invalidationChannel: incarnationInvalidationChannel(prefix, source.roomId, source.inc),
      plane: this,
      receiver,
    })
  }

  /** Each of the source's lanes' last commit, read once its SUBSCRIBE is acknowledged, by the lane key its frames carry
   *  on a fan-in channel and `''` on another; null once the source's incarnation is not the open head. */
  async readLaneStarts(source: RoomSubscriptionSource): Promise<Map<string, OrderingInfo> | null> {
    const { roomId, inc, lane } = source
    if (lane.kind === 'binary-all') return await this.memberLaneStarts(roomId, inc, lane.member)
    if (lane.kind === 'binary-track') return await this.trackLaneStarts(roomId, inc, lane.track)
    return await this.connection.run(REDIS_COMMANDS.laneStart, { ...source, laneKey: this.kit.encodeLaneKey(lane) })
  }

  private async commitOnceReady(
    target: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const { roomId, inc, lane } = target
    const channels = commitChannels(this.connection.prefix, roomId, inc, lane, this.kit.encodeLaneKey(lane), this.kit)
    const held = this.connection.hold.holdCommit(channels, payload)
    if (held !== null) payload = await held
    return this.commit(target, payload, opts)
  }

  private commit(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    const commit = () => this.sendCommit(target, payload, opts)
    if (!isCluster(this.connection.client)) return commit()
    // A Cluster re-sends a command Redis refused with TRYAGAIN (its slot migrating) or CLUSTERDOWN (a failover) after a
    // delay, so a later commit on the lane could land first: each is sent once the one before it was answered.
    return this.inLaneTurn(JSON.stringify([target.roomId, target.inc, this.kit.encodeLaneKey(target.lane)]), commit)
  }

  private inLaneTurn<T>(turnKey: string, commit: () => Promise<T>): Promise<T> {
    const previous = this.laneTurns.get(turnKey)
    const committing = previous === undefined ? commit() : previous.then(commit)
    const release = () => {
      if (this.laneTurns.get(turnKey) === turn) this.laneTurns.delete(turnKey)
    }
    const turn: Promise<void> = committing.then(release, release)
    this.laneTurns.set(turnKey, turn)
    return committing
  }

  private async sendCommit(
    { roomId, inc, lane }: CommitTarget,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const laneKey = this.kit.encodeLaneKey(lane)
    const channels = commitChannels(this.connection.prefix, roomId, inc, lane, laneKey, this.kit)
    const fence =
      lane.kind === 'inbox' || lane.kind === 'control' ? this.connection.subscriptions.prepareFence(channels) : null
    let reply
    try {
      reply = await this.connection.run(REDIS_COMMANDS.commit, {
        roomId,
        inc,
        lane,
        laneKey,
        channels,
        payload,
        retain: opts?.retain === true,
        closingLease: opts?.closingLease,
        requiredCellKeys: opts?.requiredCellKeys ?? [],
        fenceTokens: fence?.tokens ?? NO_FENCE_TOKENS,
      })
    } catch (error) {
      fence?.cancel()
      throw error
    }
    if (reply.kind !== 'accepted') {
      fence?.cancel()
      return reply
    }
    const accepted: CommitAccepted = {
      kind: 'accepted',
      seq: reply.seq,
      timestamp: reply.timestamp,
      receivers: this.connection.reportsReceivers ? reply.receivers : undefined,
    }
    // The frame carries the fence token, so observing the fence proves local dispatch.
    if (fence !== null) accepted.delivery = fence.delivery
    return accepted
  }

  async readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    const laneKey = this.kit.encodeLaneKey(lane)
    const mark =
      lane.kind === 'inbox' || lane.kind === 'binary'
        ? await this.connection.client.hget(memberOrderKey(this.connection.prefix, roomId, inc, lane.member), laneKey)
        : await this.connection.client.get(orderKey(this.connection.prefix, roomId, inc, laneKey))
    return mark === null ? null : parseOrdering(mark)
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    const frame = await this.connection.client.getBuffer(
      retainedKey(this.connection.prefix, roomId, inc, this.kit.encodeLaneKey(lane)),
    )
    if (frame === null) return null
    const { payload, info } = decodeOrderingFrame(frame)
    return { payload: Uint8Array.from(payload), seq: info.seq, timestamp: info.timestamp }
  }

  async listRetained(roomId: string, inc: string, of?: RetainedOf): Promise<LaneId[]> {
    let index = retainedLanesKey(this.connection.prefix, roomId, inc)
    if (of !== undefined) {
      index =
        'member' in of
          ? retainedOfKey(this.connection.prefix, roomId, inc, of.member)
          : retainedOfTrackKey(this.connection.prefix, roomId, inc, of.track)
    }
    return (await this.connection.client.smembers(index)).map((laneKey) => this.kit.decodeLaneKey(laneKey))
  }

  deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    return this.connection.run(REDIS_COMMANDS.retainedDelete, {
      roomId,
      inc,
      lane,
      laneKey: this.kit.encodeLaneKey(lane),
      ifSeq: opts?.ifSeq,
    })
  }

  async dropLaneOrders(roomId: string, inc: string, member: string): Promise<void> {
    const orders = memberOrderKey(this.connection.prefix, roomId, inc, member)
    let cursor = '0'
    do {
      const [next, fields] = await this.connection.client.hscan(orders, cursor, 'COUNT', LANE_PAGE_SIZE)
      cursor = next
      const lanes = fields.filter((field, i) => i % 2 === 0 && this.kit.decodeLaneKey(field).kind === 'binary')
      if (lanes.length > 0)
        await this.connection.run(REDIS_COMMANDS.forgetTrackLanes, { roomId, inc, lanes, kit: this.kit })
    } while (cursor !== '0')
    await this.connection.run(REDIS_COMMANDS.dropLaneOrders, { roomId, inc, member })
  }

  async dropIncarnation(roomId: string, inc: string): Promise<void> {
    // No head names the incarnation any more, so no commit adds to its manifest while it is paged.
    const manifest = incarnationKeysKey(this.connection.prefix, roomId, inc)
    let cursor = '0'
    do {
      const [next, keys] = await this.connection.client.sscan(manifest, cursor, 'COUNT', LANE_PAGE_SIZE)
      cursor = next
      if (keys.length > 0) await this.connection.client.unlink(...keys)
    } while (cursor !== '0')
    await this.connection.run(REDIS_COMMANDS.dropIncarnation, { roomId, inc })
  }

  directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    return this.connection.run(REDIS_COMMANDS.directoryPut, { roomId, incTag, checkAt, minDelayMs })
  }

  directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this.connection.run(REDIS_COMMANDS.directoryDelete, { roomId, incTag })
  }

  async directoryGet(roomId: string): Promise<string | undefined> {
    return (await this.connection.client.hget(directoryTagsKey(this.connection.prefix), roomId)) ?? undefined
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    const index = directoryIndexKey(this.connection.prefix)
    const min = cursor === undefined ? `[${prefix}` : `(${cursor}`
    const page = await this.connection.client.zrangebylex(index, min, '+', 'LIMIT', 0, this.kit.DIRECTORY_PAGE_SIZE + 1)
    const matching: string[] = []
    for (const member of page) {
      if (member.startsWith(prefix)) matching.push(member)
      else break
    }
    const roomIds = matching.slice(0, this.kit.DIRECTORY_PAGE_SIZE)
    const last = roomIds.at(-1)
    if (last === undefined) return { entries: [] }
    const tags = await this.connection.client.hmget(directoryTagsKey(this.connection.prefix), ...roomIds)
    const entries = roomIds.flatMap((roomId, i) => {
      const incTag = tags[i]
      return incTag === null || incTag === undefined ? [] : [{ roomId, incTag }]
    })
    return matching.length > this.kit.DIRECTORY_PAGE_SIZE ? { entries, cursor: last } : { entries }
  }

  directoryDue(): Promise<DirectoryPage['entries']> {
    return this.connection.run(REDIS_COMMANDS.directoryDue, { limit: this.kit.DIRECTORY_PAGE_SIZE })
  }

  private async memberLaneStarts(
    roomId: string,
    inc: string,
    member: string,
  ): Promise<Map<string, OrderingInfo> | null> {
    const starts = new Map<string, OrderingInfo>()
    let cursor = '0'
    do {
      const page = await this.connection.run(REDIS_COMMANDS.memberLaneStarts, { roomId, inc, member, cursor })
      if (page === null) return null
      cursor = page.cursor
      for (const [laneKey, start] of page.starts) starts.set(laneKey, start)
    } while (cursor !== '0')
    return starts
  }

  private async trackLaneStarts(roomId: string, inc: string, track: string): Promise<Map<string, OrderingInfo> | null> {
    const starts = new Map<string, OrderingInfo>()
    let cursor = '0'
    do {
      const page = await this.connection.run(REDIS_COMMANDS.trackLanes, { roomId, inc, track, cursor })
      if (page === null) return null
      cursor = page.cursor
      if (page.lanes.length === 0) continue
      const marks = await this.connection.run(REDIS_COMMANDS.laneMarks, {
        roomId,
        inc,
        lanes: page.lanes,
        kit: this.kit,
      })
      for (const [i, laneKey] of page.lanes.entries()) if (marks[i] !== null) starts.set(laneKey, marks[i]!)
    } while (cursor !== '0')
    return starts
  }
}
