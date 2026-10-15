export { RedisBackend }

import { assert } from './assert.js'
import {
  callDefinedCommand,
  createCommandClient,
  createSubscriberSocket,
  isCluster,
  type RedisClient,
} from './ioredis.js'
import type {
  BroadcastDriver,
  BroadcastPayload,
  BroadcastRoute,
  CellsCx,
  CommitAccepted,
  CommitResult,
  CxResult,
  HeadCx,
  HeadCxResult,
  HeadNext,
  LaneId,
  PublishResult,
  RoomDriver,
  RoomHead,
  CellSelector,
  CellsRead,
  CommitOptions,
  RetainedFrame,
  RetainedOf,
  DirectoryPage,
  OrderingInfo,
} from 'telefunc/__internal'
import { decodeLaneKey, decodeOrderingFrame, encodeLaneKey } from 'telefunc/__internal'
import {
  directoryIndexKey,
  directoryTagsKey,
  generationKeysKey,
  memberOrderKey,
  orderKey,
  assertKeyPrefix,
  retainedKey,
  retainedKeyPrefix,
  retainedOfKey,
  retainedOfTrackKey,
  commitChannels,
} from './keys.js'
import { LANE_PAGE_SIZE, REDIS_COMMANDS, type RedisCommand } from './commands.js'
import { RedisSubscriptionDriver, type RedisSubscriptionSource } from './subscriber.js'

const DIRECTORY_PAGE_SIZE = 100
const NO_FENCE_TOKENS = { lane: '', everyTrack: '', everyMember: '' }

type RedisBackendOptions = {
  redis: RedisClient
  prefix: string
}

class RedisBackend implements BroadcastDriver, RoomDriver {
  readonly subscriptions: RedisSubscriptionDriver

  private readonly _publisher: RedisClient
  private readonly _prefix: string
  private readonly _reportsReceivers: boolean
  /** A Cluster's last commit per lane. */
  private readonly _laneTurns = new Map<string, Promise<void>>()

  constructor(options: RedisBackendOptions) {
    assertKeyPrefix(options.prefix)
    this._publisher = createCommandClient(options.redis)
    // Pub/Sub channels span every database, so a standalone client's names carry its database.
    this._prefix = isCluster(this._publisher) ? options.prefix : `${options.prefix}${this._publisher.options.db ?? 0}:`
    // A Cluster node's PUBLISH count is node-local, so it cannot prove global absence.
    this._reportsReceivers = !isCluster(this._publisher)
    for (const { name, lua, numberOfKeys } of Object.values(REDIS_COMMANDS))
      this._publisher.defineCommand(name, numberOfKeys === null ? { lua } : { numberOfKeys, lua })
    this.subscriptions = new RedisSubscriptionDriver({
      prefix: this._prefix,
      createSubscriber: () => createSubscriberSocket(this._publisher),
      readLaneStarts: (source) => this._readLaneStarts(source),
    })
  }

  async publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    const { seq, timestamp, receivers } = await this._run(REDIS_COMMANDS.publish, { route, payload })
    return { seq, timestamp, ...(this._reportsReceivers ? { receivers } : {}) }
  }

  readHead(roomId: string): Promise<RoomHead | null> {
    return this._run(REDIS_COMMANDS.readHead, roomId)
  }

  compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    return this._run(REDIS_COMMANDS.headCx, { roomId, cx, next })
  }

  readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    return this._run(REDIS_COMMANDS.readCells, { roomId, inc, sel })
  }

  async *readCellPages(roomId: string, inc: string, cellPrefix: string): AsyncGenerator<CellsRead> {
    let after: string | undefined
    do {
      const page = await this._run(REDIS_COMMANDS.readCellPage, { roomId, inc, cellPrefix, after })
      yield page
      if ('staleInc' in page) return
      after = page.after
    } while (after !== undefined)
  }

  compareExchangeCells(roomId: string, inc: string, cx: CellsCx): Promise<CxResult> {
    return this._run(REDIS_COMMANDS.cellsCx, { roomId, inc, cx })
  }

  commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const commit = () => this._commitLane(roomId, inc, lane, payload, opts)
    if (!isCluster(this._publisher)) return commit()
    // A Cluster re-sends a command Redis refused with TRYAGAIN (its slot migrating) or CLUSTERDOWN (a failover) after a
    // delay, so a later commit on the lane could land first: each is sent once the one before it was answered.
    return this._inLaneTurn(JSON.stringify([roomId, inc, encodeLaneKey(lane)]), commit)
  }

  private _inLaneTurn<T>(turnKey: string, commit: () => Promise<T>): Promise<T> {
    const previous = this._laneTurns.get(turnKey)
    const committing = previous === undefined ? commit() : previous.then(commit)
    const release = () => {
      if (this._laneTurns.get(turnKey) === turn) this._laneTurns.delete(turnKey)
    }
    const turn: Promise<void> = committing.then(release, release)
    this._laneTurns.set(turnKey, turn)
    return committing
  }

  private async _commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const laneKey = encodeLaneKey(lane)
    const channels = commitChannels(this._prefix, roomId, inc, lane, laneKey)
    const fence = lane.kind === 'inbox' || lane.kind === 'control' ? this.subscriptions.prepareFence(channels) : null
    let reply
    try {
      reply = await this._run(REDIS_COMMANDS.commit, {
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
    if ('stale' in reply) {
      fence?.cancel()
      return reply
    }
    const accepted: CommitAccepted = {
      accepted: true,
      seq: reply.seq,
      timestamp: reply.timestamp,
      ...(this._reportsReceivers ? { receivers: reply.receivers } : {}),
    }
    // The frame carries the fence token, so observing the fence proves local dispatch.
    if (fence !== null) accepted.delivery = fence.delivery
    return accepted
  }

  async readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    const laneKey = encodeLaneKey(lane)
    const mark =
      lane.kind === 'inbox' || lane.kind === 'binary'
        ? await this._publisher.hget(memberOrderKey(this._prefix, roomId, inc, lane.member), laneKey)
        : await this._publisher.get(orderKey(this._prefix, roomId, inc, laneKey))
    if (mark === null) return null
    const [seq, timestamp] = mark.split(':').map(Number) as [number, number]
    return { seq, timestamp }
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    const frame = await this._publisher.getBuffer(retainedKey(this._prefix, roomId, inc, encodeLaneKey(lane)))
    if (frame === null) return null
    const {
      payload,
      info: { seq, timestamp },
    } = decodeOrderingFrame(frame)
    return { payload: Uint8Array.from(payload), seq, timestamp }
  }

  async listRetained(roomId: string, inc: string, of?: RetainedOf): Promise<LaneId[]> {
    if (of !== undefined) {
      const index =
        'member' in of
          ? retainedOfKey(this._prefix, roomId, inc, of.member)
          : retainedOfTrackKey(this._prefix, roomId, inc, of.track)
      return (await this._publisher.smembers(index)).map(decodeLaneKey)
    }
    const prefix = retainedKeyPrefix(this._prefix, roomId, inc)
    const keys = (await this._generationKeys(roomId, inc)).filter((key) => key.startsWith(prefix))
    return keys.map((physical) => decodeLaneKey(physical.slice(prefix.length)))
  }

  deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    return this._run(REDIS_COMMANDS.retainedDelete, { roomId, inc, lane, ifSeq: opts?.ifSeq })
  }

  async dropLaneOrders(roomId: string, inc: string, member: string): Promise<void> {
    const orders = memberOrderKey(this._prefix, roomId, inc, member)
    let cursor = '0'
    do {
      const [next, fields] = await this._publisher.hscan(orders, cursor, 'COUNT', LANE_PAGE_SIZE)
      cursor = next
      const lanes = fields.filter((field, i) => i % 2 === 0 && decodeLaneKey(field).kind === 'binary')
      if (lanes.length > 0) await this._run(REDIS_COMMANDS.forgetTrackLanes, { roomId, inc, lanes })
    } while (cursor !== '0')
    await this._run(REDIS_COMMANDS.dropLaneOrders, { roomId, inc, member })
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    const generationKeys = await this._generationKeys(roomId, inc)
    await this._run(REDIS_COMMANDS.dropGeneration, { roomId, inc, generationKeys })
  }

  directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    return this._run(REDIS_COMMANDS.directoryPut, { roomId, incTag, checkAt, minDelayMs })
  }

  directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this._run(REDIS_COMMANDS.directoryDelete, { roomId, incTag })
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    const index = directoryIndexKey(this._prefix)
    const min = cursor === undefined ? `[${prefix}` : `(${cursor}`
    const page = await this._publisher.zrangebylex(index, min, '+', 'LIMIT', 0, DIRECTORY_PAGE_SIZE + 1)
    const matching: string[] = []
    for (const member of page) {
      if (member.startsWith(prefix)) matching.push(member)
      else break
    }
    const roomIds = matching.slice(0, DIRECTORY_PAGE_SIZE)
    const last = roomIds.at(-1)
    if (last === undefined) return { entries: [] }
    const tags = await this._publisher.hmget(directoryTagsKey(this._prefix), ...roomIds)
    const entries = roomIds.flatMap((roomId, i) => {
      const incTag = tags[i]
      return incTag === null || incTag === undefined ? [] : [{ roomId, incTag }]
    })
    return matching.length > DIRECTORY_PAGE_SIZE ? { entries, cursor: last } : { entries }
  }

  directoryDue(): Promise<DirectoryPage['entries']> {
    return this._run(REDIS_COMMANDS.directoryDue, { limit: DIRECTORY_PAGE_SIZE })
  }

  /** Each of the source's lanes' last commit, read once its SUBSCRIBE is acknowledged, by the lane key its frames carry
   *  on a fan-in channel and `''` on another; null once a Room source's incarnation is not the open head. */
  private async _readLaneStarts(source: RedisSubscriptionSource): Promise<Map<string, OrderingInfo> | null> {
    if (!('roomId' in source)) return await this._run(REDIS_COMMANDS.broadcastStart, source)
    const { roomId, inc, lane } = source
    if (lane.kind === 'binary-all') return await this._memberLaneStarts(roomId, inc, lane.member)
    if (lane.kind === 'binary-track') return await this._trackLaneStarts(roomId, inc, lane.track)
    return await this._run(REDIS_COMMANDS.laneStart, source)
  }

  private async _memberLaneStarts(roomId: string, inc: string, member: string) {
    const starts = new Map<string, OrderingInfo>()
    let cursor = '0'
    do {
      const page = await this._run(REDIS_COMMANDS.memberLaneStarts, { roomId, inc, member, cursor })
      if (page === null) return null
      cursor = page.cursor
      for (const [laneKey, start] of page.starts) starts.set(laneKey, start)
    } while (cursor !== '0')
    return starts
  }

  private async _trackLaneStarts(roomId: string, inc: string, track: string) {
    const starts = new Map<string, OrderingInfo>()
    let cursor = '0'
    do {
      const page = await this._run(REDIS_COMMANDS.trackLanes, { roomId, inc, track, cursor })
      if (page === null) return null
      cursor = page.cursor
      if (page.lanes.length === 0) continue
      const marks = await this._run(REDIS_COMMANDS.laneMarks, { roomId, inc, lanes: page.lanes })
      for (const [i, laneKey] of page.lanes.entries()) if (marks[i] !== null) starts.set(laneKey, marks[i]!)
    } while (cursor !== '0')
    return starts
  }

  private _generationKeys(roomId: string, inc: string): Promise<string[]> {
    return this._publisher.smembers(generationKeysKey(this._prefix, roomId, inc))
  }

  private async _run<Input, Output>(command: RedisCommand<Input, Output>, input: Input): Promise<Output> {
    const { keys, argv } = command.invoke(this._prefix, input)
    assert(command.numberOfKeys === null || command.numberOfKeys === keys.length)
    const keysAndArgs = command.numberOfKeys === null ? [String(keys.length), ...keys, ...argv] : [...keys, ...argv]
    const name = command.binaryReply ? `${command.name}Buffer` : command.name
    return command.parse(await callDefinedCommand(this._publisher, name, keysAndArgs), input)
  }
}
