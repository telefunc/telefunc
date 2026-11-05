export { RedisRoomDriver }

import type { BackendReceiver, DriverAttempt, OrderingInfo } from 'telefunc/__internal'
import type {
  CellCounter,
  CellsExchange,
  CellsExchangeResult,
  CommitAccepted,
  CommitResult,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  LaneId,
  OperationResult,
  OperationWrites,
  RoomDriver,
  RoomHead,
  RoomOperation,
  CellSelector,
  CellsRead,
  CommitOptions,
  ExpiringCellsRead,
  CommitTarget,
  RetainedFrame,
  RetainedOf,
  DirectoryPage,
  RoomPlaneKit,
  RoomSubscriptionSource,
  StaleIncarnation,
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
  type CommitChannels,
  type FenceTokens,
} from './keys.js'
import { LANE_PAGE_SIZE, REDIS_COMMANDS, parseOrdering, type CommitInput } from './commands.js'
import { assert } from './assert.js'
import type { RedisConnection } from './RedisConnection.js'
import type { WaitingSend, WaitRule } from './NewSubscriptionWait.js'

const NO_FENCE_TOKENS: FenceTokens = { lane: '', everyTrack: '', everyMember: '' }

/** A member an operation drops, with the lanes its order hash had and the binary lanes it retained when read. */
type DroppedMember = { member: string; lanes: string[]; retained: string[] }
/** A plan run over what it read: its value and writes, each cell's version as read ('' while absent), and the members
 *  it drops as read. */
type PlannedOperation<Value> = {
  value: Value
  writes: OperationWrites
  reads: ReadonlyMap<string, string>
  members: DroppedMember[]
}

type RedisRoomDriverOptions = { connection: RedisConnection; kit: RoomPlaneKit }

/** Room's driver on a Redis backend: its store and lanes on `connection`, keyed and paged by Room's `kit`. */
class RedisRoomDriver implements RoomDriver {
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

  async readExpiringCells(roomId: string, inc: string, keys: readonly string[]): Promise<ExpiringCellsRead> {
    return await this.connection.run(REDIS_COMMANDS.readExpiringCells, { roomId, inc, keys })
  }

  compareExchangeCells(roomId: string, inc: string, exchange: CellsExchange): Promise<CellsExchangeResult> {
    return this.connection.run(REDIS_COMMANDS.cellsExchange, { roomId, inc, exchange })
  }

  /** This process plans the operation over the cells it reads, and one script applies the plan while those cells are
   *  as read. A leave, for example:
   *  1. The plan runs with no cell read yet. It asks for the member's record, which reads as absent, so this run
   *     returns early, and its result is dropped.
   *  2. One round trip reads the record, its version and the authority's time.
   *  3. The plan runs again over them. It asks for no cell it hadn't read, so planning ends, with the record's deletes,
   *     the hold counts, the departing record and the leave event recorded.
   *  4. The plan drops the member's lanes, so one round trip reads its lane orders and retained frames.
   *  5. One script checks that the record is still at the version read and the lanes as counted, then applies every
   *     write and the commit (`OPERATION_LUA`). If one changed meanwhile, the operation starts again at 1.
   *  So a leave takes three round trips. A plan whose reads depend on what it read takes one more per step. */
  async runOperation<Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
  ): Promise<OperationResult<Value>> {
    for (;;) {
      const planned = await this.planOperation(roomId, inc, operation, args)
      if (planned === null) return { kind: 'stale-incarnation' }
      const outcome = await this.applyOperation(roomId, inc, planned)
      if (outcome.kind === 'conflict') continue
      if (outcome.kind === 'stale-incarnation') return outcome
      const done: OperationResult<Value> = { kind: 'done', value: planned.value, counters: outcome.counters }
      if (outcome.commit !== undefined) done.commit = outcome.commit
      return done
    }
  }

  /** Runs the plan until a run asks for no cell it hadn't read. In each run a cell not read yet reads as absent; the
   *  cells a run missed are read together before the next. Null once the incarnation is not readable. */
  private async planOperation<Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
  ): Promise<PlannedOperation<Value> | null> {
    const cells = new Map<string, Uint8Array | undefined>()
    const reads = new Map<string, string>()
    let now: number | null = null
    for (;;) {
      const missing = new Set<string>()
      const recorded = this.kit.recordOperation(operation, args, {
        now: now ?? 0,
        cell: (key) => {
          if (!reads.has(key)) missing.add(key)
          return cells.get(key)
        },
      })
      if (now !== null && missing.size === 0) {
        const members = await Promise.all(
          recorded.writes.members.map((member) => this.memberLanes(roomId, inc, member)),
        )
        return { ...recorded, reads, members }
      }
      const read = await this.readCells(roomId, inc, { keys: [...missing] })
      if (read.kind === 'stale-incarnation') return null
      now = read.now
      for (const key of missing) {
        cells.set(key, read.cells.get(key))
        reads.set(key, read.versions.get(key) ?? '')
      }
    }
  }

  /** The lanes in `member`'s order hash and its retained binary lanes, which its drop removes. */
  private async memberLanes(roomId: string, inc: string, member: string): Promise<DroppedMember> {
    const { prefix } = this.connection
    const [lanes, retained] = await Promise.all([
      this.connection.send((client) => client.hkeys(memberOrderKey(prefix, roomId, inc, member))),
      this.connection.send((client) => client.smembers(retainedOfKey(prefix, roomId, inc, member))),
    ])
    return { member, lanes, retained }
  }

  private async applyOperation<Value>(
    roomId: string,
    inc: string,
    { writes, reads, members }: PlannedOperation<Value>,
  ): Promise<
    | { kind: 'conflict' }
    | StaleIncarnation
    | { kind: 'done'; counters: Map<string, CellCounter>; commit?: CommitAccepted }
  > {
    const input = { roomId, inc, writes, reads, members, kit: this.kit }
    if (writes.commit === null) {
      const outcome = await this.connection.run(REDIS_COMMANDS.operation, { ...input, commit: null })
      return outcome.kind === 'done' ? { kind: 'done', counters: outcome.counters } : outcome
    }
    const { sends, prefix } = this.connection
    const lane = this.kit.OPERATION_COMMIT_LANE
    const laneKey = this.kit.encodeLaneKey(lane)
    const channels = commitChannels(prefix, roomId, inc, lane, laneKey, this.kit)
    const waiting = sends.sendsMayWait ? sends.waitTurn(commitWaitRule(channels), writes.commit) : null
    const payload = waiting === null ? writes.commit : await waiting.turn
    const fence = this.connection.subscriptions.prepareFence(channels)
    const commit: CommitInput = {
      roomId,
      inc,
      lane,
      laneKey,
      channels,
      payload,
      retain: false,
      owner: undefined,
      closingLease: undefined,
      requiredCellKeys: [],
      fenceTokens: fence.tokens,
    }
    const run = () => this.connection.run(REDIS_COMMANDS.operation, { ...input, commit })
    let outcome
    try {
      outcome = await (isCluster(this.connection.client)
        ? this.inLaneTurn(laneTurnKey(roomId, inc, laneKey), run)
        : run())
    } catch (error) {
      fence.cancel()
      throw error
    } finally {
      waiting?.sent()
    }
    if (outcome.kind !== 'done' || outcome.commit?.kind !== 'accepted') {
      fence.cancel()
      if (outcome.kind !== 'done') return outcome
      assert(outcome.commit === null) // the script found the incarnation open
      return { kind: 'done', counters: outcome.counters }
    }
    const { seq, timestamp } = outcome.commit
    // The frame carries the fence token, so observing the fence proves local dispatch.
    return {
      kind: 'done',
      counters: outcome.counters,
      commit: { kind: 'accepted', seq, timestamp, delivery: fence.delivery },
    }
  }

  commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    const { sends, prefix } = this.connection
    // A close commits under its lease, which would lapse before the wait ends.
    if (!sends.sendsMayWait || opts?.closingLease !== undefined) return this.commitNow(target, payload, opts)
    const { roomId, inc, lane } = target
    const channels = commitChannels(prefix, roomId, inc, lane, this.kit.encodeLaneKey(lane), this.kit)
    const waiting = sends.waitTurn(commitWaitRule(channels), payload)
    if (waiting === null) return this.commitNow(target, payload, opts)
    return this.commitInTurn(target, waiting, opts)
  }

  subscribe(source: RoomSubscriptionSource, receiver: BackendReceiver): DriverAttempt {
    const { prefix } = this.connection
    const { kind } = source.lane
    const fanIn = kind === 'binary-all' || kind === 'binary-track'
    return this.connection.subscriptions.subscribe({
      channel: channelKey(prefix, source.roomId, source.inc, this.kit.encodeLaneKey(source.lane)),
      invalidationChannel: incarnationInvalidationChannel(prefix, source.roomId, source.inc),
      frames: {
        fanIn,
        placed: false,
        text: false,
        lanesEndWithMembers: fanIn || kind === 'inbox' || kind === 'binary',
      },
      readStarts: () => this.readStarts(source),
      receiver,
      reportMissed: (count) =>
        this.kit.reportDeliveryLost(
          source.roomId,
          `${count ?? 'An unknown number of'} commits never reached this server's Redis subscription`,
        ),
    })
  }

  /** Each of the source's lanes' last commit, read once its SUBSCRIBE is acknowledged, by the lane key its frames carry
   *  on a fan-in channel and `''` on another; null once the source's incarnation is not the open head. */
  private async readStarts(source: RoomSubscriptionSource): Promise<Map<string, OrderingInfo> | null> {
    const { roomId, inc, lane } = source
    if (lane.kind === 'binary-all') return await this.memberLaneStarts(roomId, inc, lane.member)
    if (lane.kind === 'binary-track') return await this.trackLaneStarts(roomId, inc, lane.track)
    return await this.connection.run(REDIS_COMMANDS.laneStart, { ...source, laneKey: this.kit.encodeLaneKey(lane) })
  }

  /** Commits in its turn while one of our subscriptions is new, then lets later commits past. */
  private async commitInTurn(
    target: CommitTarget,
    waiting: WaitingSend<Uint8Array>,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const payload = await waiting.turn
    try {
      return await this.commitNow(target, payload, opts)
    } finally {
      waiting.sent()
    }
  }

  private commitNow(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    const commit = () => this.sendCommit(target, payload, opts)
    if (!isCluster(this.connection.client)) return commit()
    // A Cluster re-sends a command Redis refused with TRYAGAIN (its slot migrating) or CLUSTERDOWN (a failover) after a
    // delay, so a later commit on the lane could land first: each is sent once the one before it was answered.
    return this.inLaneTurn(laneTurnKey(target.roomId, target.inc, this.kit.encodeLaneKey(target.lane)), commit)
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
        owner: opts?.owner,
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
    }
    // The frame carries the fence token, so observing the fence proves local dispatch.
    if (fence !== null) accepted.delivery = fence.delivery
    return accepted
  }

  async readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    const laneKey = this.kit.encodeLaneKey(lane)
    const mark =
      lane.kind === 'inbox' || lane.kind === 'binary'
        ? await this.connection.send((client) =>
            client.hget(memberOrderKey(this.connection.prefix, roomId, inc, lane.member), laneKey),
          )
        : await this.connection.send((client) => client.get(orderKey(this.connection.prefix, roomId, inc, laneKey)))
    return mark === null ? null : parseOrdering(mark)
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    const frame = await this.connection.send((client) =>
      client.getBuffer(retainedKey(this.connection.prefix, roomId, inc, this.kit.encodeLaneKey(lane))),
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
    return (await this.connection.send((client) => client.smembers(index))).map((laneKey) =>
      this.kit.decodeLaneKey(laneKey),
    )
  }

  async dropIncarnation(roomId: string, inc: string): Promise<void> {
    // No head names the incarnation any more, so no commit adds to its manifest while it is paged.
    const manifest = incarnationKeysKey(this.connection.prefix, roomId, inc)
    let cursor = '0'
    let unlinking: Promise<unknown> | undefined
    do {
      // A page's scan needs the last one's cursor, not its unlink, which the manifest doesn't see.
      const [[next, keys]] = await Promise.all([
        this.connection.send((client) => client.sscan(manifest, cursor, 'COUNT', LANE_PAGE_SIZE)),
        unlinking,
      ])
      cursor = next
      unlinking = keys.length > 0 ? this.connection.send((client) => client.unlink(...keys)) : undefined
    } while (cursor !== '0')
    await unlinking
    await this.connection.run(REDIS_COMMANDS.dropIncarnation, { roomId, inc })
  }

  directoryPut(roomId: string, incTag: string, checkAt: number, minDelayMs: number): Promise<void> {
    return this.connection.run(REDIS_COMMANDS.directoryPut, { roomId, incTag, checkAt, minDelayMs })
  }

  directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this.connection.run(REDIS_COMMANDS.directoryDelete, { roomId, incTag })
  }

  async directoryGet(roomId: string): Promise<string | undefined> {
    return (
      (await this.connection.send((client) => client.hget(directoryTagsKey(this.connection.prefix), roomId))) ??
      undefined
    )
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    const index = directoryIndexKey(this.connection.prefix)
    const min = cursor === undefined ? `[${prefix}` : `(${cursor}`
    const page = await this.connection.send((client) =>
      client.zrangebylex(index, min, '+', 'LIMIT', 0, this.kit.DIRECTORY_PAGE_SIZE + 1),
    )
    const matching: string[] = []
    for (const member of page) {
      if (member.startsWith(prefix)) matching.push(member)
      else break
    }
    const roomIds = matching.slice(0, this.kit.DIRECTORY_PAGE_SIZE)
    const last = roomIds.at(-1)
    if (last === undefined) return { entries: [] }
    const tags = await this.connection.send((client) =>
      client.hmget(directoryTagsKey(this.connection.prefix), ...roomIds),
    )
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
      // Its member's order hash also holds its inbox lane, which an every-track channel doesn't carry.
      for (const [laneKey, start] of page.starts) {
        if (this.kit.decodeLaneKey(laneKey).kind === 'binary') starts.set(laneKey, start)
      }
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

/** A binary commit also reaches its member's every-track and its track's every-member channels, and its waiting bytes
 *  count against its member's. */
function commitWaitRule({ lane, everyTrack, everyMember }: CommitChannels): WaitRule {
  if (everyTrack === null || everyMember === null) {
    return { queue: lane, channels: [lane], budget: lane, bufferLimit: 'bufferLimit' }
  }
  return {
    queue: lane,
    channels: [lane, everyTrack, everyMember],
    budget: everyTrack,
    bufferLimit: 'bufferLimitBinary',
  }
}

/** The turn a Cluster's commits to one lane take. */
function laneTurnKey(roomId: string, inc: string, laneKey: string): string {
  return JSON.stringify([roomId, inc, laneKey])
}
