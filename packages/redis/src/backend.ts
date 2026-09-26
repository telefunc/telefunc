export { RedisBackend }

import { assert } from './assert.js'
import {
  assertAtMostOnceClient,
  callDefinedCommand,
  createSubscriberSocket,
  isCluster,
  type RedisClient,
} from './ioredis.js'
import type {
  BroadcastDriver,
  BroadcastRoute,
  CellMutation,
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
  DirectoryPage,
} from 'telefunc/__internal'
import { decodeLaneKey, decodeOrderingFrame, encodeLaneKey } from 'telefunc/__internal'
import {
  DEFAULT_PREFIX,
  directoryIndexKey,
  directoryTagsKey,
  generationKeysKey,
  assertKeyPrefix,
  retainedKey,
  retainedKeyPrefix,
} from './keys.js'
import { REDIS_COMMANDS, type RedisCommand } from './commands.js'
import { RedisSubscriptionDriver } from './subscriber.js'

const DIRECTORY_PAGE_SIZE = 100
const STABLE_READ_ATTEMPTS = 8

type RedisBackendOptions = {
  redis: RedisClient
  prefix?: string
}

class RedisBackend implements BroadcastDriver, RoomDriver {
  readonly subscriptions: RedisSubscriptionDriver

  private readonly _publisher: RedisClient
  private readonly _prefix: string
  private readonly _reportsReceivers: boolean
  /** A Cluster's last commit per lane. */
  private readonly _laneTurns = new Map<string, Promise<void>>()

  constructor(options: RedisBackendOptions) {
    assertAtMostOnceClient(options.redis)
    this._publisher = options.redis
    const prefix = options.prefix ?? DEFAULT_PREFIX
    assertKeyPrefix(prefix)
    // Pub/Sub channels span every database, so a standalone client's names carry its database.
    this._prefix = isCluster(options.redis) ? prefix : `${prefix}${options.redis.options.db ?? 0}:`
    // A Cluster node's PUBLISH count is node-local, so it cannot prove global absence.
    this._reportsReceivers = !isCluster(options.redis)
    for (const { name, lua, numberOfKeys } of Object.values(REDIS_COMMANDS))
      this._publisher.defineCommand(name, numberOfKeys === null ? { lua } : { numberOfKeys, lua })
    this.subscriptions = new RedisSubscriptionDriver({
      prefix: this._prefix,
      createSubscriber: () => createSubscriberSocket(options.redis),
      validateGeneration: (source) => this._run(REDIS_COMMANDS.validateGeneration, source),
    })
  }

  async publish(route: BroadcastRoute, payload: Uint8Array): Promise<PublishResult> {
    const { seq, timestamp, receivers } = await this._run(REDIS_COMMANDS.publish, { route, payload })
    return { seq, timestamp, ...(this._reportsReceivers ? { receivers } : {}) }
  }

  readHead(roomId: string): Promise<RoomHead | null> {
    return this._run(REDIS_COMMANDS.readHead, roomId)
  }

  compareExchangeHead(roomId: string, cx: HeadCx, next: HeadNext): Promise<HeadCxResult> {
    return this._run(REDIS_COMMANDS.headCx, { roomId, cx, next })
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    if ('keys' in sel) {
      const read = await this._run(REDIS_COMMANDS.readCells, { roomId, inc, keys: sel.keys })
      assert(read !== 'moved') // without an expected revision there is nothing to move from
      return read
    }
    for (let attempt = 0; attempt < STABLE_READ_ATTEMPTS; attempt++) {
      const found = await this._run(REDIS_COMMANDS.findCells, { roomId, inc, cellPrefix: sel.prefix })
      if ('staleInc' in found) return found
      const read = await this._run(REDIS_COMMANDS.readCells, { roomId, inc, ...found })
      if (read !== 'moved') return read
    }
    throw new Error(`readCells: stable read did not converge in ${STABLE_READ_ATTEMPTS} attempts (room '${roomId}')`)
  }

  compareExchangeCells(roomId: string, inc: string, revision: string, mutations: CellMutation[]): Promise<CxResult> {
    return this._run(REDIS_COMMANDS.cellsCx, { roomId, inc, revision, mutations })
  }

  commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    if (!isCluster(this._publisher)) return this._commitLane(roomId, inc, lane, payload, opts)
    // A Cluster re-sends a command Redis refused with TRYAGAIN (its slot migrating) or CLUSTERDOWN (a failover) after a
    // delay, so a later commit on the lane could land first: each is sent once the one before it was answered.
    const turnKey = JSON.stringify([roomId, inc, encodeLaneKey(lane)])
    const previous = this._laneTurns.get(turnKey)
    const commit = () => this._commitLane(roomId, inc, lane, payload, opts)
    const committing = previous === undefined ? commit() : previous.then(commit)
    const turn = committing.then(
      () => {},
      () => {},
    )
    this._laneTurns.set(turnKey, turn)
    void turn.then(() => {
      if (this._laneTurns.get(turnKey) === turn) this._laneTurns.delete(turnKey)
    })
    return committing
  }

  private async _commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: CommitOptions,
  ): Promise<CommitResult> {
    const fence = this.subscriptions.prepareFence({ roomId, inc, lane })
    let reply
    try {
      reply = await this._run(REDIS_COMMANDS.commit, {
        roomId,
        inc,
        lane,
        payload,
        retain: opts?.retain === true,
        closingLease: opts?.closingLease,
        requiredCellKeys: opts?.requiredCellKeys ?? [],
        fenceToken: fence.token,
      })
    } catch (error) {
      fence.cancel()
      throw error
    }
    if ('stale' in reply) {
      fence.cancel()
      return reply
    }
    // Data and fence leave the same slot owner in order, so observing the fence proves local dispatch.
    return {
      accepted: true,
      seq: reply.seq,
      timestamp: reply.timestamp,
      ...(this._reportsReceivers ? { receivers: reply.receivers } : {}),
      delivery: fence.delivery,
    }
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

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    const prefix = retainedKeyPrefix(this._prefix, roomId, inc)
    const keys = (await this._generationKeys(roomId, inc)).filter((key) => key.startsWith(prefix))
    return keys.map((physical) => decodeLaneKey(physical.slice(prefix.length)))
  }

  deleteRetained(roomId: string, inc: string, lane: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    return this._run(REDIS_COMMANDS.retainedDelete, { roomId, inc, lane, ifSeq: opts?.ifSeq })
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    const generationKeys = await this._generationKeys(roomId, inc)
    await this._run(REDIS_COMMANDS.dropGeneration, { roomId, inc, generationKeys })
  }

  directoryPut(roomId: string, incTag: string): Promise<void> {
    return this._run(REDIS_COMMANDS.directoryPut, { roomId, incTag })
  }

  directoryDelete(roomId: string, incTag: string): Promise<void> {
    return this._run(REDIS_COMMANDS.directoryDelete, { roomId, incTag })
  }

  async directoryList(prefix: string, cursor?: string): Promise<DirectoryPage> {
    const index = directoryIndexKey(this._prefix)
    const min = cursor === undefined ? `[${prefix}` : `(${cursor}`
    const page = await this._publisher.zrangebylex(index, min, '+', 'LIMIT', 0, DIRECTORY_PAGE_SIZE)
    const matching: string[] = []
    for (const member of page) {
      if (member.startsWith(prefix)) matching.push(member)
      else break
    }
    const last = matching.at(-1)
    if (last === undefined) return { entries: [] }
    const [tags, peek] = await Promise.all([
      this._publisher.hmget(directoryTagsKey(this._prefix), ...matching),
      matching.length === DIRECTORY_PAGE_SIZE ? this._publisher.zrangebylex(index, `(${last}`, '+', 'LIMIT', 0, 1) : [],
    ])
    const entries = matching.flatMap((roomId, i) => {
      const incTag = tags[i]
      return incTag === null || incTag === undefined ? [] : [{ roomId, incTag }]
    })
    return peek[0]?.startsWith(prefix) ? { entries, cursor: last } : { entries }
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
