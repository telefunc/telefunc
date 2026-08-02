import { randomUUID } from 'node:crypto'
import { Cluster, type Redis } from 'ioredis'
import { assert } from '../assert.js'
import { callDefinedCommand } from '../callDefinedCommand.js'
import type {
  BroadcastDriver,
  BroadcastLane,
  CellMutation,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  PublishResult,
  RoomDriver,
  RoomHead,
  RoomSubscriptionSource,
} from 'telefunc/__internal'
import {
  broadcastChannel,
  broadcastSequenceKey,
  cellKey,
  cellKeyPrefix,
  DEFAULT_ROOM_PREFIX,
  decodeRedisOrderingFrame,
  directoryIndexKey,
  directoryTagsKey,
  generationKeysKey,
  generationTokensKey,
  headKey,
  laneKey,
  parseLaneKey,
  REDIS_ROOM_COMMAND_KEYS,
  REDIS_ROOM_COMMANDS,
  REDIS_ORDERING_FRAME_LUA,
  REDIS_SAFE_INTEGER_MAX,
  redisKeyPrefix,
  retainedKey,
  retainedKeyPrefix,
  revKey,
} from './layout.js'
import { RedisSubscriptionDriver } from './subscriber-transport.js'

const DIRECTORY_PAGE_SIZE = 100
const STABLE_READ_ATTEMPTS = 8

function assertOrderingPosition(seq: number, timestamp: number, context: string): void {
  if (!Number.isSafeInteger(seq) || seq <= 0 || !Number.isSafeInteger(timestamp) || timestamp < 0) {
    throw new Error(`${context}: invalid Room ordering position`)
  }
}
export type RedisBackendOptions = {
  redis: Redis | Cluster
  prefix?: string
}
const PUBLISH_CMD = 'tfPublish'
const PUBLISH_LUA = `${REDIS_ORDERING_FRAME_LUA}
local previous = redis.call('GET', KEYS[1])
if previous and tonumber(previous) >= ${REDIS_SAFE_INTEGER_MAX} then
  return redis.error_reply('publish: sequence exhausted for the ordering domain')
end
local seq = redis.call('INCR', KEYS[1])
local t = redis.call('TIME')
local ts = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if seq < 1 or seq > ${REDIS_SAFE_INTEGER_MAX} or ts < 0 or ts > ${REDIS_SAFE_INTEGER_MAX} then
  return redis.error_reply('publish: invalid ordering position')
end
local frame = tf_ordering_frame(seq, ts, ARGV[1])
local receivers = redis.call('PUBLISH', KEYS[2], frame)
return {seq, ts, receivers}
`.trim()
type StoredHead = {
  rev: string
  state: 'open' | 'closing' | 'closed'
  config: string
  inc?: string
  lease?: { id: string; until: number }
  exp?: number
}
type HeadCxReply =
  | { tag: 'head'; head: StoredHead }
  | { tag: 'deleted' }
  | { tag: 'conflict'; current: StoredHead | null }
type DropGenerationBeginReply = { exists: false } | { exists: true; token: string }
type ReadCellsFenceReply = { stale: true } | { revision: string }
type CellSelector = { keys: string[] } | { prefix: string }
type CellsRead = { revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }
function toBase64(bytes: Uint8Array): string {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString('base64')
}
function fromBase64(b64: string): Uint8Array {
  return Uint8Array.from(Buffer.from(b64, 'base64'))
}
function toBuffer(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)
}
function toPublicHead(stored: StoredHead): RoomHead {
  const head: RoomHead = {
    rev: stored.rev,
    currentInc: stored.inc ?? null,
    state: stored.state,
    config: fromBase64(stored.config),
  }
  if (stored.lease !== undefined) head.closeLease = { id: stored.lease.id, until: stored.lease.until }
  return head
}
function encodeCx(cx: HeadCx): string {
  if (cx.expect === 'absent') return JSON.stringify({ form: 'absent' })
  const expect = cx.expect
  if ('closingLeaseExpired' in expect) return JSON.stringify({ form: 'takeover', rev: expect.rev })
  if ('closingLease' in expect)
    return JSON.stringify({ form: 'finalize', rev: expect.rev, closingLease: expect.closingLease })
  return JSON.stringify({ form: 'generic', rev: expect.rev })
}
function encodeNext(next: HeadNext): string {
  if ('delete' in next) return JSON.stringify({ kind: 'delete' })
  const { head, ttlMs } = next
  const payload: Record<string, unknown> = { kind: 'head', state: head.state, config: toBase64(head.config) }
  if (head.currentInc !== null) payload.inc = head.currentInc
  if (head.closeLease !== undefined) payload.lease = { id: head.closeLease.id, durationMs: head.closeLease.durationMs }
  if (ttlMs !== undefined) payload.ttlMs = ttlMs
  return JSON.stringify(payload)
}

export class RedisBackend implements BroadcastDriver, RoomDriver {
  readonly subscriptions: RedisSubscriptionDriver

  private readonly _publisher: Redis | Cluster
  private readonly _prefix: string
  private readonly _receivers: 'global' | 'none'
  private _disposed = false

  constructor(options: RedisBackendOptions) {
    if (options.redis instanceof Cluster && options.redis.options.scaleReads !== 'master') {
      throw new Error("RedisBackend: ioredis Cluster scaleReads must be 'master' for consistent Room reads")
    }
    const retries =
      options.redis instanceof Cluster
        ? options.redis.options.retryDelayOnFailover !== 0 ||
          options.redis.options.redisOptions?.maxRetriesPerRequest !== 0 ||
          options.redis.options.redisOptions?.reconnectOnError != null
        : options.redis.options.maxRetriesPerRequest !== 0 || options.redis.options.reconnectOnError != null
    if (retries)
      throw new Error(
        'RedisBackend: at-most-once requires maxRetriesPerRequest: 0 (standalone Redis), or retryDelayOnFailover: 0 and redisOptions.maxRetriesPerRequest: 0 (Cluster); reconnectOnError must be unset',
      )
    this._publisher = options.redis
    let clusterSubscriberSelection = 0
    const createSubscriber = async (): Promise<Redis> => {
      let source: Redis
      if (options.redis instanceof Cluster) {
        // Keep SUBSCRIBE, dispatch, and the delivery-fence PING on one Room-owned live-master socket.
        const masters = options.redis
          .nodes('master')
          .filter((master) => master.status !== 'end')
          .sort((left, right) =>
            `${left.options.host ?? ''}:${left.options.port ?? ''}`.localeCompare(
              `${right.options.host ?? ''}:${right.options.port ?? ''}`,
            ),
          )
        if (masters.length === 0) throw new Error('RedisBackend: Cluster has no available masters')
        source = masters[clusterSubscriberSelection % masters.length] as Redis
        clusterSubscriberSelection++
      } else source = options.redis
      return source.duplicate({
        connectionName: `telefunc-subscriber-${randomUUID()}`,
        autoResubscribe: false,
        lazyConnect: true,
        maxRetriesPerRequest: 1,
        retryStrategy: () => null,
      })
    }
    this._prefix = redisKeyPrefix(options.prefix ?? DEFAULT_ROOM_PREFIX)
    this._receivers = options.redis instanceof Cluster ? 'none' : 'global'
    this._publisher.defineCommand(PUBLISH_CMD, { numberOfKeys: 2, lua: PUBLISH_LUA })
    for (const command of Object.values(REDIS_ROOM_COMMANDS)) {
      if (command.numberOfKeys === null) this._publisher.defineCommand(command.name, { lua: command.lua })
      else this._publisher.defineCommand(command.name, { numberOfKeys: command.numberOfKeys, lua: command.lua })
    }
    this.subscriptions = new RedisSubscriptionDriver({
      prefix: this._prefix,
      createSubscriber,
      captureGeneration: (source) => this._captureGeneration(source),
      validateGeneration: (source, token) => this._validateGeneration(source, token),
    })
  }

  async publish(lane: BroadcastLane, payload: Uint8Array): Promise<PublishResult> {
    this._assertLive()
    const reply = await callDefinedCommand(this._publisher, PUBLISH_CMD, [
      broadcastSequenceKey(this._prefix, lane.key),
      broadcastChannel(this._prefix, lane),
      toBuffer(payload),
    ])
    assert(Array.isArray(reply) && reply.length === 3, 'Publish script returned an unexpected shape')
    const [seq, timestamp, receivers] = reply
    assert(
      typeof seq === 'number' && typeof timestamp === 'number' && typeof receivers === 'number',
      'Publish script returned non-numeric seq/ts/receivers',
    )
    assertOrderingPosition(seq, timestamp, 'RedisBackend.publish')
    return {
      seq,
      timestamp,
      ...(this._receivers === 'none' ? {} : { receivers }),
    }
  }

  async readHead(roomId: string): Promise<{ head: RoomHead } | null> {
    this._assertLive()
    const reply = JSON.parse(
      (await this._call(REDIS_ROOM_COMMANDS.readHead.name, [
        ...REDIS_ROOM_COMMAND_KEYS.readHead(this._prefix, roomId),
      ])) as string,
    ) as { head: StoredHead | null }
    return reply.head === null ? null : { head: toPublicHead(reply.head) }
  }

  async compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<
    { ok: true; head: RoomHead } | { ok: true; deleted: true } | { conflict: true; current: RoomHead | null }
  > {
    this._assertLive()
    const reply = (await this._call(REDIS_ROOM_COMMANDS.headCx.name, [
      ...REDIS_ROOM_COMMAND_KEYS.headCx(this._prefix, roomId),
      '',
      encodeCx(cx),
      encodeNext(next),
    ])) as string
    const parsed = JSON.parse(reply) as HeadCxReply
    if (parsed.tag === 'head') return { ok: true, head: toPublicHead(parsed.head) }
    if (parsed.tag === 'deleted') return { ok: true, deleted: true }
    return { conflict: true, current: parsed.current === null ? null : toPublicHead(parsed.current) }
  }

  async readCells(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead> {
    this._assertLive()
    for (let attempt = 0; attempt < STABLE_READ_ATTEMPTS; attempt++) {
      const result = await this._readCellAttempt(roomId, inc, sel)
      if (result !== null) return result
    }
    throw new Error(`readCells: stable read did not converge in ${STABLE_READ_ATTEMPTS} attempts (room '${roomId}')`)
  }

  private async _readCellAttempt(roomId: string, inc: string, sel: CellSelector): Promise<CellsRead | null> {
    const [headRaw, revBefore] = await this._publisher.mget(
      headKey(this._prefix, roomId),
      revKey(this._prefix, roomId, inc),
    )
    const head = this._parseHead(headRaw ?? null)
    if (head === null || (head.inc ?? null) !== inc) return { staleInc: true }
    const logicalKeys = await this._resolveLogicalCellKeys(roomId, inc, sel)
    const physicalKeys = logicalKeys.map((key) => cellKey(this._prefix, roomId, inc, key))
    const values = physicalKeys.length > 0 ? await this._publisher.mgetBuffer(...physicalKeys) : []
    const before = revBefore ?? '0'
    const fenceKeys = REDIS_ROOM_COMMAND_KEYS.readCellsFence(this._prefix, roomId, inc)
    const fence = JSON.parse(
      (await this._call(REDIS_ROOM_COMMANDS.readCellsFence.name, [...fenceKeys, inc])) as string,
    ) as ReadCellsFenceReply
    if ('stale' in fence) return { staleInc: true }
    if (before !== fence.revision) return null
    return { revision: before, cells: collectCells(logicalKeys, values) }
  }
  async compareExchangeCells(
    roomId: string,
    inc: string,
    revision: string,
    mutations: CellMutation[],
  ): Promise<CxResult> {
    this._assertLive()
    const keys = REDIS_ROOM_COMMAND_KEYS.cellsCx(
      this._prefix,
      roomId,
      inc,
      mutations.map((mutation) => mutation.key),
    )
    const argv: Array<string | Buffer> = ['', inc, revision]
    for (const mutation of mutations) {
      if (mutation.set === undefined) {
        argv.push('del', '')
      } else {
        argv.push('set', toBuffer(mutation.set.bytes))
      }
    }
    const reply = (await this._call(REDIS_ROOM_COMMANDS.cellsCx.name, [
      String(keys.length),
      ...keys,
      ...argv,
    ])) as string
    return reply as CxResult
  }

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitResult> {
    this._assertLive()
    const source = { roomId, inc, lane }
    const keys = REDIS_ROOM_COMMAND_KEYS.commit(this._prefix, roomId, inc, lane, opts?.requiredCellKeys)
    const flush = this.subscriptions.prepareFlush(source)
    let reply: string
    try {
      reply = (await this._call(REDIS_ROOM_COMMANDS.commit.name, [
        String(keys.length),
        ...keys,
        '',
        inc,
        lane.kind,
        opts?.closingLease ?? '',
        opts?.retain === true ? '1' : '0',
        toBuffer(payload),
        flush.token,
      ])) as string
    } catch (error) {
      flush.cancel()
      throw error
    }
    const parsed = JSON.parse(reply) as
      | { stale: true }
      | { accepted: true; seq: number; timestamp: number; receivers: number }
    if ('stale' in parsed) {
      flush.cancel()
      return { stale: true }
    }
    assertOrderingPosition(parsed.seq, parsed.timestamp, 'RedisBackend.commitLane')
    // Data and fence leave the same slot owner in order, so observing the fence proves local dispatch.
    const delivery = flush.delivery
    // Observe rejection now so later-awaited delivery has no interim unhandledRejection.
    void delivery.catch(() => {})
    return {
      accepted: true,
      seq: parsed.seq,
      timestamp: parsed.timestamp,
      ...(this._receivers === 'none' ? {} : { receivers: parsed.receivers }),
      delivery,
    }
  }

  async readRetained(
    roomId: string,
    inc: string,
    lane: LaneId,
  ): Promise<{ payload: Uint8Array; seq: number; timestamp: number } | null> {
    this._assertLive()
    const frame = await this._publisher.getBuffer(retainedKey(this._prefix, roomId, inc, laneKey(lane)))
    if (frame === null) return null
    const {
      payload,
      info: { seq, timestamp },
    } = decodeRedisOrderingFrame(frame)
    return { payload: Uint8Array.from(payload), seq, timestamp }
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    this._assertLive()
    const prefix = retainedKeyPrefix(this._prefix, roomId, inc)
    const keys = (await this._generationKeys(roomId, inc)).filter((key) => key.startsWith(prefix))
    return keys.map((physical) => parseLaneKey(physical.slice(prefix.length)))
  }

  async deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this._assertLive()
    if (lane === undefined && opts?.ifSeq !== undefined) {
      throw new Error('deleteRetained: ifSeq requires a lane')
    }
    if (opts?.ifSeq !== undefined && (!Number.isSafeInteger(opts.ifSeq) || opts.ifSeq <= 0)) {
      throw new Error('deleteRetained: ifSeq must be a positive safe integer')
    }
    const retainedKeys =
      lane === undefined
        ? (await this._generationKeys(roomId, inc)).filter((key) =>
            key.startsWith(retainedKeyPrefix(this._prefix, roomId, inc)),
          )
        : [retainedKey(this._prefix, roomId, inc, laneKey(lane))]
    const keys = REDIS_ROOM_COMMAND_KEYS.retainedDelete(this._prefix, roomId, inc, retainedKeys)
    await this._call(REDIS_ROOM_COMMANDS.retainedDelete.name, [
      String(keys.length),
      ...keys,
      opts?.ifSeq === undefined ? '' : String(opts.ifSeq),
    ])
  }

  private _captureGeneration(source: RoomSubscriptionSource): Promise<string | null> {
    return this._publisher.hget(generationTokensKey(this._prefix, source.roomId), source.inc)
  }

  private async _validateGeneration(source: RoomSubscriptionSource, token: string): Promise<boolean> {
    return (
      (await this._call(REDIS_ROOM_COMMANDS.validateGeneration.name, [
        ...REDIS_ROOM_COMMAND_KEYS.validateGeneration(this._prefix, source.roomId),
        '',
        source.inc,
        token,
      ])) === 1
    )
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    this._assertLive()
    const begin = JSON.parse(
      (await this._call(REDIS_ROOM_COMMANDS.dropGenerationBegin.name, [
        ...REDIS_ROOM_COMMAND_KEYS.dropGenerationBegin(this._prefix, roomId),
        '',
        inc,
      ])) as string,
    ) as DropGenerationBeginReply
    if (!begin.exists) return
    const keys = await this._publisher.smembers(generationKeysKey(this._prefix, roomId, inc))
    const finalizeKeys = REDIS_ROOM_COMMAND_KEYS.dropGenerationFinalize(this._prefix, roomId, inc, keys)
    await this._call(REDIS_ROOM_COMMANDS.dropGenerationFinalize.name, [
      String(finalizeKeys.length),
      ...finalizeKeys,
      inc,
      begin.token,
    ])
  }

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this._assertLive()
    await this._call(REDIS_ROOM_COMMANDS.directoryPut.name, [
      ...REDIS_ROOM_COMMAND_KEYS.directoryPut(this._prefix),
      roomId,
      incTag,
    ])
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this._assertLive()
    await this._call(REDIS_ROOM_COMMANDS.directoryDelete.name, [
      ...REDIS_ROOM_COMMAND_KEYS.directoryDelete(this._prefix),
      roomId,
      incTag,
    ])
  }

  async directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> {
    this._assertLive()
    const index = directoryIndexKey(this._prefix)
    const min = cursor === undefined ? `[${prefix}` : `(${cursor}`
    const page = await this._publisher.zrangebylex(index, min, '+', 'LIMIT', 0, DIRECTORY_PAGE_SIZE)
    const matching: string[] = []
    for (const member of page) {
      if (member.startsWith(prefix)) matching.push(member)
      else break
    }
    if (matching.length === 0) return { entries: [] }
    // Prefix matches are contiguous; the independent tag/peek reads remain all-or-error through Promise.all.
    const last = matching[matching.length - 1] as string
    const [tags, peek] = await Promise.all([
      this._publisher.hmget(directoryTagsKey(this._prefix), ...matching),
      matching.length === DIRECTORY_PAGE_SIZE && page.length === DIRECTORY_PAGE_SIZE
        ? this._publisher.zrangebylex(index, `(${last}`, '+', 'LIMIT', 0, 1)
        : [],
    ])
    const entries = matching.flatMap((roomId, i) => {
      const incTag = tags[i]
      return incTag === null || incTag === undefined ? [] : [{ roomId, incTag }]
    })
    return peek.length > 0 && (peek[0] as string).startsWith(prefix) ? { entries, cursor: last } : { entries }
  }

  async dispose(): Promise<void> {
    if (this._disposed) return
    this._disposed = true
  }

  private _assertLive(): void {
    if (this._disposed) throw new Error('RedisBackend: used after dispose()')
  }

  private _parseHead(raw: string | null): StoredHead | null {
    return raw === null ? null : (JSON.parse(raw) as StoredHead)
  }

  private async _scanCellKeys(roomId: string, inc: string, prefix: string): Promise<string[]> {
    const physicalPrefix = cellKeyPrefix(this._prefix, roomId, inc)
    const physical = (await this._generationKeys(roomId, inc)).filter((key) => key.startsWith(physicalPrefix + prefix))
    return physical.map((key) => key.slice(physicalPrefix.length))
  }
  private _resolveLogicalCellKeys(roomId: string, inc: string, sel: CellSelector): Promise<string[]> {
    return 'keys' in sel ? Promise.resolve(sel.keys) : this._scanCellKeys(roomId, inc, sel.prefix)
  }
  private _generationKeys(roomId: string, inc: string): Promise<string[]> {
    return this._publisher.smembers(generationKeysKey(this._prefix, roomId, inc))
  }

  private _call(command: string, keysAndArgs: ReadonlyArray<string | Uint8Array>): Promise<unknown> {
    return callDefinedCommand(this._publisher, command, keysAndArgs)
  }
}

function collectCells(logicalKeys: string[], values: Array<Buffer | null>): Map<string, Uint8Array> {
  const cells = new Map<string, Uint8Array>()
  for (let i = 0; i < logicalKeys.length; i++) {
    const value = values[i]
    if (value === null || value === undefined) continue
    cells.set(logicalKeys[i] as string, Uint8Array.from(value))
  }
  return cells
}
