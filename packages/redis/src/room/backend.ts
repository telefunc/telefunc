// The released Redis BackendDriver for standalone Redis and Redis Cluster deployments.
//
// Mechanism map (spi.md §5.2), with atomic pieces in layout.ts's Lua:
//   head/cells CX  validate ownership, compare, and mutate one room slot
//   cells read     revision → manifest/MGET → keyed head/revision/time fence, bounded to 8 attempts
//   commit         validate, advance order, retain, and publish data plus its in-band delivery fence
//   subscription   SUBSCRIBE ack establishes; generation invalidation terminates; late sequence drops

import { randomUUID } from 'node:crypto'
import { Cluster, type Redis } from 'ioredis'
import { assert } from '../assert.js'
import { callDefinedCommand } from '../callDefinedCommand.js'
import type {
  BackendDriver,
  BackendSubscriptionSource,
  BroadcastLane,
  CellMutation,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  PublishResult,
  RoomHead,
} from 'telefunc/backend'
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
  gensKey,
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
const NEWLINE = 0x0a

function assertOrderingPosition(seq: number, timestamp: number, context: string): void {
  if (!Number.isSafeInteger(seq) || seq <= 0 || !Number.isSafeInteger(timestamp) || timestamp < 0) {
    throw new Error(`${context}: invalid Room ordering position`)
  }
}

export type RedisRoomBackendOptions = {
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

// The stored head, exactly as the Lua encodes it. `config` is opaque base64; `until`/`exp` are authority
// timestamps; `inc`/`lease`/`exp` are present only when meaningful (keeps the cjson clean).
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
type ReadCellsFenceReply = { stale: true } | { revision: string; now: number }

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

export class RedisRoomBackend implements BackendDriver {
  readonly spiVersion = 1 as const
  readonly subscriptions: RedisSubscriptionDriver

  readonly #publisher: Redis | Cluster
  readonly #prefix: string
  readonly #receivers: 'global' | 'none'
  #disposed = false

  constructor(options: RedisRoomBackendOptions) {
    if (options.redis instanceof Cluster && options.redis.options.scaleReads !== 'master') {
      throw new Error("RedisRoomBackend: ioredis Cluster scaleReads must be 'master' for consistent Room reads")
    }
    const retries =
      options.redis instanceof Cluster
        ? options.redis.options.retryDelayOnFailover !== 0 ||
          options.redis.options.redisOptions?.maxRetriesPerRequest !== 0 ||
          options.redis.options.redisOptions?.reconnectOnError != null
        : options.redis.options.maxRetriesPerRequest !== 0 || options.redis.options.reconnectOnError != null
    if (retries) throw new Error('RedisRoomBackend: ioredis retries violate at-most-once delivery')
    this.#publisher = options.redis
    let clusterSubscriberSelection = 0
    const createSubscriber = async (): Promise<Redis> => {
      let source: Redis
      if (options.redis instanceof Cluster) {
        // Use a supported direct Redis duplicate of a live master. Unlike Cluster's hidden Pub/Sub
        // connection, this exact client is owned by the Room transport, so SUBSCRIBE, dispatch and the
        // delivery-fence PING necessarily share one socket. Re-evaluate topology on every replacement.
        let topology = options.redis.nodes('master')
        if (topology.length === 0) {
          await options.redis.ping()
          topology = options.redis.nodes('master')
        }
        const masters = topology
          .filter((master) => master.status !== 'end')
          .sort((left, right) =>
            `${left.options.host ?? ''}:${left.options.port ?? ''}`.localeCompare(
              `${right.options.host ?? ''}:${right.options.port ?? ''}`,
            ),
          )
        if (masters.length === 0) throw new Error('RedisRoomBackend: Cluster has no available masters')
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
    this.#prefix = redisKeyPrefix(options.prefix ?? DEFAULT_ROOM_PREFIX)
    this.#receivers = options.redis instanceof Cluster ? 'none' : 'global'
    this.#publisher.defineCommand(PUBLISH_CMD, { numberOfKeys: 2, lua: PUBLISH_LUA })
    for (const command of Object.values(REDIS_ROOM_COMMANDS)) {
      if (command.numberOfKeys === null) this.#publisher.defineCommand(command.name, { lua: command.lua })
      else this.#publisher.defineCommand(command.name, { numberOfKeys: command.numberOfKeys, lua: command.lua })
    }
    this.subscriptions = new RedisSubscriptionDriver({
      prefix: this.#prefix,
      createSubscriber,
      captureGeneration: (source) => this.#captureGeneration(source),
      validateGeneration: (source, token) => this.#validateGeneration(source, token),
    })
  }

  // ── head ──

  async publish(lane: BroadcastLane, payload: Uint8Array): Promise<PublishResult> {
    this.#assertLive()
    const reply = await callDefinedCommand(this.#publisher, PUBLISH_CMD, [
      broadcastSequenceKey(this.#prefix, lane.key),
      broadcastChannel(this.#prefix, lane),
      toBuffer(payload),
    ])
    assert(Array.isArray(reply) && reply.length === 3, 'Publish script returned an unexpected shape')
    const [seq, timestamp, receivers] = reply
    assert(
      typeof seq === 'number' && typeof timestamp === 'number' && typeof receivers === 'number',
      'Publish script returned non-numeric seq/ts/receivers',
    )
    assertOrderingPosition(seq, timestamp, 'RedisRoomBackend.publish')
    return {
      seq,
      timestamp,
      ...(this.#receivers === 'none' ? {} : { receivers }),
    }
  }

  async readHead(roomId: string): Promise<{ head: RoomHead } | null> {
    this.#assertLive()
    const reply = JSON.parse(
      (await this.#call(REDIS_ROOM_COMMANDS.readHead.name, [
        ...REDIS_ROOM_COMMAND_KEYS.readHead(this.#prefix, roomId),
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
    this.#assertLive()
    const reply = (await this.#call(REDIS_ROOM_COMMANDS.headCx.name, [
      ...REDIS_ROOM_COMMAND_KEYS.headCx(this.#prefix, roomId),
      '',
      encodeCx(cx),
      encodeNext(next),
    ])) as string
    const parsed = JSON.parse(reply) as HeadCxReply
    if (parsed.tag === 'head') return { ok: true, head: toPublicHead(parsed.head) }
    if (parsed.tag === 'deleted') return { ok: true, deleted: true }
    return { conflict: true, current: parsed.current === null ? null : toPublicHead(parsed.current) }
  }

  // ── generation cells ──

  async readCells(
    roomId: string,
    inc: string,
    sel: { keys: string[] } | { prefix: string },
  ): Promise<{ revision: string; cells: Map<string, Uint8Array> } | { staleInc: true }> {
    this.#assertLive()
    const hKey = headKey(this.#prefix, roomId)
    const rKey = revKey(this.#prefix, roomId, inc)
    // Stable read: fence the enumerated set against the per-generation revision. A concurrent deliberate
    // mutation bumps rev between rev_before and rev_after, so an inconsistent snapshot is retried; a
    // silent PX expiry does NOT bump rev and is hidden by the logical expiresAt filter instead.
    for (let attempt = 0; attempt < STABLE_READ_ATTEMPTS; attempt++) {
      const [headRaw, revBefore] = await this.#publisher.mget(hKey, rKey)
      const head = this.#parseHead(headRaw ?? null)
      // Reads need only generation existence — available while 'closing' (the closer's tail needs them).
      if (head === null || (head.inc ?? null) !== inc) return { staleInc: true }
      const logicalKeys = 'keys' in sel ? sel.keys : await this.#scanCellKeys(roomId, inc, sel.prefix)
      const physicalKeys = logicalKeys.map((key) => cellKey(this.#prefix, roomId, inc, key))
      const values = physicalKeys.length > 0 ? await this.#publisher.mgetBuffer(...physicalKeys) : []
      const before = revBefore ?? '0'
      const fence = JSON.parse(
        (await this.#call(REDIS_ROOM_COMMANDS.readCellsFence.name, [
          ...REDIS_ROOM_COMMAND_KEYS.readCellsFence(this.#prefix, roomId, inc),
          inc,
        ])) as string,
      ) as ReadCellsFenceReply
      if ('stale' in fence) return { staleInc: true }
      if (before !== fence.revision) continue
      const cells = new Map<string, Uint8Array>()
      for (let i = 0; i < logicalKeys.length; i++) {
        const value = values[i]
        if (value === null || value === undefined) continue
        const parsed = parseCellValue(value)
        if (parsed.expiresAt !== null && parsed.expiresAt <= fence.now) continue
        cells.set(logicalKeys[i] as string, parsed.payload)
      }
      return { revision: before, cells }
    }
    throw new Error(`readCells: stable read did not converge in ${STABLE_READ_ATTEMPTS} attempts (room '${roomId}')`)
  }

  async compareExchangeCells(
    roomId: string,
    inc: string,
    revision: string,
    mutations: CellMutation[],
  ): Promise<CxResult> {
    this.#assertLive()
    const keys = REDIS_ROOM_COMMAND_KEYS.cellsCx(
      this.#prefix,
      roomId,
      inc,
      mutations.map((mutation) => mutation.key),
    )
    const argv: Array<string | Buffer> = ['', inc, revision]
    for (const mutation of mutations) {
      if (mutation.set === undefined) {
        argv.push('del', '', '')
      } else {
        argv.push(
          'set',
          mutation.set.ttlMs === undefined ? '' : String(mutation.set.ttlMs),
          toBuffer(mutation.set.bytes),
        )
      }
    }
    const reply = (await this.#call(REDIS_ROOM_COMMANDS.cellsCx.name, [
      String(keys.length),
      ...keys,
      ...argv,
    ])) as string
    return reply as CxResult
  }

  // ── lane commit ──

  async commitLane(
    roomId: string,
    inc: string,
    lane: LaneId,
    payload: Uint8Array,
    opts?: { retain?: boolean; closingLease?: string; requiredCellKeys?: string[] },
  ): Promise<CommitResult> {
    this.#assertLive()
    const source = { kind: 'durable' as const, roomId, inc, lane }
    const keys = REDIS_ROOM_COMMAND_KEYS.commit(this.#prefix, roomId, inc, lane, opts?.requiredCellKeys)
    const flush = this.subscriptions.prepareFlush(source)
    let reply: string
    try {
      reply = (await this.#call(REDIS_ROOM_COMMANDS.commit.name, [
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
    assertOrderingPosition(parsed.seq, parsed.timestamp, 'RedisRoomBackend.commitLane')
    // Data and fence leave the same slot owner in order, so observing the fence proves local dispatch.
    const delivery = flush.delivery
    // Consumers may observe delivery later (or intentionally only use acceptance). Install an immediate
    // rejection observer so a connection/lifecycle epoch crossing is surfaced by `delivery` without an
    // ambient unhandledRejection in the interim.
    void delivery.catch(() => {})
    return {
      accepted: true,
      seq: parsed.seq,
      timestamp: parsed.timestamp,
      ...(this.#receivers === 'none' ? {} : { receivers: parsed.receivers }),
      delivery,
    }
  }

  // ── retained ──

  async readRetained(
    roomId: string,
    inc: string,
    lane: LaneId,
  ): Promise<{ payload: Uint8Array; seq: number; timestamp: number } | null> {
    this.#assertLive()
    const frame = await this.#publisher.getBuffer(retainedKey(this.#prefix, roomId, inc, laneKey(lane)))
    if (frame === null) return null
    const {
      payload,
      info: { seq, timestamp },
    } = decodeRedisOrderingFrame(frame)
    return { payload: Uint8Array.from(payload), seq, timestamp }
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    this.#assertLive()
    const prefix = retainedKeyPrefix(this.#prefix, roomId, inc)
    const keys = (await this.#generationKeys(roomId, inc)).filter((key) => key.startsWith(prefix))
    return keys.map((physical) => parseLaneKey(physical.slice(prefix.length)))
  }

  async deleteRetained(roomId: string, inc: string, lane?: LaneId, opts?: { ifSeq?: number }): Promise<void> {
    this.#assertLive()
    if (lane === undefined && opts?.ifSeq !== undefined) {
      throw new Error('deleteRetained: ifSeq requires a lane')
    }
    if (opts?.ifSeq !== undefined && (!Number.isSafeInteger(opts.ifSeq) || opts.ifSeq <= 0)) {
      throw new Error('deleteRetained: ifSeq must be a positive safe integer')
    }
    const retainedKeys =
      lane === undefined
        ? (await this.#generationKeys(roomId, inc)).filter((key) =>
            key.startsWith(retainedKeyPrefix(this.#prefix, roomId, inc)),
          )
        : [retainedKey(this.#prefix, roomId, inc, laneKey(lane))]
    const keys = REDIS_ROOM_COMMAND_KEYS.retainedDelete(this.#prefix, roomId, inc, retainedKeys)
    await this.#call(REDIS_ROOM_COMMANDS.retainedDelete.name, [
      String(keys.length),
      ...keys,
      opts?.ifSeq === undefined ? '' : String(opts.ifSeq),
    ])
  }

  // ── subscriptions ──

  #captureGeneration(source: Extract<BackendSubscriptionSource, { kind: 'durable' }>): Promise<string | null> {
    return this.#publisher.hget(generationTokensKey(this.#prefix, source.roomId), source.inc)
  }

  async #validateGeneration(
    source: Extract<BackendSubscriptionSource, { kind: 'durable' }>,
    token: string,
  ): Promise<boolean> {
    return (
      (await this.#call(REDIS_ROOM_COMMANDS.validateGeneration.name, [
        ...REDIS_ROOM_COMMAND_KEYS.validateGeneration(this.#prefix, source.roomId),
        '',
        source.inc,
        token,
      ])) === 1
    )
  }

  // ── generation lifecycle ──

  async listGenerations(roomId: string): Promise<string[]> {
    this.#assertLive()
    return this.#publisher.smembers(gensKey(this.#prefix, roomId))
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    this.#assertLive()
    const begin = JSON.parse(
      (await this.#call(REDIS_ROOM_COMMANDS.dropGenerationBegin.name, [
        ...REDIS_ROOM_COMMAND_KEYS.dropGenerationBegin(this.#prefix, roomId),
        '',
        inc,
      ])) as string,
    ) as DropGenerationBeginReply
    if (!begin.exists) return
    const keys = await this.#publisher.smembers(generationKeysKey(this.#prefix, roomId, inc))
    const finalizeKeys = REDIS_ROOM_COMMAND_KEYS.dropGenerationFinalize(this.#prefix, roomId, inc, keys)
    await this.#call(REDIS_ROOM_COMMANDS.dropGenerationFinalize.name, [
      String(finalizeKeys.length),
      ...finalizeKeys,
      inc,
      begin.token,
    ])
  }

  // ── directory (global; its own two co-slotted keys) ──

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this.#assertLive()
    await this.#call(REDIS_ROOM_COMMANDS.directoryPut.name, [
      ...REDIS_ROOM_COMMAND_KEYS.directoryPut(this.#prefix),
      roomId,
      incTag,
    ])
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.#assertLive()
    await this.#call(REDIS_ROOM_COMMANDS.directoryDelete.name, [
      ...REDIS_ROOM_COMMAND_KEYS.directoryDelete(this.#prefix),
      roomId,
      incTag,
    ])
  }

  async directoryList(
    prefix: string,
    cursor?: string,
  ): Promise<{ entries: { roomId: string; incTag: string }[]; cursor?: string }> {
    this.#assertLive()
    const index = directoryIndexKey(this.#prefix)
    const min = cursor === undefined ? `[${prefix}` : `(${cursor}`
    const page = await this.#publisher.zrangebylex(index, min, '+', 'LIMIT', 0, DIRECTORY_PAGE_SIZE)
    // Prefix-matching members are contiguous from `min`; the first non-match ends the prefix range.
    const matching: string[] = []
    for (const member of page) {
      if (member.startsWith(prefix)) matching.push(member)
      else break
    }
    if (matching.length === 0) return { entries: [] }
    // Prefix matches are contiguous; the independent tag/peek reads remain all-or-error through Promise.all.
    const last = matching[matching.length - 1] as string
    const [tags, peek] = await Promise.all([
      this.#publisher.hmget(directoryTagsKey(this.#prefix), ...matching),
      matching.length === DIRECTORY_PAGE_SIZE && page.length === DIRECTORY_PAGE_SIZE
        ? this.#publisher.zrangebylex(index, `(${last}`, '+', 'LIMIT', 0, 1)
        : [],
    ])
    const entries = matching.flatMap((roomId, i) => {
      const incTag = tags[i]
      return incTag === null || incTag === undefined ? [] : [{ roomId, incTag }]
    })
    return peek.length > 0 && (peek[0] as string).startsWith(prefix) ? { entries, cursor: last } : { entries }
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return
    this.#disposed = true
  }

  // ── internals ──

  #assertLive(): void {
    if (this.#disposed) throw new Error('RedisRoomBackend: used after dispose()')
  }

  #parseHead(raw: string | null): StoredHead | null {
    return raw === null ? null : (JSON.parse(raw) as StoredHead)
  }

  async #scanCellKeys(roomId: string, inc: string, prefix: string): Promise<string[]> {
    const physicalPrefix = cellKeyPrefix(this.#prefix, roomId, inc)
    const physical = (await this.#generationKeys(roomId, inc)).filter((key) => key.startsWith(physicalPrefix + prefix))
    return physical.map((key) => key.slice(physicalPrefix.length))
  }

  #generationKeys(roomId: string, inc: string): Promise<string[]> {
    return this.#publisher.smembers(generationKeysKey(this.#prefix, roomId, inc))
  }

  #call(command: string, keysAndArgs: ReadonlyArray<string | Uint8Array>): Promise<unknown> {
    return callDefinedCommand(this.#publisher, command, keysAndArgs)
  }
}

// A stored cell is "<expiresAt|''>\n<payload bytes>"; the header is ASCII digits (or empty) with no
// newline, so the FIRST newline is always the separator even when the payload contains newlines.
function parseCellValue(value: Buffer): { expiresAt: number | null; payload: Uint8Array } {
  const nl = value.indexOf(NEWLINE)
  if (nl < 0) return { expiresAt: null, payload: Uint8Array.from(value) }
  const header = value.subarray(0, nl).toString('ascii')
  return { expiresAt: header === '' ? null : Number(header), payload: Uint8Array.from(value.subarray(nl + 1)) }
}
