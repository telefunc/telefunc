// The released Redis realization of RoomBackendSpi, proved against the shared conformance suite to the
// same outcomes as the memory reference and certified on a disposable real three-master Redis Cluster.
//
// Mechanism map (spi.md §5.2), all atomic pieces in layout.ts's Lua:
//   head CX      one atomic record: legality (throw) · compare (conflict) · fresh-inc guard · mint · store
//   cells CX     head+inc+open precondition, coarse per-generation revision (INCR'd), all-or-none
//   commit       one boolean precondition (default open / closing-control), order advance, retain, PUBLISH
//   cells read   the stable-read algorithm: rev_before → SCAN/MGET → rev_after, 8-attempt bound, logical
//                expiresAt filtering, PX as a physical backstop only
//   delivery     PUBLISH is the broker handoff (settles inside acceptance); the broker's per-connection
//                FIFO realizes the ordered at-most-once attempt chain; receivers = the PUBLISH count
//   subscription SUBSCRIBE ack = establishment (fail-closed); channels keyed by (inc, lane) — I11

import { createHash, randomUUID } from 'node:crypto'
import { Cluster, type Redis } from 'ioredis'
import { assert } from '../assert.js'
import { callDefinedCommand } from '../callDefinedCommand.js'
import type {
  CellMutation,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  LaneReceiver,
  LaneSubscription,
  RoomBackendSpi,
  RoomHead,
} from 'telefunc/backend'
import { MAX_CLOSE_LEASE_MS, MIN_CLOSE_LEASE_MS, ROOM_SPI_VERSION } from 'telefunc/backend'
import {
  cellKey,
  cellKeyPrefix,
  channelKey,
  decodeFrameHeader,
  DEFAULT_ROOM_PREFIX,
  directoryIndexKey,
  directoryTagsKey,
  escapeGlob,
  generationInvalidationChannel,
  generationTokensKey,
  gensKey,
  genPrefix,
  headKey,
  laneKey,
  parseLaneKey,
  REDIS_ROOM_COMMAND_KEYS,
  REDIS_ROOM_COMMANDS,
  retainedKey,
  retainedKeyPrefix,
  revKey,
} from './layout.js'
import {
  RedisGenerationInvalidError,
  RedisSubscriberTransport,
  type RedisSubscriberChannelBinding,
} from './subscriber-transport.js'

const DEFAULT_MAX_RETAINED_BYTES = 16 * 1024 * 1024
const DIRECTORY_PAGE_SIZE = 100
const STABLE_READ_ATTEMPTS = 8
const SCAN_COUNT = 250
const NEWLINE = 0x0a
const SUBSCRIPTION_RETRY_ATTEMPTS = 5
export const REDIS_GENERATION_CAPTURE_TTL_MS = 90_000

function defaultSubscriptionRetryDelay(attempt: number): number {
  const ceiling = Math.min(4_000, 250 * 2 ** (attempt - 1))
  return Math.floor(ceiling * (0.5 + Math.random() * 0.5))
}

function roomGenerationKey(roomId: string, inc: string): string {
  return `${roomId.length}:${roomId}${inc}`
}

export type RedisRoomBackendOptions = {
  redis: Redis | Cluster
  prefix?: string
  maxRetainedPayloadBytes?: number
}

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

// The next-head SHAPE rules that hold regardless of the current head (spi.md §2): the lease is present
// iff closing and finite/bounded, a tombstone is the only head with a TTL, and only a tombstone has a
// null incarnation. Validated in JS and thrown before the atomic record runs.
function assertHeadNextWellFormed(next: HeadNext): void {
  if ('delete' in next) return
  const { head, ttlMs } = next
  if (head.state === 'closing') {
    if (head.closeLease === undefined) throw new Error('head CX: a head entering closing must carry a close lease')
    const { durationMs } = head.closeLease
    if (!(durationMs >= MIN_CLOSE_LEASE_MS && durationMs <= MAX_CLOSE_LEASE_MS)) {
      throw new Error(
        `head CX: close lease durationMs ${durationMs} outside [${MIN_CLOSE_LEASE_MS}, ${MAX_CLOSE_LEASE_MS}]`,
      )
    }
  } else if (head.closeLease !== undefined) {
    throw new Error(`head CX: a '${head.state}' head must not carry a close lease`)
  }
  if (ttlMs !== undefined && head.state !== 'closed') {
    throw new Error(`head CX: ttlMs is only valid for a 'closed' tombstone, got '${head.state}'`)
  }
  if (head.state === 'closed' && head.currentInc !== null) {
    throw new Error('head CX: a closed tombstone must clear currentInc to null')
  }
  if (head.state !== 'closed' && head.currentInc === null) {
    throw new Error(`head CX: a '${head.state}' head must name an incarnation`)
  }
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

type RedisGenerationBinding = RedisSubscriberChannelBinding & {
  roomId: string
  inc: string
  attemptId: string
  createdAt: number | null
  generationToken: string | null
}

export class RedisRoomBackend implements RoomBackendSpi {
  readonly spiVersion = ROOM_SPI_VERSION
  readonly capabilities: RoomBackendSpi['capabilities']

  readonly #publisher: Redis | Cluster
  readonly #transport: RedisSubscriberTransport
  readonly #prefix: string
  // Durable capture/generation identity stays here. The transport receives this object only as an opaque
  // channel binding and delegates capture/validation back before it can settle readiness.
  readonly #generationBindings = new Map<string, RedisGenerationBinding>()
  readonly #locallyDroppingGenerations = new Set<string>()
  #disposed = false

  constructor(options: RedisRoomBackendOptions) {
    assert(
      options.maxRetainedPayloadBytes === undefined ||
        (Number.isFinite(options.maxRetainedPayloadBytes) && options.maxRetainedPayloadBytes >= 0),
      'RedisRoomBackend: maxRetainedPayloadBytes must be a finite non-negative number',
    )
    this.#publisher = options.redis
    let clusterSubscriberSelection = 0
    const createSubscriber = async (): Promise<Redis> => {
      if (!(options.redis instanceof Cluster)) {
        return options.redis.duplicate({
          connectionName: `telefunc-room-subscriber-${randomUUID()}`,
          autoResubscribe: false,
          lazyConnect: false,
          retryStrategy: (attempt) =>
            attempt <= SUBSCRIPTION_RETRY_ATTEMPTS ? defaultSubscriptionRetryDelay(attempt) : null,
        })
      }
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
      const master = masters[clusterSubscriberSelection % masters.length] as Redis
      clusterSubscriberSelection++
      return master.duplicate({
        connectionName: `telefunc-room-subscriber-${randomUUID()}`,
        autoResubscribe: false,
        lazyConnect: false,
        maxRetriesPerRequest: 1,
        retryStrategy: (attempt) =>
          attempt <= SUBSCRIPTION_RETRY_ATTEMPTS ? defaultSubscriptionRetryDelay(attempt) : null,
      })
    }
    this.#prefix = options.prefix ?? DEFAULT_ROOM_PREFIX
    this.capabilities = {
      receivers: options.redis instanceof Cluster ? 'node-local' : 'global',
      maxRetainedPayloadBytes: options.maxRetainedPayloadBytes ?? DEFAULT_MAX_RETAINED_BYTES,
      clusterSafe: true,
      directory: true,
    }
    for (const command of Object.values(REDIS_ROOM_COMMANDS)) {
      if (command.numberOfKeys === null) this.#publisher.defineCommand(command.name, { lua: command.lua })
      else this.#publisher.defineCommand(command.name, { numberOfKeys: command.numberOfKeys, lua: command.lua })
    }
    this.#transport = new RedisSubscriberTransport({
      createSubscriber,
      retryDelay: defaultSubscriptionRetryDelay,
      captureGeneration: (binding) => this.#ensureGenerationCaptured(this.#requireGenerationBinding(binding)),
      validateGeneration: (binding, includeCapture) =>
        this.#validateGeneration(this.#requireGenerationBinding(binding), includeCapture),
      onGenerationInvalidation: (owner, token) => this.#generationInvalidated(owner, token),
      onChannelRemoved: (binding) => {
        if (this.#generationBindings.get(binding.channel) === binding) {
          this.#generationBindings.delete(binding.channel)
        }
      },
    })
  }

  // ── head ──

  async readHead(roomId: string): Promise<{ head: RoomHead } | null> {
    this.#assertLive()
    const raw = await this.#publisher.get(headKey(this.#prefix, roomId))
    const stored = this.#liveHead(raw, await this.#authorityNowMs())
    return stored === null ? null : { head: toPublicHead(stored) }
  }

  async compareExchangeHead(
    roomId: string,
    cx: HeadCx,
    next: HeadNext,
  ): Promise<
    { ok: true; head: RoomHead } | { ok: true; deleted: true } | { conflict: true; current: RoomHead | null }
  > {
    this.#assertLive()
    assertHeadNextWellFormed(next)
    const reply = (await this.#call(REDIS_ROOM_COMMANDS.headCx.name, [
      ...REDIS_ROOM_COMMAND_KEYS.headCx(this.#prefix, roomId),
      this.#nowArg(),
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
      const revAfter = await this.#publisher.get(rKey)
      const before = revBefore ?? '0'
      const after = revAfter ?? '0'
      if (before !== after) continue
      const now = await this.#authorityNowMs()
      const cells = new Map<string, Uint8Array>()
      for (let i = 0; i < logicalKeys.length; i++) {
        const value = values[i]
        if (value === null || value === undefined) continue
        const parsed = parseCellValue(value)
        if (parsed.expiresAt !== null && parsed.expiresAt <= now) continue
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
    const argv: Array<string | Buffer> = [this.#nowArg(), inc, revision]
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
    opts?: { retain?: boolean; orderTtlMs?: number; closingLease?: string },
  ): Promise<CommitResult> {
    this.#assertLive()
    const channel = channelKey(this.#prefix, roomId, inc, laneKey(lane))
    const reply = (await this.#call(REDIS_ROOM_COMMANDS.commit.name, [
      ...REDIS_ROOM_COMMAND_KEYS.commit(this.#prefix, roomId, inc, lane),
      this.#nowArg(),
      inc,
      lane.kind,
      opts?.closingLease ?? '',
      opts?.retain === true ? '1' : '0',
      opts?.orderTtlMs === undefined ? '' : String(opts.orderTtlMs),
      toBuffer(payload),
      String(this.capabilities.maxRetainedPayloadBytes),
    ])) as string
    const parsed = JSON.parse(reply) as
      | { stale: true }
      | { accepted: true; seq: number; timestamp: number; receivers: number }
    if ('stale' in parsed) return { stale: true }
    // The PUBLISH inside the atomic record IS the broker handoff, and the broker's per-connection FIFO
    // is what orders the attempt (receivers = the PUBLISH count). `delivery` then flushes local dispatch:
    // the frame was queued to the subscriber socket during the awaited commit, so a PING round-trip on
    // that connection resolves only after ioredis has dispatched the frame to the local receiver — WITHOUT
    // awaiting the receiver's own completion, so handoffAwaitsReceiver stays false.
    const delivery = this.#transport.flush(channel)
    // Consumers may observe delivery later (or intentionally only use acceptance). Install an immediate
    // rejection observer so a connection/lifecycle epoch crossing is surfaced by `delivery` without an
    // ambient unhandledRejection in the interim.
    void delivery.catch(() => {})
    return {
      accepted: true,
      seq: parsed.seq,
      timestamp: parsed.timestamp,
      receivers: parsed.receivers,
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
    const { seq, timestamp, payload } = decodeFrameHeader(frame)
    return { payload: Uint8Array.from(payload), seq, timestamp }
  }

  async listRetained(roomId: string, inc: string): Promise<LaneId[]> {
    this.#assertLive()
    const prefix = retainedKeyPrefix(this.#prefix, roomId, inc)
    const keys = await this.#scanKeys(`${escapeGlob(prefix)}*`)
    return keys.map((physical) => parseLaneKey(physical.slice(prefix.length)))
  }

  async deleteRetained(roomId: string, inc: string, lane?: LaneId): Promise<void> {
    this.#assertLive()
    if (lane !== undefined) {
      const keys = REDIS_ROOM_COMMAND_KEYS.retainedDelete(this.#prefix, roomId, inc, [
        retainedKey(this.#prefix, roomId, inc, laneKey(lane)),
      ])
      await this.#call(REDIS_ROOM_COMMANDS.retainedDelete.name, [String(keys.length), ...keys])
      return
    }
    const keys = await this.#scanKeys(`${escapeGlob(retainedKeyPrefix(this.#prefix, roomId, inc))}*`)
    const commandKeys = REDIS_ROOM_COMMAND_KEYS.retainedDelete(this.#prefix, roomId, inc, keys)
    await this.#call(REDIS_ROOM_COMMANDS.retainedDelete.name, [String(commandKeys.length), ...commandKeys])
  }

  // ── subscriptions ──

  subscribeLane(roomId: string, inc: string, lane: LaneId, receiver: LaneReceiver): LaneSubscription {
    this.#assertLive()
    const channel = channelKey(this.#prefix, roomId, inc, laneKey(lane))
    let binding = this.#generationBindings.get(channel)
    if (binding === undefined) {
      binding = {
        channel,
        roomId,
        inc,
        owner: roomGenerationKey(roomId, inc),
        invalidationChannel: generationInvalidationChannel(this.#prefix, roomId, inc),
        attemptId: randomUUID(),
        createdAt: null,
        generationToken: null,
      }
      this.#generationBindings.set(channel, binding)
    }
    return this.#transport.attach(binding, receiver)
  }

  async #ensureGenerationCaptured(binding: RedisGenerationBinding): Promise<void> {
    if (binding.generationToken !== null) return
    const captured = await this.#captureGeneration(binding)
    // Local observation follows the awaited durable command. If its response is lost, a retry therefore
    // reuses the exact attempt id and creation epoch instead of minting a second capture.
    binding.generationToken = captured
  }

  #requireGenerationBinding(binding: RedisSubscriberChannelBinding): RedisGenerationBinding {
    const current = this.#generationBindings.get(binding.channel)
    if (current !== binding) {
      throw new RedisGenerationInvalidError(`subscribeLane: stale channel binding '${binding.channel}'`)
    }
    return current
  }

  async #captureGeneration(context: RedisGenerationBinding): Promise<string> {
    if (context.createdAt === null) context.createdAt = await this.#authorityNowMs()
    const reply = (await this.#call(REDIS_ROOM_COMMANDS.captureGeneration.name, [
      ...REDIS_ROOM_COMMAND_KEYS.captureGeneration(this.#prefix, context.roomId),
      this.#nowArg(),
      context.inc,
      context.attemptId,
      String(context.createdAt),
      String(REDIS_GENERATION_CAPTURE_TTL_MS),
    ])) as string
    const parsed = JSON.parse(reply) as { ok: true; token: string } | { rejected: true; terminal: true; reason: string }
    if ('rejected' in parsed) throw new RedisGenerationInvalidError(`subscribeLane: ${parsed.reason}`)
    return parsed.token
  }

  async #validateGeneration(context: RedisGenerationBinding, includeCapture: boolean): Promise<boolean> {
    if (context.generationToken === null) return false
    const reply = (await this.#call(REDIS_ROOM_COMMANDS.validateGeneration.name, [
      ...REDIS_ROOM_COMMAND_KEYS.validateGeneration(this.#prefix, context.roomId),
      this.#nowArg(),
      context.inc,
      context.generationToken,
      includeCapture ? context.attemptId : '',
      includeCapture ? String(context.createdAt) : '',
      String(REDIS_GENERATION_CAPTURE_TTL_MS),
    ])) as string
    return (JSON.parse(reply) as { ok: boolean }).ok
  }

  #generationInvalidated(owner: string, token: string): void {
    if (this.#locallyDroppingGenerations.has(owner)) return
    const channels = new Set(
      [...this.#generationBindings.values()]
        .filter(
          (binding) =>
            binding.owner === owner && (binding.generationToken === null || binding.generationToken === token),
        )
        .map((binding) => binding.channel),
    )
    if (channels.size > 0) this.#transport.invalidateChannels(channels)
  }

  // ── generation lifecycle ──

  async listGenerations(roomId: string): Promise<string[]> {
    this.#assertLive()
    return this.#publisher.smembers(gensKey(this.#prefix, roomId))
  }

  async dropGeneration(roomId: string, inc: string): Promise<void> {
    this.#assertLive()
    const head = this.#parseHead(await this.#publisher.get(headKey(this.#prefix, roomId)))
    if ((head?.inc ?? null) === inc) {
      throw new Error(`dropGeneration: refusing to drop the current incarnation '${inc}' of room '${roomId}'`)
    }
    // Physical data cleanup and fallible transport teardown happen while BOTH durable identity sources
    // remain present. A crash or failed UNSUBSCRIBE is therefore resumable and cannot let the incarnation
    // be reused while an old local mux can still bind to its channel.
    const keys = await this.#scanKeys(`${escapeGlob(genPrefix(this.#prefix, roomId, inc))}:*`)
    if (keys.length > 0) await this.#publisher.unlink(...keys)
    const owner = roomGenerationKey(roomId, inc)
    this.#locallyDroppingGenerations.add(owner)
    try {
      const generationToken = await this.#publisher.hget(generationTokensKey(this.#prefix, roomId), inc)
      if (generationToken !== null) {
        // Broker FIFO delivers this invalidation before any later publish for a legally reused generation,
        // including to other backend instances. A disconnected instance instead fails its exact token
        // validation before re-SUBSCRIBE. The initiating backend ignores its echo and performs the same
        // exact cleanup synchronously below, so its local and broadcast teardown cannot race each other.
        await this.#publisher.publish(generationInvalidationChannel(this.#prefix, roomId, inc), generationToken)
      }
      await this.#transport.dropGeneration(owner)
      await this.#call(REDIS_ROOM_COMMANDS.dropGenerationFinalize.name, [
        ...REDIS_ROOM_COMMAND_KEYS.dropGenerationFinalize(this.#prefix, roomId),
        inc,
      ])
    } finally {
      this.#locallyDroppingGenerations.delete(owner)
    }
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
    const tags = await this.#publisher.hmget(directoryTagsKey(this.#prefix), ...matching)
    const entries = matching.map((roomId, i) => ({ roomId, incTag: tags[i] as string }))
    const last = matching[matching.length - 1] as string
    let more = false
    if (matching.length === DIRECTORY_PAGE_SIZE && page.length === DIRECTORY_PAGE_SIZE) {
      const peek = await this.#publisher.zrangebylex(index, `(${last}`, '+', 'LIMIT', 0, 1)
      more = peek.length > 0 && (peek[0] as string).startsWith(prefix)
    }
    return more ? { entries, cursor: last } : { entries }
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return
    this.#disposed = true
    await this.#transport.dispose()
    this.#generationBindings.clear()
  }

  // ── internals ──

  #assertLive(): void {
    if (this.#disposed) throw new Error('RedisRoomBackend: used after dispose()')
  }

  #nowArg(): string {
    return ''
  }

  async #authorityNowMs(): Promise<number> {
    const [seconds, microseconds] = await this.#publisher.time()
    return Number(seconds) * 1_000 + Math.floor(Number(microseconds) / 1_000)
  }

  #parseHead(raw: string | null): StoredHead | null {
    return raw === null ? null : (JSON.parse(raw) as StoredHead)
  }

  // Decode a stored head, treating a logically-expired tombstone as absent (a lapsed tombstone reopens
  // the absence epoch — I1). A pure read never deletes; the head-CX/commit Lua reclaim the PX backstop.
  #liveHead(raw: string | null, authorityNow: number): StoredHead | null {
    const stored = this.#parseHead(raw)
    if (stored === null) return null
    if (stored.exp !== undefined && stored.exp <= authorityNow) return null
    return stored
  }

  async #scanCellKeys(roomId: string, inc: string, prefix: string): Promise<string[]> {
    const physicalPrefix = cellKeyPrefix(this.#prefix, roomId, inc)
    const physical = await this.#scanKeys(`${escapeGlob(physicalPrefix + prefix)}*`)
    return physical.map((key) => key.slice(physicalPrefix.length))
  }

  async #call(command: string, keysAndArgs: ReadonlyArray<string | Uint8Array>): Promise<unknown> {
    if (this.#publisher instanceof Cluster) {
      const descriptor = Object.values(REDIS_ROOM_COMMANDS).find((candidate) => candidate.name === command)
      if (descriptor === undefined) throw new Error(`RedisRoomBackend: unknown command '${command}'`)
      const dynamic = descriptor.numberOfKeys === null
      const numberOfKeys = dynamic ? Number(keysAndArgs[0]) : descriptor.numberOfKeys
      if (!Number.isInteger(numberOfKeys) || numberOfKeys < 0) {
        throw new Error(`RedisRoomBackend: invalid key count for '${command}'`)
      }
      const args = dynamic ? keysAndArgs.slice(1) : keysAndArgs
      const redisArgs = args.map((arg) =>
        typeof arg === 'string' ? arg : Buffer.from(arg.buffer, arg.byteOffset, arg.byteLength),
      )
      try {
        return await this.#publisher.eval(descriptor.lua, numberOfKeys, ...redisArgs)
      } catch (err) {
        // NOSCRIPT proves EVALSHA did not execute. Retrying the complete script with EVAL is therefore
        // non-duplicating, and ioredis preserves MOVED plus ASKING-on-the-same-connection routing for the
        // fallback command. This also covers SCRIPT FLUSH, restarted masters, and newly promoted owners.
        if (!(err instanceof Error) || !err.message.includes('NOSCRIPT')) throw err
        return await this.#publisher.evalsha(redisScriptSha(descriptor.lua), numberOfKeys, ...redisArgs)
      }
    }
    return await callDefinedCommand(this.#publisher, command, keysAndArgs)
  }

  async #scanKeys(pattern: string): Promise<string[]> {
    const nodes = this.#publisher instanceof Cluster ? this.#publisher.nodes('master') : [this.#publisher]
    const keys = new Set<string>()
    for (const node of nodes) {
      let cursor = '0'
      do {
        const [next, page] = await node.scan(cursor, 'MATCH', pattern, 'COUNT', SCAN_COUNT)
        cursor = next
        for (const key of page) keys.add(key)
      } while (cursor !== '0')
    }
    return [...keys]
  }
}

function redisScriptSha(lua: string): string {
  return createHash('sha1').update(lua).digest('hex')
}

// A stored cell is "<expiresAt|''>\n<payload bytes>"; the header is ASCII digits (or empty) with no
// newline, so the FIRST newline is always the separator even when the payload contains newlines.
function parseCellValue(value: Buffer): { expiresAt: number | null; payload: Uint8Array } {
  const nl = value.indexOf(NEWLINE)
  if (nl < 0) return { expiresAt: null, payload: Uint8Array.from(value) }
  const header = value.subarray(0, nl).toString('ascii')
  return { expiresAt: header === '' ? null : Number(header), payload: Uint8Array.from(value.subarray(nl + 1)) }
}
