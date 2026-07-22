// The standalone-Redis realization of RoomBackendSpi, proved against the shared conformance suite to the
// SAME outcomes as the memory reference (convergence W2). DARK: not exported from `@telefunc/redis`'s
// barrel and used by no Room call site — the final hash-tagged, incarnation-scoped backend is W3-R and
// the real 3-master Cluster slot proof is W4-R, so `capabilities.clusterSafe` stays false here (D1).
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

import { randomUUID } from 'node:crypto'
import type { Redis } from 'ioredis'
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
  CAPTURE_GENERATION_CMD,
  CAPTURE_GENERATION_KEYS,
  CAPTURE_GENERATION_LUA,
  channelKey,
  CELLS_CX_CMD,
  CELLS_CX_LUA,
  COMMIT_CMD,
  COMMIT_KEYS,
  COMMIT_LUA,
  decodeFrameHeader,
  DEFAULT_ROOM_PREFIX,
  DIRECTORY_DELETE_CMD,
  DIRECTORY_DELETE_KEYS,
  DIRECTORY_DELETE_LUA,
  directoryIndexKey,
  DIRECTORY_PUT_CMD,
  DIRECTORY_PUT_KEYS,
  DIRECTORY_PUT_LUA,
  directoryTagsKey,
  DROP_GENERATION_FINALIZE_CMD,
  DROP_GENERATION_FINALIZE_KEYS,
  DROP_GENERATION_FINALIZE_LUA,
  escapeGlob,
  generationInvalidationChannel,
  generationTokensKey,
  gensKey,
  genPrefix,
  HEAD_CX_CMD,
  HEAD_CX_KEYS,
  HEAD_CX_LUA,
  headKey,
  headRevKey,
  laneKey,
  orderKey,
  parseLaneKey,
  retainedKey,
  retainedKeyPrefix,
  RETAINED_DELETE_CMD,
  RETAINED_DELETE_LUA,
  retainedSizeKey,
  revKey,
  routeCaptureExpiriesKey,
  routeCapturesKey,
  VALIDATE_GENERATION_CMD,
  VALIDATE_GENERATION_KEYS,
  VALIDATE_GENERATION_LUA,
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

type RedisRoomBackendTestHooks = {
  beforeSubscribe?: (channel: string) => void | Promise<void>
  afterSubscribeAck?: (channel: string) => void | Promise<void>
  afterGenerationCapture?: (info: {
    roomId: string
    inc: string
    attemptId: string
    createdAt: number
    token: string
  }) => void | Promise<void>
  beforeDropGenerationUnregister?: (info: { roomId: string; inc: string }) => void | Promise<void>
  beforeDirectoryDeleteApply?: (info: { roomId: string; incTag: string }) => void | Promise<void>
}

export type RedisRoomBackendOptions = {
  redis: Redis
  prefix?: string
  maxRetainedPayloadBytes?: number
}

// Internal construction dependencies. They are not re-exported by the package: the public constructor
// only needs a publisher client and optional namespace/cap. Hosts may use the same runtime seams to
// control subscriber ownership, time, and retry scheduling.
type RedisRoomBackendRuntime = {
  // The authority clock. When supplied (conformance), every time-sensitive Lua receives this frozen,
  // advanceable value as `now_ms` and JS-side logical filtering resolves against it too; a backend that
  // instead consulted a caller's `Date.now()` would fail every I13 killer. When absent (production), the
  // Lua and JS read paths fall back to Redis TIME — the central server clock. Date.now() is never an
  // authority source.
  authorityNow?: () => number
  // Test seam (sanctioned by readiness-ordering §2.2's holdRegistration/holdRetainedRead hooks): a
  // callback invoked inside the stable-read window — after the result set is enumerated, before
  // rev_after — so a scenario can force an insert/delete/expiry between rev_before and rev_after.
  stableReadProbe?: (info: { roomId: string; inc: string }) => void | Promise<void>
  // A host may inject the subscriber connection; otherwise the backend creates its own duplicate.
  subscriber?: Redis
  subscriptionRetryDelay?: (attempt: number) => number
}

const TEST_HOOKS = Symbol.for('telefunc.redis.room.test-hooks')
type TestHookStore = WeakMap<RedisRoomBackend, RedisRoomBackendTestHooks>
function redisRoomBackendTestHooks(backend: RedisRoomBackend): RedisRoomBackendTestHooks | undefined {
  return (globalThis as typeof globalThis & { [TEST_HOOKS]?: TestHookStore })[TEST_HOOKS]?.get(backend)
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

  readonly #publisher: Redis
  readonly #transport: RedisSubscriberTransport
  readonly #prefix: string
  readonly #authorityNow?: () => number
  readonly #stableReadProbe?: (info: { roomId: string; inc: string }) => void | Promise<void>
  // Durable capture/generation identity stays here. The transport receives this object only as an opaque
  // channel binding and delegates capture/validation back before it can settle readiness.
  readonly #generationBindings = new Map<string, RedisGenerationBinding>()
  #disposed = false

  constructor(options: RedisRoomBackendOptions, runtime: RedisRoomBackendRuntime = {}) {
    this.#publisher = options.redis
    const subscriptionRetryDelay = runtime.subscriptionRetryDelay ?? defaultSubscriptionRetryDelay
    const subscriber =
      runtime.subscriber ??
      options.redis.duplicate({
        autoResubscribe: false,
        retryStrategy: (attempt) => (attempt <= SUBSCRIPTION_RETRY_ATTEMPTS ? subscriptionRetryDelay(attempt) : null),
      })
    this.#prefix = options.prefix ?? DEFAULT_ROOM_PREFIX
    this.#authorityNow = runtime.authorityNow
    this.#stableReadProbe = runtime.stableReadProbe
    this.capabilities = {
      receivers: 'global',
      maxRetainedPayloadBytes: options.maxRetainedPayloadBytes ?? DEFAULT_MAX_RETAINED_BYTES,
      clusterSafe: false,
      directory: true,
    }
    this.#publisher.defineCommand(HEAD_CX_CMD, { numberOfKeys: HEAD_CX_KEYS, lua: HEAD_CX_LUA })
    this.#publisher.defineCommand(CAPTURE_GENERATION_CMD, {
      numberOfKeys: CAPTURE_GENERATION_KEYS,
      lua: CAPTURE_GENERATION_LUA,
    })
    this.#publisher.defineCommand(VALIDATE_GENERATION_CMD, {
      numberOfKeys: VALIDATE_GENERATION_KEYS,
      lua: VALIDATE_GENERATION_LUA,
    })
    this.#publisher.defineCommand(DROP_GENERATION_FINALIZE_CMD, {
      numberOfKeys: DROP_GENERATION_FINALIZE_KEYS,
      lua: DROP_GENERATION_FINALIZE_LUA,
    })
    this.#publisher.defineCommand(COMMIT_CMD, { numberOfKeys: COMMIT_KEYS, lua: COMMIT_LUA })
    // Variable key count (head, rev, then one key per mutation) — numberOfKeys is supplied per call.
    this.#publisher.defineCommand(CELLS_CX_CMD, { lua: CELLS_CX_LUA })
    this.#publisher.defineCommand(RETAINED_DELETE_CMD, { lua: RETAINED_DELETE_LUA })
    this.#publisher.defineCommand(DIRECTORY_PUT_CMD, { numberOfKeys: DIRECTORY_PUT_KEYS, lua: DIRECTORY_PUT_LUA })
    this.#publisher.defineCommand(DIRECTORY_DELETE_CMD, {
      numberOfKeys: DIRECTORY_DELETE_KEYS,
      lua: DIRECTORY_DELETE_LUA,
    })
    this.#transport = new RedisSubscriberTransport({
      subscriber,
      retryDelay: subscriptionRetryDelay,
      hooks: {
        beforeSubscribe: (channel) => redisRoomBackendTestHooks(this)?.beforeSubscribe?.(channel),
        afterSubscribeAck: (channel) => redisRoomBackendTestHooks(this)?.afterSubscribeAck?.(channel),
      },
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
    const reply = (await callDefinedCommand(this.#publisher, HEAD_CX_CMD, [
      headKey(this.#prefix, roomId),
      gensKey(this.#prefix, roomId),
      headRevKey(this.#prefix, roomId),
      generationTokensKey(this.#prefix, roomId),
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
      if (this.#stableReadProbe !== undefined) await this.#stableReadProbe({ roomId, inc })
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
    const keys: string[] = [headKey(this.#prefix, roomId), revKey(this.#prefix, roomId, inc)]
    const argv: Array<string | Buffer> = [this.#nowArg(), inc, revision]
    for (const mutation of mutations) {
      keys.push(cellKey(this.#prefix, roomId, inc, mutation.key))
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
    const reply = (await callDefinedCommand(this.#publisher, CELLS_CX_CMD, [
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
    const key = laneKey(lane)
    const channel = channelKey(this.#prefix, roomId, inc, key)
    const reply = (await callDefinedCommand(this.#publisher, COMMIT_CMD, [
      headKey(this.#prefix, roomId),
      orderKey(this.#prefix, roomId, inc, key),
      retainedKey(this.#prefix, roomId, inc, key),
      channel,
      retainedSizeKey(this.#prefix, roomId, inc),
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
    return {
      accepted: true,
      seq: parsed.seq,
      timestamp: parsed.timestamp,
      receivers: parsed.receivers,
      delivery: this.#transport.flush(channel),
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
    const size = retainedSizeKey(this.#prefix, roomId, inc)
    if (lane !== undefined) {
      await callDefinedCommand(this.#publisher, RETAINED_DELETE_CMD, [
        '2',
        size,
        retainedKey(this.#prefix, roomId, inc, laneKey(lane)),
      ])
      return
    }
    const keys = await this.#scanKeys(`${escapeGlob(retainedKeyPrefix(this.#prefix, roomId, inc))}*`)
    await callDefinedCommand(this.#publisher, RETAINED_DELETE_CMD, [String(keys.length + 1), size, ...keys])
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
    // This seam is after the durable command and before local observation, so throwing faithfully
    // models a lost first response. A retry reuses the exact attempt id and creation epoch.
    await redisRoomBackendTestHooks(this)?.afterGenerationCapture?.({
      roomId: binding.roomId,
      inc: binding.inc,
      attemptId: binding.attemptId,
      createdAt: binding.createdAt as number,
      token: captured,
    })
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
    const reply = (await callDefinedCommand(this.#publisher, CAPTURE_GENERATION_CMD, [
      headKey(this.#prefix, context.roomId),
      gensKey(this.#prefix, context.roomId),
      generationTokensKey(this.#prefix, context.roomId),
      routeCapturesKey(this.#prefix, context.roomId),
      routeCaptureExpiriesKey(this.#prefix, context.roomId),
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
    const reply = (await callDefinedCommand(this.#publisher, VALIDATE_GENERATION_CMD, [
      headKey(this.#prefix, context.roomId),
      gensKey(this.#prefix, context.roomId),
      generationTokensKey(this.#prefix, context.roomId),
      routeCapturesKey(this.#prefix, context.roomId),
      routeCaptureExpiriesKey(this.#prefix, context.roomId),
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
    const generationToken = await this.#publisher.hget(generationTokensKey(this.#prefix, roomId), inc)
    if (generationToken !== null) {
      // Broker FIFO delivers this invalidation before any later publish for a legally reused generation,
      // including to other backend instances. A disconnected instance instead fails its exact token
      // validation before re-SUBSCRIBE.
      await this.#publisher.publish(generationInvalidationChannel(this.#prefix, roomId, inc), generationToken)
    }
    const owner = roomGenerationKey(roomId, inc)
    await this.#transport.dropGeneration(owner)
    await redisRoomBackendTestHooks(this)?.beforeDropGenerationUnregister?.({ roomId, inc })
    await callDefinedCommand(this.#publisher, DROP_GENERATION_FINALIZE_CMD, [
      gensKey(this.#prefix, roomId),
      generationTokensKey(this.#prefix, roomId),
      inc,
    ])
  }

  // ── directory (global; its own two co-slotted keys) ──

  async directoryPut(roomId: string, incTag: string): Promise<void> {
    this.#assertLive()
    await callDefinedCommand(this.#publisher, DIRECTORY_PUT_CMD, [
      directoryIndexKey(this.#prefix),
      directoryTagsKey(this.#prefix),
      roomId,
      incTag,
    ])
  }

  async directoryDelete(roomId: string, incTag: string): Promise<void> {
    this.#assertLive()
    await redisRoomBackendTestHooks(this)?.beforeDirectoryDeleteApply?.({ roomId, incTag })
    await callDefinedCommand(this.#publisher, DIRECTORY_DELETE_CMD, [
      directoryIndexKey(this.#prefix),
      directoryTagsKey(this.#prefix),
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
    return this.#authorityNow !== undefined ? String(this.#authorityNow()) : ''
  }

  async #authorityNowMs(): Promise<number> {
    if (this.#authorityNow !== undefined) return this.#authorityNow()
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

  async #scanKeys(pattern: string): Promise<string[]> {
    const keys: string[] = []
    let cursor = '0'
    do {
      const [next, page] = await this.#publisher.scan(cursor, 'MATCH', pattern, 'COUNT', SCAN_COUNT)
      cursor = next
      for (const key of page) keys.push(key)
    } while (cursor !== '0')
    return keys
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
