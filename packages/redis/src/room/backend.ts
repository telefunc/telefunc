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
  ReadinessState,
  RoomBackendSpi,
  RoomHead,
} from '../../../telefunc/wire-protocol/backend/spi.js'
import {
  MAX_CLOSE_LEASE_MS,
  MIN_CLOSE_LEASE_MS,
  ROOM_SPI_VERSION,
} from '../../../telefunc/wire-protocol/backend/spi.js'
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

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

class GenerationInvalidError extends Error {}

type RedisRoomBackendTestHooks = {
  beforeSubscribe?: (channel: string) => void | Promise<void>
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
  // The fixture injects the real subscriber connection so live reconnect / PING-failure probes exercise
  // the production state machine. Production callers omit this and the backend creates the duplicate.
  subscriber?: Redis
  subscriptionRetryDelay?: (attempt: number) => number
  testHooks?: RedisRoomBackendTestHooks
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

// One live subscription over the shared subscriber connection. The handoff to the broker does not await
// the receiver (BackendTraces.handoffAwaitsReceiver is false for Redis), so `deliver` is synchronous.
class RedisLaneSubscription implements LaneSubscription {
  readonly ready: Promise<void>
  #state: ReadinessState = 'establishing'
  #settle!: { resolve: () => void; reject: (err: unknown) => void }
  #readySettled = false
  readonly #listeners = new Set<(state: ReadinessState) => void>()
  readonly #receiver: LaneReceiver
  #detach: () => Promise<void> | void

  constructor(receiver: LaneReceiver, detach: () => Promise<void> | void) {
    this.#receiver = receiver
    this.#detach = detach
    this.ready = new Promise<void>((resolve, reject) => {
      this.#settle = { resolve, reject }
    })
    void this.ready.catch(() => {})
  }

  get closed(): boolean {
    return this.#state === 'closed'
  }

  state(): ReadinessState {
    return this.#state
  }

  onStateChange(cb: (state: ReadinessState) => void): () => void {
    this.#listeners.add(cb)
    return () => this.#listeners.delete(cb)
  }

  async unsubscribe(): Promise<void> {
    if (this.closed) return
    // Local-first is also the validation fence: once this returns to the event loop, no in-flight head
    // check or SUBSCRIBE ack may make this attachment ready again. Remote cleanup remains fallible and
    // retryable from the channel lifecycle's retained ownership record.
    this.#close(new Error('subscribeLane: unsubscribed before initial establishment'))
    await this.#detach()
  }

  established(): void {
    if (this.closed) return
    if (!this.#readySettled) {
      // The first establishing -> ready edge settles the one ready promise and is deliberately NOT an
      // onStateChange emission. A ready after any loss is a re-establishment and is emitted below.
      this.#readySettled = true
      this.#state = 'ready'
      this.#settle.resolve()
      return
    }
    this.#transition('ready')
  }

  connectionLost(reason: unknown): void {
    if (this.closed || this.#state === 'lost') return
    if (!this.#readySettled) {
      this.#readySettled = true
      this.#settle.reject(reason)
    }
    this.#transition('lost')
  }

  failPermanently(reason: unknown): void {
    this.#close(reason)
  }

  // The generation this subscription belongs to was dropped: its channel is gone for good, so the
  // subscription is terminal rather than merely lost.
  generationDropped(): void {
    this.#detach = () => {}
    this.#close(new Error('subscribeLane: generation dropped before initial establishment'))
  }

  deliver(payload: Uint8Array, info: { seq: number; timestamp: number }): void {
    this.#receiver(payload, info)
  }

  #transition(state: ReadinessState): void {
    if (this.#state === state) return
    this.#state = state
    for (const cb of this.#listeners) {
      try {
        cb(state)
      } catch {
        // State observation is advisory; one observer cannot corrupt lifecycle teardown or its siblings.
      }
    }
  }

  #close(reason: unknown): void {
    if (this.closed) return
    if (!this.#readySettled) {
      this.#readySettled = true
      this.#settle.reject(reason)
    }
    this.#transition('closed')
  }
}

type RedisChannelContext = {
  roomId: string
  inc: string
  owner: string
  attemptId: string
  createdAt: number | null
  generationToken: string | null
  operationEpoch: number
  initialEstablished: boolean
}

export class RedisRoomBackend implements RoomBackendSpi {
  readonly spiVersion = ROOM_SPI_VERSION
  readonly capabilities: RoomBackendSpi['capabilities']

  readonly #publisher: Redis
  readonly #subscriber: Redis
  readonly #prefix: string
  readonly #authorityNow?: () => number
  readonly #stableReadProbe?: (info: { roomId: string; inc: string }) => void | Promise<void>
  readonly #subscriptionRetryDelay: (attempt: number) => number
  readonly #testHooks?: RedisRoomBackendTestHooks
  // Dispatch: a channel's live subscriptions; the broker delivers on the shared subscriber connection.
  readonly #channelSubs = new Map<string, Set<RedisLaneSubscription>>()
  // Every same-channel subscriber awaits one shared SUBSCRIBE ack. Successful channels stay recorded
  // until connection loss or the final local unsubscribe.
  readonly #channelAcks = new Map<string, Promise<void>>()
  readonly #subscribedChannels = new Set<string>()
  // One refcounted local mux lifecycle per physical channel. Its generation token and operation epoch
  // fence delayed capture/SUBSCRIBE results, while the retained owner record makes failed cleanup
  // retryable until the durable generation sources can be removed last.
  readonly #channelContexts = new Map<string, RedisChannelContext>()
  // Room-generation identity owns teardown. An inc string alone is not globally unique across rooms.
  readonly #roomGenSubs = new Map<string, Set<RedisLaneSubscription>>()
  readonly #subOwners = new Map<RedisLaneSubscription, string>()
  #disposed = false

  constructor(options: RedisRoomBackendOptions) {
    this.#publisher = options.redis
    this.#subscriptionRetryDelay = options.subscriptionRetryDelay ?? defaultSubscriptionRetryDelay
    this.#subscriber =
      options.subscriber ??
      options.redis.duplicate({
        autoResubscribe: false,
        retryStrategy: (attempt) =>
          attempt <= SUBSCRIPTION_RETRY_ATTEMPTS ? this.#subscriptionRetryDelay(attempt) : null,
      })
    this.#prefix = options.prefix ?? DEFAULT_ROOM_PREFIX
    this.#authorityNow = options.authorityNow
    this.#stableReadProbe = options.stableReadProbe
    this.#testHooks = options.testHooks
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
    this.#subscriber.on('messageBuffer', this.#onMessage)
    this.#subscriber.on('close', this.#onSubscriberClose)
    this.#subscriber.on('ready', this.#onSubscriberReady)
    this.#subscriber.on('end', this.#onSubscriberEnd)
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
      delivery: this.#deliveryFlush(channel),
    }
  }

  // Resolves once every pub/sub frame published before it on `channel` has been dispatched to the local
  // receiver(s). A no-op when the channel has no local subscriber (nothing to flush). A PING transport
  // failure rejects THIS frame's delivery; Redis still exposes no receiver-callback failure.
  #deliveryFlush(channel: string): Promise<void> {
    const set = this.#channelSubs.get(channel)
    if (set === undefined || set.size === 0) return Promise.resolve()
    return this.#subscriber.ping().then(() => {})
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
    const owner = roomGenerationKey(roomId, inc)
    let context = this.#channelContexts.get(channel)
    if (context === undefined) {
      context = {
        roomId,
        inc,
        owner,
        attemptId: randomUUID(),
        createdAt: null,
        generationToken: null,
        operationEpoch: 0,
        initialEstablished: false,
      }
      this.#channelContexts.set(channel, context)
    }
    let sub: RedisLaneSubscription
    sub = new RedisLaneSubscription(receiver, () => this.#detach(sub, owner, channel))
    this.#track(this.#roomGenSubs, owner, sub)
    this.#subOwners.set(sub, owner)
    // Install local dispatch synchronously. This makes immediate unsubscribe lease-exact: it closes and
    // removes the attachment before any awaited capture/validation result can authorize readiness.
    this.#track(this.#channelSubs, channel, sub)
    void this.#establishAttachment(sub, channel)
    return sub
  }

  async #establishAttachment(sub: RedisLaneSubscription, channel: string): Promise<void> {
    try {
      await this.#ensureChannelSubscribed(channel)
      if (!sub.closed) sub.established()
    } catch {
      // #ensureChannelSubscribed closes every subscriber on bounded exhaustion. Connection loss before
      // exhaustion moves this subscription to lost and a later ack emits ready via onStateChange.
    }
  }

  #ensureChannelSubscribed(channel: string): Promise<void> {
    if (this.#subscribedChannels.has(channel)) return Promise.resolve()
    const existing = this.#channelAcks.get(channel)
    if (existing !== undefined) return existing
    const context = this.#channelContexts.get(channel)
    if (context === undefined) return Promise.reject(new Error(`subscribeLane: missing channel lifecycle '${channel}'`))
    const operationEpoch = context.operationEpoch

    let ack: Promise<void>
    ack = this.#subscribeWithRetry(channel, context, operationEpoch)
      .then(async (installed) => {
        const set = this.#channelSubs.get(channel)
        if (!installed || context.operationEpoch !== operationEpoch || set === undefined || set.size === 0) {
          if (installed && !this.#disposed && this.#subscriber.status !== 'end') {
            await this.#subscriber.unsubscribe(channel)
          }
          this.#subscribedChannels.delete(channel)
          return
        }
        this.#subscribedChannels.add(channel)
        context.initialEstablished = true
        for (const sub of set) sub.established()
      })
      .catch((err: unknown) => {
        this.#subscriptionExhausted(channel, err)
        throw err
      })
      .finally(() => {
        if (this.#channelAcks.get(channel) === ack) this.#channelAcks.delete(channel)
      })
    this.#channelAcks.set(channel, ack)
    return ack
  }

  async #subscribeWithRetry(channel: string, context: RedisChannelContext, operationEpoch: number): Promise<boolean> {
    let lastError: unknown = new Error(`subscribeLane: SUBSCRIBE '${channel}' failed`)
    for (let attempt = 1; attempt <= SUBSCRIPTION_RETRY_ATTEMPTS; attempt++) {
      if (this.#disposed) throw new Error('RedisRoomBackend: disposed during subscription establishment')
      const set = this.#channelSubs.get(channel)
      if (context.operationEpoch !== operationEpoch || set === undefined || set.size === 0) return false
      try {
        if (context.generationToken === null) {
          const captured = await this.#captureGeneration(context)
          // This seam is after the durable command and before local observation, so throwing faithfully
          // models a lost first response. The retry reuses the exact attempt id/creation epoch.
          await this.#testHooks?.afterGenerationCapture?.({
            roomId: context.roomId,
            inc: context.inc,
            attemptId: context.attemptId,
            createdAt: context.createdAt as number,
            token: captured,
          })
          if (context.operationEpoch !== operationEpoch) return false
          context.generationToken = captured
        }
        await this.#testHooks?.beforeSubscribe?.(channel)
        if (context.operationEpoch !== operationEpoch) return false
        await this.#subscriber.subscribe(channel)
        if (context.operationEpoch !== operationEpoch) return true
        const valid = await this.#validateGeneration(context, !context.initialEstablished)
        if (!valid) {
          await this.#subscriber.unsubscribe(channel)
          throw new GenerationInvalidError(`subscribeLane: generation '${context.inc}' was invalidated`)
        }
        return true
      } catch (err) {
        lastError = err
        if (err instanceof GenerationInvalidError) throw err
        for (const sub of set) sub.connectionLost(err)
        if (attempt < SUBSCRIPTION_RETRY_ATTEMPTS) await delay(this.#subscriptionRetryDelay(attempt))
      }
    }
    throw lastError
  }

  async #captureGeneration(context: RedisChannelContext): Promise<string> {
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
    if ('rejected' in parsed) throw new GenerationInvalidError(`subscribeLane: ${parsed.reason}`)
    return parsed.token
  }

  async #validateGeneration(context: RedisChannelContext, includeCapture: boolean): Promise<boolean> {
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

  #subscriptionExhausted(channel: string, reason: unknown): void {
    this.#subscribedChannels.delete(channel)
    const set = this.#channelSubs.get(channel)
    if (set === undefined) return
    this.#channelSubs.delete(channel)
    this.#channelContexts.delete(channel)
    for (const sub of set) {
      this.#untrackSubscriptionOwner(sub)
      sub.failPermanently(reason)
    }
  }

  async #detach(sub: RedisLaneSubscription, owner: string, channel: string): Promise<void> {
    this.#untrack(this.#roomGenSubs, owner, sub)
    this.#subOwners.delete(sub)
    await this.#detachChannel(sub, channel)
  }

  async #detachChannel(sub: RedisLaneSubscription, channel: string): Promise<void> {
    const set = this.#channelSubs.get(channel)
    if (set === undefined) return
    set.delete(sub)
    if (set.size !== 0) return
    const context = this.#channelContexts.get(channel)
    if (context !== undefined) context.operationEpoch++
    const subscribed = this.#subscribedChannels.has(channel)
    if (subscribed && !this.#disposed && this.#subscriber.status !== 'end') {
      try {
        await this.#subscriber.unsubscribe(channel)
      } catch (err) {
        // Keep the empty mux + exact ownership source for retry. Local detach already closed the
        // attachment, so the state machine can never claim ready without a live callback.
        void this.#retryEmptyChannelCleanup(channel)
        throw err
      }
    }
    this.#finishEmptyChannelCleanup(channel)
  }

  async #retryEmptyChannelCleanup(channel: string): Promise<void> {
    for (let attempt = 1; attempt <= SUBSCRIPTION_RETRY_ATTEMPTS; attempt++) {
      if (this.#disposed) return
      await delay(this.#subscriptionRetryDelay(attempt))
      const set = this.#channelSubs.get(channel)
      if (set === undefined || set.size !== 0) return
      try {
        if (this.#subscribedChannels.has(channel) && this.#subscriber.status !== 'end') {
          await this.#subscriber.unsubscribe(channel)
        }
        this.#finishEmptyChannelCleanup(channel)
        return
      } catch {
        // Per-attempt isolation: retain this exact item and continue; other channel cleanup is untouched.
      }
    }
  }

  #finishEmptyChannelCleanup(channel: string): void {
    const set = this.#channelSubs.get(channel)
    if (set !== undefined && set.size !== 0) return
    this.#subscribedChannels.delete(channel)
    this.#channelSubs.delete(channel)
    this.#channelContexts.delete(channel)
    this.#channelAcks.delete(channel)
  }

  #untrackSubscriptionOwner(sub: RedisLaneSubscription): void {
    const owner = this.#subOwners.get(sub)
    if (owner === undefined) return
    this.#untrack(this.#roomGenSubs, owner, sub)
    this.#subOwners.delete(sub)
  }

  readonly #onMessage = (channelBytes: Buffer, frame: Buffer): void => {
    const channel = channelBytes.toString()
    const set = this.#channelSubs.get(channel)
    if (set === undefined || set.size === 0) return
    const { seq, timestamp, payload } = decodeFrameHeader(frame)
    const info = { seq, timestamp }
    const copy = Uint8Array.from(payload)
    for (const sub of [...set]) if (!sub.closed) sub.deliver(copy, info)
  }

  readonly #onSubscriberClose = (): void => {
    if (this.#disposed) return
    this.#subscribedChannels.clear()
    const reason = new Error('subscribeLane: Redis subscriber connection lost')
    for (const set of this.#channelSubs.values()) for (const sub of set) sub.connectionLost(reason)
  }

  readonly #onSubscriberReady = (): void => {
    if (this.#disposed) return
    for (const [channel, set] of this.#channelSubs) {
      if (set.size === 0) void this.#retryEmptyChannelCleanup(channel)
      else void this.#ensureChannelSubscribed(channel).catch(() => {})
    }
  }

  readonly #onSubscriberEnd = (): void => {
    if (this.#disposed) return
    const reason = new Error('subscribeLane: Redis subscriber reconnect attempts exhausted')
    for (const [channel, set] of this.#channelSubs) {
      this.#subscribedChannels.delete(channel)
      for (const sub of set) {
        this.#untrackSubscriptionOwner(sub)
        sub.failPermanently(reason)
      }
    }
    this.#channelSubs.clear()
    this.#channelContexts.clear()
    this.#channelAcks.clear()
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
    const subs = this.#roomGenSubs.get(owner)
    if (subs !== undefined) {
      // Generation invalidation is terminal locally even if transport cleanup fails. Closing every
      // attachment first prevents a retry or late ack from making it ready in a reused incarnation.
      for (const sub of subs) sub.generationDropped()
    }
    await this.#dropGenerationChannels(owner)
    if (subs !== undefined) for (const sub of subs) this.#subOwners.delete(sub)
    this.#roomGenSubs.delete(owner)
    await this.#testHooks?.beforeDropGenerationUnregister?.({ roomId, inc })
    await callDefinedCommand(this.#publisher, DROP_GENERATION_FINALIZE_CMD, [
      gensKey(this.#prefix, roomId),
      generationTokensKey(this.#prefix, roomId),
      inc,
    ])
  }

  async #dropGenerationChannels(owner: string): Promise<void> {
    // Snapshot each exact mux independently. One failure retains only that item and does not hide the
    // others from a later retry; the caller withholds the durable gens/token finalization on any failure.
    const failures: unknown[] = []
    for (const [channel, context] of [...this.#channelContexts]) {
      if (context.owner !== owner) continue
      const set = this.#channelSubs.get(channel)
      set?.clear()
      context.operationEpoch++
      try {
        if (this.#subscribedChannels.has(channel) && !this.#disposed && this.#subscriber.status !== 'end') {
          await this.#subscriber.unsubscribe(channel)
        }
        this.#finishEmptyChannelCleanup(channel)
      } catch (err) {
        failures.push(err)
      }
    }
    if (failures.length > 0) throw failures[0]
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
    await this.#testHooks?.beforeDirectoryDeleteApply?.({ roomId, incTag })
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

  // Dark conformance primitives: these call the exact production capture command, never a fake store.
  // They exist so the authority-time reclamation and delayed-response fence can be driven without a
  // real 90-second wait; no package barrel exports this backend in W2.
  async captureGenerationAttemptForTest(
    roomId: string,
    inc: string,
    attemptId: string,
    createdAt: number,
  ): Promise<string> {
    return this.#captureGeneration({
      roomId,
      inc,
      owner: roomGenerationKey(roomId, inc),
      attemptId,
      createdAt,
      generationToken: null,
      operationEpoch: 0,
      initialEstablished: false,
    })
  }

  async countGenerationCapturesForTest(roomId: string): Promise<number> {
    return this.#publisher.hlen(routeCapturesKey(this.#prefix, roomId))
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return
    this.#disposed = true
    for (const set of this.#channelSubs.values()) for (const sub of set) sub.generationDropped()
    this.#channelSubs.clear()
    this.#channelContexts.clear()
    this.#channelAcks.clear()
    this.#subscribedChannels.clear()
    this.#roomGenSubs.clear()
    this.#subOwners.clear()
    this.#subscriber.off('messageBuffer', this.#onMessage)
    this.#subscriber.off('close', this.#onSubscriberClose)
    this.#subscriber.off('ready', this.#onSubscriberReady)
    this.#subscriber.off('end', this.#onSubscriberEnd)
    try {
      await this.#subscriber.quit()
    } catch {
      this.#subscriber.disconnect()
    }
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

  #track(map: Map<string, Set<RedisLaneSubscription>>, key: string, sub: RedisLaneSubscription): void {
    const set = map.get(key) ?? new Set<RedisLaneSubscription>()
    set.add(sub)
    map.set(key, set)
  }

  #untrack(map: Map<string, Set<RedisLaneSubscription>>, key: string, sub: RedisLaneSubscription): void {
    const set = map.get(key)
    if (set === undefined) return
    set.delete(sub)
    if (set.size === 0) map.delete(key)
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
