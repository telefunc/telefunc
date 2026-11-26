/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastTransport, CloudflareBroadcastAuthorityState }
export type { BroadcastDeliverRequest, BroadcastPublishRequest, TelefuncBroadcastStub }

import { PUBLISH_HOLD_BYTES } from '../../constants.js'
import { KNOWN_BROADCAST_BUCKETS, getBucketCoordinatorScale, getDeterministicKeyBucketIndex } from './routing.js'
import { assert, assertUsage, assertWarning } from '../../../utils/assert.js'
import { utf8ByteLength } from '../../../utils/utf8ByteLength.js'
import { Fifo } from '../../../utils/Fifo.js'
import { OrderedStubs } from './OrderedStubs.js'
import { Outcome } from '../../channel/channel.js'
import type {
  BroadcastBinaryOnMessage,
  BroadcastDelivery,
  BroadcastOnMessage,
  BroadcastPublishResult,
  BroadcastAdapter,
  Listening,
} from '../../broadcast/broadcastAdapter.js'
import type { AsyncLocalStorage } from 'node:async_hooks'
import type { WirePublishInfo } from '../../wire/publishInfo.js'
import type { CloudflareScale, LocationBucket } from './routing.js'

const PRESENCE_TTL_SECONDS = 90
const PRESENCE_REFRESH_INTERVAL_MS = 30_000

/** Unwrap Cloudflare DO RPC proxy into a plain object.
 *  RPC properties are lazy stubs that must be awaited to resolve their values. */
async function unwrapRpcResult(rpc: Promise<BroadcastPublishResult>): Promise<BroadcastPublishResult> {
  const r = await rpc
  const [seq, timestamp, meta] = await Promise.all([r.seq, r.timestamp, r.meta])
  return { seq, timestamp, ...(meta ? { meta } : undefined) }
}

type BroadcastPublishRequest = {
  key: string
  locationBucket: LocationBucket
  forwarded?: boolean
  doNames?: string[]
  info?: WirePublishInfo
  serialized?: string
  binaryData?: Uint8Array
}

type BroadcastDeliverRequest = {
  key: string
  /** The Durable Object it goes to: it delivers to its own subscribers only. */
  doName: string
  info: WirePublishInfo
  serialized?: string
  binaryData?: Uint8Array
}

type TelefuncBroadcastStub = DurableObjectStub & {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<BroadcastPublishResult>
  telefuncBroadcastDeliver(request: BroadcastDeliverRequest): Promise<void>
}

type PendingBucketPublish = {
  resolve: (ack: BroadcastPublishResult) => void
  reject: (err: Error) => void
}

type BufferedPublish = {
  send: () => Promise<BroadcastPublishResult>
  bytes: number
  pending: PendingBucketPublish
}

/** KV takes one write a second per key, so a write it refuses is expected. Whether KV took it. */
async function warnRefusedWrite(write: Promise<void>, warning: string): Promise<boolean> {
  try {
    await write
    return true
  } catch (err) {
    assertWarning(false, `${warning} (${String(err)})`, { onlyOnce: false })
    return false
  }
}

/**
 * Hard-capped ring buffer for bucket publishes that arrive before the source bucket setup finishes.
 * This bounds cold-path memory while preserving a contiguous FIFO suffix of pending publishes.
 * Payload-agnostic — each entry carries its own send function (works for both text and binary).
 */
class CloudflareBucketPublishBuffer {
  private readonly entries = new Fifo<BufferedPublish>()
  private totalBytes = 0
  private readonly maxBytes: number
  private isFlushing = false

  constructor(maxBytes: number) {
    assert(maxBytes > 0, 'Cloudflare bucket publish buffer size must be > 0.')
    this.maxBytes = maxBytes
  }

  push(entry: BufferedPublish): void {
    if (this.admit(entry.bytes, entry.pending)) this.append(entry)
  }

  /** Holds a copy of `data` for `send`, made only once the buffer admits it. */
  pushCopy(
    data: Uint8Array,
    send: (data: Uint8Array) => Promise<BroadcastPublishResult>,
    pending: PendingBucketPublish,
  ): void {
    if (!this.admit(data.byteLength, pending)) return
    const copy = new Uint8Array(data)
    this.append({ send: () => send(copy), bytes: copy.byteLength, pending })
  }

  get flushing(): boolean {
    return this.isFlushing
  }

  async flush(): Promise<void> {
    if (this.isFlushing) return
    this.isFlushing = true
    try {
      // Each leaves before its send: eviction and clear() are for what still waits, and its send holds its bytes.
      for (let entry = this.entries.shift(); entry !== undefined; entry = this.entries.shift()) {
        this.totalBytes -= entry.bytes
        try {
          const ack = await entry.send()
          entry.pending.resolve(ack)
        } catch (err) {
          entry.pending.reject(err instanceof Error ? err : new Error(String(err)))
        }
      }
    } finally {
      this.isFlushing = false
    }
  }

  clear(): void {
    for (let entry = this.entries.shift(); entry !== undefined; entry = this.entries.shift()) {
      entry.pending.reject(new Error('Keyed publish was dropped from the Cloudflare bucket buffer'))
    }
    this.totalBytes = 0
  }

  /** Whether an entry of `bytes` fits; one that can't clears the buffer and rejects `pending`. */
  private admit(bytes: number, pending: PendingBucketPublish): boolean {
    if (bytes <= this.maxBytes) return true
    pending.reject(new Error('Keyed publish exceeded the Cloudflare bucket buffer limit during cold-path setup'))
    this.clear()
    return false
  }

  private append(entry: BufferedPublish): void {
    this.entries.push(entry)
    this.totalBytes += entry.bytes
    this.evict()
  }

  private evict(): void {
    while (this.totalBytes > this.maxBytes) {
      const entry = this.entries.shift()
      if (entry === undefined) return
      entry.pending.reject(
        new Error('Keyed publish was evicted from the Cloudflare bucket buffer before bucket setup completed'),
      )
      this.totalBytes -= entry.bytes
    }
  }
}

class MemberBucketState {
  readonly pendingPublishes: CloudflareBucketPublishBuffer
  /** Presences being written: publishes from here wait for them, so their own subscribers get them. */
  setupsInFlight = 0
  /** Each Durable Object with subscribers on the key here, with how many: each has a presence of its own. */
  readonly owners = new Map<string, number>()
  /** Each owner's presence KV hasn't written, by owner: being written, its subscribers are in effect once it is; refused,
   *  they never are, nor any it gets until a refresh writes it or it has none left. */
  readonly unwritten = new Map<string, Outcome>()
  refreshTimer: ReturnType<typeof setInterval> | null = null
  /** A stub can only be used by the Durable Object that got it, and this state is the isolate's, so it holds none. */
  private readonly authority: () => TelefuncBroadcastStub
  private readonly key: string
  private readonly locationBucket: LocationBucket

  constructor(key: string, locationBucket: LocationBucket, authority: () => TelefuncBroadcastStub) {
    this.key = key
    this.locationBucket = locationBucket
    this.authority = authority
    this.pendingPublishes = new CloudflareBucketPublishBuffer(PUBLISH_HOLD_BYTES)
  }

  publish(serialized: string): Promise<BroadcastPublishResult> {
    return unwrapRpcResult(
      this.authority().telefuncBroadcastPublish({
        key: this.key,
        locationBucket: this.locationBucket,
        serialized,
      }),
    )
  }

  publishBinary(data: Uint8Array): Promise<BroadcastPublishResult> {
    return unwrapRpcResult(
      this.authority().telefuncBroadcastPublish({
        key: this.key,
        locationBucket: this.locationBucket,
        binaryData: data,
      }),
    )
  }

  stopRefresh(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer)
      this.refreshTimer = null
    }
  }
}

// ---------------------------------------------------------------------------
// CloudflareBroadcastAuthorityState — wraps DurableObjectState for seq counters
// and authority bucket persistence. One per DO instance, passed to publishToSubscribers.
// ---------------------------------------------------------------------------

class CloudflareBroadcastAuthorityState {
  readonly broadcastCalls = new OrderedStubs<TelefuncBroadcastStub>()
  private readonly state: DurableObjectState
  private authorityPublishChain: Promise<void> = Promise.resolve()
  private readonly keySeqCache = new Map<string, number>()
  private readonly authorityBucketCache = new Map<string, LocationBucket>()

  constructor(state: DurableObjectState) {
    this.state = state
  }

  async getNextKeySeq(key: string): Promise<number> {
    const cachedSeq = this.keySeqCache.get(key)
    const currentSeq = cachedSeq ?? (await this.state.storage.get<number>(`broadcast:${key}:sequence`)) ?? 0
    const nextSeq = currentSeq + 1
    this.keySeqCache.set(key, nextSeq)
    await this.state.storage.put(`broadcast:${key}:sequence`, nextSeq)
    return nextSeq
  }

  async runInAuthorityChain<T>(fn: () => Promise<T>): Promise<T> {
    const previous = this.authorityPublishChain
    let release!: () => void
    this.authorityPublishChain = new Promise<void>((resolve) => {
      release = resolve
    })
    await previous
    try {
      return await fn()
    } finally {
      release()
    }
  }

  async getOrInitAuthorityBucket(key: string, preferredBucket: LocationBucket): Promise<LocationBucket> {
    const cachedBucket = this.authorityBucketCache.get(key)
    if (cachedBucket) return cachedBucket

    const storageKey = `broadcast:${key}:authority-bucket`
    const authorityBucket = (await this.state.storage.get<LocationBucket>(storageKey)) ?? preferredBucket

    this.authorityBucketCache.set(key, authorityBucket)
    await this.state.storage.put(storageKey, authorityBucket)
    return authorityBucket
  }
}

// ---------------------------------------------------------------------------
// CloudflareBroadcastTransport — per-isolate broadcast transport.
//
// A single CloudflareBroadcastTransport instance exists per worker isolate.
// Multiple Durable Object instances in the same isolate share this transport.
//
// State stored here:
//   - textSubs/binarySubs: local subscriber callbacks by key, each with the DO that owns it
//   - memberStates: per key, the presences of the DOs with subscribers here and the publishes waiting on them
//   - requestOwners: each request a DO received, with its name
//   - locationBucket: set once on first attachIsolateInfo call
// The DO whose code runs is kept in `runningObject` (`runInObject`).
// ---------------------------------------------------------------------------

class CloudflareBroadcastTransport implements BroadcastAdapter {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private bindingName: string | null = null
  private binding: DurableObjectNamespace | null = null
  private kv: KVNamespace | null = null
  private locationBucket: LocationBucket | null = null
  private readonly memberStates = new Map<string, MemberBucketState>()
  /** Each presence's KV write or delete under way, by its key and owner: the next one waits for it. */
  private readonly presenceOperations = new Map<string, Promise<void>>()
  /** Each subscriber with the Durable Object whose context may write to its page. */
  private readonly textSubs = new Map<string, Map<BroadcastOnMessage, string>>()
  private readonly binarySubs = new Map<string, Map<BroadcastBinaryOnMessage, string>>()
  /** Each request a Durable Object received, with its name: a wire it opens is that Durable Object's. */
  private readonly requestOwners = new WeakMap<Request, string>()
  /** The stub to each key's authority of this synchronous run, which is one Durable Object's: its publishes in one run
   *  stay in order. Later runs may be another's, so they get their own. */
  private readonly authorityStubsOfThisRun = new Map<string, TelefuncBroadcastStub>()
  /** The Durable Object whose code runs (`runInObject`); null on a Worker without AsyncLocalStorage. */
  private readonly runningObject: AsyncLocalStorage<string> | null

  constructor({
    baseInstanceName,
    scale,
    runningObject = null,
  }: {
    baseInstanceName: string
    scale?: CloudflareScale
    runningObject?: AsyncLocalStorage<string> | null
  }) {
    this.baseInstanceName = baseInstanceName
    this.scale = scale
    this.runningObject = runningObject
  }

  attachBinding(binding: DurableObjectNamespace, bindingName: string): void {
    this.binding = binding
    this.bindingName = bindingName
  }

  attachKV(kv: KVNamespace): void {
    this.kv = kv
  }

  attachRequest(request: Request, doName: string): void {
    this.requestOwners.set(request, doName)
  }

  /** Runs `run` as the Durable Object `doName`'s code: what it subscribes with no page's wire is `doName`'s. Without
   *  AsyncLocalStorage no scope survives an await, so nothing is kept. */
  runInObject<T>(doName: string, run: () => T): T {
    return this.runningObject ? this.runningObject.run(doName, run) : run()
  }

  attachIsolateInfo(locationBucket: LocationBucket): void {
    assert(
      KNOWN_BROADCAST_BUCKETS.has(locationBucket),
      `attachIsolateInfo received invalid locationBucket "${locationBucket}".`,
    )
    this.locationBucket ??= locationBucket
  }

  // --- KV presence ---

  private requireKV(): KVNamespace {
    assert(this.kv, 'Cloudflare KV binding is not attached. Broadcast requires a KV namespace.')
    return this.kv
  }

  private requireLocationBucket(): LocationBucket {
    assert(this.locationBucket, 'Expected attachIsolateInfo() to be called before subscribe/publish.')
    return this.locationBucket
  }

  private getPresenceKey(key: string, owner: string): string {
    return `tfps:${encodeURIComponent(key)}:${this.locationBucket}:${owner}`
  }

  private getPresencePrefix(key: string): string {
    return `tfps:${encodeURIComponent(key)}:`
  }

  /** Writes `owner`'s presence again; once KV takes it, a refusal of that presence is no longer its subscribers'
   *  outcome. One its owner got after leaving and coming back meanwhile is a presence the write didn't make. */
  private async refreshPresence(key: string, memberState: MemberBucketState, owner: string): Promise<void> {
    const unwritten = memberState.unwritten.get(owner)
    const wrote = await this.warnRefusedPresence(key, this.writePresence(key, owner))
    if (wrote && unwritten?.failure && memberState.unwritten.get(owner) === unwritten)
      memberState.unwritten.delete(owner)
  }

  private writePresence(key: string, owner: string): Promise<void> {
    return this.inPresenceOrder(key, owner, (kv, presenceKey) =>
      kv.put(presenceKey, owner, { expirationTtl: PRESENCE_TTL_SECONDS }),
    )
  }

  /** Runs `operation` on `owner`'s presence once every write and delete asked of it before has settled, so they go to
   *  KV in the order asked: the delete of an owner that left after a write still under way, and the write of one that
   *  came back after a delete still under way, which its subscribers' `opened` so waits for. An operation that throws at
   *  once fails as one KV refuses does. */
  private inPresenceOrder(
    key: string,
    owner: string,
    operation: (kv: KVNamespace, presenceKey: string) => Promise<void>,
  ): Promise<void> {
    // Its KV name is formed in the operation too: `encodeURIComponent` throws on a key with a lone surrogate.
    const presence = JSON.stringify([key, owner])
    const previous = this.presenceOperations.get(presence) ?? Promise.resolve()
    const done = previous.then(() => operation(this.requireKV(), this.getPresenceKey(key, owner)))
    const settled = done.then(
      () => {},
      () => {},
    )
    this.presenceOperations.set(presence, settled)
    void settled.then(() => {
      if (this.presenceOperations.get(presence) === settled) this.presenceOperations.delete(presence)
    })
    return done
  }

  private warnRefusedPresence(key: string, write: Promise<void>): Promise<boolean> {
    return warnRefusedWrite(
      write,
      `KV refused to record that this Durable Object has subscribers to the Broadcast key ${JSON.stringify(key)}, so they may miss publishes made on other Durable Objects until a later write of that record succeeds`,
    )
  }

  private async deletePresence(key: string, owner: string): Promise<void> {
    await warnRefusedWrite(
      this.inPresenceOrder(key, owner, (kv, presenceKey) => kv.delete(presenceKey)),
      `KV refused to remove the record that this Durable Object has subscribers to the Broadcast key ${JSON.stringify(key)}, so publishes to that key may still reach this Durable Object until the record expires, ${PRESENCE_TTL_SECONDS} s after its last write`,
    )
  }

  private async listPresenceByBucket(key: string): Promise<Map<LocationBucket, string[]>> {
    const kv = this.requireKV()
    const prefix = this.getPresencePrefix(key)
    const result = new Map<LocationBucket, string[]>()
    let cursor: string | undefined

    do {
      const list = await kv.list({ prefix, cursor })
      for (const entry of list.keys) {
        const suffix = entry.name.slice(prefix.length)
        const sepIdx = suffix.indexOf(':')
        if (sepIdx === -1) continue
        const bucket = suffix.slice(0, sepIdx) as LocationBucket
        const doName = suffix.slice(sepIdx + 1)
        let doNames = result.get(bucket)
        if (!doNames) {
          doNames = []
          result.set(bucket, doNames)
        }
        doNames.push(doName)
      }
      cursor = list.list_complete ? undefined : list.cursor
    } while (cursor)

    return result
  }

  // --- Local subscriber tracking ---

  private hasLocalCallbacks(key: string): boolean {
    const text = this.textSubs.get(key)
    if (text && text.size > 0) return true
    const binary = this.binarySubs.get(key)
    return binary !== undefined && binary.size > 0
  }

  /** The Durable Object that may write to a subscriber's page: its wire's; else the one whose code subscribes, where
   *  the page will be, as a call and its page's wire reach one object. */
  ownerOf(wireRequest: Request | undefined): string {
    if (!wireRequest) return this.requireRunningObject()
    const owner = this.requestOwners.get(wireRequest)
    assert(owner, 'a wire opened by a request no Durable Object received')
    return owner
  }

  /** The Durable Object whose code runs (`runInObject`). */
  private requireRunningObject(): string {
    assertUsage(
      this.runningObject,
      'Broadcast on Cloudflare finds the Durable Object its server-side subscriber runs in through AsyncLocalStorage: ' +
        // spellcheck-ignore  nodejs_als is Cloudflare's AsyncLocalStorage compatibility flag
        'add "nodejs_als" to the compatibility_flags of your wrangler.jsonc',
    )
    const doName = this.runningObject.getStore()
    assertUsage(
      doName !== undefined,
      'Broadcast on Cloudflare subscribes on the server only in code a Durable Object runs: a telefunction, or a channel or broadcast callback',
    )
    return doName
  }

  /** Adds `onMessage` to `subsByKey` under `key`, with its owner's presence, which it is in effect once KV wrote, and
   *  never where KV refused it. */
  private addSubscriber<OnMessage>(
    subsByKey: Map<string, Map<OnMessage, string>>,
    key: string,
    onMessage: OnMessage,
    wireRequest: Request | undefined,
  ): Listening {
    const locationBucket = this.requireLocationBucket()
    const owner = this.ownerOf(wireRequest)
    let subs = subsByKey.get(key)
    if (!subs) {
      subs = new Map()
      subsByKey.set(key, subs)
    }
    // A function subscribed again keeps its one entry and its one presence count, and either unsubscribe removes it.
    if (!subs.has(onMessage)) {
      subs.set(onMessage, owner)
      this.ensurePresence(key, locationBucket, owner)
    }
    const unsubscribe = () => {
      const current = subsByKey.get(key)
      const entryOwner = current?.get(onMessage)
      if (!current || entryOwner === undefined) return
      current.delete(onMessage)
      if (current.size === 0) subsByKey.delete(key)
      this.releasePresence(key, entryOwner)
    }
    return { unsubscribe, opened: this.memberStates.get(key)?.unwritten.get(owner) }
  }

  // --- BroadcastAdapter interface ---

  subscribe(key: string, onMessage: BroadcastOnMessage, wireRequest?: Request): Listening {
    return this.addSubscriber(this.textSubs, key, onMessage, wireRequest)
  }

  subscribeBinary(key: string, onMessage: BroadcastBinaryOnMessage, wireRequest?: Request): Listening {
    return this.addSubscriber(this.binarySubs, key, onMessage, wireRequest)
  }

  publish(key: string, serialized: string): Promise<BroadcastPublishResult> {
    const memberState = this.memberStates.get(key)

    if (memberState) {
      if (memberState.setupsInFlight > 0 || memberState.pendingPublishes.flushing) {
        return new Promise<BroadcastPublishResult>((resolve, reject) => {
          memberState.pendingPublishes.push({
            send: () => memberState.publish(serialized),
            bytes: utf8ByteLength(serialized),
            pending: { resolve, reject },
          })
        })
      }
      return memberState.publish(serialized)
    }

    const locationBucket = this.requireLocationBucket()
    const authority = this.getAuthorityStub(key, locationBucket)
    return unwrapRpcResult(authority.telefuncBroadcastPublish({ key, locationBucket, serialized }))
  }

  publishBinary(key: string, data: Uint8Array): Promise<BroadcastPublishResult> {
    const memberState = this.memberStates.get(key)

    if (memberState) {
      if (memberState.setupsInFlight > 0 || memberState.pendingPublishes.flushing) {
        // The caller may reuse or transfer its buffer once the publish returns.
        return new Promise<BroadcastPublishResult>((resolve, reject) => {
          memberState.pendingPublishes.pushCopy(data, (queuedData) => memberState.publishBinary(queuedData), {
            resolve,
            reject,
          })
        })
      }
      return memberState.publishBinary(data)
    }

    const locationBucket = this.requireLocationBucket()
    const authority = this.getAuthorityStub(key, locationBucket)
    return unwrapRpcResult(authority.telefuncBroadcastPublish({ key, locationBucket, binaryData: data }))
  }

  /**
   * Fans out one keyed publish from either a key authority or a bucket coordinator.
   *
   * Non-forwarded: reads KV presence, sequences through authority, forwards to bucket coordinators.
   * Forwarded: delivers to listed DO names without reading KV again.
   */
  async publishToSubscribers(
    authorityState: CloudflareBroadcastAuthorityState,
    request: BroadcastPublishRequest,
  ): Promise<BroadcastPublishResult> {
    const { key, locationBucket, serialized, binaryData, forwarded = false } = request

    if (forwarded) {
      assert(request.info, 'Forwarded publish must include info')
      const info = request.info
      const doNames = request.doNames ?? []
      await Promise.all(
        doNames.map((doName) =>
          authorityState.broadcastCalls.call(
            doName,
            () => this.getBoundStub(doName),
            (stub) => stub.telefuncBroadcastDeliver({ key, doName, serialized, binaryData, info }),
          ),
        ),
      )
      return { seq: info.seq, timestamp: info.timestamp }
    }

    const [authorityBucket, seq, presenceByBucket] = await authorityState.runInAuthorityChain(() =>
      Promise.all([
        authorityState.getOrInitAuthorityBucket(key, locationBucket),
        authorityState.getNextKeySeq(key),
        this.listPresenceByBucket(key),
      ]),
    )

    const timestamp = Date.now()
    const info = { seq, timestamp }
    const activeBuckets = Array.from(presenceByBucket.keys())
    await Promise.all(
      activeBuckets.map((activeBucket) => {
        const coordinator = this.getBucketCoordinatorName(key, activeBucket)
        return authorityState.broadcastCalls.call(
          coordinator,
          () => this.getBoundStub(coordinator, activeBucket),
          (stub) =>
            stub.telefuncBroadcastPublish({
              key,
              serialized,
              binaryData,
              forwarded: true,
              locationBucket: activeBucket,
              doNames: presenceByBucket.get(activeBucket)!,
              info,
            }),
        )
      }),
    )
    return { seq, timestamp, meta: { authorityBucket, fanoutBuckets: activeBuckets } }
  }

  /**
   * Delivers a publish to the subscribers here of the Durable Object `request.doName`, which the RPC went to.
   */
  deliverToLocal({ key, doName, serialized, binaryData, info }: BroadcastDeliverRequest): void {
    if (serialized !== undefined) deliverToOwned(this.textSubs.get(key), doName, serialized, info)
    if (binaryData !== undefined) deliverToOwned(this.binarySubs.get(key), doName, binaryData, info)
  }

  // --- Private ---

  private ensurePresence(key: string, locationBucket: LocationBucket, owner: string): void {
    let memberState = this.memberStates.get(key)
    if (!memberState) {
      memberState = new MemberBucketState(key, locationBucket, () => this.getAuthorityStub(key, locationBucket))
      this.memberStates.set(key, memberState)
    }
    const count = memberState.owners.get(owner) ?? 0
    memberState.owners.set(owner, count + 1)
    if (count === 0) void this.initializePresence(key, memberState, owner)
  }

  private releasePresence(key: string, owner: string): void {
    const memberState = this.memberStates.get(key)
    // Its subscriber was still there, so the key's state is too.
    assert(memberState)
    const count = memberState.owners.get(owner)! - 1
    if (count > 0) memberState.owners.set(owner, count)
    else {
      memberState.owners.delete(owner)
      memberState.unwritten.delete(owner)
      void this.deletePresence(key, owner)
    }
    this.teardownPresenceIfEmpty(key)
  }

  private teardownPresenceIfEmpty(key: string): void {
    if (this.hasLocalCallbacks(key)) return
    const memberState = this.memberStates.get(key)
    if (!memberState) return

    // The setup in flight tears it down once done, if no subscriber came meanwhile.
    if (memberState.setupsInFlight > 0 || memberState.pendingPublishes.flushing) return

    memberState.stopRefresh()
    this.memberStates.delete(key)
  }

  private async initializePresence(key: string, memberState: MemberBucketState, owner: string): Promise<void> {
    memberState.setupsInFlight++
    const written = this.writePresence(key, owner)
    const unwritten = new Outcome()
    void unwritten.follow(written)
    memberState.unwritten.set(owner, unwritten)
    const wrote = await this.warnRefusedPresence(key, written)
    memberState.setupsInFlight--
    // A refused write stays its owner's outcome until the refresh below writes it.
    if (wrote && memberState.unwritten.get(owner) === unwritten) memberState.unwritten.delete(owner)
    if (memberState.setupsInFlight > 0) return

    memberState.refreshTimer ??= setInterval(() => {
      for (const current of memberState.owners.keys()) void this.refreshPresence(key, memberState, current)
    }, PRESENCE_REFRESH_INTERVAL_MS)

    await memberState.pendingPublishes.flush()

    this.teardownPresenceIfEmpty(key)
  }

  private getBucketCoordinatorName(key: string, locationBucket: LocationBucket): string {
    const bucketShardCount = getBucketCoordinatorScale(this.scale, locationBucket)
    const bucketShardOrdinal = getDeterministicKeyBucketIndex(key, bucketShardCount)
    return `${this.baseInstanceName}:broadcast:${locationBucket}:${bucketShardOrdinal}`
  }

  private getAuthorityStub(key: string, locationHint?: DurableObjectLocationHint): TelefuncBroadcastStub {
    let stub = this.authorityStubsOfThisRun.get(key)
    if (!stub) {
      if (this.authorityStubsOfThisRun.size === 0) queueMicrotask(() => this.authorityStubsOfThisRun.clear())
      stub = this.getBoundStub(`${this.baseInstanceName}:broadcast:authority:${key}`, locationHint)
      this.authorityStubsOfThisRun.set(key, stub)
    }
    return stub
  }

  private getBoundStub(instanceName: string, locationHint?: DurableObjectLocationHint): TelefuncBroadcastStub {
    assert(this.binding, `Missing Cloudflare Durable Object binding "${this.bindingName ?? 'unknown'}".`)
    return this.binding.get(
      this.binding.idFromName(instanceName),
      locationHint ? { locationHint } : undefined,
    ) as TelefuncBroadcastStub
  }
}

/** Calls each of `subs` the Durable Object `doName` owns with `payload`. */
function deliverToOwned<T>(
  subs: Map<(payload: T, info: WirePublishInfo, delivery?: BroadcastDelivery) => void, string> | undefined,
  doName: string,
  payload: T,
  info: WirePublishInfo,
): void {
  if (subs === undefined) return
  const delivery: BroadcastDelivery = { body: undefined }
  for (const [cb, owner] of subs) if (owner === doName) cb(payload, info, delivery)
}
