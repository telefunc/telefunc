/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcastTransport, CloudflareBroadcastAuthorityState, CloudflareBroadcastMember }
export type {
  BroadcastCalls,
  BroadcastDeliverRequest,
  BroadcastForwardRequest,
  BroadcastPresenceRequest,
  BroadcastPublishRequest,
  TelefuncDurableObjectStub,
}

import { KNOWN_BROADCAST_BUCKETS, getBucketCoordinatorShardIndices, getDeterministicKeyBucketIndex } from './routing.js'
import type { OrderedStubs } from './ordered-stubs.js'
import { assert } from '../../../../utils/assert.js'
import type { BroadcastLane, PublishResult } from '../../../backend/broadcast/contract.js'
import { broadcastRouteKey } from '../../../backend/broadcast/route-key.js'
import type { BackendReceiver } from '../../../backend/subscription.js'
import { DriverAttempt } from '../../../backend/attempt.js'
import { createDeferred } from '../../../../utils/createDeferred.js'
import type { OrderingInfo } from '../../../ordering-frame.js'
import type { CloudflareScale, LocationBucket } from './routing.js'
import { currentCloudflareSession } from './session.js'

const PRESENCE_TTL_MS = 90_000
const PRESENCE_REFRESH_INTERVAL_MS = 30_000

/** Unwrap Cloudflare DO RPC proxy into a plain object.
 *  RPC properties are lazy stubs that must be awaited to resolve their values. */
async function unwrapRpcResult(rpc: Promise<PublishResult>): Promise<PublishResult> {
  const r = await rpc
  const [seq, timestamp, meta, receivers] = await Promise.all([r.seq, r.timestamp, r.meta, r.receivers])
  return {
    seq,
    timestamp,
    ...(meta ? { meta } : undefined),
    ...(receivers === undefined ? undefined : { receivers }),
  }
}

/** `locationBucket` is the publishing session's; a publish from outside a session, as from a cron trigger, has none. */
type BroadcastPublishRequest = {
  key: string
  kind: BroadcastLane['kind']
  locationBucket: LocationBucket | null
  payload: Uint8Array
}

/** The authority's sequenced publish, handed to one bucket coordinator for the member DOs it names by id. */
type BroadcastForwardRequest = {
  key: string
  kind: BroadcastLane['kind']
  payload: Uint8Array
  info: OrderingInfo
  members: string[]
}

/** A member DO's presence on a lane, at the key's authority, by DO id; `bucket: null` withdraws it. */
type BroadcastPresenceRequest = {
  key: string
  kind: BroadcastLane['kind']
  member: string
  bucket: LocationBucket | null
}

type BroadcastDeliverRequest = {
  key: string
  kind: BroadcastLane['kind']
  payload: Uint8Array
  info: OrderingInfo
}

type TelefuncDurableObjectStub = DurableObjectStub & {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<PublishResult>
  telefuncBroadcastForward(request: BroadcastForwardRequest): Promise<void>
  telefuncBroadcastDeliver(request: BroadcastDeliverRequest): Promise<void>
  telefuncBroadcastPresence(request: BroadcastPresenceRequest): Promise<void>
}

/** One DO's outgoing Broadcast calls. */
type BroadcastCalls = OrderedStubs<TelefuncDurableObjectStub>

/** One lane's presence at the key's authority, for one member DO. */
class MemberLane {
  state: 'establishing' | 'ready' | 'lost' = 'establishing'
  teardownRequested = false
  refreshTimer: ReturnType<typeof setInterval> | null = null
  readonly lane: BroadcastLane
  readonly #setup = createDeferred()
  readonly #presenceListeners = new Set<(state: 'ready' | 'lost') => void>()

  constructor(lane: BroadcastLane) {
    this.lane = lane
    void this.#setup.promise.catch(() => {})
  }

  /** Settles once the authority first holds this presence. */
  get ready(): Promise<void> {
    return this.#setup.promise
  }

  acknowledgePresence(): void {
    const recovered = this.state === 'lost'
    this.state = 'ready'
    this.#setup.resolve()
    if (recovered) this.#notifyPresenceState('ready')
  }

  rejectPresence(error: unknown): void {
    this.#setup.reject(error)
  }

  losePresence(): void {
    if (this.state !== 'ready') return
    this.state = 'lost'
    this.#notifyPresenceState('lost')
  }

  onPresenceStateChange(cb: (state: 'ready' | 'lost') => void): () => void {
    this.#presenceListeners.add(cb)
    return () => this.#presenceListeners.delete(cb)
  }

  stopRefresh(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer)
      this.refreshTimer = null
    }
  }

  #notifyPresenceState(state: 'ready' | 'lost'): void {
    for (const listener of [...this.#presenceListeners]) listener(state)
  }
}

/** Follows its lane's presence: ready once the authority holds it, lost while a refresh fails. */
class CloudflareBroadcastSubscriptionAttempt extends DriverAttempt {
  readonly #receiver: BackendReceiver
  readonly #detach: () => Promise<void>
  readonly #stopPresenceObservation: () => void
  #unsubscribed = false

  constructor(member: MemberLane, receiver: BackendReceiver, detach: () => Promise<void>) {
    super()
    this.#receiver = receiver
    this.#detach = detach
    this.#stopPresenceObservation = member.onPresenceStateChange((state) => this.transition(state))
    member.ready.then(
      () => this.transition('ready'),
      (error: unknown) => {
        this.#stopPresenceObservation()
        this.transition('closed', error)
      },
    )
  }

  async deliver(payload: Uint8Array, info: OrderingInfo): Promise<void> {
    if (this.state() !== 'ready') return
    await this.#receiver(payload, info)
  }

  async unsubscribe(): Promise<void> {
    if (this.#unsubscribed) return
    this.#unsubscribed = true
    this.#stopPresenceObservation()
    this.transition('closed')
    await this.#detach()
  }
}

const AUTHORITY_SCHEMA = `
  CREATE TABLE IF NOT EXISTS broadcast_key (key TEXT PRIMARY KEY, seq INTEGER NOT NULL, authority_bucket TEXT);
  CREATE TABLE IF NOT EXISTS broadcast_presence
    (route_key TEXT NOT NULL, member TEXT NOT NULL, bucket TEXT NOT NULL, expires_at INTEGER NOT NULL, PRIMARY KEY (route_key, member));
`

/** A key authority DO's Broadcast state, in its SQLite storage: each key's `seq` and first-touch bucket, and each
 *  lane's member presence. The tables are created on first use, so a DO in another role never has them. */
class CloudflareBroadcastAuthorityState {
  readonly #storage: DurableObjectStorage
  #schema = false

  constructor(state: DurableObjectState) {
    this.#storage = state.storage
  }

  /** The key's next `seq`; the key's first publish from a session fixes its authority bucket. */
  sequence(
    key: string,
    preferredBucket: LocationBucket | null,
  ): { seq: number; authorityBucket: LocationBucket | null } {
    const sql = this.#sql()
    return this.#storage.transactionSync(() => {
      const row = sql
        .exec<{ seq: number; authority_bucket: LocationBucket | null }>(
          'SELECT seq, authority_bucket FROM broadcast_key WHERE key = ?',
          key,
        )
        .toArray()[0]
      const current = row?.seq ?? 0
      assert(current < Number.MAX_SAFE_INTEGER, 'Cloudflare Broadcast sequence exhausted for the ordering domain')
      const authorityBucket = row?.authority_bucket ?? preferredBucket
      sql.exec(
        'INSERT OR REPLACE INTO broadcast_key (key, seq, authority_bucket) VALUES (?, ?, ?)',
        key,
        current + 1,
        authorityBucket,
      )
      return { seq: current + 1, authorityBucket }
    })
  }

  /** Records or withdraws a member DO, dropping the lane's lapsed entries; the RPC reply waits for the write. */
  setPresence({ key, kind, member, bucket }: BroadcastPresenceRequest): void {
    const sql = this.#sql()
    const routeKey = broadcastRouteKey({ key, kind })
    const now = Date.now()
    this.#storage.transactionSync(() => {
      sql.exec(
        'DELETE FROM broadcast_presence WHERE route_key = ? AND (member = ? OR expires_at <= ?)',
        routeKey,
        member,
        now,
      )
      if (bucket === null) return
      sql.exec(
        'INSERT INTO broadcast_presence (route_key, member, bucket, expires_at) VALUES (?, ?, ?, ?)',
        routeKey,
        member,
        bucket,
        now + PRESENCE_TTL_MS,
      )
    })
  }

  /** The lane's unexpired member DOs by bucket. */
  livePresence(routeKey: string, now: number): Map<LocationBucket, string[]> {
    const byBucket = new Map<LocationBucket, string[]>()
    const rows = this.#sql()
      .exec<{ member: string; bucket: LocationBucket }>(
        'SELECT member, bucket FROM broadcast_presence WHERE route_key = ? AND expires_at > ?',
        routeKey,
        now,
      )
      .toArray()
    for (const { member, bucket } of rows) {
      const doNames = byBucket.get(bucket)
      if (doNames === undefined) byBucket.set(bucket, [member])
      else doNames.push(member)
    }
    return byBucket
  }

  #sql(): SqlStorage {
    if (!this.#schema) {
      this.#storage.sql.exec(AUTHORITY_SCHEMA)
      this.#schema = true
    }
    return this.#storage.sql
  }
}

/** A session DO's Broadcast membership: its lanes' presence at their key authorities, its subscriptions, and the
 *  deliveries to them. It is addressed by the DO's id and sends through the DO's own ordered stubs. */
class CloudflareBroadcastMember {
  /** Subscriptions share an attempt only within one session DO. */
  readonly partition = crypto.randomUUID()
  readonly calls: BroadcastCalls
  readonly #transport: CloudflareBroadcastTransport
  readonly #id: string
  #bucket: LocationBucket | null = null
  readonly #lanes = new Map<string, MemberLane>()
  readonly #subscriptions = new Map<string, CloudflareBroadcastSubscriptionAttempt>()
  readonly #presenceWrites = new Map<string, Promise<void>>()

  constructor(transport: CloudflareBroadcastTransport, id: string, calls: BroadcastCalls) {
    this.#transport = transport
    this.#id = id
    this.calls = calls
  }

  get bucket(): LocationBucket | null {
    return this.#bucket
  }

  /** The bucket the session was placed in, from the request that reached it. */
  locate(bucket: LocationBucket): void {
    assert(KNOWN_BROADCAST_BUCKETS.has(bucket), `Invalid Broadcast location bucket "${bucket}".`)
    this.#bucket = bucket
  }

  openSubscription(lane: BroadcastLane, receiver: BackendReceiver): CloudflareBroadcastSubscriptionAttempt {
    const routeKey = broadcastRouteKey(lane)
    const memberLane = this.#ensureLane(lane, routeKey)
    const attempt: CloudflareBroadcastSubscriptionAttempt = new CloudflareBroadcastSubscriptionAttempt(
      memberLane,
      receiver,
      async () => {
        if (this.#subscriptions.get(routeKey) === attempt) this.#subscriptions.delete(routeKey)
        await this.#teardownIfEmpty(routeKey)
      },
    )
    this.#subscriptions.set(routeKey, attempt)
    return attempt
  }

  /** A coordinator's delivery to this DO's subscription. */
  async deliver(request: BroadcastDeliverRequest): Promise<void> {
    await this.#subscriptions.get(broadcastRouteKey(request))?.deliver(request.payload, request.info)
  }

  #ensureLane(lane: BroadcastLane, routeKey: string): MemberLane {
    const existing = this.#lanes.get(routeKey)
    if (existing !== undefined) {
      existing.teardownRequested = false
      return existing
    }
    const memberLane = new MemberLane(lane)
    this.#lanes.set(routeKey, memberLane)
    // Nobody awaits a deferred teardown; a failed withdrawal lapses with the presence TTL.
    void this.#initializeLane(routeKey, memberLane).catch(() => {})
    return memberLane
  }

  async #initializeLane(routeKey: string, memberLane: MemberLane): Promise<void> {
    try {
      await this.#writePresence(memberLane.lane, true)
    } catch (error) {
      memberLane.rejectPresence(error)
      if (this.#lanes.get(routeKey) === memberLane) this.#lanes.delete(routeKey)
      return
    }
    memberLane.acknowledgePresence()
    if (memberLane.teardownRequested) return this.#release(routeKey, memberLane)
    memberLane.refreshTimer = setInterval(() => {
      void this.#writePresence(memberLane.lane, true).then(
        () => memberLane.acknowledgePresence(),
        () => memberLane.losePresence(),
      )
    }, PRESENCE_REFRESH_INTERVAL_MS)
  }

  async #teardownIfEmpty(routeKey: string): Promise<void> {
    const memberLane = this.#lanes.get(routeKey)
    if (memberLane === undefined || this.#subscriptions.has(routeKey)) return
    if (memberLane.state === 'establishing') {
      memberLane.teardownRequested = true
      return
    }
    await this.#release(routeKey, memberLane)
  }

  async #release(routeKey: string, memberLane: MemberLane): Promise<void> {
    memberLane.stopRefresh()
    if (this.#lanes.get(routeKey) === memberLane) this.#lanes.delete(routeKey)
    await this.#writePresence(memberLane.lane, false)
  }

  /** One lane's presence writes go to the authority one at a time. */
  async #writePresence(lane: BroadcastLane, present: boolean): Promise<void> {
    const routeKey = broadcastRouteKey(lane)
    assert(this.#bucket, 'A Broadcast member registers from a session that knows its bucket')
    const request = { key: lane.key, kind: lane.kind, member: this.#id, bucket: present ? this.#bucket : null }
    const current = (this.#presenceWrites.get(routeKey) ?? Promise.resolve())
      .catch(() => {})
      .then(() => this.#transport.sendPresence(this.calls, request))
    this.#presenceWrites.set(routeKey, current)
    try {
      await current
    } finally {
      if (this.#presenceWrites.get(routeKey) === current) this.#presenceWrites.delete(routeKey)
    }
  }
}

/** The isolate's Cloudflare Broadcast driver: where each key's authority and each bucket's coordinators live, and
 *  their RPC handlers. Each session DO subscribes through its own `CloudflareBroadcastMember`. */
class CloudflareBroadcastTransport {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private bindingName: string | null = null
  private binding: DurableObjectNamespace | null = null

  constructor({ baseInstanceName, scale }: { baseInstanceName: string; scale?: CloudflareScale }) {
    this.baseInstanceName = baseInstanceName
    this.scale = scale
  }

  attachBinding(binding: DurableObjectNamespace, bindingName: string): void {
    this.binding = binding
    this.bindingName = bindingName
  }

  member(id: string, calls: BroadcastCalls): CloudflareBroadcastMember {
    return new CloudflareBroadcastMember(this, id, calls)
  }

  /** From a session DO, through its ordered stubs; from elsewhere, as a cron trigger, through a fresh stub. */
  publish(lane: BroadcastLane, payload: Uint8Array): Promise<PublishResult> {
    const member = currentCloudflareSession()?.broadcast()
    const locationBucket = member?.bucket ?? null
    const request = { key: lane.key, kind: lane.kind, locationBucket, payload }
    const send = (authority: TelefuncDurableObjectStub) => authority.telefuncBroadcastPublish(request)
    const name = this.authorityName(lane.key)
    return unwrapRpcResult(
      member === undefined
        ? send(this.stubByName(name, locationBucket))
        : this.callByName(member.calls, name, locationBucket, send),
    )
  }

  sendPresence(calls: BroadcastCalls, request: BroadcastPresenceRequest): Promise<void> {
    return this.callByName(calls, this.authorityName(request.key), request.bucket, (authority) =>
      authority.telefuncBroadcastPresence(request),
    )
  }

  /** At the key's authority: sequences the publish, reads its presence and forwards once per populated bucket, all in
   *  one synchronous turn, so forwards leave through this DO's ordered stubs in `seq` order. */
  async publishToSubscribers(
    authorityState: CloudflareBroadcastAuthorityState,
    calls: BroadcastCalls,
    request: BroadcastPublishRequest,
  ): Promise<PublishResult> {
    const { key, kind, locationBucket, payload } = request
    const { seq, authorityBucket } = authorityState.sequence(key, locationBucket)
    const info = { seq, timestamp: Date.now() }
    const presenceByBucket = authorityState.livePresence(broadcastRouteKey({ key, kind }), info.timestamp)
    const fanoutBuckets = Array.from(presenceByBucket.keys())
    let receivers = 0
    for (const members of presenceByBucket.values()) receivers += members.length
    await Promise.all(
      fanoutBuckets.map((bucket) =>
        this.callByName(calls, this.coordinatorName(key, bucket), bucket, (coordinator) =>
          coordinator.telefuncBroadcastForward({ key, kind, payload, info, members: presenceByBucket.get(bucket)! }),
        ),
      ),
    )
    return { ...info, receivers, meta: { authorityBucket, fanoutBuckets } }
  }

  /** At a bucket coordinator: delivers the authority's sequenced publish to the named member DOs, in arrival order. */
  async forwardToBucket(calls: BroadcastCalls, request: BroadcastForwardRequest): Promise<void> {
    const { members, ...delivery } = request
    await Promise.all(
      members.map((member) =>
        calls.call(
          member,
          () => this.stubById(member),
          (stub) => stub.telefuncBroadcastDeliver(delivery),
        ),
      ),
    )
  }

  /** A call from the DO owning `calls`, through its stub for the named instance. */
  private callByName<T>(
    calls: BroadcastCalls,
    name: string,
    locationHint: LocationBucket | null,
    invoke: (stub: TelefuncDurableObjectStub) => Promise<T>,
  ): Promise<T> {
    return calls.call(name, () => this.stubByName(name, locationHint), invoke)
  }

  private authorityName(key: string): string {
    return `${this.baseInstanceName}:broadcast:authority:${key}`
  }

  private coordinatorName(key: string, locationBucket: LocationBucket): string {
    const bucketShardCount = getBucketCoordinatorShardIndices(this.scale, locationBucket).length
    const bucketShardOrdinal = getDeterministicKeyBucketIndex(key, bucketShardCount)
    return `${this.baseInstanceName}:broadcast:${locationBucket}:${bucketShardOrdinal}`
  }

  private stubByName(name: string, locationHint: LocationBucket | null): TelefuncDurableObjectStub {
    const binding = this.requireBinding()
    return binding.get(
      binding.idFromName(name),
      locationHint === null ? undefined : { locationHint },
    ) as TelefuncDurableObjectStub
  }

  private stubById(id: string): TelefuncDurableObjectStub {
    const binding = this.requireBinding()
    return binding.get(binding.idFromString(id)) as TelefuncDurableObjectStub
  }

  private requireBinding(): DurableObjectNamespace {
    assert(this.binding, `Missing Cloudflare Durable Object binding "${this.bindingName ?? 'unknown'}".`)
    return this.binding
  }
}
