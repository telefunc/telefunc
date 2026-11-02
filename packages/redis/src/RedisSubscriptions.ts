export { RedisSubscriptions }
export type { RedisDriver, RedisSubscriptionSource }

import { randomUUID } from 'node:crypto'
import type { BackendReceiver, BroadcastRoute, OrderingInfo } from 'telefunc/__internal'
import type { RoomSubscriptionSource } from '@telefunc/room/backend'
import { DriverAttempt, decodeOrderingFrame } from 'telefunc/__internal'
import { createDeferred, type Deferred } from './createDeferred.js'
import type { CommitChannels, FenceTokens } from './keys.js'
import { REDIS_DELIVERY_FENCE_BYTE, REDIS_DELIVERY_FENCE_TOKEN_LENGTH } from './commands.js'
import type { SubscriberSocket } from './ioredis.js'
import type { RedisConnection } from './RedisConnection.js'

type RedisSubscriptionSource = BroadcastRoute | RoomSubscriptionSource

type RedisSubscriptionsOptions = { connection: RedisConnection }

/** The driver a subscription's source belongs to. */
interface RedisDriver {
  /** Each of the source's lanes' last commit, by the lane key its frames carry on a fan-in channel and `''` on
   *  another; null once a Room source's incarnation is not the open head. */
  readStarts(source: RedisSubscriptionSource): Promise<Map<string, OrderingInfo> | null>
}

/** Where a subscription's frames arrive, which driver it belongs to, and whom it delivers to. */
type RedisAttemptOptions = {
  source: RedisSubscriptionSource
  channel: string
  /** A Room lane's incarnation channel: a message on it means the incarnation was dropped. */
  invalidationChannel: string | null
  driver: RedisDriver
  receiver: BackendReceiver
}

/** The subscriber connection; `id` tells work that awaited across a drop that it is stale. */
type Connection =
  | { phase: 'idle' }
  | { phase: 'waiting'; timer: ReturnType<typeof setTimeout> }
  | { phase: 'connecting'; id: number; socket: SubscriberSocket | null }
  | { phase: 'connected'; id: number; socket: SubscriberSocket; subscribed: Set<string> }

type Connected = Extract<Connection, { phase: 'connected' }>

/** A lane's last frame delivered, and the frames parked past a missing one, by their predecessor's seq,
 *  until it arrives or the wait ends. */
type LaneOrder = {
  last: number
  lastTimestamp: number
  parked: Map<number, ParkedFrame> | null
  release: ReturnType<typeof setTimeout> | null
}

type ParkedFrame = { payload: Uint8Array; info: OrderingInfo; token: string | null }

const textDecoder = new TextDecoder()

const RECONNECT_DELAY_MIN_MS = 50
const RECONNECT_DELAY_MAX_MS = 2_000
// Redis Cluster forwards a channel's frames from the old and the new slot owner over separate bus links during a
// reshard or a failover, so the old owner's last frame can arrive after the new owner's first.
const REORDER_WAIT_MS = 1_000

/**
 * Redis's only backend-specific subscription edge: one subscriber connection, its channels refcounted across
 * subscriptions, which Broadcast's and Room's drivers subscribe their sources on. When it drops, every ready
 * subscription is lost; a fresh connection subscribes them again and tells them ready (a Room lane only while its
 * incarnation is still the open head).
 */
class RedisSubscriptions {
  private readonly connection: RedisConnection
  /** Every channel some subscription needs, with the subscriptions that listen on it. */
  private readonly byChannel = new Map<string, Set<RedisAttempt>>()
  /** The channels some subscription started or stopped needing since the last sync read them. */
  private changedChannels = new Set<string>()
  /** The subscriptions that may await confirmation; a sync confirms those its connection covers. */
  private readonly unconfirmed = new Set<RedisAttempt>()
  private socket: Connection = { phase: 'idle' }
  private nextId = 0
  private syncing: Promise<void> = Promise.resolve()
  private syncQueued = false
  private reconnectDelay = RECONNECT_DELAY_MIN_MS
  /** An establishing subscription reports no failure, so the first one of an outage is reported here. */
  private outageReported = false

  constructor({ connection }: RedisSubscriptionsOptions) {
    this.connection = connection
  }

  subscribe(options: RedisAttemptOptions): DriverAttempt {
    const subscription = new RedisAttempt({ ...options, owner: this })
    for (const channel of subscription.channels) {
      const subscriptions = this.byChannel.get(channel)
      if (subscriptions !== undefined) {
        subscriptions.add(subscription)
      } else {
        this.byChannel.set(channel, new Set([subscription]))
        this.changedChannels.add(channel)
      }
    }
    this.unconfirmed.add(subscription)
    if (this.socket.phase === 'connected') this.syncChannels()
    else if (this.socket.phase === 'idle') void this.connect()
    return subscription
  }

  /** Arms a delivery fence on the ready subscriptions of each channel the commit is published on: `delivery` settles
   *  once each saw the commit. */
  prepareFence(channels: CommitChannels): {
    tokens: FenceTokens
    delivery: Promise<void>
    cancel(): void
  } {
    const token = randomUUID()
    const arm = (channel: string | null) => {
      const subscriptions = channel === null ? [] : [...(this.byChannel.get(channel) ?? [])]
      return { subscriptions, armed: subscriptions.flatMap((subscription) => subscription.prepareFence(token) ?? []) }
    }
    const lane = arm(channels.lane)
    const everyTrack = arm(channels.everyTrack)
    const everyMember = arm(channels.everyMember)
    const all = [lane, everyTrack, everyMember]
    const delivery = Promise.all(all.flatMap((channel) => channel.armed)).then(() => {})
    // The subscriber connection can drop before the commit awaiting this delivery returns.
    void delivery.catch(() => {})
    const tokenOf = (channel: { armed: unknown[] }) => (channel.armed.length === 0 ? '' : token)
    return {
      tokens: { lane: tokenOf(lane), everyTrack: tokenOf(everyTrack), everyMember: tokenOf(everyMember) },
      delivery,
      cancel: () => {
        for (const channel of all) for (const subscription of channel.subscriptions) subscription.settleFence(token)
      },
    }
  }

  /** Forgets a subscription that unsubscribed or ended. */
  detach(subscription: RedisAttempt): void {
    this.unconfirmed.delete(subscription)
    for (const channel of subscription.channels) {
      const subscriptions = this.byChannel.get(channel)
      subscriptions?.delete(subscription)
      if (subscriptions?.size !== 0) continue
      this.byChannel.delete(channel)
      this.changedChannels.add(channel)
    }
    if (this.byChannel.size > 0) return this.syncChannels()
    // Nothing left to deliver to: release the connection until the next subscription, which starts afresh.
    this.disconnect()
    this.outageReported = false
    this.reconnectDelay = RECONNECT_DELAY_MIN_MS
  }

  private async connect(): Promise<void> {
    const id = ++this.nextId
    const connecting: Extract<Connection, { phase: 'connecting' }> = { phase: 'connecting', id, socket: null }
    this.socket = connecting
    let socket: SubscriberSocket
    try {
      socket = await this.connection.openSubscriberSocket()
    } catch (error) {
      return this.lost(id, error)
    }
    if (!this.isCurrent(id)) return socket.disconnect()
    connecting.socket = socket
    socket.on('messageBuffer', (channel: Buffer, frame: Buffer) => {
      if (this.isCurrent(id)) this.dispatch(channel.toString(), frame)
    })
    let lastError: unknown = new Error('Redis subscriber connection closed')
    socket.on('error', (error: unknown) => {
      lastError = error
    })
    socket.on('close', () => this.lost(id, lastError))
    try {
      await socket.connect()
    } catch (error) {
      return this.lost(id, error)
    }
    if (!this.isCurrent(id)) return
    this.socket = { phase: 'connected', id, socket, subscribed: new Set() }
    this.changedChannels = new Set(this.byChannel.keys())
    this.syncChannels()
  }

  private lost(id: number, error: unknown): void {
    if (!this.isCurrent(id)) return
    this.disconnect()
    for (const subscription of this.subscriptions()) {
      subscription.lose(error)
      this.unconfirmed.add(subscription)
    }
    // A listener may have released the last subscription, or opened one that is already connecting.
    if (this.byChannel.size === 0 || this.socket.phase !== 'idle') return
    if (!this.outageReported) {
      this.outageReported = true
      console.error(error)
    }
    const timer = setTimeout(() => void this.connect(), this.reconnectDelay)
    timer.unref()
    this.socket = { phase: 'waiting', timer }
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, RECONNECT_DELAY_MAX_MS)
  }

  private disconnect(): void {
    const connection = this.socket
    this.socket = { phase: 'idle' }
    if (connection.phase === 'waiting') clearTimeout(connection.timer)
    else if (connection.phase !== 'idle') connection.socket?.disconnect()
  }

  private isCurrent(id: number): boolean {
    const connection = this.socket
    return (connection.phase === 'connecting' || connection.phase === 'connected') && connection.id === id
  }

  /** Serialized: brings the connection's channel set to the subscriptions' and confirms what it covers. A sync reads
   *  the changed channels when it starts, so one that is queued already covers any later request. */
  private syncChannels(): void {
    if (this.syncQueued) return
    this.syncQueued = true
    this.syncing = this.syncing.then(async () => {
      this.syncQueued = false
      const connection = this.socket
      if (connection.phase !== 'connected') return
      try {
        await this.syncChannelsOnce(connection)
      } catch (error) {
        // A failed (UN)SUBSCRIBE leaves the channel set unknown: start over on a fresh connection.
        return this.lost(connection.id, error)
      }
      // Recovered once the channels are subscribed, not when the socket connects:
      // a refused SUBSCRIBE keeps backing off.
      this.reconnectDelay = RECONNECT_DELAY_MIN_MS
      this.outageReported = false
    })
  }

  private async syncChannelsOnce({ id, socket, subscribed }: Connected): Promise<void> {
    const changed = this.changedChannels
    this.changedChannels = new Set()
    const stale: string[] = []
    const missing: string[] = []
    for (const channel of changed) {
      const wanted = this.byChannel.has(channel)
      if (wanted && !subscribed.has(channel)) missing.push(channel)
      else if (!wanted && subscribed.has(channel)) stale.push(channel)
    }
    if (stale.length > 0) {
      for (const channel of stale) subscribed.delete(channel)
      await socket.unsubscribe(...stale)
    }
    if (missing.length > 0) {
      await socket.subscribe(...missing)
      if (!this.isCurrent(id)) return
      for (const channel of missing) subscribed.add(channel)
    }
    // A subscription made during the SUBSCRIBE above waits for the sync it queued.
    const covered: RedisAttempt[] = []
    for (const subscription of this.unconfirmed) {
      if (!subscription.awaitsConfirmation()) {
        this.unconfirmed.delete(subscription)
      } else if (subscription.channels.every((channel) => subscribed.has(channel))) {
        this.unconfirmed.delete(subscription)
        covered.push(subscription)
      }
    }
    await Promise.all(covered.map((subscription) => this.confirm(subscription, id)))
  }

  /** Its channels are subscribed on a live connection: ready from where each lane stands now, unless its incarnation is
   *  no longer open. */
  private async confirm(subscription: RedisAttempt, id: number): Promise<void> {
    let starts: Map<string, OrderingInfo> | null
    try {
      starts = await subscription.driver.readStarts(subscription.source)
    } catch (error) {
      // A failed read (a command-connection error) fails this subscription, not the shared subscriber connection.
      if (this.isCurrent(id)) subscription.fail(error)
      return
    }
    if (!this.isCurrent(id)) return
    if (starts !== null) return subscription.markReady(starts)
    // Closed on purpose since the caller read it open; gone after a loss, it may have gone with Redis's data.
    if (subscription.subscribing) return subscription.end()
    subscription.fail(new Error(`subscribeLane: the incarnation of '${subscription.channel}' is not open`))
  }

  private subscriptions(): Set<RedisAttempt> {
    const all = new Set<RedisAttempt>()
    for (const subscriptions of this.byChannel.values()) for (const subscription of subscriptions) all.add(subscription)
    return all
  }

  private dispatch(channel: string, frame: Buffer): void {
    for (const subscription of [...(this.byChannel.get(channel) ?? [])]) subscription.receive(channel, frame)
  }
}

/** One source on the shared connection: delivers its lanes in seq order to core's `receiver`. */
class RedisAttempt extends DriverAttempt {
  readonly source: RedisSubscriptionSource
  readonly channel: string
  readonly invalidationChannel: string | null
  readonly driver: RedisDriver
  readonly channels: readonly string[]
  private readonly owner: RedisSubscriptions
  private readonly receiver: BackendReceiver
  private readonly fences = new Map<string, Deferred<void>>()
  /** Each lane's order: an every-track or every-member channel carries many binary lanes, keyed by lane key; any other
   *  channel one lane, keyed `''`. */
  private orders = new Map<string, LaneOrder>()
  /** The frames that arrived while it awaited confirmation, ordered once it knows where each lane starts. */
  private early: Buffer[] = []
  private readonly fanIn: boolean
  private readonly broadcast: boolean
  /** A Broadcast text source's deliveries are strings. */
  private readonly text: boolean

  constructor({
    owner,
    source,
    channel,
    invalidationChannel,
    driver,
    receiver,
  }: RedisAttemptOptions & { owner: RedisSubscriptions }) {
    super()
    this.owner = owner
    this.source = source
    this.channel = channel
    this.invalidationChannel = invalidationChannel
    this.driver = driver
    this.receiver = receiver
    this.channels = invalidationChannel === null ? [channel] : [channel, invalidationChannel]
    if ('roomId' in source) {
      this.fanIn = source.lane.kind === 'binary-all' || source.lane.kind === 'binary-track'
      this.broadcast = false
      this.text = false
    } else {
      this.fanIn = false
      this.broadcast = true
      this.text = source.kind === 'text'
    }
  }

  /** It has never been ready, and hasn't ended. */
  get subscribing(): boolean {
    return this.state() === 'establishing'
  }

  async unsubscribe(): Promise<void> {
    if (this.state() === 'closed') return
    this.transition('closed')
    this.dropParked()
    for (const token of this.fences.keys()) this.settleFence(token)
    this.owner.detach(this)
  }

  prepareFence(token: string): Promise<void> | null {
    if (this.state() !== 'ready') return null
    const fence = createDeferred()
    this.fences.set(token, fence)
    return fence.promise
  }

  /** Resolves the fence: its token arrived, the commit that armed it was refused, or no receiver is left. */
  settleFence(token: string): void {
    this.fences.get(token)?.resolve()
    this.fences.delete(token)
  }

  awaitsConfirmation(): boolean {
    const state = this.state()
    return state === 'establishing' || state === 'lost'
  }

  /** A lane missing from `starts` has no commit yet. */
  markReady(starts: Map<string, OrderingInfo>): void {
    if (!this.awaitsConfirmation()) return
    this.orders = new Map()
    for (const [lane, start] of starts) {
      this.orders.set(lane, { last: start.seq, lastTimestamp: start.timestamp, parked: null, release: null })
    }
    const early = this.early
    this.early = []
    this.transition('ready')
    for (const message of early) {
      // A consumer may close the lane.
      if (this.state() !== 'ready') return
      this.receiveFrame(message)
    }
  }

  fail(error: unknown): void {
    if (this.state() === 'closed') return
    this.close(error)
    this.transition('closed', error)
  }

  lose(error: unknown): void {
    this.early = []
    if (this.state() !== 'ready') return
    // What arrived goes out: what it waits for can no longer arrive on this connection.
    for (const order of this.orders.values()) if (order.parked !== null) this.release(order)
    this.rejectFences(error)
    this.transition('lost')
  }

  receive(channel: string, message: Buffer): void {
    if (channel === this.invalidationChannel) return this.end()
    if (this.state() === 'ready') return this.receiveFrame(message)
    if (this.awaitsConfirmation()) this.early.push(message)
  }

  /** Its incarnation was dropped: closed on purpose, with no reason. */
  end(): void {
    if (this.state() === 'closed') return
    this.close(new Error(`Redis incarnation of '${this.channel}' was dropped`))
    this.transition('closed')
  }

  /** Over for good: the commits awaiting its fences see `reason`. Its consumers are told after, by `closed`. */
  private close(reason: unknown): void {
    this.dropParked()
    this.rejectFences(reason)
    this.owner.detach(this)
  }

  private receiveFrame(message: Buffer): void {
    if (message[0] !== REDIS_DELIVERY_FENCE_BYTE) return this.order(message, null)
    const token = message.toString('latin1', 1, 1 + REDIS_DELIVERY_FENCE_TOKEN_LENGTH)
    this.order(message.subarray(1 + REDIS_DELIVERY_FENCE_TOKEN_LENGTH), token)
  }

  /** Delivers each lane in seq order: a frame whose predecessor is past the last delivered waits `REORDER_WAIT_MS` for
   *  it, and one at or below the last delivered is dropped. Its commit's fence settles once it is delivered or
   *  dropped. */
  private order(frame: Buffer, token: string | null): void {
    let lane = ''
    if (this.fanIn) {
      const end = 2 + frame.readUInt16BE(0)
      lane = frame.toString('latin1', 2, end)
      frame = frame.subarray(end)
    }
    // A Broadcast key's kinds share one sequence, so its frame leads with the seq of the previous publish of its kind.
    const { payload, info } = decodeOrderingFrame(this.broadcast ? frame.subarray(8) : frame)
    const predecessor = this.broadcast ? Number(frame.readBigUInt64BE(0)) : info.seq - 1
    let order = this.orders.get(lane)
    if (order === undefined) {
      // A lane with no commit when the subscription went live: every one of its commits is after.
      order = { last: 0, lastTimestamp: 0, parked: null, release: null }
      this.orders.set(lane, order)
    }
    if (predecessor <= order.last && info.seq > order.last) {
      this.advance(order, payload, info, token)
      if (order.parked !== null) this.drain(order)
      return
    }
    if (predecessor > order.last) return this.park(order, predecessor, payload, info, token)
    // A commit never sets its lane's time back, so a later time on a seq at or below the last is a failover's rewind.
    if (info.timestamp <= order.lastTimestamp) return this.settle(token)
    if (order.parked !== null) this.release(order)
    this.advance(order, payload, info, token)
  }

  private advance(order: LaneOrder, payload: Uint8Array, info: OrderingInfo, token: string | null): void {
    order.last = info.seq
    order.lastTimestamp = info.timestamp
    this.deliver(payload, info, token)
  }

  private park(
    order: LaneOrder,
    predecessor: number,
    payload: Uint8Array,
    info: OrderingInfo,
    token: string | null,
  ): void {
    if (order.parked?.has(predecessor)) return this.settle(token)
    order.parked ??= new Map()
    order.parked.set(predecessor, { payload, info, token })
    if (order.release !== null) return
    order.release = setTimeout(() => this.release(order), REORDER_WAIT_MS)
    order.release.unref()
  }

  /** The parked frames that now follow in order. */
  private drain(order: LaneOrder): void {
    const parked = order.parked!
    for (let next = parked.get(order.last); next !== undefined; next = parked.get(order.last)) {
      // A consumer may close the lane.
      if (this.state() !== 'ready') return
      parked.delete(order.last)
      this.advance(order, next.payload, next.info, next.token)
    }
    if (parked.size > 0) return
    clearTimeout(order.release!)
    order.parked = null
    order.release = null
  }

  /** The wait ended: every parked frame goes out in seq order, past what never arrived. */
  private release(order: LaneOrder): void {
    clearTimeout(order.release!)
    const parked = [...order.parked!.values()].sort((a, b) => a.info.seq - b.info.seq)
    order.parked = null
    order.release = null
    for (const frame of parked) {
      if (this.state() !== 'ready') return
      this.advance(order, frame.payload, frame.info, frame.token)
    }
  }

  private dropParked(): void {
    for (const order of this.orders.values()) if (order.release !== null) clearTimeout(order.release)
    this.orders.clear()
    this.early = []
  }

  private deliver(payload: Uint8Array, info: OrderingInfo, token: string | null): void {
    try {
      this.receiver.deliver(this.text ? textDecoder.decode(payload) : Uint8Array.from(payload), info)
    } finally {
      this.settle(token)
    }
  }

  private settle(token: string | null): void {
    if (token !== null) this.settleFence(token)
  }

  private rejectFences(error: unknown): void {
    for (const fence of this.fences.values()) fence.reject(error)
    this.fences.clear()
  }
}
