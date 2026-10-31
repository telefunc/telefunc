export { RedisSubscriber }
export type { RedisSubscriptionSource }

import { randomUUID } from 'node:crypto'
import type {
  BackendPayload,
  BackendReceiver,
  BroadcastRoute,
  OrderingInfo,
  SubscriptionAttempt,
  SubscriptionBinding,
} from 'telefunc/__internal'
import type { RoomSubscriptionSource } from '@telefunc/room/backend'
import { DriverAttempt, SourceDroppedError, decodeOrderingFrame } from 'telefunc/__internal'
import { createDeferred, type Deferred } from './createDeferred.js'
import type { CommitChannels, FenceTokens } from './keys.js'
import { REDIS_DELIVERY_FENCE_BYTE, REDIS_DELIVERY_FENCE_TOKEN_LENGTH } from './commands.js'
import type { SubscriberSocket } from './ioredis.js'

type RedisSubscriptionSource = BroadcastRoute | RoomSubscriptionSource

type RedisSubscriberOptions = {
  /** A fresh, unconnected subscriber socket that never retries on its own. */
  createSubscriber: () => Promise<SubscriberSocket>
}

/** Where a source's frames arrive, and how its attempts learn where its lanes start. */
type SourceChannels<Source extends RedisSubscriptionSource> = {
  laneChannel: string
  /** A Room lane's generation channel: a message on it means the generation was dropped. */
  invalidationChannel: string | null
  /** Each of the source's lanes' last commit, by the lane key its frames carry on a fan-in channel and `''` on another;
   *  null once a Room source's incarnation is not the open head. */
  readLaneStarts: (source: Source) => Promise<Map<string, OrderingInfo> | null>
}

/** The subscriber connection; `id` tells work that awaited across a drop that it is stale. */
type Connection =
  | { phase: 'idle' }
  | { phase: 'waiting'; timer: ReturnType<typeof setTimeout> }
  | { phase: 'connecting'; id: number; socket: SubscriberSocket | null }
  | { phase: 'connected'; id: number; socket: SubscriberSocket; subscribed: Set<string> }

type Connected = Extract<Connection, { phase: 'connected' }>

/** A lane's last frame delivered, and the frames that arrived past a missing one, by their predecessor's seq,
 *  until it arrives or the wait ends. */
type LaneOrder = {
  last: number
  lastTimestamp: number
  held: Map<number, HeldFrame> | null
  release: ReturnType<typeof setTimeout> | null
}

type HeldFrame = { payload: Uint8Array; info: OrderingInfo; token: string | null }

const textDecoder = new TextDecoder()

const RECONNECT_DELAY_MIN_MS = 50
const RECONNECT_DELAY_MAX_MS = 2_000
// Redis Cluster forwards a channel's frames from the old and the new slot owner over separate bus links during a
// reshard or a failover, so the old owner's last frame can arrive after the new owner's first.
const REORDER_WAIT_MS = 1_000

/**
 * Redis's only backend-specific subscription edge: one subscriber connection, its channels refcounted
 * across attempts, which Broadcast's and Room's drivers bind their sources on. When it drops, every ready attempt
 * goes `lost`; a fresh connection re-subscribes them and reports them `ready` again (a Room lane only while its
 * incarnation is still the open head).
 */
class RedisSubscriber {
  private readonly createSubscriber: () => Promise<SubscriberSocket>
  /** Every channel some attempt needs, with the attempts that listen on it. */
  private readonly byChannel = new Map<string, Set<RedisSubscriptionAttempt>>()
  /** The channels some attempt started or stopped needing since the last sync read them. */
  private changedChannels = new Set<string>()
  /** The attempts that may await confirmation; a sync confirms those its connection covers. */
  private readonly unconfirmed = new Set<RedisSubscriptionAttempt>()
  private connection: Connection = { phase: 'idle' }
  private nextId = 0
  private syncing: Promise<void> = Promise.resolve()
  private syncQueued = false
  private reconnectDelay = RECONNECT_DELAY_MIN_MS
  /** An establishing attempt reports no failure, so the first one of an outage is reported here. */
  private outageReported = false

  constructor(options: RedisSubscriberOptions) {
    this.createSubscriber = options.createSubscriber
  }

  bind<Source extends RedisSubscriptionSource>(source: Source, channels: SourceChannels<Source>): SubscriptionBinding {
    return {
      partition: '',
      open: (receiver) => this.open(source, channels, receiver),
    }
  }

  /** Arms a delivery fence on the ready attempts of each channel the commit is published on: `delivery` settles once
   *  each saw the commit. */
  prepareFence(channels: CommitChannels): {
    tokens: FenceTokens
    delivery: Promise<void>
    cancel(): void
  } {
    const token = randomUUID()
    const arm = (channel: string | null) => {
      const attempts = channel === null ? [] : [...(this.byChannel.get(channel) ?? [])]
      return { attempts, armed: attempts.flatMap((attempt) => attempt.prepareFence(token) ?? []) }
    }
    const lane = arm(channels.lane)
    const everyTrack = arm(channels.everyTrack)
    const everyMember = arm(channels.everyMember)
    const all = [lane, everyTrack, everyMember]
    const delivery = Promise.all(all.flatMap((channel) => channel.armed)).then(() => {})
    // A subscriber can drop before the commit awaiting this delivery returns.
    void delivery.catch(() => {})
    const tokenOf = (channel: { armed: unknown[] }) => (channel.armed.length === 0 ? '' : token)
    return {
      tokens: { lane: tokenOf(lane), everyTrack: tokenOf(everyTrack), everyMember: tokenOf(everyMember) },
      delivery,
      cancel: () => {
        for (const channel of all) for (const attempt of channel.attempts) attempt.settleFence(token)
      },
    }
  }

  private open<Source extends RedisSubscriptionSource>(
    source: Source,
    channels: SourceChannels<Source>,
    receiver: BackendReceiver<BackendPayload>,
  ): SubscriptionAttempt {
    const attempt: RedisSubscriptionAttempt = new RedisSubscriptionAttempt({
      source,
      // The attempt hands `readLaneStarts` its own source only.
      channels: channels as SourceChannels<RedisSubscriptionSource>,
      receiver,
      onDetach: () => this.detach(attempt),
    })
    for (const channel of attempt.channels) {
      const attempts = this.byChannel.get(channel)
      if (attempts !== undefined) {
        attempts.add(attempt)
      } else {
        this.byChannel.set(channel, new Set([attempt]))
        this.changedChannels.add(channel)
      }
    }
    this.unconfirmed.add(attempt)
    if (this.connection.phase === 'connected') this.syncChannels()
    else if (this.connection.phase === 'idle') void this.connect()
    return attempt
  }

  private detach(attempt: RedisSubscriptionAttempt): void {
    this.unconfirmed.delete(attempt)
    for (const channel of attempt.channels) {
      const attempts = this.byChannel.get(channel)
      attempts?.delete(attempt)
      if (attempts?.size !== 0) continue
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
    this.connection = connecting
    let socket: SubscriberSocket
    try {
      socket = await this.createSubscriber()
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
    this.connection = { phase: 'connected', id, socket, subscribed: new Set() }
    this.changedChannels = new Set(this.byChannel.keys())
    this.syncChannels()
  }

  private lost(id: number, error: unknown): void {
    if (!this.isCurrent(id)) return
    this.disconnect()
    for (const attempt of this.attempts()) {
      attempt.lose(error)
      this.unconfirmed.add(attempt)
    }
    // A listener may have released the last attempt, or opened one that is already connecting.
    if (this.byChannel.size === 0 || this.connection.phase !== 'idle') return
    if (!this.outageReported) {
      this.outageReported = true
      console.error(error)
    }
    const timer = setTimeout(() => void this.connect(), this.reconnectDelay)
    timer.unref()
    this.connection = { phase: 'waiting', timer }
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, RECONNECT_DELAY_MAX_MS)
  }

  private disconnect(): void {
    const connection = this.connection
    this.connection = { phase: 'idle' }
    if (connection.phase === 'waiting') clearTimeout(connection.timer)
    else if (connection.phase !== 'idle') connection.socket?.disconnect()
  }

  private isCurrent(id: number): boolean {
    const connection = this.connection
    return (connection.phase === 'connecting' || connection.phase === 'connected') && connection.id === id
  }

  /** Serialized: brings the connection's channel set to the attempts' and confirms what it covers. A sync reads the
   *  changed channels when it starts, so one that is queued already covers any later request. */
  private syncChannels(): void {
    if (this.syncQueued) return
    this.syncQueued = true
    this.syncing = this.syncing.then(async () => {
      this.syncQueued = false
      const connection = this.connection
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
    // An attempt attached during the SUBSCRIBE above waits for the sync it queued.
    const covered: RedisSubscriptionAttempt[] = []
    for (const attempt of this.unconfirmed) {
      if (!attempt.awaitsConfirmation()) {
        this.unconfirmed.delete(attempt)
      } else if (attempt.channels.every((channel) => subscribed.has(channel))) {
        this.unconfirmed.delete(attempt)
        covered.push(attempt)
      }
    }
    await Promise.all(covered.map((attempt) => this.confirm(attempt, id)))
  }

  /** Its channels are subscribed on a live connection: ready from where each lane stands now, unless its incarnation is
   *  no longer open. */
  private async confirm(attempt: RedisSubscriptionAttempt, id: number): Promise<void> {
    let starts: Map<string, OrderingInfo> | null
    try {
      starts = await attempt.readLaneStarts(attempt.source)
    } catch (error) {
      // A failed read (a command-connection error) fails this attempt, not the shared subscriber connection.
      if (this.isCurrent(id)) attempt.terminate(error)
      return
    }
    if (!this.isCurrent(id)) return
    if (starts !== null) return attempt.markReady(starts)
    attempt.terminate(new Error(`subscribeLane: the generation of '${attempt.laneChannel}' is not open`))
  }

  private attempts(): Set<RedisSubscriptionAttempt> {
    const all = new Set<RedisSubscriptionAttempt>()
    for (const attempts of this.byChannel.values()) for (const attempt of attempts) all.add(attempt)
    return all
  }

  private dispatch(channel: string, frame: Buffer): void {
    for (const attempt of [...(this.byChannel.get(channel) ?? [])]) attempt.receive(channel, frame)
  }
}

class RedisSubscriptionAttempt extends DriverAttempt {
  readonly source: RedisSubscriptionSource
  readonly laneChannel: string
  readonly invalidationChannel: string | null
  readonly readLaneStarts: SourceChannels<RedisSubscriptionSource>['readLaneStarts']
  readonly channels: readonly string[]
  private readonly receiver: BackendReceiver<BackendPayload>
  private readonly onDetach: () => void
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
    source,
    channels: { laneChannel, invalidationChannel, readLaneStarts },
    receiver,
    onDetach,
  }: {
    source: RedisSubscriptionSource
    channels: SourceChannels<RedisSubscriptionSource>
    receiver: BackendReceiver<BackendPayload>
    onDetach: () => void
  }) {
    super()
    this.source = source
    this.laneChannel = laneChannel
    this.invalidationChannel = invalidationChannel
    this.readLaneStarts = readLaneStarts
    this.receiver = receiver
    this.onDetach = onDetach
    this.channels = invalidationChannel === null ? [laneChannel] : [laneChannel, invalidationChannel]
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

  async unsubscribe(): Promise<void> {
    this.dropHeld()
    for (const token of this.fences.keys()) this.settleFence(token)
    this.transition('closed', new Error(`Redis subscription '${this.laneChannel}' was closed`))
    this.onDetach()
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
    return this.state() === 'establishing' || this.state() === 'lost'
  }

  /** A lane missing from `starts` has no commit yet. */
  markReady(starts: Map<string, OrderingInfo>): void {
    if (!this.awaitsConfirmation()) return
    this.orders = new Map()
    for (const [lane, start] of starts) {
      this.orders.set(lane, { last: start.seq, lastTimestamp: start.timestamp, held: null, release: null })
    }
    const early = this.early
    this.early = []
    this.transition('ready')
    for (const message of early) {
      // A receiver may close the lane.
      if (this.state() !== 'ready') return
      this.receiveFrame(message)
    }
  }

  terminate(error: unknown): void {
    if (this.ended) return
    this.dropHeld()
    this.rejectFences(error)
    this.transition('closed', error)
  }

  lose(error: unknown): void {
    this.early = []
    if (this.state() !== 'ready') return
    // What arrived goes out: what it waits for can no longer arrive on this connection.
    for (const order of this.orders.values()) if (order.held !== null) this.release(order)
    this.rejectFences(error)
    this.transition('lost')
  }

  receive(channel: string, message: Buffer): void {
    if (channel === this.invalidationChannel) {
      return this.terminate(new SourceDroppedError('Redis generation subscription was invalidated'))
    }
    if (this.state() === 'ready') return this.receiveFrame(message)
    if (this.awaitsConfirmation()) this.early.push(message)
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
      order = { last: 0, lastTimestamp: 0, held: null, release: null }
      this.orders.set(lane, order)
    }
    if (predecessor <= order.last && info.seq > order.last) {
      this.advance(order, payload, info, token)
      if (order.held !== null) this.drain(order)
      return
    }
    if (predecessor > order.last) return this.hold(order, predecessor, payload, info, token)
    // A commit never sets its lane's time back, so a later time on a seq at or below the last is a failover's rewind.
    if (info.timestamp <= order.lastTimestamp) return this.settle(token)
    if (order.held !== null) this.release(order)
    this.advance(order, payload, info, token)
  }

  private advance(order: LaneOrder, payload: Uint8Array, info: OrderingInfo, token: string | null): void {
    order.last = info.seq
    order.lastTimestamp = info.timestamp
    this.deliver(payload, info, token)
  }

  private hold(
    order: LaneOrder,
    predecessor: number,
    payload: Uint8Array,
    info: OrderingInfo,
    token: string | null,
  ): void {
    if (order.held?.has(predecessor)) return this.settle(token)
    order.held ??= new Map()
    order.held.set(predecessor, { payload, info, token })
    if (order.release !== null) return
    order.release = setTimeout(() => this.release(order), REORDER_WAIT_MS)
    order.release.unref()
  }

  /** The held frames that now follow in order. */
  private drain(order: LaneOrder): void {
    const held = order.held!
    for (let next = held.get(order.last); next !== undefined; next = held.get(order.last)) {
      // A receiver may close the lane.
      if (this.state() !== 'ready') return
      held.delete(order.last)
      this.advance(order, next.payload, next.info, next.token)
    }
    if (held.size > 0) return
    clearTimeout(order.release!)
    order.held = null
    order.release = null
  }

  /** The wait ended: every held frame goes out in seq order, past what never arrived. */
  private release(order: LaneOrder): void {
    clearTimeout(order.release!)
    const held = [...order.held!.values()].sort((a, b) => a.info.seq - b.info.seq)
    order.held = null
    order.release = null
    for (const frame of held) {
      if (this.state() !== 'ready') return
      this.advance(order, frame.payload, frame.info, frame.token)
    }
  }

  private dropHeld(): void {
    for (const order of this.orders.values()) if (order.release !== null) clearTimeout(order.release)
    this.orders.clear()
    this.early = []
  }

  private deliver(payload: Uint8Array, info: OrderingInfo, token: string | null): void {
    try {
      this.receiver(this.text ? textDecoder.decode(payload) : Uint8Array.from(payload), info)
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
