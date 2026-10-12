export { RedisSubscriptionDriver }
export type { RedisSubscriptionSource }

import { randomUUID } from 'node:crypto'
import type {
  BackendPayload,
  BackendReceiver,
  BroadcastRoute,
  Deferred,
  OrderingInfo,
  RoomSubscriptionSource,
  SubscriptionAttempt,
  SubscriptionBinding,
  SubscriptionDriver,
} from 'telefunc/__internal'
import { DriverAttempt, createDeferred, decodeOrderingFrame, encodeLaneKey } from 'telefunc/__internal'
import { broadcastChannel, channelKey, generationInvalidationChannel, type CommitChannels } from './keys.js'
import { REDIS_DELIVERY_FENCE_BYTE, REDIS_DELIVERY_FENCE_TOKEN_LENGTH } from './commands.js'
import type { SubscriberSocket } from './ioredis.js'

type RedisSubscriptionSource = BroadcastRoute | RoomSubscriptionSource

type RedisSubscriptionDriverOptions = {
  prefix: string
  /** A fresh, unconnected subscriber socket that never retries on its own. */
  createSubscriber: () => Promise<SubscriberSocket>
  /** Each of the source's lanes' last commit, by the lane key its frames carry on a fan-in channel and `''` on another;
   *  null once a Room source's incarnation is not the open head. */
  readLaneStarts: (source: RedisSubscriptionSource) => Promise<Map<string, OrderingInfo> | null>
}

/** The subscriber connection; `id` tells work that awaited across a drop that it is stale. */
type Connection =
  | { phase: 'idle' }
  | { phase: 'waiting'; timer: ReturnType<typeof setTimeout> }
  | { phase: 'connecting'; id: number; socket: SubscriberSocket | null }
  | { phase: 'connected'; id: number; socket: SubscriberSocket; subscribed: Set<string> }

type Connected = Extract<Connection, { phase: 'connected' }>

/** A lane's last frame delivered, and the frames that arrived past a missing one, by seq, until it arrives or the wait
 *  ends. */
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
 * across attempts. When it drops, every ready attempt goes `lost`; a fresh connection re-subscribes them
 * and reports them `ready` again (a Room lane only while its incarnation is still the open head).
 */
class RedisSubscriptionDriver implements SubscriptionDriver<RedisSubscriptionSource> {
  private readonly _prefix: string
  private readonly _createSubscriber: () => Promise<SubscriberSocket>
  private readonly _readLaneStarts: RedisSubscriptionDriverOptions['readLaneStarts']
  /** Every channel some attempt needs, with the attempts that listen on it. */
  private readonly _byChannel = new Map<string, Set<RedisSubscriptionAttempt>>()
  /** The channels some attempt started or stopped needing since the last sync read them. */
  private _changedChannels = new Set<string>()
  /** The attempts that may await confirmation; a sync confirms those its connection covers. */
  private readonly _unconfirmed = new Set<RedisSubscriptionAttempt>()
  private _connection: Connection = { phase: 'idle' }
  private _nextId = 0
  private _syncing: Promise<void> = Promise.resolve()
  private _syncQueued = false
  private _reconnectDelay = RECONNECT_DELAY_MIN_MS
  /** An establishing attempt reports no failure, so the first one of an outage is reported here. */
  private _outageReported = false

  constructor(options: RedisSubscriptionDriverOptions) {
    this._prefix = options.prefix
    this._createSubscriber = options.createSubscriber
    this._readLaneStarts = options.readLaneStarts
  }

  bind(source: RedisSubscriptionSource): SubscriptionBinding {
    return {
      partition: '',
      open: (receiver) => this._open(source, receiver),
    }
  }

  partitionHere(): string {
    return ''
  }

  /** Arms a delivery fence on the ready attempts of each channel the commit is published on: `delivery` settles once
   *  each saw the commit. */
  prepareFence(channels: CommitChannels): {
    tokens: { lane: string; everyTrack: string; everyMember: string }
    delivery: Promise<void>
    cancel(): void
  } {
    const token = randomUUID()
    const arm = (channel: string | null) => {
      const attempts = channel === null ? [] : [...(this._byChannel.get(channel) ?? [])]
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

  private _open(source: RedisSubscriptionSource, receiver: BackendReceiver<BackendPayload>): SubscriptionAttempt {
    const attempt: RedisSubscriptionAttempt = new RedisSubscriptionAttempt(
      source,
      laneChannel(this._prefix, source),
      'roomId' in source ? generationInvalidationChannel(this._prefix, source.roomId, source.inc) : null,
      receiver,
      () => this._detach(attempt),
    )
    for (const channel of attempt.channels) {
      const attempts = this._byChannel.get(channel)
      if (attempts !== undefined) attempts.add(attempt)
      else {
        this._byChannel.set(channel, new Set([attempt]))
        this._changedChannels.add(channel)
      }
    }
    this._unconfirmed.add(attempt)
    if (this._connection.phase === 'connected') this._syncChannels()
    else if (this._connection.phase === 'idle') void this._connect()
    return attempt
  }

  private _detach(attempt: RedisSubscriptionAttempt): void {
    this._unconfirmed.delete(attempt)
    for (const channel of attempt.channels) {
      const attempts = this._byChannel.get(channel)
      attempts?.delete(attempt)
      if (attempts?.size !== 0) continue
      this._byChannel.delete(channel)
      this._changedChannels.add(channel)
    }
    if (this._byChannel.size > 0) return this._syncChannels()
    // Nothing left to deliver to: release the connection until the next subscription, which starts afresh.
    this._disconnect()
    this._outageReported = false
    this._reconnectDelay = RECONNECT_DELAY_MIN_MS
  }

  private async _connect(): Promise<void> {
    const id = ++this._nextId
    const connecting: Extract<Connection, { phase: 'connecting' }> = { phase: 'connecting', id, socket: null }
    this._connection = connecting
    let socket: SubscriberSocket
    try {
      socket = await this._createSubscriber()
    } catch (error) {
      return this._lost(id, error)
    }
    if (!this._isCurrent(id)) return socket.disconnect()
    connecting.socket = socket
    socket.on('messageBuffer', (channel: Buffer, frame: Buffer) => {
      if (this._isCurrent(id)) this._dispatch(channel.toString(), frame)
    })
    let lastError: unknown = new Error('Redis subscriber connection closed')
    socket.on('error', (error: unknown) => {
      lastError = error
    })
    socket.on('close', () => this._lost(id, lastError))
    try {
      await socket.connect()
    } catch (error) {
      return this._lost(id, error)
    }
    if (!this._isCurrent(id)) return
    this._connection = { phase: 'connected', id, socket, subscribed: new Set() }
    this._changedChannels = new Set(this._byChannel.keys())
    this._syncChannels()
  }

  private _lost(id: number, error: unknown): void {
    if (!this._isCurrent(id)) return
    this._disconnect()
    for (const attempt of this._attempts()) {
      attempt.lose(error)
      this._unconfirmed.add(attempt)
    }
    // A listener may have released the last attempt, or opened one that is already connecting.
    if (this._byChannel.size === 0 || this._connection.phase !== 'idle') return
    if (!this._outageReported) {
      this._outageReported = true
      console.error(error)
    }
    const timer = setTimeout(() => void this._connect(), this._reconnectDelay)
    timer.unref()
    this._connection = { phase: 'waiting', timer }
    this._reconnectDelay = Math.min(this._reconnectDelay * 2, RECONNECT_DELAY_MAX_MS)
  }

  private _disconnect(): void {
    const connection = this._connection
    this._connection = { phase: 'idle' }
    if (connection.phase === 'waiting') clearTimeout(connection.timer)
    else if (connection.phase !== 'idle') connection.socket?.disconnect()
  }

  private _isCurrent(id: number): boolean {
    const connection = this._connection
    return (connection.phase === 'connecting' || connection.phase === 'connected') && connection.id === id
  }

  /** Serialized: brings the connection's channel set to the attempts' and confirms what it covers. A sync reads the
   *  changed channels when it starts, so one that is queued already covers any later request. */
  private _syncChannels(): void {
    if (this._syncQueued) return
    this._syncQueued = true
    this._syncing = this._syncing.then(async () => {
      this._syncQueued = false
      const connection = this._connection
      if (connection.phase !== 'connected') return
      try {
        await this._syncChannelsOnce(connection)
      } catch (error) {
        // A failed (UN)SUBSCRIBE leaves the channel set unknown: start over on a fresh connection.
        return this._lost(connection.id, error)
      }
      // Recovered once the channels are subscribed, not when the socket connects: a refused SUBSCRIBE keeps backing off.
      this._reconnectDelay = RECONNECT_DELAY_MIN_MS
      this._outageReported = false
    })
  }

  private async _syncChannelsOnce({ id, socket, subscribed }: Connected): Promise<void> {
    const changed = this._changedChannels
    this._changedChannels = new Set()
    const stale: string[] = []
    const missing: string[] = []
    for (const channel of changed) {
      const wanted = this._byChannel.has(channel)
      if (wanted && !subscribed.has(channel)) missing.push(channel)
      else if (!wanted && subscribed.has(channel)) stale.push(channel)
    }
    if (stale.length > 0) {
      for (const channel of stale) subscribed.delete(channel)
      await socket.unsubscribe(...stale)
    }
    if (missing.length > 0) {
      await socket.subscribe(...missing)
      if (!this._isCurrent(id)) return
      for (const channel of missing) subscribed.add(channel)
    }
    // An attempt attached during the SUBSCRIBE above waits for the sync it queued.
    const covered: RedisSubscriptionAttempt[] = []
    for (const attempt of this._unconfirmed) {
      if (!attempt.awaitsConfirmation()) this._unconfirmed.delete(attempt)
      else if (attempt.channels.every((channel) => subscribed.has(channel))) {
        this._unconfirmed.delete(attempt)
        covered.push(attempt)
      }
    }
    await Promise.all(covered.map((attempt) => this._confirm(attempt, id)))
  }

  /** Its channels are subscribed on a live connection: ready from where each lane stands now, unless its incarnation is
   *  no longer open. */
  private async _confirm(attempt: RedisSubscriptionAttempt, id: number): Promise<void> {
    let starts: Map<string, OrderingInfo> | null
    try {
      starts = await this._readLaneStarts(attempt.source)
    } catch (error) {
      // A failed read (a command-connection error) fails this attempt, not the shared subscriber connection.
      if (this._isCurrent(id)) attempt.terminate(error)
      return
    }
    if (!this._isCurrent(id)) return
    if (starts !== null) return attempt.markReady(starts)
    attempt.terminate(new Error(`subscribeLane: the generation of '${attempt.laneChannel}' is not open`))
  }

  private _attempts(): Set<RedisSubscriptionAttempt> {
    return new Set([...this._byChannel.values()].flatMap((attempts) => [...attempts]))
  }

  private _dispatch(channel: string, frame: Buffer): void {
    for (const attempt of [...(this._byChannel.get(channel) ?? [])]) attempt.receive(channel, frame)
  }
}

class RedisSubscriptionAttempt extends DriverAttempt {
  readonly channels: readonly string[]
  private readonly _fences = new Map<string, Deferred<void>>()
  /** Each lane's order: an every-track or every-member channel carries many binary lanes, keyed by lane key; any other
   *  channel one lane, keyed `''`. */
  private _orders = new Map<string, LaneOrder>()
  /** The frames that arrived while it awaited confirmation, ordered once it knows where each lane starts. */
  private _early: Buffer[] = []
  private readonly _fanIn: boolean

  constructor(
    readonly source: RedisSubscriptionSource,
    readonly laneChannel: string,
    /** A Room lane's generation channel: a message on it means the generation was dropped. */
    readonly invalidationChannel: string | null,
    private readonly _receiver: BackendReceiver<BackendPayload>,
    private readonly _onDetach: () => void,
  ) {
    super()
    this.channels = invalidationChannel === null ? [laneChannel] : [laneChannel, invalidationChannel]
    this._fanIn = 'roomId' in source && (source.lane.kind === 'binary-all' || source.lane.kind === 'binary-track')
  }

  async unsubscribe(): Promise<void> {
    this._dropHeld()
    for (const token of [...this._fences.keys()]) this.settleFence(token)
    this.transition('closed', new Error(`Redis subscription '${this.laneChannel}' was closed`))
    this._onDetach()
  }

  prepareFence(token: string): Promise<void> | null {
    if (this.state() !== 'ready') return null
    const fence = createDeferred()
    this._fences.set(token, fence)
    return fence.promise
  }

  /** Resolves the fence: its token arrived, the commit that armed it was refused, or no receiver is left. */
  settleFence(token: string): void {
    this._fences.get(token)?.resolve()
    this._fences.delete(token)
  }

  awaitsConfirmation(): boolean {
    return this.state() === 'establishing' || this.state() === 'lost'
  }

  /** A lane missing from `starts` has no commit yet. */
  markReady(starts: Map<string, OrderingInfo>): void {
    if (!this.awaitsConfirmation()) return
    this._orders = new Map()
    for (const [lane, start] of starts)
      this._orders.set(lane, { last: start.seq, lastTimestamp: start.timestamp, held: null, release: null })
    const early = this._early
    this._early = []
    this.transition('ready')
    for (const message of early) {
      // A receiver may close the lane.
      if (this.state() !== 'ready') return
      this._receiveFrame(message)
    }
  }

  terminate(error: unknown): void {
    if (this.ended) return
    this._dropHeld()
    this._rejectFences(error)
    this.transition('closed', error)
  }

  lose(error: unknown): void {
    this._early = []
    if (this.state() !== 'ready') return
    // What arrived goes out: what it waits for can no longer arrive on this connection.
    for (const order of this._orders.values()) if (order.held !== null) this._release(order)
    this._rejectFences(error)
    this.transition('lost')
  }

  receive(channel: string, message: Buffer): void {
    if (channel === this.invalidationChannel)
      return this.terminate(new Error('Redis generation subscription was invalidated'))
    if (this.state() === 'ready') return this._receiveFrame(message)
    if (this.awaitsConfirmation()) this._early.push(message)
  }

  private _receiveFrame(message: Buffer): void {
    if (message[0] !== REDIS_DELIVERY_FENCE_BYTE) return this._order(message, null)
    const token = message.toString('latin1', 1, 1 + REDIS_DELIVERY_FENCE_TOKEN_LENGTH)
    this._order(message.subarray(1 + REDIS_DELIVERY_FENCE_TOKEN_LENGTH), token)
  }

  /** Delivers each lane in seq order: a frame past a missing seq waits `REORDER_WAIT_MS` for it, and one at or below
   *  the last delivered is dropped. Its commit's fence settles once it is delivered or dropped. */
  private _order(frame: Buffer, token: string | null): void {
    let lane = ''
    if (this._fanIn) {
      const end = 2 + frame.readUInt16BE(0)
      lane = frame.toString('latin1', 2, end)
      frame = frame.subarray(end)
    }
    const { payload, info } = decodeOrderingFrame(frame)
    let order = this._orders.get(lane)
    if (order === undefined) {
      // A lane with no commit when the subscription went live: every one of its commits is after.
      order = { last: 0, lastTimestamp: 0, held: null, release: null }
      this._orders.set(lane, order)
    }
    if (info.seq === order.last + 1) {
      this._advance(order, payload, info, token)
      if (order.held !== null) this._drain(order)
      return
    }
    if (info.seq > order.last + 1) return this._hold(order, payload, info, token)
    // A commit never sets its lane's time back, so a later time on a seq at or below the last is a failover's rewind.
    if (info.timestamp <= order.lastTimestamp) return this._settle(token)
    if (order.held !== null) this._release(order)
    this._advance(order, payload, info, token)
  }

  private _advance(order: LaneOrder, payload: Uint8Array, info: OrderingInfo, token: string | null): void {
    order.last = info.seq
    order.lastTimestamp = info.timestamp
    this._deliver(payload, info, token)
  }

  private _hold(order: LaneOrder, payload: Uint8Array, info: OrderingInfo, token: string | null): void {
    if (order.held?.has(info.seq)) return this._settle(token)
    ;(order.held ??= new Map()).set(info.seq, { payload, info, token })
    if (order.release !== null) return
    order.release = setTimeout(() => this._release(order), REORDER_WAIT_MS)
    order.release.unref()
  }

  /** The held frames that now follow in order. */
  private _drain(order: LaneOrder): void {
    const held = order.held!
    for (let next = held.get(order.last + 1); next !== undefined; next = held.get(order.last + 1)) {
      // A receiver may close the lane.
      if (this.state() !== 'ready') return
      held.delete(next.info.seq)
      this._advance(order, next.payload, next.info, next.token)
    }
    if (held.size > 0) return
    clearTimeout(order.release!)
    order.held = null
    order.release = null
  }

  /** The wait ended: every held frame goes out in seq order, past what never arrived. */
  private _release(order: LaneOrder): void {
    clearTimeout(order.release!)
    const held = [...order.held!.values()].sort((a, b) => a.info.seq - b.info.seq)
    order.held = null
    order.release = null
    for (const frame of held) {
      if (this.state() !== 'ready') return
      this._advance(order, frame.payload, frame.info, frame.token)
    }
  }

  private _dropHeld(): void {
    for (const order of this._orders.values()) if (order.release !== null) clearTimeout(order.release)
    this._orders.clear()
    this._early = []
  }

  private _deliver(payload: Uint8Array, info: OrderingInfo, token: string | null): void {
    try {
      const text = !('roomId' in this.source) && this.source.kind === 'text'
      this._receiver(text ? textDecoder.decode(payload) : Uint8Array.from(payload), info)
    } finally {
      this._settle(token)
    }
  }

  private _settle(token: string | null): void {
    if (token !== null) this.settleFence(token)
  }

  private _rejectFences(error: unknown): void {
    for (const fence of this._fences.values()) fence.reject(error)
    this._fences.clear()
  }
}

function laneChannel(prefix: string, source: RedisSubscriptionSource): string {
  return 'roomId' in source
    ? channelKey(prefix, source.roomId, source.inc, encodeLaneKey(source.lane))
    : broadcastChannel(prefix, source)
}
