export { RedisSubscriptions, neverReachedSubscription }

import { randomUUID } from 'node:crypto'
import type { BackendReceiver, OrderingInfo } from 'telefunc/__internal'
import { DriverAttempt, createDeferred, decodeOrderingFrame, type Deferred } from 'telefunc/__internal'
import type { CommitChannels, FenceTokens } from './keys.js'
import { REDIS_DELIVERY_FENCE_BYTE, REDIS_DELIVERY_FENCE_TOKEN_LENGTH } from './commands.js'
import { isRefusal, type SubscriberSocket } from './ioredis.js'
import type { RedisConnection } from './RedisConnection.js'

type RedisSubscriptionsOptions = { connection: RedisConnection }

/** What a driver's frames carry, and how long its lanes' order lasts. */
type FrameShape = {
  /** The channel carries many binary lanes: each frame leads with its lane key. */
  fanIn: boolean
  /** The source's kinds share one seq: each frame leads with its place among its kind's publishes. */
  placed: boolean
  /** Its deliveries are strings. */
  text: boolean
  /** A member's lanes' order goes when the member leaves; any other lane's lasts as long as its room or key. */
  lanesEndWithMembers: boolean
}

/** Where an attempt's frames arrive, what they carry, where its lanes start, and whom it delivers to. */
type RedisAttemptOptions = {
  channel: string
  /** A Room lane's incarnation channel: a message on it means the incarnation was dropped. */
  invalidationChannel: string | null
  frames: FrameShape
  /** Each of its lanes' last commit, by the lane key its frames carry on a fan-in channel and `''` on another; null
   *  once a Room source's incarnation is not the open head. */
  readStarts: () => Promise<Map<string, OrderingInfo> | null>
  receiver: BackendReceiver
  /** Pub/sub keeps no log, so what a drop or the reorder wait passed by is gone. `null`: how many isn't known. */
  reportMissed: (count: number | null) => void
}

/** The subscriber connection; `id` tells work that awaited across a drop that it is stale. */
type SocketState =
  | { phase: 'idle' }
  | { phase: 'waiting'; timer: ReturnType<typeof setTimeout> }
  | { phase: 'connecting'; id: number; socket: SubscriberSocket | null }
  | { phase: 'connected'; id: number; socket: SubscriberSocket; subscribed: Set<string> }

type ConnectedSocket = Extract<SocketState, { phase: 'connected' }>

/** A lane's last place delivered, and the frames parked past a missing one, by their predecessor's place, until it
 *  arrives or the wait ends. */
type LaneOrder = {
  last: number
  lastTimestamp: number
  parked: Map<number, LaneFrame> | null
  releaseTimer: ReturnType<typeof setTimeout> | null
}

type LaneFrame = { lane: string; place: number; payload: Uint8Array; info: OrderingInfo; token: string | null }

const textDecoder = new TextDecoder()

/** What a driver reports its attempt passed by: `1 commit`, `3 commits` or `An unknown number of commits` never reached. */
function neverReachedSubscription(count: number | null, noun: string): string {
  const missed = count === 1 ? `1 ${noun}` : `${count ?? 'An unknown number of'} ${noun}s`
  return `${missed} never reached this server's Redis subscription`
}

const RECONNECT_DELAY_MIN_MS = 50
const RECONNECT_DELAY_MAX_MS = 2_000
// Redis Cluster forwards a channel's frames from the old and the new slot owner over separate bus links during a
// reshard or a failover, so the old owner's last frame can arrive after the new owner's first.
const REORDER_WAIT_MS = 1_000

/**
 * Redis's only backend-specific subscription edge: one subscriber connection, its channels refcounted across
 * attempts, which Broadcast's and Room's drivers subscribe their sources on. When it drops, every ready
 * attempt is lost; a fresh connection subscribes them again and tells them ready (a Room lane only while its
 * incarnation is still the open head).
 */
class RedisSubscriptions {
  private readonly connection: RedisConnection
  /** Every channel some attempt needs, with the attempts that listen on it. */
  private readonly byChannel = new Map<string, Set<RedisAttempt>>()
  /** The channels some attempt started or stopped needing since the last sync read them. */
  private changedChannels = new Set<string>()
  /** The attempts that may await confirmation; a sync confirms those its connection covers. */
  private readonly unconfirmed = new Set<RedisAttempt>()
  /** Attempts never ready yet, counted down a microtask after: at 0, no send waits for one. */
  private establishingCount = 0
  private socket: SocketState = { phase: 'idle' }
  private nextId = 0
  private syncing: Promise<void> = Promise.resolve()
  private syncQueued = false
  private reconnectDelay = RECONNECT_DELAY_MIN_MS
  /** An establishing attempt reports no failure, so the first one of an outage is reported here. */
  private outageReported = false

  constructor({ connection }: RedisSubscriptionsOptions) {
    this.connection = connection
  }

  subscribe(options: RedisAttemptOptions): DriverAttempt {
    const attempt = new RedisAttempt({ ...options, owner: this })
    this.establishingCount++
    const settled = () => this.establishingCount--
    void attempt.firstReady.then(settled, settled)
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
    if (this.socket.phase === 'connected') this.syncChannels()
    else if (this.socket.phase === 'idle') void this.connect()
    return attempt
  }

  /** Whether one of ours was never ready yet. */
  get establishing(): boolean {
    return this.establishingCount > 0
  }

  /** When each of ours on `channels` that was never ready yet is first ready, ends or fails. */
  establishingOn(channels: readonly string[]): Promise<void>[] {
    const pending: Promise<void>[] = []
    for (const channel of channels) {
      for (const attempt of this.byChannel.get(channel) ?? [])
        if (attempt.establishing) pending.push(attempt.firstReady)
    }
    return pending
  }

  /** Arms a delivery fence on the ready attempts of each channel the commit is published on: `delivery` settles
   *  once each saw the commit. */
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
    // The subscriber connection can drop before the commit awaiting this delivery returns.
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

  /** Forgets an attempt that unsubscribed or ended. */
  detach(attempt: RedisAttempt): void {
    this.unconfirmed.delete(attempt)
    for (const channel of attempt.channels) {
      const attempts = this.byChannel.get(channel)
      attempts?.delete(attempt)
      if (attempts?.size !== 0) continue
      this.byChannel.delete(channel)
      this.changedChannels.add(channel)
    }
    if (this.byChannel.size > 0) return this.syncChannels()
    // Nothing left to deliver to: release the connection until the next attempt, which starts afresh.
    this.disconnect()
    this.outageReported = false
    this.reconnectDelay = RECONNECT_DELAY_MIN_MS
  }

  private async connect(): Promise<void> {
    const id = ++this.nextId
    const connecting: Extract<SocketState, { phase: 'connecting' }> = { phase: 'connecting', id, socket: null }
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
    for (const attempt of this.attempts()) {
      attempt.lose(error)
      this.unconfirmed.add(attempt)
    }
    // A listener may have released the last attempt, or opened one that is already connecting.
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

  /** Serialized: brings the connection's channel set to the attempts' and confirms what it covers. A sync reads
   *  the changed channels when it starts, so one that is queued already covers any later request. */
  private syncChannels(): void {
    if (this.syncQueued) return
    this.syncQueued = true
    this.syncing = this.syncing.then(() => this.syncQueuedChannels())
  }

  private async syncQueuedChannels(): Promise<void> {
    this.syncQueued = false
    const connection = this.socket
    if (connection.phase !== 'connected') return
    try {
      await this.syncChannelsOnce(connection)
    } catch (error) {
      // The connection dropped, or Redis can't serve it now (`BUSY`, `LOADING`): start over on a fresh one.
      return this.lost(connection.id, error)
    }
    // Recovered once the channels are subscribed, not when the socket connects: a SUBSCRIBE Redis can't serve keeps
    // backing off.
    this.reconnectDelay = RECONNECT_DELAY_MIN_MS
    this.outageReported = false
  }

  private async syncChannelsOnce(connection: ConnectedSocket): Promise<void> {
    const { id, socket, subscribed } = connection
    const changed = this.changedChannels
    this.changedChannels = new Set()
    const stale: string[] = []
    const missing: string[] = []
    for (const channel of changed) {
      const wanted = this.byChannel.has(channel)
      if (wanted && !subscribed.has(channel)) missing.push(channel)
      else if (!wanted && subscribed.has(channel)) stale.push(channel)
    }
    for (const channel of stale) subscribed.delete(channel)
    await Promise.all([
      // With no channels, `unsubscribe()` drops them all.
      stale.length > 0 && socket.unsubscribe(...stale),
      missing.length > 0 && this.subscribeMissing(connection, missing),
    ])
    // After the SUBSCRIBE's acknowledgment: a lane's start read before it would miss a publish between the two.
    if (this.isCurrent(id)) await this.confirmCovered(id, subscribed)
  }

  /** Redis refuses a whole SUBSCRIBE over one channel it denies, leaving the connection as it was: each channel then
   *  goes alone, so only a denied one's attempts fail. */
  private async subscribeMissing(connection: ConnectedSocket, channels: string[]): Promise<void> {
    const { id, socket, subscribed } = connection
    try {
      await socket.subscribe(...channels)
    } catch (error) {
      if (!isRefusal(error)) throw error
      if (!this.isCurrent(id)) return
      if (channels.length > 1) {
        await Promise.all(channels.map((channel) => this.subscribeMissing(connection, [channel])))
      } else {
        for (const attempt of [...(this.byChannel.get(channels[0]!) ?? [])]) attempt.fail(error)
      }
      return
    }
    if (this.isCurrent(id)) for (const channel of channels) subscribed.add(channel)
  }

  /** Confirms the attempts `subscribed` covers. An attempt made during a SUBSCRIBE waits for the sync it queued. */
  private async confirmCovered(id: ConnectedSocket['id'], subscribed: ConnectedSocket['subscribed']): Promise<void> {
    const covered: RedisAttempt[] = []
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
  private async confirm(attempt: RedisAttempt, id: number): Promise<void> {
    let starts: Map<string, OrderingInfo> | null
    try {
      starts = await attempt.readStarts()
    } catch (error) {
      // A failed read (a command-connection error) fails this attempt, not the shared subscriber connection.
      if (this.isCurrent(id)) attempt.fail(error)
      return
    }
    if (!this.isCurrent(id)) return
    if (starts !== null) return attempt.markReady(starts)
    // Closed on purpose since the caller read it open; gone after a loss, it may have gone with Redis's data.
    if (attempt.establishing) return attempt.end()
    attempt.fail(new Error(`subscribeLane: the incarnation of '${attempt.channel}' is not open`))
  }

  private attempts(): Set<RedisAttempt> {
    const all = new Set<RedisAttempt>()
    for (const attempts of this.byChannel.values()) for (const attempt of attempts) all.add(attempt)
    return all
  }

  private dispatch(channel: string, frame: Buffer): void {
    for (const attempt of [...(this.byChannel.get(channel) ?? [])]) attempt.receive(channel, frame)
  }
}

/** One source on the shared connection: delivers its lanes in place order to core's `receiver`. */
class RedisAttempt extends DriverAttempt {
  readonly channel: string
  readonly invalidationChannel: string | null
  readonly readStarts: RedisAttemptOptions['readStarts']
  readonly channels: readonly string[]
  private readonly owner: RedisSubscriptions
  private readonly receiver: BackendReceiver
  private readonly reportMissed: RedisAttemptOptions['reportMissed']
  private readonly fences = new Map<string, Deferred<void>>()
  /** Each lane's order: an every-track or every-member channel carries many binary lanes, keyed by lane key; any other
   *  channel one lane, keyed `''`. */
  private orders = new Map<string, LaneOrder>()
  /** The frames that arrived while it awaited confirmation, ordered once it knows where each lane starts. */
  private early: Buffer[] = []
  private readonly firstReadiness = createDeferred()
  /** Settles on its first ready or its end. Rejects if it fails before either, for the sends waiting on it. */
  readonly firstReady: Promise<void> = this.firstReadiness.promise
  private readonly frames: FrameShape

  constructor({
    owner,
    channel,
    invalidationChannel,
    frames,
    readStarts,
    receiver,
    reportMissed,
  }: RedisAttemptOptions & { owner: RedisSubscriptions }) {
    super()
    this.owner = owner
    this.channel = channel
    this.invalidationChannel = invalidationChannel
    this.frames = frames
    this.readStarts = readStarts
    this.receiver = receiver
    this.reportMissed = reportMissed
    this.channels = invalidationChannel === null ? [channel] : [channel, invalidationChannel]
    // Its first state change ends its establishment: ready, or closed.
    this.onStateChange(() => this.firstReadiness.resolve())
  }

  /** It has never been ready, and hasn't ended. */
  get establishing(): boolean {
    return this.state() === 'establishing'
  }

  async unsubscribe(): Promise<void> {
    if (this.state() === 'closed') return
    this.transition('closed')
    this.forgetFrames()
    for (const token of this.fences.keys()) this.settleFence(token)
    this.owner.detach(this)
  }

  prepareFence(token: string): Promise<void> | null {
    if (this.state() !== 'ready') return null
    const fence = createDeferred()
    this.fences.set(token, fence)
    return fence.promise
  }

  /** Resolves the fence, if the frame carried one: its token arrived, the commit that armed it was refused, or no
   *  receiver is left. */
  settleFence(token: string | null): void {
    if (token === null) return
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
    const missed = this.state() === 'lost' ? this.missedBefore(starts) : 0
    this.orders = new Map()
    for (const [lane, start] of starts) {
      this.orders.set(lane, { last: start.seq, lastTimestamp: start.timestamp, parked: null, releaseTimer: null })
    }
    const early = this.early
    this.early = []
    this.transition('ready')
    if (missed !== 0) this.reportMissed(missed)
    for (const message of early) {
      // A consumer may close the lane.
      if (this.state() !== 'ready') return
      this.order(this.decodeLaneFrame(message))
    }
  }

  /** What its lost connection passed by, or null once a lane's order is behind what it delivered: a failover's rewind or
   *  a flush took the count with it. */
  private missedBefore(starts: Map<string, OrderingInfo>): number | null {
    if (!this.frames.lanesEndWithMembers && this.orders.size > starts.size) return null
    let missed = 0
    for (const [lane, start] of starts) {
      const last = this.orders.get(lane)?.last ?? 0
      if (start.seq < last) return null
      missed += start.seq - last
    }
    return missed
  }

  fail(error: unknown): void {
    if (this.state() === 'closed') return
    this.firstReadiness.reject(error)
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
    if (this.state() === 'ready') return this.order(this.decodeLaneFrame(message))
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
    this.forgetFrames()
    this.rejectFences(reason)
    this.owner.detach(this)
  }

  /** A frame on the wire: its commit's fence token, if armed; its lane key and its place, if its shape has them; then
   *  the ordering frame. */
  private decodeLaneFrame(message: Buffer): LaneFrame {
    const fenced = message[0] === REDIS_DELIVERY_FENCE_BYTE
    const token = fenced ? message.toString('latin1', 1, 1 + REDIS_DELIVERY_FENCE_TOKEN_LENGTH) : null
    let frame = fenced ? message.subarray(1 + REDIS_DELIVERY_FENCE_TOKEN_LENGTH) : message
    let lane = ''
    const { fanIn, placed } = this.frames
    if (fanIn) {
      const end = 2 + frame.readUInt16BE(0)
      lane = frame.toString('latin1', 2, end)
      frame = frame.subarray(end)
    }
    const { payload, info } = decodeOrderingFrame(placed ? frame.subarray(8) : frame)
    const place = placed ? Number(frame.readBigUInt64BE(0)) : info.seq
    return { lane, place, payload, info, token }
  }

  /** Delivers each lane in order: a frame whose predecessor is past the last delivered waits `REORDER_WAIT_MS` for it,
   *  and one at or below the last delivered is dropped. Its commit's fence settles once it is delivered or dropped. */
  private order(frame: LaneFrame): void {
    let order = this.orders.get(frame.lane)
    if (order === undefined) {
      // A lane with no commit when the attempt went live: every one of its commits is after.
      order = { last: 0, lastTimestamp: 0, parked: null, releaseTimer: null }
      this.orders.set(frame.lane, order)
    }
    if (frame.place === order.last + 1) {
      this.advance(order, frame)
      if (order.parked !== null) this.drain(order)
      return
    }
    if (frame.place > order.last) return this.park(order, frame)
    // A commit never sets its lane's time back, so a later time on a place at or below the last is a failover's rewind.
    if (frame.info.timestamp <= order.lastTimestamp) return this.settleFence(frame.token)
    if (order.parked !== null) this.release(order)
    this.advance(order, frame)
  }

  private advance(order: LaneOrder, { place, payload, info, token }: LaneFrame): void {
    order.last = place
    order.lastTimestamp = info.timestamp
    this.deliver(payload, info, token)
  }

  private park(order: LaneOrder, frame: LaneFrame): void {
    const predecessor = frame.place - 1
    if (order.parked?.has(predecessor)) return this.settleFence(frame.token)
    order.parked ??= new Map()
    order.parked.set(predecessor, frame)
    if (order.releaseTimer !== null) return
    order.releaseTimer = setTimeout(() => this.gapTimedOut(order), REORDER_WAIT_MS)
    order.releaseTimer.unref()
  }

  /** What the lane waited for isn't coming. `lost` says it may have missed messages, which is what makes Room catch up
   *  (`deliveries-lost`), and it goes on from the parked frames. */
  private gapTimedOut(order: LaneOrder): void {
    this.transition('lost')
    if (this.state() !== 'lost') return
    this.transition('ready')
    this.release(order)
  }

  /** The parked frames that now follow in order. */
  private drain(order: LaneOrder): void {
    const parked = order.parked!
    for (let next = parked.get(order.last); next !== undefined; next = parked.get(order.last)) {
      // A consumer may close the lane.
      if (this.state() !== 'ready') return
      parked.delete(order.last)
      this.advance(order, next)
    }
    if (parked.size > 0) return
    clearTimeout(order.releaseTimer!)
    order.parked = null
    order.releaseTimer = null
  }

  /** The wait ended: every parked frame goes out in place order, past what never arrived. */
  private release(order: LaneOrder): void {
    clearTimeout(order.releaseTimer!)
    const parked = [...order.parked!.values()].sort((a, b) => a.place - b.place)
    order.parked = null
    order.releaseTimer = null
    let missed = 0
    for (const frame of parked) {
      if (this.state() !== 'ready') break
      missed += frame.place - order.last - 1
      this.advance(order, frame)
    }
    if (missed > 0) this.reportMissed(missed)
  }

  private forgetFrames(): void {
    for (const order of this.orders.values()) if (order.releaseTimer !== null) clearTimeout(order.releaseTimer)
    this.orders.clear()
    this.early = []
  }

  private deliver(payload: Uint8Array, info: OrderingInfo, token: string | null): void {
    try {
      this.receiver.deliver(this.frames.text ? textDecoder.decode(payload) : Uint8Array.from(payload), info)
    } catch (error) {
      // Thrown back into the socket's read, it would reach neither the attempts beside this one nor anyone's handler.
      console.error(error)
    } finally {
      this.settleFence(token)
    }
  }

  private rejectFences(error: unknown): void {
    for (const fence of this.fences.values()) fence.reject(error)
    this.fences.clear()
  }
}
