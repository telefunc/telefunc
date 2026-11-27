export { RedisSubscriptions, neverReachedSubscription }

import { randomUUID } from 'node:crypto'
import type { BackendReceiver } from 'telefunc/__internal'
import { DriverAttempt, createDeferred, getOrCreate, unrefTimer, type Deferred } from 'telefunc/__internal'
import { assert } from './assert.js'
import type { CommitChannels, FenceTokens } from './keys.js'
import { slotOf, type FormerMaster, type RedisClient, type SubscriberSocket } from './ioredis.js'
import { LaneHistory, type LaneFrame, type LaneRoute } from './LaneHistory.js'
import { decodeLaneFrame } from './orderingFrame.js'
import { historyOf, type LaneStart, type Starts } from './orderRecord.js'
import { isLoginRefusal, isRefusal, redisFailure } from './redisFailure.js'
import type { RedisConnection } from './RedisConnection.js'

/** What the subscriptions read of the backend's connection: their socket, how many nodes it may sit on, and the client
 *  a failure is told against. */
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
  /** On a fan-in channel, the channel of the lane a frame's lane key names: a lane is one across the channels that
   *  carry it. */
  laneChannel: ((laneKey: string) => string) | null
  /** Each of its lanes' last commit, by the lane key its frames carry on a fan-in channel and `''` on another, and the
   *  history that says so; null once a Room source's incarnation is not the open head. `on`: a former master
   *  (`followMoves`) to read them from. */
  readStarts: (on?: RedisClient) => Promise<Starts | null>
  receiver: BackendReceiver
  /** Pub/sub keeps no log, so what a drop or the reorder wait passed by is gone. `null`: how many isn't known. */
  reportMissed: (count: number | null) => void
  /** The consumer its frames reach on this connection, by name (`SubscriptionDriver.subscribe`'s). The attempts of one
   *  holder share each lane's order, so a commit two of them carry reaches it once, and they tell it once of each
   *  history they leave. */
  holder: string
}

/** The subscriber connection; `id` tells work that awaited across a drop that it is stale. */
type SocketState =
  | { phase: 'idle' }
  | { phase: 'waiting'; timer: ReturnType<typeof setTimeout> }
  | { phase: 'connecting'; id: number; socket: SubscriberSocket | null }
  | {
      phase: 'connected'
      id: number
      socket: SubscriberSocket
      subscribed: Set<string>
      pings: ReturnType<typeof setInterval>
      /** A ping is on its way, unanswered. */
      awaitingPong: boolean
    }

type ConnectedSocket = Extract<SocketState, { phase: 'connected' }>

/** A lane an attempt carries, and the highest place the attempt brought it. */
type CarriedLane = { history: LaneHistory; passed: number }

/** The attempts with one holder: each lane's order they share, and the histories they told it they left. */
class Holder {
  readonly name: string
  attempts = 0
  private readonly lanes = new Map<string, LaneHistory>()
  private readonly left = new Set<string>()

  constructor(name: string) {
    this.name = name
  }

  /** The lane named `channel`, which `route` now carries. */
  joinLane(channel: string, route: LaneRoute): LaneHistory {
    const lane = getOrCreate(this.lanes, channel, () => new LaneHistory())
    lane.routes.add(route)
    return lane
  }

  /** `route` carries the lane no more; it is forgotten once no attempt does. */
  leaveLane(channel: string, lane: LaneHistory, route: LaneRoute): void {
    lane.leave(route)
    if (lane.done && this.lanes.get(channel) === lane) this.lanes.delete(channel)
  }

  /** Frames an attempt that left brought on the lane named `channel`, which the lane didn't decide yet. */
  adopt(channel: string, frames: LaneFrame[]): void {
    this.lanes.get(channel)?.adopt(frames)
  }

  /** Whether moving from the history `from` to `to` is news to the holder: it is told once of each history it leaves.
   *  A read that finds a history it was told it left (its old master still answering) is of that failover. A promotion
   *  starts a new history, so a later failover leaves another one. */
  leaves(from: string, to: string): boolean {
    if (this.left.has(from) || this.left.has(to)) return false
    this.left.add(from)
    return true
  }
}

const textDecoder = new TextDecoder()

/** What a driver reports its attempt passed by: `1 commit`, `3 commits` or `An unknown number of commits` never reached. */
function neverReachedSubscription(count: number | null, noun: string): string {
  const missed = count === 1 ? `1 ${noun}` : `${count ?? 'An unknown number of'} ${noun}s`
  return `${missed} never reached this server's Redis subscription`
}

const RECONNECT_DELAY_MIN_MS = 50
const RECONNECT_DELAY_MAX_MS = 2_000
// A partition closes no socket, and pub/sub is silent while nothing is published, so only a ping can tell.
const SUBSCRIBER_PING_MS = 10_000

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
  /** Each holder some attempt delivers to, by its name. */
  private readonly holders = new Map<string, Holder>()
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
  /** The subscriber's logins refused in a row, each on the node after the last one's. */
  private refusedLogins = 0
  /** The next try of start reads that failed, the wait before it, and whether this run of failures was reported. */
  private readRetry: ReturnType<typeof setTimeout> | null = null
  private readRetryDelay = RECONNECT_DELAY_MIN_MS
  private readFailureReported = false

  constructor({ connection }: RedisSubscriptionsOptions) {
    this.connection = connection
  }

  subscribe(options: RedisAttemptOptions): DriverAttempt {
    const holder = getOrCreate(this.holders, options.holder, () => new Holder(options.holder))
    holder.attempts++
    const attempt = new RedisAttempt({ ...options, owner: this, holder })
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

  /** Sends that waited for each of ours on `channels` that was never ready yet went out before it was. */
  passedBySends(channels: readonly string[]): void {
    for (const channel of channels) for (const attempt of this.byChannel.get(channel) ?? []) attempt.passedBySends()
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
    if (--attempt.holder.attempts === 0) this.holders.delete(attempt.holder.name)
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
      return this.lostSocket(id, this.failure(error))
    }
    if (!this.isCurrent(id)) return socket.disconnect()
    connecting.socket = socket
    socket.on('messageBuffer', (channel: Buffer, frame: Buffer) => {
      if (this.isCurrent(id)) this.dispatch(channel.toString(), frame)
    })
    let lastError: unknown = new Error('Redis subscriber connection closed')
    socket.on('error', (error: unknown) => {
      lastError = this.failure(error)
    })
    socket.on('close', () => this.lostSocket(id, lastError))
    try {
      await socket.connect()
    } catch (error) {
      return this.lostSocket(id, this.failure(error))
    }
    if (!this.isCurrent(id)) return
    const connected: ConnectedSocket = {
      phase: 'connected',
      id,
      socket,
      subscribed: new Set(),
      pings: unrefTimer(setInterval(() => void this.ping(connected), SUBSCRIBER_PING_MS)),
      awaitingPong: false,
    }
    this.socket = connected
    this.changedChannels = new Set(this.byChannel.keys())
    this.syncChannels()
  }

  /** A ping still unanswered at the next one means the connection went silent. */
  private async ping(connection: ConnectedSocket): Promise<void> {
    if (connection.awaitingPong) {
      return this.lost(connection.id, new Error('Redis subscriber connection went silent'), false)
    }
    connection.awaitingPong = true
    try {
      await connection.socket.ping()
    } catch {
      // A refusal is an answer, and a closed connection reports itself through `close`.
    }
    connection.awaitingPong = false
  }

  /** Moves the subscriptions to a connection of the connection's current client, as a resume after a loss. */
  reconnect(): void {
    if (this.socket.phase === 'idle') return
    this.loseAll(new Error("Telefunc's Redis backend moved to a client installed with other connection options"))
    this.reconnectDelay = RECONNECT_DELAY_MIN_MS
    void this.connect()
  }

  /** Cluster slots whose master changed, each with the master it had. A ready attempt on one goes on, under its key's
   *  master now, where it already follows that master's history or the old master shows it lost nothing
   *  (`lostNothing`). Anywhere else the old master may have accepted what the new one lacks, and the attempt reads
   *  where its key stands again (`reread`). `former` reaches a node. */
  async followMoves(moved: Map<number, string>, former: (node: string) => Promise<FormerMaster | null>): Promise<void> {
    const nodes = new Map<string, Promise<FormerMaster | null>>()
    const follow = async (attempt: RedisAttempt) => {
      const before = moved.get(slotOf(attempt.channel))
      if (before === undefined) return
      const history = attempt.history
      if (history === null || attempt.state() !== 'ready') return
      const old = await getOrCreate(nodes, before, () => former(before))
      let starts: Starts | null = null
      try {
        starts = await lostNothing(attempt, old, history)
      } catch {
        // An answer it can't read proves nothing.
      }
      if (starts !== null) return attempt.followHistory(starts.authority)
      this.reread(attempt)
    }
    try {
      await Promise.all([...this.attempts()].map(follow))
    } finally {
      for (const node of nodes.values()) void node.then((old) => old?.close())
    }
  }

  /** A ready attempt reads where its key stands again, on its connection, as a resume after a cut does; its key is told
   *  if that read finds a history it left. Its fences stay, as their frames can still arrive, and a failed read is
   *  tried again (`retryRead`). */
  reread(attempt: RedisAttempt): void {
    if (attempt.state() !== 'ready') return
    attempt.reread()
    this.unconfirmed.add(attempt)
    this.syncChannels()
  }

  private loseAll(error: unknown): void {
    this.disconnect()
    for (const attempt of this.attempts()) {
      attempt.lose(error)
      this.unconfirmed.add(attempt)
    }
  }

  /** The subscriber socket's end, or its opening's, or its first SUBSCRIBE's refusal of its login: the one place a
   *  refusal of the subscriber's login is decided. It stands once each node the subscriber can sit on refused it in
   *  turn, at connect or at the first command: while a node of a Cluster may still let it in, the next socket goes
   *  there, as after a drop. A login counts as let in once a sync of the channels succeeds on it, not when its socket
   *  connects, which it does before Redis checks the login where the client has no ready check. */
  private lostSocket(id: number, error: unknown): void {
    if (!this.isCurrent(id)) return
    this.refusedLogins = isRefusal(error) ? this.refusedLogins + 1 : 0
    this.lost(id, error, this.refusedLogins >= this.connection.subscriberNodeCount())
  }

  private lost(id: number, error: unknown, stands: boolean): void {
    if (!this.isCurrent(id)) return
    // A refusal, such as a refused login, stands until an operator changes the password or the ACL: the subscriptions
    // end with it, and the backend doesn't connect again for them.
    if (stands) {
      this.refusedLogins = 0
      this.disconnect()
      for (const attempt of this.attempts()) attempt.fail(error)
      return
    }
    this.loseAll(error)
    // A listener may have released the last attempt, or opened one that is already connecting.
    if (this.byChannel.size === 0 || this.socket.phase !== 'idle') return
    if (!this.outageReported) {
      this.outageReported = true
      console.error(error)
    }
    const timer = unrefTimer(setTimeout(() => void this.connect(), this.reconnectDelay))
    this.socket = { phase: 'waiting', timer }
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, RECONNECT_DELAY_MAX_MS)
  }

  private disconnect(): void {
    const connection = this.socket
    this.socket = { phase: 'idle' }
    if (connection.phase === 'waiting') clearTimeout(connection.timer)
    else if (connection.phase !== 'idle') connection.socket?.disconnect()
    if (connection.phase === 'connected') clearInterval(connection.pings)
    if (this.readRetry !== null) clearTimeout(this.readRetry)
    this.readRetry = null
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
      // The connection dropped, Redis can't serve it now (`BUSY`, `LOADING`), or it refused the login: start over on a
      // fresh one.
      return this.lostSocket(connection.id, this.failure(error))
    }
    // Recovered once the channels are subscribed, not when the socket connects: a SUBSCRIBE Redis can't serve keeps
    // backing off, and a node that refused the login at it counts as refusing.
    this.refusedLogins = 0
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
    if (this.isCurrent(id)) await this.confirmCovered(connection)
  }

  /** Redis refuses a whole SUBSCRIBE over one channel it denies, leaving the connection as it was: each channel then
   *  goes alone, so only a denied one's attempts fail. A refused login is the node's, not a channel's: it ends the
   *  socket (`lostSocket()`). */
  private async subscribeMissing(connection: ConnectedSocket, channels: string[]): Promise<void> {
    const { id, socket, subscribed } = connection
    try {
      await socket.subscribe(...channels)
    } catch (thrown) {
      const error = this.failure(thrown)
      if (!isRefusal(error) || isLoginRefusal(error)) throw error
      if (!this.isCurrent(id)) return
      if (channels.length > 1) {
        await Promise.all(channels.map((channel) => this.subscribeMissing(connection, [channel])))
      } else {
        const [channel] = channels
        assert(channel !== undefined)
        for (const attempt of [...(this.byChannel.get(channel) ?? [])]) attempt.fail(error)
      }
      return
    }
    if (this.isCurrent(id)) for (const channel of channels) subscribed.add(channel)
  }

  /** Confirms the attempts `subscribed` covers. An attempt made during a SUBSCRIBE waits for the sync it queued. */
  private async confirmCovered(connection: ConnectedSocket): Promise<void> {
    const covered: RedisAttempt[] = []
    for (const attempt of this.unconfirmed) {
      if (!attempt.awaitsConfirmation()) {
        this.unconfirmed.delete(attempt)
      } else if (attempt.channels.every((channel) => connection.subscribed.has(channel))) {
        this.unconfirmed.delete(attempt)
        covered.push(attempt)
      }
    }
    await Promise.all(covered.map((attempt) => this.confirm(attempt, connection)))
  }

  /** Its channels are subscribed on a live connection: ready from where each lane stands now, unless its incarnation is
   *  no longer open. */
  private async confirm(attempt: RedisAttempt, { id, socket }: ConnectedSocket): Promise<void> {
    let starts: Starts | null
    try {
      starts = await attempt.readStarts()
      // A resume delivers what the starts count that reached this connection. A frame published just before the read can
      // still be on its way, and the acknowledgment of a repeated SUBSCRIBE comes after it.
      if (!attempt.establishing) await socket.subscribe(...attempt.channels)
    } catch (thrown) {
      if (!this.isCurrent(id)) return
      const error = this.failure(thrown)
      // A refusal stands until an operator changes Redis's ACL or the password. Any other failure (a command-connection
      // error, `READONLY` during a failover, `LOADING`) is tried again, as a drop is: the attempt awaits confirmation
      // meanwhile, and the shared subscriber connection stays.
      if (isRefusal(error)) return attempt.fail(error)
      return this.retryRead(attempt, error)
    }
    if (!this.isCurrent(id)) return
    this.readRetryDelay = RECONNECT_DELAY_MIN_MS
    this.readFailureReported = false
    if (starts !== null) return attempt.markReady(starts)
    // Closed on purpose since the caller read it open; gone after a loss, it may have gone with Redis's data.
    if (attempt.establishing) return attempt.end()
    attempt.fail(new Error(`subscribeLane: the incarnation of '${attempt.channel}' is not open`))
  }

  private retryRead(attempt: RedisAttempt, error: unknown): void {
    this.unconfirmed.add(attempt)
    if (this.readRetry !== null) return
    if (!this.readFailureReported) {
      this.readFailureReported = true
      console.error(error)
    }
    this.readRetry = unrefTimer(
      setTimeout(() => {
        this.readRetry = null
        this.syncChannels()
      }, this.readRetryDelay),
    )
    this.readRetryDelay = Math.min(this.readRetryDelay * 2, RECONNECT_DELAY_MAX_MS)
  }

  /** What the backend reports of an error of the subscriber connection (`redisFailure()`): SUBSCRIBE and PING write
   *  nothing. */
  private failure(error: unknown): unknown {
    return redisFailure(error, this.connection.client, false)
  }

  private attempts(): Set<RedisAttempt> {
    const all = new Set<RedisAttempt>()
    for (const attempts of this.byChannel.values()) for (const attempt of attempts) all.add(attempt)
    return all
  }

  private dispatch(channel: string, frame: Buffer): void {
    // One that leaves meanwhile is passed over; one that joins holds the frame as early, for its start to order.
    for (const attempt of this.byChannel.get(channel) ?? []) attempt.receive(channel, frame)
  }
}

/** One source on the shared connection: hands each frame to its lane, which decides it once across the attempts that
 *  carry the lane, and delivers what its lane delivers through it to core's `receiver`. */
class RedisAttempt extends DriverAttempt implements LaneRoute {
  readonly channel: string
  readonly invalidationChannel: string | null
  readonly readStarts: RedisAttemptOptions['readStarts']
  readonly channels: readonly string[]
  readonly holder: Holder
  private readonly tell: RedisAttemptOptions['reportMissed']
  private readonly owner: RedisSubscriptions
  private readonly receiver: BackendReceiver
  private readonly laneChannel: RedisAttemptOptions['laneChannel']
  private readonly fences = new Map<string, Deferred<void>>()
  /** Each lane it carries: an every-track or every-member channel carries many binary lanes, keyed by lane key; any
   *  other channel one lane, keyed `''`. */
  private readonly lanes = new Map<string, CarriedLane>()
  /** The history of its last ready start, from its first: a start from another can lack what it missed. */
  private authority: string | null = null
  /** The frames that arrived while it awaited confirmation, ordered once it knows where each lane starts. */
  private early: LaneFrame[] = []
  /** A frame in another form was dropped and reported. */
  private foreignReported = false
  /** Sends that waited for its first ready went out before it: that ready tells it may have missed some. */
  private passedBySend = false
  private readonly firstReadiness = createDeferred()
  /** Settles on its first ready or its end. Rejects if it fails before either, for the sends waiting on it. */
  readonly firstReady: Promise<void> = this.firstReadiness.promise
  private readonly frames: FrameShape

  constructor({
    owner,
    channel,
    invalidationChannel,
    frames,
    laneChannel,
    readStarts,
    receiver,
    reportMissed: tell,
    holder,
  }: Omit<RedisAttemptOptions, 'holder'> & { owner: RedisSubscriptions; holder: Holder }) {
    super()
    this.owner = owner
    this.channel = channel
    this.invalidationChannel = invalidationChannel
    this.frames = frames
    this.laneChannel = laneChannel
    this.readStarts = readStarts
    this.receiver = receiver
    this.tell = tell
    this.holder = holder
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

  /** The history its last ready start came from, its node's replication id. */
  get history(): string | null {
    return this.authority === null ? null : historyOf(this.authority)
  }

  /** Whether its lanes got all that `there`, a former master's starts, holds: each lane's commit there is one the lane
   *  delivered or started at, or the one `here`, its key's master now, holds too. */
  holds(there: ReadonlyMap<string, LaneStart>, here: ReadonlyMap<string, LaneStart>): boolean {
    for (const [laneKey, start] of there) {
      const now = here.get(laneKey)
      if (now !== undefined && now.place === start.place && now.id === start.id) continue
      if (this.lanes.get(laneKey)?.history.knows(start) !== true) return false
    }
    return true
  }

  /** Sends that waited for its first ready went out before it, if it is still to come. */
  passedBySends(): void {
    if (this.establishing) this.passedBySend = true
  }

  /** `LaneRoute`: what a drop or the reorder wait passed by. */
  reportMissed(count: number): void {
    this.tell(count)
  }

  /** `LaneRoute`: its start read found a lane's record replaced within the history it follows. */
  replaced(): void {
    this.tell(null)
  }

  /** `LaneRoute`: a frame its lane can't place. It reads where its key stands, and the frame waits for that read. */
  forked(frame: LaneFrame): void {
    this.owner.reread(this)
    if (this.awaitsConfirmation()) this.early.push(frame)
    else this.dropped(frame)
  }

  /** Its key's history goes on under `authority`, the master its key moved to, where it lost nothing in the move. */
  followHistory(authority: string): void {
    if (this.state() !== 'ready') return
    this.authority = authority
    for (const { history } of this.lanes.values()) history.follows = historyOf(authority)
  }

  /** Its lanes start at `starts`; a lane missing from them has no commit yet. A first establish promises nothing from
   *  before it is ready; each lane compares its start with what it delivered and the history it follows. A read that
   *  leaves a history the attempt or one of its lanes followed tells its key, once for that history, that it doesn't
   *  know what it missed. */
  markReady({ authority, starts }: Starts): void {
    if (!this.awaitsConfirmation()) return
    const resuming = this.state() === 'lost'
    const before = resuming ? this.history : null
    this.authority = authority
    const now = historyOf(authority)
    const early = new Map<string, LaneFrame[]>()
    for (const frame of this.early) getOrCreate(early, frame.lane, () => []).push(frame)
    this.early = []
    const lanes = new Set([...starts.keys(), ...early.keys()])
    for (const [laneKey, { history }] of resuming ? this.lanes : []) {
      if (lanes.has(laneKey)) continue
      // A member's lane missing from the starts left with its member; any other lane's order went with Redis's data.
      if (!this.frames.lanesEndWithMembers) {
        lanes.add(laneKey)
      } else {
        this.holder.leaveLane(this.laneName(laneKey), history, this)
        this.lanes.delete(laneKey)
      }
    }
    this.transition('ready')
    const left = new Set<string>()
    if (before !== null && before !== now) left.add(before)
    for (const laneKey of lanes) {
      const follows = this.laneOf(laneKey).history.follows
      if (follows !== null && follows !== now) left.add(follows)
    }
    let news = this.passedBySend
    this.passedBySend = false
    for (const history of left) news = this.holder.leaves(history, now) || news
    if (news) this.tell(null)
    for (const laneKey of lanes) {
      // A consumer may close the lane.
      if (this.state() !== 'ready') return
      const lane = this.laneOf(laneKey)
      const pass = (frame: LaneFrame) => this.pass(lane, frame)
      lane.history.establish(this, starts.get(laneKey), early.get(laneKey) ?? [], resuming, pass, now, before ?? now)
    }
  }

  fail(error: unknown): void {
    if (this.state() === 'closed') return
    // Only a refusal fails the sends waiting on it: a closed incarnation says nothing of them, and `closed` ends their wait.
    if (isRefusal(error)) this.firstReadiness.reject(error)
    this.close(error)
    this.transition('closed', error)
  }

  /** Its connection dropped: what its lanes wait for can't arrive, and the commits awaiting its fences see `error`. */
  lose(error: unknown): void {
    this.early = []
    if (this.state() !== 'ready') return
    for (const { history } of this.lanes.values()) history.releaseParked()
    this.rejectFences(error)
    this.transition('lost')
  }

  /** It reads where its key stands again, on the connection it has: what arrives meanwhile waits for that read. */
  reread(): void {
    this.transition('lost')
  }

  receive(channel: string, message: Buffer): void {
    if (channel === this.invalidationChannel) return this.end()
    if (this.state() === 'ready') {
      const frame = this.decode(message)
      if (frame === null) return
      const lane = this.laneOf(frame.lane)
      return lane.history.offer(frame, this.pass(lane, frame))
    }
    if (!this.awaitsConfirmation()) return
    const frame = this.decode(message)
    if (frame !== null) this.early.push(frame)
  }

  /** `LaneRoute`: a frame its lane delivers. */
  deliver({ payload, info, token }: LaneFrame, rewound: boolean): void {
    try {
      if (this.state() === 'closed') return
      const delivered = this.frames.text ? textDecoder.decode(payload) : Uint8Array.from(payload)
      this.receiver.deliver(delivered, rewound ? { ...info, rewound } : info)
    } catch (error) {
      // Thrown back into the socket's read, it would reach neither the attempts beside this one nor anyone's handler.
      console.error(error)
    } finally {
      this.settleFence(token)
    }
  }

  /** `LaneRoute`: a frame its lane drops. */
  dropped({ token }: LaneFrame): void {
    this.settleFence(token)
  }

  /** `LaneRoute`: what a lane waited for isn't coming. `lost` says it may have missed messages, which is what makes
   *  Room catch up (`deliveries-lost`). */
  waitEnded(): void {
    if (this.state() !== 'ready') return
    this.transition('lost')
    if (this.state() === 'lost') this.transition('ready')
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

  /** The lane a frame's lane key names, which it carries from now on. */
  private laneOf(laneKey: string): CarriedLane {
    return getOrCreate(this.lanes, laneKey, () => ({
      history: this.holder.joinLane(this.laneName(laneKey), this),
      passed: 0,
    }))
  }

  private laneName(laneKey: string): string {
    return this.laneChannel === null ? this.channel : this.laneChannel(laneKey)
  }

  /** The highest place it brought `lane` before `frame`, which it now brought. */
  private pass(lane: CarriedLane, frame: LaneFrame): number {
    const before = lane.passed
    lane.passed = Math.max(before, frame.place)
    return before
  }

  /** The message's frame, or null for one in another form. */
  private decode(message: Buffer): LaneFrame | null {
    return decodeLaneFrame(message, this.frames, this) ?? this.dropForeign()
  }

  /** A frame in a form Telefunc's scripts don't write, from another client publishing to the channel: reported once. */
  private dropForeign(): null {
    if (this.foreignReported) return null
    this.foreignReported = true
    console.error(
      new Error(
        `Telefunc's Redis backend dropped a message on ${this.channel} that isn't in the form Telefunc publishes: a ` +
          'client other than Telefunc published to its channel',
      ),
    )
    return null
  }

  private forgetFrames(): void {
    const early = new Map<string, LaneFrame[]>()
    for (const frame of this.early) getOrCreate(early, frame.lane, () => []).push(frame)
    this.early = []
    for (const [laneKey, { history }] of this.lanes) {
      this.holder.leaveLane(this.laneName(laneKey), history, this)
    }
    this.lanes.clear()
    // What it brought and its lanes didn't decide yet stays theirs, where another attempt carries them.
    for (const [laneKey, frames] of early) this.holder.adopt(this.laneName(laneKey), frames)
  }

  private rejectFences(error: unknown): void {
    for (const fence of this.fences.values()) fence.reject(error)
    this.fences.clear()
  }
}

/** The starts of `attempt`'s key's master now, where the attempt follows that master's history already, or `old`, the
 *  master its slot had, shows that it lost nothing of `history`, its own, in the move; else null. It lost nothing where
 *  `old` gave the slot away by resharding, as master of that history (the slot took its keys, and the bus delivers);
 *  where it rejoined the new master by a partial resync, which Redis grants only to a node with nothing the new master
 *  lacks; and where, not yet resynced, each lane got all its record holds there. */
async function lostNothing(attempt: RedisAttempt, old: FormerMaster | null, history: string): Promise<Starts | null> {
  const here = await attempt.readStarts()
  if (here === null) return null
  if (historyOf(here.authority) === history) return here
  if (old === null) return null
  if (old.master) {
    // One that still serves the slot may yet accept what the new master lacks.
    if (old.history !== history || (await old.serves(attempt.channel)) !== false) return null
    return here
  }
  if (old.previous === history) return here
  if (old.history !== history) return null
  const there = await attempt.readStarts(old.client)
  if (there === null || historyOf(there.authority) !== history) return null
  return attempt.holds(there.starts, here.starts) ? here : null
}
