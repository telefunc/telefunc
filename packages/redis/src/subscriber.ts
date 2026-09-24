import { randomUUID } from 'node:crypto'
import type {
  BackendReceiver,
  BroadcastLane,
  Deferred,
  RoomSubscriptionSource,
  SubscriptionAttempt,
  SubscriptionBinding,
  SubscriptionDriver,
} from 'telefunc/__internal'
import { DriverAttempt, createDeferred, decodeOrderingFrame, encodeLaneKey } from 'telefunc/__internal'
import { broadcastChannel, channelKey, generationInvalidationChannel } from './keys.js'
import { REDIS_DELIVERY_FENCE_BYTE } from './commands.js'
import type { SubscriberSocket } from './ioredis.js'

type RedisSubscriptionSource = BroadcastLane | RoomSubscriptionSource

type RedisSubscriptionDriverOptions = {
  prefix: string
  /** A fresh, unconnected subscriber socket that never retries on its own. */
  createSubscriber: () => Promise<SubscriberSocket>
  /** Whether the source's incarnation is still the open head. */
  validateGeneration: (source: RoomSubscriptionSource) => Promise<boolean>
}

/** The subscriber connection; `id` tells work that awaited across a drop that it is stale. */
type Connection =
  | { phase: 'idle' }
  | { phase: 'waiting'; timer: ReturnType<typeof setTimeout> }
  | { phase: 'connecting'; id: number; socket: SubscriberSocket | null }
  | { phase: 'connected'; id: number; socket: SubscriberSocket; subscribed: Set<string> }

type Connected = Extract<Connection, { phase: 'connected' }>

const RECONNECT_DELAY_MIN_MS = 50
const RECONNECT_DELAY_MAX_MS = 2_000

/**
 * Redis's only backend-specific subscription edge: one subscriber connection, its channels refcounted
 * across attempts. When it drops, every ready attempt goes `lost`; a fresh connection re-subscribes them
 * and reports them `ready` again — a Room lane only while its incarnation is still the open head.
 */
export class RedisSubscriptionDriver implements SubscriptionDriver<RedisSubscriptionSource> {
  private readonly _prefix: string
  private readonly _createSubscriber: () => Promise<SubscriberSocket>
  private readonly _validateGeneration: RedisSubscriptionDriverOptions['validateGeneration']
  /** Every channel some attempt needs, with the attempts that listen on it. */
  private readonly _byChannel = new Map<string, Set<RedisSubscriptionAttempt>>()
  private _connection: Connection = { phase: 'idle' }
  private _nextId = 0
  private _reconciling: Promise<void> = Promise.resolve()
  private _reconnectDelay = RECONNECT_DELAY_MIN_MS
  /** An establishing attempt reports no failure, so the first one of an outage is reported here. */
  private _outageReported = false
  private _lastError: unknown = new Error('Redis subscriber connection closed')

  constructor(options: RedisSubscriptionDriverOptions) {
    this._prefix = options.prefix
    this._createSubscriber = options.createSubscriber
    this._validateGeneration = options.validateGeneration
  }

  bind(source: RedisSubscriptionSource): SubscriptionBinding {
    return {
      partition: '',
      valid: () => true,
      open: (receiver, localReceiverCount) => this._open(source, receiver, localReceiverCount),
    }
  }

  /** Arms a delivery fence on the lane's ready attempts: `delivery` settles once each saw the commit. */
  prepareFence(source: RoomSubscriptionSource): { token: string; delivery: Promise<void>; cancel(): void } {
    const token = randomUUID()
    const attempts = [...(this._byChannel.get(laneChannel(this._prefix, source)) ?? [])]
    const armed = attempts.flatMap((attempt) => attempt.prepareFence(token) ?? [])
    const delivery = Promise.all(armed).then(() => {})
    // A subscriber can drop before the commit awaiting this delivery returns.
    void delivery.catch(() => {})
    return {
      token: armed.length === 0 ? '' : token,
      delivery,
      cancel: () => {
        for (const attempt of attempts) attempt.settleFence(token)
      },
    }
  }

  private _open(
    source: RedisSubscriptionSource,
    receiver: BackendReceiver,
    localReceiverCount: () => number,
  ): SubscriptionAttempt {
    const attempt: RedisSubscriptionAttempt = new RedisSubscriptionAttempt(
      source,
      laneChannel(this._prefix, source),
      'roomId' in source ? generationInvalidationChannel(this._prefix, source.roomId, source.inc) : null,
      receiver,
      localReceiverCount,
      () => this._detach(attempt),
    )
    for (const channel of attempt.channels) {
      const attempts = this._byChannel.get(channel) ?? new Set()
      this._byChannel.set(channel, attempts.add(attempt))
    }
    if (this._connection.phase === 'connected') this._reconcile()
    else if (this._connection.phase === 'idle') void this._connect()
    return attempt
  }

  private _detach(attempt: RedisSubscriptionAttempt): void {
    for (const channel of attempt.channels) {
      const attempts = this._byChannel.get(channel)
      attempts?.delete(attempt)
      if (attempts?.size === 0) this._byChannel.delete(channel)
    }
    if (this._byChannel.size > 0) return this._reconcile()
    // Nothing left to deliver to: release the connection until the next subscription.
    this._disconnect()
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
    socket.on('error', (error: unknown) => {
      this._lastError = error
    })
    socket.on('close', () => this._lost(id, this._lastError))
    try {
      await socket.connect()
    } catch (error) {
      return this._lost(id, error)
    }
    if (!this._isCurrent(id)) return
    this._connection = { phase: 'connected', id, socket, subscribed: new Set() }
    this._reconcile()
  }

  private _lost(id: number, error: unknown): void {
    if (!this._isCurrent(id)) return
    this._disconnect()
    for (const attempt of this._attempts()) attempt.lose(error)
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

  /** Serialized: brings the connection's channel set to the attempts' and confirms what it covers. */
  private _reconcile(): void {
    this._reconciling = this._reconciling.then(async () => {
      const connection = this._connection
      if (connection.phase !== 'connected') return
      try {
        await this._reconcileOnce(connection)
      } catch (error) {
        // A failed (UN)SUBSCRIBE leaves the channel set unknown: start over on a fresh connection.
        return this._lost(connection.id, error)
      }
      // Recovered once the channels are subscribed, not when the socket connects: a refused SUBSCRIBE keeps backing off.
      this._reconnectDelay = RECONNECT_DELAY_MIN_MS
      this._outageReported = false
    })
  }

  private async _reconcileOnce({ id, socket, subscribed }: Connected): Promise<void> {
    const stale = [...subscribed].filter((channel) => !this._byChannel.has(channel))
    const missing = [...this._byChannel.keys()].filter((channel) => !subscribed.has(channel))
    if (stale.length > 0) {
      for (const channel of stale) subscribed.delete(channel)
      await socket.unsubscribe(...stale)
    }
    if (missing.length > 0) {
      await socket.subscribe(...missing)
      if (!this._isCurrent(id)) return
      for (const channel of missing) subscribed.add(channel)
    }
    // An attempt attached during the SUBSCRIBE above waits for the reconcile it queued.
    const covered = [...this._attempts()].filter(
      (attempt) => attempt.awaitsConfirmation() && attempt.channels.every((channel) => subscribed.has(channel)),
    )
    await Promise.all(covered.map((attempt) => this._confirm(attempt, id)))
  }

  /** Its channels are subscribed on a live connection: ready, unless its incarnation is no longer open. */
  private async _confirm(attempt: RedisSubscriptionAttempt, id: number): Promise<void> {
    const { source } = attempt
    if ('roomId' in source && !(await this._validateGeneration(source))) {
      if (this._isCurrent(id))
        attempt.terminate(new Error(`subscribeLane: generation '${source.roomId}/${source.inc}' is not open`))
      return
    }
    if (this._isCurrent(id)) attempt.markReady()
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
  private _lastSequence = 0
  private _cleanup: Promise<void> | null = null

  constructor(
    readonly source: RedisSubscriptionSource,
    readonly laneChannel: string,
    /** A Room lane's generation channel: a message on it means the generation was dropped. */
    readonly invalidationChannel: string | null,
    private readonly _receiver: BackendReceiver,
    private readonly _localReceiverCount: () => number,
    private readonly _onDetach: () => void,
  ) {
    super()
    this.channels = invalidationChannel === null ? [laneChannel] : [laneChannel, invalidationChannel]
  }

  unsubscribe(): Promise<void> {
    this._cleanup ??= this._dispose()
    return this._cleanup
  }

  prepareFence(token: string): Promise<void> | null {
    if (this._localReceiverCount() === 0 || this.state() !== 'ready') return null
    const fence = createDeferred()
    this._fences.set(token, fence)
    return fence.promise
  }

  /** Resolves the fence: its token arrived, or the commit that armed it was refused. */
  settleFence(token: string): void {
    this._fences.get(token)?.resolve()
    this._fences.delete(token)
  }

  awaitsConfirmation(): boolean {
    return this.state() === 'establishing' || this.state() === 'lost'
  }

  markReady(): void {
    if (!this.awaitsConfirmation()) return
    // Frames published while lost are gone; a Redis restarted without its data may restart the sequence.
    this._lastSequence = 0
    this.transition('ready')
  }

  terminate(error: unknown): void {
    if (this.ended) return
    this._rejectFences(error)
    this.transition('terminated', error)
  }

  lose(error: unknown): void {
    if (this.state() !== 'ready') return
    this._rejectFences(error)
    this.transition('lost')
  }

  receive(channel: string, frame: Buffer): void {
    if (channel === this.invalidationChannel)
      return this.terminate(new Error('Redis generation subscription was invalidated'))
    if (this.state() !== 'ready') return
    if (frame[0] === REDIS_DELIVERY_FENCE_BYTE) {
      return this.settleFence(frame.subarray(1).toString())
    }
    const { payload, info } = decodeOrderingFrame(frame)
    // Redis Cluster can forward publications from the old and new slot owners over independent bus
    // paths during resharding. Preserve ordered at-most-once delivery by dropping a late frame; gaps
    // remain loss, never replay.
    if (info.seq <= this._lastSequence) return
    this._lastSequence = info.seq
    void this._receiver(Uint8Array.from(payload), info)
  }

  private async _dispose(): Promise<void> {
    this._rejectFences(new Error('Redis delivery fence was closed'))
    this.transition('closed', new Error(`Redis subscription '${this.laneChannel}' was closed`))
    this._onDetach()
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
