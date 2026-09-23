import { randomUUID } from 'node:crypto'
import type {
  BackendReceiver,
  BroadcastLane,
  RoomSubscriptionSource,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionBinding,
  SubscriptionDriver,
} from 'telefunc/__internal'
import { decodeOrderingFrame } from 'telefunc/__internal'
import {
  broadcastChannel,
  channelKey,
  generationInvalidationChannel,
  laneKey,
  REDIS_DELIVERY_FENCE_BYTE,
} from './layout.js'
import type { SubscriberSocket } from '../ioredis.js'
type RedisSubscriptionSource = BroadcastLane | RoomSubscriptionSource
type RedisSubscriptionDriverOptions = {
  prefix: string
  /** A fresh, unconnected subscriber socket that never retries on its own. */
  createSubscriber: () => Promise<SubscriberSocket>
  /** Whether the source's incarnation is still the open head. */
  validateGeneration: (source: RoomSubscriptionSource) => Promise<boolean>
}

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
  private readonly _attempts = new Map<string, Set<RedisSubscriptionAttempt>>()
  private _subscriber: SubscriberSocket | null = null
  /** Bumped per connection, so work that awaited across a drop sees it is stale. */
  private _connection = 0
  private _connecting = false
  private _connected = false
  private readonly _subscribed = new Set<string>()
  private _reconciling: Promise<void> = Promise.resolve()
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private _reconnectDelay = RECONNECT_DELAY_MIN_MS
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

  prepareFlush(source: RoomSubscriptionSource): { token: string; delivery: Promise<void>; cancel(): void } {
    const token = randomUUID()
    const attempts = [...(this._attempts.get(redisSubscriptionChannel(this._prefix, source)) ?? [])]
    const armed = attempts.flatMap((attempt) => {
      const delivery = attempt.prepareFlush(token)
      return delivery === null ? [] : [delivery]
    })
    const delivery = Promise.all(armed).then(() => {})
    // A subscriber can drop before the commit awaiting this delivery returns.
    void delivery.catch(() => {})
    return {
      token: armed.length === 0 ? '' : token,
      delivery,
      cancel: () => {
        for (const attempt of attempts) attempt.cancelFlush(token)
      },
    }
  }

  private _open(
    source: RedisSubscriptionSource,
    receiver: BackendReceiver,
    localReceiverCount: () => number,
  ): SubscriptionAttempt {
    const channel = redisSubscriptionChannel(this._prefix, source)
    const attempt: RedisSubscriptionAttempt = new RedisSubscriptionAttempt(
      source,
      redisSubscriptionChannels(this._prefix, source),
      receiver,
      localReceiverCount,
      () => this._detach(channel, attempt),
    )
    const attempts = this._attempts.get(channel) ?? new Set<RedisSubscriptionAttempt>()
    attempts.add(attempt)
    this._attempts.set(channel, attempts)
    if (this._connected) this._reconcile()
    else if (!this._connecting && this._reconnectTimer === null) void this._connect()
    return attempt
  }

  private _detach(channel: string, attempt: RedisSubscriptionAttempt): void {
    const attempts = this._attempts.get(channel)
    attempts?.delete(attempt)
    if (attempts?.size === 0) this._attempts.delete(channel)
    if (this._attempts.size > 0) return this._reconcile()
    // Nothing left to deliver to: release the connection until the next subscription.
    if (this._reconnectTimer !== null) clearTimeout(this._reconnectTimer)
    this._reconnectTimer = null
    const subscriber = this._subscriber
    this._down()
    subscriber?.disconnect()
  }

  private async _connect(): Promise<void> {
    this._reconnectTimer = null
    this._connecting = true
    const connection = ++this._connection
    let subscriber: SubscriberSocket
    try {
      subscriber = await this._createSubscriber()
    } catch (error) {
      return this._lost(connection, error)
    }
    if (connection !== this._connection || this._attempts.size === 0) return subscriber.disconnect()
    this._subscriber = subscriber
    subscriber.on('messageBuffer', (channel: Buffer, frame: Buffer) => {
      if (connection === this._connection) this._dispatch(channel.toString(), frame)
    })
    subscriber.on('error', (error: unknown) => {
      this._lastError = error
    })
    subscriber.on('close', () => this._lost(connection, this._lastError))
    try {
      await subscriber.connect()
    } catch (error) {
      return this._lost(connection, error)
    }
    if (connection !== this._connection) return
    this._connecting = false
    this._connected = true
    this._reconnectDelay = RECONNECT_DELAY_MIN_MS
    this._reconcile()
  }

  private _lost(connection: number, error: unknown): void {
    if (connection !== this._connection) return
    const subscriber = this._subscriber
    this._down()
    subscriber?.disconnect()
    for (const attempts of this._attempts.values()) for (const attempt of attempts) attempt.lose(error)
    if (this._attempts.size === 0 || this._reconnectTimer !== null) return
    this._reconnectTimer = setTimeout(() => void this._connect(), this._reconnectDelay)
    this._reconnectTimer.unref()
    this._reconnectDelay = Math.min(this._reconnectDelay * 2, RECONNECT_DELAY_MAX_MS)
  }

  private _down(): void {
    this._connection++
    this._subscriber = null
    this._connecting = false
    this._connected = false
    this._subscribed.clear()
  }

  /** Serialized: brings the connection's channel set to the attempts' and confirms what it covers. */
  private _reconcile(): void {
    this._reconciling = this._reconciling.then(async () => {
      const connection = this._connection
      try {
        await this._reconcileOnce(connection)
      } catch (error) {
        // A failed (UN)SUBSCRIBE leaves the channel set unknown: start over on a fresh connection.
        this._lost(connection, error)
      }
    })
  }

  private async _reconcileOnce(connection: number): Promise<void> {
    const subscriber = this._subscriber
    if (subscriber === null || !this._connected) return
    const wanted = new Set<string>()
    for (const attempts of this._attempts.values())
      for (const attempt of attempts) for (const channel of attempt.channels) wanted.add(channel)
    const stale = [...this._subscribed].filter((channel) => !wanted.has(channel))
    const missing = [...wanted].filter((channel) => !this._subscribed.has(channel))
    if (stale.length > 0) {
      for (const channel of stale) this._subscribed.delete(channel)
      await subscriber.unsubscribe(...stale)
    }
    if (missing.length > 0) {
      await subscriber.subscribe(...missing)
      if (connection !== this._connection) return
      for (const channel of missing) this._subscribed.add(channel)
    }
    // An attempt attached during the SUBSCRIBE above waits for the reconcile it queued.
    const pending = [...this._attempts.values()].flatMap((attempts) =>
      [...attempts].filter(
        (attempt) => attempt.awaitsConfirmation() && attempt.channels.every((channel) => this._subscribed.has(channel)),
      ),
    )
    await Promise.all(
      pending.map((attempt) => attempt.confirm(this._validateGeneration, () => connection === this._connection)),
    )
  }

  private _dispatch(channel: string, frame: Buffer): void {
    // A lane's own channel indexes its attempts; its generation's invalidation channel is shared by all lanes.
    const attempts =
      this._attempts.get(channel) ??
      [...this._attempts.values()].flatMap((lane) => [...lane].filter((attempt) => attempt.channels.includes(channel)))
    for (const attempt of [...attempts]) attempt.receive(channel, frame)
  }
}

class RedisSubscriptionAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  private readonly _listeners = new Set<(state: SubscriptionAttemptState) => void>()
  private readonly _flushes = new Map<string, { resolve(): void; reject(error: unknown): void }>()
  private _settle!: { resolve: () => void; reject: (error: unknown) => void }
  private _state: SubscriptionAttemptState = 'establishing'
  private _lastSequence = 0
  private _cleanup: Promise<void> | null = null

  constructor(
    private readonly _source: RedisSubscriptionSource,
    readonly channels: readonly string[],
    private readonly _receiver: BackendReceiver,
    private readonly _localReceiverCount: () => number,
    private readonly _onDetach: () => void,
  ) {
    this.ready = new Promise<void>((resolve, reject) => {
      this._settle = { resolve, reject }
    })
    void this.ready.catch(() => {})
  }

  state(): SubscriptionAttemptState {
    return this._state
  }

  onStateChange(listener: (state: SubscriptionAttemptState) => void): () => void {
    this._listeners.add(listener)
    return () => this._listeners.delete(listener)
  }

  unsubscribe(): Promise<void> {
    this._cleanup ??= this._dispose()
    return this._cleanup
  }

  prepareFlush(token: string): Promise<void> | null {
    if (this._localReceiverCount() === 0 || this._state !== 'ready') return null
    return new Promise<void>((resolve, reject) => this._flushes.set(token, { resolve, reject }))
  }

  cancelFlush(token: string): void {
    const flush = this._flushes.get(token)
    if (flush === undefined) return
    this._flushes.delete(token)
    flush.resolve()
  }

  awaitsConfirmation(): boolean {
    return this._state === 'establishing' || this._state === 'lost'
  }

  /** Its channels are subscribed on a live connection: ready, unless its incarnation is no longer open. */
  async confirm(
    validateGeneration: RedisSubscriptionDriverOptions['validateGeneration'],
    isCurrent: () => boolean,
  ): Promise<void> {
    if ('roomId' in this._source && !(await validateGeneration(this._source))) {
      if (isCurrent())
        this._terminate(new Error(`subscribeLane: generation '${this._source.roomId}/${this._source.inc}' is not open`))
      return
    }
    if (!isCurrent() || !this.awaitsConfirmation()) return
    // Frames published while lost are gone; a Redis restarted without its data may restart the sequence.
    this._lastSequence = 0
    this._resolveReady()
    this._transition('ready')
  }

  lose(error: unknown): void {
    if (this._state !== 'ready') return
    this._rejectFlushes(error)
    this._transition('lost')
  }

  receive(channel: string, frame: Buffer): void {
    if ('roomId' in this._source && channel === this.channels[1]) {
      this._terminate(new Error('Redis generation subscription was invalidated'))
      return
    }
    if (this._state !== 'ready') return
    if (frame[0] === REDIS_DELIVERY_FENCE_BYTE) {
      const token = frame.subarray(1).toString()
      const flush = this._flushes.get(token)
      if (flush !== undefined) {
        this._flushes.delete(token)
        flush.resolve()
      }
      return
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
    this._rejectFlushes(new Error('Redis delivery fence was closed'))
    if (this._state !== 'terminated') {
      this._transition('closed')
      this._rejectReady(new Error(`Redis subscription '${this.channels[0]}' was closed`))
    }
    this._onDetach()
  }

  private _terminate(error: unknown): void {
    if (this._state === 'closed' || this._state === 'terminated') return
    this._resolveReady()
    this._rejectFlushes(error)
    this._transition('terminated')
  }

  private _rejectFlushes(error: unknown): void {
    for (const flush of this._flushes.values()) flush.reject(error)
    this._flushes.clear()
  }

  private _resolveReady(): void {
    this._settle.resolve()
  }

  private _rejectReady(error: unknown): void {
    this._settle.reject(error)
  }

  private _transition(state: SubscriptionAttemptState): void {
    if (this._state === state) return
    this._state = state
    for (const listener of [...this._listeners]) listener(state)
  }
}

function redisSubscriptionChannels(prefix: string, source: RedisSubscriptionSource): string[] {
  const channel = redisSubscriptionChannel(prefix, source)
  return 'roomId' in source ? [channel, generationInvalidationChannel(prefix, source.roomId, source.inc)] : [channel]
}

function redisSubscriptionChannel(prefix: string, source: RedisSubscriptionSource): string {
  if (!('roomId' in source)) return broadcastChannel(prefix, source)
  return channelKey(prefix, source.roomId, source.inc, laneKey(source.lane))
}
