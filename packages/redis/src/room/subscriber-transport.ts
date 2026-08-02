import { randomUUID } from 'node:crypto'
import type { Redis } from 'ioredis'
import type {
  BackendReceiver,
  BroadcastLane,
  RoomSubscriptionSource,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionBinding,
  SubscriptionDriver,
} from 'telefunc/__internal'
import {
  broadcastChannel,
  channelKey,
  decodeRedisOrderingFrame,
  generationInvalidationChannel,
  laneKey,
  REDIS_DELIVERY_FENCE_BYTE,
} from './layout.js'
type RedisSubscriptionSource = BroadcastLane | RoomSubscriptionSource
type RedisSubscriptionDriverOptions = {
  prefix: string
  createSubscriber: () => Promise<Redis>
  captureGeneration: (source: RoomSubscriptionSource) => Promise<string | null>
  validateGeneration: (source: RoomSubscriptionSource, token: string) => Promise<boolean>
}
/**
 * Redis's only backend-specific subscription edge. General fan-out, refcounts, readiness generations,
 * attempt epochs, ownership checks, and terminal signalling live in core's supervised backend.
 */
export class RedisSubscriptionDriver implements SubscriptionDriver<RedisSubscriptionSource> {
  private readonly _prefix: string
  private readonly _createSubscriber: () => Promise<Redis>
  private readonly _captureGeneration: RedisSubscriptionDriverOptions['captureGeneration']
  private readonly _validateGeneration: RedisSubscriptionDriverOptions['validateGeneration']
  private readonly _attempts = new Map<string, Set<RedisSubscriptionAttempt>>()
  constructor(options: RedisSubscriptionDriverOptions) {
    this._prefix = options.prefix
    this._createSubscriber = options.createSubscriber
    this._captureGeneration = options.captureGeneration
    this._validateGeneration = options.validateGeneration
  }
  bind(source: RedisSubscriptionSource): SubscriptionBinding {
    return {
      partition: '',
      valid: () => true,
      open: (receiver, localReceiverCount) => this._open(source, receiver, localReceiverCount),
    }
  }
  private _open(
    source: RedisSubscriptionSource,
    receiver: BackendReceiver,
    localReceiverCount: () => number,
  ): SubscriptionAttempt {
    const key = redisSubscriptionChannel(this._prefix, source)
    let attempt!: RedisSubscriptionAttempt
    attempt = new RedisSubscriptionAttempt({
      prefix: this._prefix,
      source,
      receiver,
      localReceiverCount,
      createSubscriber: this._createSubscriber,
      captureGeneration: this._captureGeneration,
      validateGeneration: this._validateGeneration,
      onDisposed: () => {
        const attempts = this._attempts.get(key)
        attempts?.delete(attempt)
        if (attempts?.size === 0) this._attempts.delete(key)
      },
    })
    const attempts = this._attempts.get(key) ?? new Set<RedisSubscriptionAttempt>()
    attempts.add(attempt)
    this._attempts.set(key, attempts)
    return attempt
  }
  prepareFlush(source: RoomSubscriptionSource): { token: string; delivery: Promise<void>; cancel(): void } {
    const token = randomUUID()
    const attempts = [...(this._attempts.get(redisSubscriptionChannel(this._prefix, source)) ?? [])]
    const armed = attempts.flatMap((attempt) => {
      const delivery = attempt.prepareFlush(token)
      return delivery === null ? [] : [delivery]
    })
    return {
      token: armed.length === 0 ? '' : token,
      delivery: Promise.all(armed).then(() => {}),
      cancel: () => {
        for (const attempt of attempts) attempt.cancelFlush(token)
      },
    }
  }
}
type RedisSubscriptionAttemptOptions = RedisSubscriptionDriverOptions & {
  source: RedisSubscriptionSource
  receiver: BackendReceiver
  localReceiverCount: () => number
  onDisposed: () => void
}
class RedisSubscriptionAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  private readonly _prefix: string
  private readonly _source: RedisSubscriptionSource
  private readonly _receiver: BackendReceiver
  private readonly _localReceiverCount: () => number
  private readonly _createSubscriber: () => Promise<Redis>
  private readonly _captureGeneration: RedisSubscriptionDriverOptions['captureGeneration']
  private readonly _validateGeneration: RedisSubscriptionDriverOptions['validateGeneration']
  private readonly _onDisposed: () => void
  private readonly _listeners = new Set<(state: SubscriptionAttemptState) => void>()
  private _generationToken: string | null = null
  private _settle!: { resolve: () => void; reject: (error: unknown) => void }
  private _state: SubscriptionAttemptState = 'establishing'
  private _subscriber: Redis | null = null
  private _subscribed = false
  private _readySettled = false
  private _cleanup: Promise<void> | null = null
  private _lastError: unknown = new Error('Redis subscriber connection closed')
  private _lastSequence = 0
  private readonly _flushes = new Map<string, { resolve(): void; reject(error: unknown): void }>()
  constructor(options: RedisSubscriptionAttemptOptions) {
    this._prefix = options.prefix
    this._source = options.source
    this._receiver = options.receiver
    this._localReceiverCount = options.localReceiverCount
    this._createSubscriber = options.createSubscriber
    this._captureGeneration = options.captureGeneration
    this._validateGeneration = options.validateGeneration
    this._onDisposed = options.onDisposed
    this.ready = new Promise<void>((resolve, reject) => {
      this._settle = { resolve, reject }
    })
    void this.ready.catch(() => {})
    void this._establish()
  }
  state(): SubscriptionAttemptState {
    return this._state
  }

  onStateChange(listener: (state: SubscriptionAttemptState) => void): () => void {
    this._listeners.add(listener)
    return () => this._listeners.delete(listener)
  }

  unsubscribe(): Promise<void> {
    if (this._cleanup !== null) return this._cleanup
    this._cleanup = this._dispose()
    return this._cleanup
  }

  prepareFlush(token: string): Promise<void> | null {
    const subscriber = this._subscriber
    if (
      this._localReceiverCount() === 0 ||
      this._state !== 'ready' ||
      subscriber === null ||
      subscriber.status !== 'ready'
    )
      return null
    return new Promise<void>((resolve, reject) => this._flushes.set(token, { resolve, reject }))
  }

  cancelFlush(token: string): void {
    const flush = this._flushes.get(token)
    if (flush === undefined) return
    this._flushes.delete(token)
    flush.resolve()
  }

  private async _establish(): Promise<void> {
    try {
      if ((await this._captureAttemptGeneration()) !== 'valid') return
      const subscriber = await this._createSubscriber()
      if (this._isStopped()) {
        subscriber.disconnect()
        return
      }
      this._bindSubscriber(subscriber)
      await subscriber.subscribe(...redisSubscriptionChannels(this._prefix, this._source))
      this._subscribed = true
      if (!(await this._validateEstablishedAttempt(subscriber))) return
      this._resolveReady()
      this._transition('ready')
    } catch (error) {
      this._failEstablish(error)
    }
  }
  private async _captureAttemptGeneration(): Promise<'valid' | 'absent' | 'stopped'> {
    if (!('roomId' in this._source)) return 'valid'
    this._generationToken = await this._captureGeneration(this._source)
    if (this._generationToken === null) {
      this._terminate(new Error(`subscribeLane: generation '${this._source.roomId}/${this._source.inc}' is absent`))
      return 'absent'
    }
    return this._isStopped() ? 'stopped' : 'valid'
  }
  private _bindSubscriber(subscriber: Redis): void {
    this._subscriber = subscriber
    subscriber.on('messageBuffer', this._onMessage)
    subscriber.on('close', this._onConnectionClosed)
    subscriber.on('end', this._onConnectionClosed)
    subscriber.on('error', this._onError)
  }
  private async _validateEstablishedAttempt(subscriber: Redis): Promise<boolean> {
    if (this._isStopped() || this._subscriber !== subscriber) return false
    if (
      'roomId' in this._source &&
      (this._generationToken === null || !(await this._validateGeneration(this._source, this._generationToken)))
    ) {
      this._terminate(
        new Error(`subscribeLane: generation '${this._source.roomId}/${this._source.inc}' was invalidated`),
      )
      return false
    }
    return !this._isStopped() && this._subscriber === subscriber
  }
  private _failEstablish(error: unknown): void {
    if (this._isStopped()) return
    this._lastError = error
    this._rejectReady(error)
    this._transition('closed')
  }

  private async _dispose(): Promise<void> {
    this._rejectFlushes(new Error('Redis delivery fence was closed'))
    if (this._state !== 'terminated') {
      this._transition('closed')
      this._rejectReady(
        new Error(`Redis subscription '${redisSubscriptionChannel(this._prefix, this._source)}' was closed`),
      )
    }
    const subscriber = this._subscriber
    this._subscriber = null
    if (subscriber !== null) {
      subscriber.off('messageBuffer', this._onMessage)
      subscriber.off('close', this._onConnectionClosed)
      subscriber.off('end', this._onConnectionClosed)
      subscriber.off('error', this._onError)
      try {
        if (this._subscribed && subscriber.status === 'ready') {
          await subscriber.unsubscribe(...redisSubscriptionChannels(this._prefix, this._source))
        }
        await subscriber.quit()
      } catch {
        subscriber.disconnect()
      }
    }
    this._onDisposed()
  }

  private readonly _onMessage = (channelBytes: Buffer, frame: Buffer): void => {
    const channel = channelBytes.toString()
    if ('roomId' in this._source && channel === redisInvalidationChannel(this._prefix, this._source)) {
      if (this._generationToken === null || frame.toString() === this._generationToken) {
        this._terminate(new Error('Redis generation subscription was invalidated'))
      }
      return
    }
    if (channel !== redisSubscriptionChannel(this._prefix, this._source) || this._state !== 'ready') return
    if (frame[0] === REDIS_DELIVERY_FENCE_BYTE) {
      const token = frame.subarray(1).toString()
      const flush = this._flushes.get(token)
      if (flush !== undefined) {
        this._flushes.delete(token)
        flush.resolve()
      }
      return
    }
    const { payload, info } = decodeRedisOrderingFrame(frame)
    // Redis Cluster can forward publications from the old and new slot owners over independent bus
    // paths during resharding. Preserve ordered at-most-once delivery by dropping a late frame; gaps
    // remain loss, never replay.
    if (info.seq <= this._lastSequence) return
    this._lastSequence = info.seq
    try {
      const result = this._receiver(Uint8Array.from(payload), info) as unknown
      if (result instanceof Promise) void result.catch((error: unknown) => console.error(error))
    } catch (error) {
      console.error(error)
    }
  }

  private readonly _onConnectionClosed = (): void => {
    this._connectionClosed()
  }

  private readonly _onError = (error: unknown): void => {
    this._lastError = error
  }

  private _connectionClosed(): void {
    if (this._isStopped()) return
    this._rejectReady(this._lastError)
    this._rejectFlushes(this._lastError)
    this._transition('closed')
  }

  private _rejectFlushes(error: unknown): void {
    for (const flush of this._flushes.values()) flush.reject(error)
    this._flushes.clear()
  }

  private _isStopped(): boolean {
    return this._state === 'closed' || this._state === 'terminated'
  }

  private _terminate(error: unknown): void {
    if (this._isStopped()) return
    this._lastError = error
    this._resolveReady()
    this._rejectFlushes(error)
    this._transition('terminated')
  }

  private _resolveReady(): void {
    if (this._readySettled) return
    this._readySettled = true
    this._settle.resolve()
  }

  private _rejectReady(error: unknown): void {
    if (this._readySettled) return
    this._readySettled = true
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
  return 'roomId' in source ? [channel, redisInvalidationChannel(prefix, source)] : [channel]
}

function redisSubscriptionChannel(prefix: string, source: RedisSubscriptionSource): string {
  if (!('roomId' in source)) return broadcastChannel(prefix, source)
  return channelKey(prefix, source.roomId, source.inc, laneKey(source.lane))
}

function redisInvalidationChannel(prefix: string, source: RoomSubscriptionSource): string {
  return generationInvalidationChannel(prefix, source.roomId, source.inc)
}
