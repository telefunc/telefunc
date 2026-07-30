import { randomUUID } from 'node:crypto'
import type { Redis } from 'ioredis'
import type {
  BackendReceiver,
  BackendSubscriptionSource,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionBinding,
  SubscriptionDriver,
} from 'telefunc/backend'
import { channelKey, decodeRedisOrderingFrame, generationInvalidationChannel, laneKey } from './layout.js'

export type RedisGenerationAttempt = {
  attemptId: string
  createdAt: number | null
  generationToken: string | null
}

type RedisDurableSource = Extract<BackendSubscriptionSource, { kind: 'durable' }>

type RedisSubscriptionDriverOptions = {
  prefix: string
  createSubscriber: () => Promise<Redis>
  captureGeneration: (source: RedisDurableSource, attempt: RedisGenerationAttempt) => Promise<void>
  validateGeneration: (source: RedisDurableSource, attempt: RedisGenerationAttempt) => Promise<boolean>
}

/**
 * Redis's only backend-specific subscription edge. General fan-out, refcounts, readiness generations,
 * attempt epochs, ownership checks, and terminal signalling live in core's supervised backend.
 */
export class RedisSubscriptionDriver implements SubscriptionDriver {
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

  bind(source: BackendSubscriptionSource): SubscriptionBinding {
    return {
      partition: '',
      valid: () => true,
      open: (receiver, localReceiverCount) => this._open(source, receiver, localReceiverCount),
    }
  }

  private _open(
    source: BackendSubscriptionSource,
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

  async flush(source: BackendSubscriptionSource): Promise<void> {
    const attempts = [...(this._attempts.get(redisSubscriptionChannel(this._prefix, source)) ?? [])]
    await Promise.all(attempts.map((attempt) => attempt.flush()))
  }
}

type RedisSubscriptionAttemptOptions = RedisSubscriptionDriverOptions & {
  source: BackendSubscriptionSource
  receiver: BackendReceiver
  localReceiverCount: () => number
  onDisposed: () => void
}

class RedisSubscriptionAttempt implements SubscriptionAttempt {
  readonly ready: Promise<void>
  private readonly _prefix: string
  private readonly _source: BackendSubscriptionSource
  private readonly _receiver: BackendReceiver
  private readonly _localReceiverCount: () => number
  private readonly _createSubscriber: () => Promise<Redis>
  private readonly _captureGeneration: RedisSubscriptionDriverOptions['captureGeneration']
  private readonly _validateGeneration: RedisSubscriptionDriverOptions['validateGeneration']
  private readonly _onDisposed: () => void
  private readonly _listeners = new Set<(state: SubscriptionAttemptState) => void>()
  private readonly _generation: RedisGenerationAttempt = {
    attemptId: randomUUID(),
    createdAt: null,
    generationToken: null,
  }
  private _settle!: { resolve: () => void; reject: (error: unknown) => void }
  private _state: SubscriptionAttemptState = 'establishing'
  private _subscriber: Redis | null = null
  private _subscribed = false
  private _readySettled = false
  private _cleanup: Promise<void> | null = null
  private _lastError: unknown = new Error('Redis subscriber connection closed')

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

  async flush(): Promise<void> {
    if (this._localReceiverCount() === 0) return
    await this.ready
    if (this._localReceiverCount() === 0) return
    const subscriber = this._subscriber
    const source = redisSubscriptionChannel(this._prefix, this._source)
    if (this._state !== 'ready' || subscriber === null || subscriber.status !== 'ready') {
      throw new Error(`Redis subscriber PING cannot fence unavailable source '${source}'`)
    }
    await subscriber.ping()
    if (this._state !== 'ready' || this._subscriber !== subscriber) {
      throw new Error(`Redis subscriber PING crossed source '${source}'`)
    }
  }

  private async _establish(): Promise<void> {
    try {
      if (this._source.kind === 'durable') {
        await this._captureGeneration(this._source, this._generation)
        if (this._state === 'closed') return
      }
      const subscriber = await this._createSubscriber()
      if (this._state === 'closed') {
        subscriber.disconnect()
        return
      }
      this._subscriber = subscriber
      subscriber.on('messageBuffer', this._onMessage)
      subscriber.on('close', this._onClose)
      subscriber.on('end', this._onEnd)
      subscriber.on('error', this._onError)

      const channels = redisSubscriptionChannels(this._prefix, this._source)
      // Raw readiness settles only after the real Redis acknowledgement. Core, not this driver, owns
      // the deadline, retries, readiness generations and attempt epochs.
      await subscriber.subscribe(...channels)
      this._subscribed = true
      if (this._isClosed() || this._subscriber !== subscriber) return

      if (this._source.kind === 'durable' && !(await this._validateGeneration(this._source, this._generation))) {
        throw new Error(`subscribeLane: generation '${this._source.roomId}/${this._source.inc}' was invalidated`)
      }
      if (this._isClosed() || this._subscriber !== subscriber) return
      this._readySettled = true
      this._settle.resolve()
      this._transition('ready')
    } catch (error) {
      if (this._state === 'closed') return
      this._lastError = error
      this._rejectReady(error)
      this._transition('closed')
    }
  }

  private async _dispose(): Promise<void> {
    this._transition('closed')
    this._rejectReady(
      new Error(`Redis subscription '${redisSubscriptionChannel(this._prefix, this._source)}' was closed`),
    )
    const subscriber = this._subscriber
    this._subscriber = null
    if (subscriber !== null) {
      subscriber.off('messageBuffer', this._onMessage)
      subscriber.off('close', this._onClose)
      subscriber.off('end', this._onEnd)
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
    if (this._source.kind === 'durable' && channel === redisInvalidationChannel(this._prefix, this._source)) {
      if (this._generation.generationToken === null || frame.toString() === this._generation.generationToken) {
        this._transition('closed')
      }
      return
    }
    if (channel !== redisSubscriptionChannel(this._prefix, this._source) || this._state !== 'ready') return
    const { payload, info } = decodeRedisOrderingFrame(frame)
    try {
      const result = this._receiver(Uint8Array.from(payload), info) as unknown
      if (result instanceof Promise) void result.catch((error: unknown) => console.error(error))
    } catch (error) {
      console.error(error)
    }
  }

  private readonly _onClose = (): void => {
    this._connectionClosed()
  }

  private readonly _onEnd = (): void => {
    this._connectionClosed()
  }

  private readonly _onError = (error: unknown): void => {
    this._lastError = error
  }

  private _connectionClosed(): void {
    if (this._state === 'closed') return
    this._rejectReady(this._lastError)
    this._transition('closed')
  }

  private _isClosed(): boolean {
    return this._state === 'closed'
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

function redisSubscriptionChannels(prefix: string, source: BackendSubscriptionSource): string[] {
  const channel = redisSubscriptionChannel(prefix, source)
  return source.kind === 'durable' ? [channel, redisInvalidationChannel(prefix, source)] : [channel]
}

function redisSubscriptionChannel(prefix: string, source: BackendSubscriptionSource): string {
  if (source.kind === 'broadcast') {
    const route = source.lane.kind === 'text' ? 't' : 'b'
    return `${prefix}${route}:{${source.lane.key}}`
  }
  return channelKey(prefix, source.roomId, source.inc, laneKey(source.lane))
}

function redisInvalidationChannel(prefix: string, source: RedisDurableSource): string {
  return generationInvalidationChannel(prefix, source.roomId, source.inc)
}
