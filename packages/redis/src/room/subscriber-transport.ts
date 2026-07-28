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
 * replacement epochs, bounded replanning and watchdogs live in core's supervised backend.
 */
export class RedisSubscriptionDriver implements SubscriptionDriver {
  readonly #prefix: string
  readonly #createSubscriber: () => Promise<Redis>
  readonly #captureGeneration: RedisSubscriptionDriverOptions['captureGeneration']
  readonly #validateGeneration: RedisSubscriptionDriverOptions['validateGeneration']
  readonly #attempts = new Map<string, Set<RedisSubscriptionAttempt>>()

  constructor(options: RedisSubscriptionDriverOptions) {
    this.#prefix = options.prefix
    this.#createSubscriber = options.createSubscriber
    this.#captureGeneration = options.captureGeneration
    this.#validateGeneration = options.validateGeneration
  }

  bind(source: BackendSubscriptionSource): SubscriptionBinding {
    return {
      partition: '',
      valid: () => true,
      open: (receiver, localReceiverCount) => this.#open(source, receiver, localReceiverCount),
    }
  }

  #open(
    source: BackendSubscriptionSource,
    receiver: BackendReceiver,
    localReceiverCount: () => number,
  ): SubscriptionAttempt {
    const key = redisSubscriptionChannel(this.#prefix, source)
    let attempt!: RedisSubscriptionAttempt
    attempt = new RedisSubscriptionAttempt({
      prefix: this.#prefix,
      source,
      receiver,
      localReceiverCount,
      createSubscriber: this.#createSubscriber,
      captureGeneration: this.#captureGeneration,
      validateGeneration: this.#validateGeneration,
      onDisposed: () => {
        const attempts = this.#attempts.get(key)
        attempts?.delete(attempt)
        if (attempts?.size === 0) this.#attempts.delete(key)
      },
    })
    const attempts = this.#attempts.get(key) ?? new Set<RedisSubscriptionAttempt>()
    attempts.add(attempt)
    this.#attempts.set(key, attempts)
    return attempt
  }

  async flush(source: BackendSubscriptionSource): Promise<void> {
    const attempts = [...(this.#attempts.get(redisSubscriptionChannel(this.#prefix, source)) ?? [])]
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
  readonly #prefix: string
  readonly #source: BackendSubscriptionSource
  readonly #receiver: BackendReceiver
  readonly #localReceiverCount: () => number
  readonly #createSubscriber: () => Promise<Redis>
  readonly #captureGeneration: RedisSubscriptionDriverOptions['captureGeneration']
  readonly #validateGeneration: RedisSubscriptionDriverOptions['validateGeneration']
  readonly #onDisposed: () => void
  readonly #listeners = new Set<(state: SubscriptionAttemptState) => void>()
  readonly #generation: RedisGenerationAttempt = {
    attemptId: randomUUID(),
    createdAt: null,
    generationToken: null,
  }
  #settle!: { resolve: () => void; reject: (error: unknown) => void }
  #state: SubscriptionAttemptState = 'establishing'
  #subscriber: Redis | null = null
  #subscribed = false
  #readySettled = false
  #cleanup: Promise<void> | null = null
  #lastError: unknown = new Error('Redis subscriber connection closed')

  constructor(options: RedisSubscriptionAttemptOptions) {
    this.#prefix = options.prefix
    this.#source = options.source
    this.#receiver = options.receiver
    this.#localReceiverCount = options.localReceiverCount
    this.#createSubscriber = options.createSubscriber
    this.#captureGeneration = options.captureGeneration
    this.#validateGeneration = options.validateGeneration
    this.#onDisposed = options.onDisposed
    this.ready = new Promise<void>((resolve, reject) => {
      this.#settle = { resolve, reject }
    })
    void this.ready.catch(() => {})
    void this.#establish()
  }

  state(): SubscriptionAttemptState {
    return this.#state
  }

  onStateChange(listener: (state: SubscriptionAttemptState) => void): () => void {
    this.#listeners.add(listener)
    return () => this.#listeners.delete(listener)
  }

  unsubscribe(): Promise<void> {
    if (this.#cleanup !== null) return this.#cleanup
    this.#cleanup = this.#dispose()
    return this.#cleanup
  }

  async flush(): Promise<void> {
    if (this.#state !== 'ready' || this.#localReceiverCount() === 0) return
    const subscriber = this.#subscriber
    const source = redisSubscriptionChannel(this.#prefix, this.#source)
    if (subscriber === null || subscriber.status !== 'ready') {
      throw new Error(`Redis subscriber PING cannot fence unavailable source '${source}'`)
    }
    await subscriber.ping()
    if (this.#state !== 'ready' || this.#subscriber !== subscriber) {
      throw new Error(`Redis subscriber PING crossed source '${source}'`)
    }
  }

  async #establish(): Promise<void> {
    try {
      if (this.#source.kind === 'durable') {
        await this.#captureGeneration(this.#source, this.#generation)
        if (this.#state === 'closed') return
      }
      const subscriber = await this.#createSubscriber()
      if (this.#state === 'closed') {
        subscriber.disconnect()
        return
      }
      this.#subscriber = subscriber
      subscriber.on('messageBuffer', this.#onMessage)
      subscriber.on('close', this.#onClose)
      subscriber.on('end', this.#onEnd)
      subscriber.on('error', this.#onError)

      const channels = redisSubscriptionChannels(this.#prefix, this.#source)
      // Raw readiness settles only after the real Redis acknowledgement. Core, not this driver, owns
      // the deadline, retries, readiness generations and attempt epochs.
      await subscriber.subscribe(...channels)
      this.#subscribed = true
      if (this.#isClosed() || this.#subscriber !== subscriber) return

      if (this.#source.kind === 'durable' && !(await this.#validateGeneration(this.#source, this.#generation))) {
        throw new Error(`subscribeLane: generation '${this.#source.roomId}/${this.#source.inc}' was invalidated`)
      }
      if (this.#isClosed() || this.#subscriber !== subscriber) return
      this.#readySettled = true
      this.#settle.resolve()
      this.#transition('ready')
    } catch (error) {
      if (this.#state === 'closed') return
      this.#lastError = error
      this.#rejectReady(error)
      this.#transition('closed')
    }
  }

  async #dispose(): Promise<void> {
    this.#transition('closed')
    this.#rejectReady(
      new Error(`Redis subscription '${redisSubscriptionChannel(this.#prefix, this.#source)}' was closed`),
    )
    const subscriber = this.#subscriber
    this.#subscriber = null
    if (subscriber !== null) {
      subscriber.off('messageBuffer', this.#onMessage)
      subscriber.off('close', this.#onClose)
      subscriber.off('end', this.#onEnd)
      subscriber.off('error', this.#onError)
      try {
        if (this.#subscribed && subscriber.status === 'ready') {
          await subscriber.unsubscribe(...redisSubscriptionChannels(this.#prefix, this.#source))
        }
        await subscriber.quit()
      } catch {
        subscriber.disconnect()
      }
    }
    this.#onDisposed()
  }

  readonly #onMessage = (channelBytes: Buffer, frame: Buffer): void => {
    const channel = channelBytes.toString()
    if (this.#source.kind === 'durable' && channel === redisInvalidationChannel(this.#prefix, this.#source)) {
      if (this.#generation.generationToken === null || frame.toString() === this.#generation.generationToken) {
        this.#transition('closed')
      }
      return
    }
    if (channel !== redisSubscriptionChannel(this.#prefix, this.#source) || this.#state !== 'ready') return
    const { payload, info } = decodeRedisOrderingFrame(frame)
    try {
      const result = this.#receiver(Uint8Array.from(payload), info) as unknown
      if (result instanceof Promise) void result.catch((error: unknown) => console.error(error))
    } catch (error) {
      console.error(error)
    }
  }

  readonly #onClose = (): void => {
    this.#connectionClosed()
  }

  readonly #onEnd = (): void => {
    this.#connectionClosed()
  }

  readonly #onError = (error: unknown): void => {
    this.#lastError = error
  }

  #connectionClosed(): void {
    if (this.#state === 'closed') return
    this.#rejectReady(this.#lastError)
    this.#transition('closed')
  }

  #isClosed(): boolean {
    return this.#state === 'closed'
  }

  #rejectReady(error: unknown): void {
    if (this.#readySettled) return
    this.#readySettled = true
    this.#settle.reject(error)
  }

  #transition(state: SubscriptionAttemptState): void {
    if (this.#state === state) return
    this.#state = state
    for (const listener of [...this.#listeners]) listener(state)
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
