// Cohesive lifecycle boundary for Redis's one shared subscriber connection.
//
// The durable backend owns generation/capture identity and all data SPI operations. This transport owns
// only local callback multiplexing and connection-scoped broker state: channel refcounts, SUBSCRIBE
// acknowledgements, connection epochs, reconnect, readiness, detach, and fallible cleanup. Its central
// invariant is that no RedisLaneSubscription can become ready unless the channel acknowledgement and
// the delegated generation validation both completed on the CURRENT subscriber connection epoch.

import type { Redis } from 'ioredis'
import type { LaneReceiver, LaneSubscription, ReadinessState } from '../../../telefunc/wire-protocol/backend/spi.js'
import { decodeFrameHeader } from './layout.js'

const SUBSCRIPTION_RETRY_ATTEMPTS = 5

export class RedisGenerationInvalidError extends Error {}

export type RedisSubscriberChannelBinding = {
  channel: string
  owner: string
  invalidationChannel: string
}

export type RedisSubscriberTransportHooks = {
  beforeSubscribe?: (channel: string) => void | Promise<void>
  afterSubscribeAck?: (channel: string) => void | Promise<void>
}

type RedisSubscriberTransportOptions = {
  subscriber: Redis
  retryDelay: (attempt: number) => number
  hooks?: RedisSubscriberTransportHooks
  captureGeneration: (binding: RedisSubscriberChannelBinding) => Promise<void>
  validateGeneration: (binding: RedisSubscriberChannelBinding, includeCapture: boolean) => Promise<boolean>
  onGenerationInvalidation: (owner: string, token: string) => void
  onChannelRemoved: (binding: RedisSubscriberChannelBinding) => void
}

type ChannelLifecycle = {
  binding: RedisSubscriberChannelBinding
  subscriptions: Set<RedisLaneSubscription>
  operationEpoch: number
  initialEstablished: boolean
}

type ChannelAck = {
  connectionEpoch: number
  promise: Promise<boolean>
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

class RedisLaneSubscription implements LaneSubscription {
  readonly ready: Promise<void>
  #state: ReadinessState = 'establishing'
  #settle!: { resolve: () => void; reject: (err: unknown) => void }
  #readySettled = false
  readonly #listeners = new Set<(state: ReadinessState) => void>()
  readonly #receiver: LaneReceiver
  #detach: () => Promise<void> | void

  constructor(receiver: LaneReceiver, detach: () => Promise<void> | void) {
    this.#receiver = receiver
    this.#detach = detach
    this.ready = new Promise<void>((resolve, reject) => {
      this.#settle = { resolve, reject }
    })
    void this.ready.catch(() => {})
  }

  get closed(): boolean {
    return this.#state === 'closed'
  }

  state(): ReadinessState {
    return this.#state
  }

  onStateChange(cb: (state: ReadinessState) => void): () => void {
    this.#listeners.add(cb)
    return () => this.#listeners.delete(cb)
  }

  async unsubscribe(): Promise<void> {
    if (this.closed) return
    // Local-first close is also the attachment fence. Remote ownership remains in the channel lifecycle
    // until exact cleanup succeeds or the dead connection makes it unnecessary.
    this.#close(new Error('subscribeLane: unsubscribed before initial establishment'))
    await this.#detach()
  }

  established(): void {
    if (this.closed) return
    if (!this.#readySettled) {
      this.#readySettled = true
      this.#state = 'ready'
      this.#settle.resolve()
      return
    }
    this.#transition('ready')
  }

  connectionLost(reason: unknown): void {
    if (this.closed || this.#state === 'lost') return
    if (!this.#readySettled) {
      this.#readySettled = true
      this.#settle.reject(reason)
    }
    this.#transition('lost')
  }

  failPermanently(reason: unknown): void {
    this.#close(reason)
  }

  generationDropped(): void {
    this.#detach = () => {}
    this.#close(new Error('subscribeLane: generation dropped before initial establishment'))
  }

  deliver(payload: Uint8Array, info: { seq: number; timestamp: number }): void {
    this.#receiver(payload, info)
  }

  #transition(state: ReadinessState): void {
    if (this.#state === state) return
    this.#state = state
    for (const cb of this.#listeners) {
      try {
        cb(state)
      } catch {
        // Observation is advisory; a listener cannot corrupt lifecycle teardown or its siblings.
      }
    }
  }

  #close(reason: unknown): void {
    if (this.closed) return
    if (!this.#readySettled) {
      this.#readySettled = true
      this.#settle.reject(reason)
    }
    this.#transition('closed')
  }
}

export class RedisSubscriberTransport {
  readonly #subscriber: Redis
  readonly #retryDelay: (attempt: number) => number
  readonly #hooks?: RedisSubscriberTransportHooks
  readonly #captureGeneration: (binding: RedisSubscriberChannelBinding) => Promise<void>
  readonly #validateGeneration: (binding: RedisSubscriberChannelBinding, includeCapture: boolean) => Promise<boolean>
  readonly #onGenerationInvalidation: (owner: string, token: string) => void
  readonly #onChannelRemoved: (binding: RedisSubscriberChannelBinding) => void

  readonly #channels = new Map<string, ChannelLifecycle>()
  readonly #channelAcks = new Map<string, ChannelAck>()
  readonly #subscribedChannels = new Set<string>()
  readonly #subscribedInvalidations = new Set<string>()
  readonly #invalidationOwners = new Map<string, string>()

  #connectionEpoch = 0
  #readyEpoch: number | null
  readonly #connectionWaiters = new Set<() => void>()
  #disposed = false

  constructor(options: RedisSubscriberTransportOptions) {
    this.#subscriber = options.subscriber
    this.#retryDelay = options.retryDelay
    this.#hooks = options.hooks
    this.#captureGeneration = options.captureGeneration
    this.#validateGeneration = options.validateGeneration
    this.#onGenerationInvalidation = options.onGenerationInvalidation
    this.#onChannelRemoved = options.onChannelRemoved
    this.#readyEpoch = this.#subscriber.status === 'ready' ? this.#connectionEpoch : null

    this.#subscriber.on('messageBuffer', this.#onMessage)
    this.#subscriber.on('close', this.#onClose)
    this.#subscriber.on('ready', this.#onReady)
    this.#subscriber.on('end', this.#onEnd)
  }

  attach(binding: RedisSubscriberChannelBinding, receiver: LaneReceiver): LaneSubscription {
    if (this.#disposed) throw new Error('RedisSubscriberTransport: used after dispose()')
    let lifecycle = this.#channels.get(binding.channel)
    if (lifecycle === undefined) {
      lifecycle = {
        binding,
        subscriptions: new Set(),
        operationEpoch: 0,
        initialEstablished: false,
      }
      this.#channels.set(binding.channel, lifecycle)
      this.#invalidationOwners.set(binding.invalidationChannel, binding.owner)
    } else if (
      lifecycle.binding.owner !== binding.owner ||
      lifecycle.binding.invalidationChannel !== binding.invalidationChannel
    ) {
      throw new Error(`RedisSubscriberTransport: channel identity collision '${binding.channel}'`)
    }

    let subscription: RedisLaneSubscription
    subscription = new RedisLaneSubscription(receiver, () => this.#detach(lifecycle as ChannelLifecycle, subscription))
    lifecycle.subscriptions.add(subscription)
    void this.#ensureSubscribed(lifecycle).then(
      (currentAck) => {
        if (currentAck && !subscription.closed) subscription.established()
      },
      () => {},
    )
    return subscription
  }

  flush(channel: string): Promise<void> {
    const lifecycle = this.#channels.get(channel)
    if (lifecycle === undefined || lifecycle.subscriptions.size === 0) return Promise.resolve()
    return this.#subscriber.ping().then(() => {})
  }

  invalidateChannels(channels: ReadonlySet<string>): void {
    const owners = new Set<string>()
    for (const channel of channels) {
      const lifecycle = this.#channels.get(channel)
      if (lifecycle === undefined) continue
      owners.add(lifecycle.binding.owner)
      lifecycle.operationEpoch++
      for (const subscription of lifecycle.subscriptions) subscription.generationDropped()
      lifecycle.subscriptions.clear()
      void this.#retryEmptyChannelCleanup(lifecycle)
    }
    for (const owner of owners) void this.#retryOrphanInvalidationCleanup(owner)
  }

  async dropGeneration(owner: string): Promise<void> {
    const failures: unknown[] = []
    const lifecycles = [...this.#channels.values()].filter((candidate) => candidate.binding.owner === owner)
    const invalidations = new Set(lifecycles.map((candidate) => candidate.binding.invalidationChannel))

    for (const lifecycle of lifecycles) {
      lifecycle.operationEpoch++
      for (const subscription of lifecycle.subscriptions) subscription.generationDropped()
      lifecycle.subscriptions.clear()
      try {
        if (
          this.#subscribedChannels.has(lifecycle.binding.channel) &&
          !this.#disposed &&
          this.#subscriber.status !== 'end'
        ) {
          await this.#subscriber.unsubscribe(lifecycle.binding.channel)
        }
        this.#subscribedChannels.delete(lifecycle.binding.channel)
        this.#finishEmptyChannelCleanup(lifecycle)
      } catch (err) {
        failures.push(err)
      }
    }

    if (failures.length === 0) {
      for (const invalidation of invalidations) {
        try {
          if (this.#subscribedInvalidations.has(invalidation) && this.#subscriber.status !== 'end') {
            await this.#subscriber.unsubscribe(invalidation)
          }
          this.#subscribedInvalidations.delete(invalidation)
          this.#invalidationOwners.delete(invalidation)
        } catch (err) {
          failures.push(err)
        }
      }
    }
    if (failures.length > 0) throw failures[0]
  }

  async dispose(): Promise<void> {
    if (this.#disposed) return
    this.#disposed = true
    this.#wakeConnectionWaiters()
    for (const lifecycle of this.#channels.values()) {
      for (const subscription of lifecycle.subscriptions) subscription.generationDropped()
      this.#onChannelRemoved(lifecycle.binding)
    }
    this.#channels.clear()
    this.#channelAcks.clear()
    this.#subscribedChannels.clear()
    this.#subscribedInvalidations.clear()
    this.#invalidationOwners.clear()
    this.#subscriber.off('messageBuffer', this.#onMessage)
    this.#subscriber.off('close', this.#onClose)
    this.#subscriber.off('ready', this.#onReady)
    this.#subscriber.off('end', this.#onEnd)
    try {
      await this.#subscriber.quit()
    } catch {
      this.#subscriber.disconnect()
    }
  }

  #ensureSubscribed(lifecycle: ChannelLifecycle): Promise<boolean> {
    const channel = lifecycle.binding.channel
    const existing = this.#channelAcks.get(channel)
    if (existing !== undefined && existing.connectionEpoch === this.#connectionEpoch) return existing.promise
    if (this.#subscribedChannels.has(channel) && this.#readyEpoch === this.#connectionEpoch) {
      return Promise.resolve(true)
    }

    const operationEpoch = lifecycle.operationEpoch
    const connectionEpoch = this.#connectionEpoch
    const record: ChannelAck = { connectionEpoch, promise: Promise.resolve(false) }
    record.promise = this.#subscribeWithRetry(lifecycle, operationEpoch, connectionEpoch)
      .then(async (installed) => {
        if (!installed || !this.#attemptIsCurrent(lifecycle, operationEpoch, connectionEpoch)) return false
        if (lifecycle.subscriptions.size === 0) {
          if (this.#subscribedChannels.has(channel) && this.#subscriber.status !== 'end') {
            await this.#subscriber.unsubscribe(channel)
          }
          this.#subscribedChannels.delete(channel)
          return false
        }
        lifecycle.initialEstablished = true
        for (const subscription of lifecycle.subscriptions) subscription.established()
        return true
      })
      .catch((err: unknown) => {
        if (this.#attemptIsCurrent(lifecycle, operationEpoch, connectionEpoch)) {
          this.#subscriptionExhausted(lifecycle, err)
        }
        throw err
      })
      .finally(() => {
        if (this.#channelAcks.get(channel) === record) this.#channelAcks.delete(channel)
      })
    this.#channelAcks.set(channel, record)
    return record.promise
  }

  async #subscribeWithRetry(
    lifecycle: ChannelLifecycle,
    operationEpoch: number,
    connectionEpoch: number,
  ): Promise<boolean> {
    const { binding } = lifecycle
    let lastError: unknown = new Error(`subscribeLane: SUBSCRIBE '${binding.channel}' failed`)
    for (let attempt = 1; attempt <= SUBSCRIPTION_RETRY_ATTEMPTS; attempt++) {
      if (this.#disposed) throw new Error('RedisSubscriberTransport: disposed during subscription establishment')
      if (!this.#attemptIsCurrent(lifecycle, operationEpoch, connectionEpoch)) return false
      try {
        await this.#captureGeneration(binding)
        if (!this.#attemptIsCurrent(lifecycle, operationEpoch, connectionEpoch)) return false
        if (!(await this.#awaitConnectionReady(connectionEpoch))) return false
        await this.#hooks?.beforeSubscribe?.(binding.channel)
        if (!this.#attemptIsCurrent(lifecycle, operationEpoch, connectionEpoch)) return false

        await this.#subscriber.subscribe(binding.channel, binding.invalidationChannel)
        if (!this.#attemptIsCurrent(lifecycle, operationEpoch, connectionEpoch)) return false
        // Remote ownership is recorded at the acknowledgement boundary, before any held durable check,
        // so last detach can uninstall this exact current-connection attempt.
        this.#subscribedChannels.add(binding.channel)
        this.#subscribedInvalidations.add(binding.invalidationChannel)
        await this.#hooks?.afterSubscribeAck?.(binding.channel)
        if (!this.#attemptIsCurrent(lifecycle, operationEpoch, connectionEpoch)) return false

        const valid = await this.#validateGeneration(binding, !lifecycle.initialEstablished)
        if (!this.#attemptIsCurrent(lifecycle, operationEpoch, connectionEpoch)) return false
        if (!valid) {
          await this.#subscriber.unsubscribe(binding.channel)
          this.#subscribedChannels.delete(binding.channel)
          throw new RedisGenerationInvalidError(
            `subscribeLane: generation for channel '${binding.channel}' was invalidated`,
          )
        }
        return true
      } catch (err) {
        if (!this.#attemptIsCurrent(lifecycle, operationEpoch, connectionEpoch)) return false
        lastError = err
        if (err instanceof RedisGenerationInvalidError) throw err
        for (const subscription of lifecycle.subscriptions) subscription.connectionLost(err)
        if (attempt < SUBSCRIPTION_RETRY_ATTEMPTS) await delay(this.#retryDelay(attempt))
      }
    }
    throw lastError
  }

  // This one predicate guards every asynchronous transport boundary. A connection close increments the
  // connection epoch and clears current ack ownership, so completions from either of the two prior
  // connections cannot install readiness, unsubscribe a replacement, or satisfy a late attachment.
  #attemptIsCurrent(lifecycle: ChannelLifecycle, operationEpoch: number, connectionEpoch: number): boolean {
    return (
      this.#channels.get(lifecycle.binding.channel) === lifecycle &&
      lifecycle.operationEpoch === operationEpoch &&
      this.#connectionEpoch === connectionEpoch
    )
  }

  async #awaitConnectionReady(connectionEpoch: number): Promise<boolean> {
    while (!this.#disposed && this.#connectionEpoch === connectionEpoch && this.#readyEpoch !== connectionEpoch) {
      await new Promise<void>((resolve) => this.#connectionWaiters.add(resolve))
    }
    return !this.#disposed && this.#connectionEpoch === connectionEpoch && this.#readyEpoch === connectionEpoch
  }

  #wakeConnectionWaiters(): void {
    const waiters = [...this.#connectionWaiters]
    this.#connectionWaiters.clear()
    for (const resolve of waiters) resolve()
  }

  #subscriptionExhausted(lifecycle: ChannelLifecycle, reason: unknown): void {
    this.#subscribedChannels.delete(lifecycle.binding.channel)
    for (const subscription of lifecycle.subscriptions) subscription.failPermanently(reason)
    lifecycle.subscriptions.clear()
    lifecycle.operationEpoch++
    void this.#retryEmptyChannelCleanup(lifecycle)
  }

  async #detach(lifecycle: ChannelLifecycle, subscription: RedisLaneSubscription): Promise<void> {
    if (this.#channels.get(lifecycle.binding.channel) !== lifecycle) return
    lifecycle.subscriptions.delete(subscription)
    if (lifecycle.subscriptions.size !== 0) return
    lifecycle.operationEpoch++
    const subscribed = this.#subscribedChannels.has(lifecycle.binding.channel)
    if (subscribed && !this.#disposed && this.#subscriber.status !== 'end') {
      try {
        await this.#subscriber.unsubscribe(lifecycle.binding.channel)
        this.#subscribedChannels.delete(lifecycle.binding.channel)
        await this.#unsubscribeInvalidationIfLast(lifecycle)
      } catch (err) {
        void this.#retryEmptyChannelCleanup(lifecycle)
        throw err
      }
    }
    if (!subscribed) {
      try {
        await this.#unsubscribeInvalidationIfLast(lifecycle)
      } catch (err) {
        void this.#retryEmptyChannelCleanup(lifecycle)
        throw err
      }
    }
    this.#finishEmptyChannelCleanup(lifecycle)
  }

  async #retryEmptyChannelCleanup(lifecycle: ChannelLifecycle): Promise<void> {
    for (let attempt = 1; attempt <= SUBSCRIPTION_RETRY_ATTEMPTS; attempt++) {
      if (this.#disposed || this.#channels.get(lifecycle.binding.channel) !== lifecycle) return
      await delay(this.#retryDelay(attempt))
      if (lifecycle.subscriptions.size !== 0) return
      try {
        if (this.#subscribedChannels.has(lifecycle.binding.channel) && this.#subscriber.status !== 'end') {
          await this.#subscriber.unsubscribe(lifecycle.binding.channel)
          this.#subscribedChannels.delete(lifecycle.binding.channel)
        }
        await this.#unsubscribeInvalidationIfLast(lifecycle)
        this.#finishEmptyChannelCleanup(lifecycle)
        return
      } catch {
        // Retain this exact item and retry; other channels and generations continue independently.
      }
    }
  }

  async #unsubscribeInvalidationIfLast(lifecycle: ChannelLifecycle): Promise<void> {
    for (const candidate of this.#channels.values()) {
      if (candidate !== lifecycle && candidate.binding.owner === lifecycle.binding.owner) return
    }
    const invalidation = lifecycle.binding.invalidationChannel
    if (this.#subscribedInvalidations.has(invalidation) && this.#subscriber.status !== 'end') {
      await this.#subscriber.unsubscribe(invalidation)
    }
    this.#subscribedInvalidations.delete(invalidation)
    this.#invalidationOwners.delete(invalidation)
  }

  #finishEmptyChannelCleanup(lifecycle: ChannelLifecycle): void {
    if (lifecycle.subscriptions.size !== 0) return
    const { channel } = lifecycle.binding
    if (this.#channels.get(channel) !== lifecycle) return
    this.#subscribedChannels.delete(channel)
    this.#channels.delete(channel)
    this.#channelAcks.delete(channel)
    this.#onChannelRemoved(lifecycle.binding)
  }

  async #retryOrphanInvalidationCleanup(owner: string): Promise<void> {
    for (let attempt = 1; attempt <= SUBSCRIPTION_RETRY_ATTEMPTS; attempt++) {
      if (this.#disposed) return
      await delay(this.#retryDelay(attempt))
      if ([...this.#channels.values()].some((candidate) => candidate.binding.owner === owner)) continue
      const invalidations = [...this.#invalidationOwners.entries()]
        .filter(([, candidateOwner]) => candidateOwner === owner)
        .map(([channel]) => channel)
      try {
        for (const invalidation of invalidations) {
          if (this.#subscribedInvalidations.has(invalidation) && this.#subscriber.status !== 'end') {
            await this.#subscriber.unsubscribe(invalidation)
          }
          this.#subscribedInvalidations.delete(invalidation)
          this.#invalidationOwners.delete(invalidation)
        }
        return
      } catch {
        // Keep the invalidation owner as the retry source.
      }
    }
  }

  readonly #onMessage = (channelBytes: Buffer, frame: Buffer): void => {
    const channel = channelBytes.toString()
    const invalidationOwner = this.#invalidationOwners.get(channel)
    if (invalidationOwner !== undefined) {
      this.#onGenerationInvalidation(invalidationOwner, frame.toString())
      return
    }
    const lifecycle = this.#channels.get(channel)
    if (lifecycle === undefined || lifecycle.subscriptions.size === 0) return
    const { seq, timestamp, payload } = decodeFrameHeader(frame)
    const info = { seq, timestamp }
    const copy = Uint8Array.from(payload)
    for (const subscription of [...lifecycle.subscriptions]) {
      if (!subscription.closed) subscription.deliver(copy, info)
    }
  }

  #invalidateConnectionAcks(): void {
    this.#connectionEpoch++
    this.#readyEpoch = null
    this.#channelAcks.clear()
    this.#wakeConnectionWaiters()
  }

  readonly #onClose = (): void => {
    if (this.#disposed) return
    this.#invalidateConnectionAcks()
    this.#subscribedChannels.clear()
    this.#subscribedInvalidations.clear()
    const reason = new Error('subscribeLane: Redis subscriber connection lost')
    for (const lifecycle of this.#channels.values()) {
      for (const subscription of lifecycle.subscriptions) subscription.connectionLost(reason)
    }
  }

  readonly #onReady = (): void => {
    if (this.#disposed) return
    this.#readyEpoch = this.#connectionEpoch
    this.#wakeConnectionWaiters()
    for (const lifecycle of this.#channels.values()) {
      if (lifecycle.subscriptions.size === 0) void this.#retryEmptyChannelCleanup(lifecycle)
      else void this.#ensureSubscribed(lifecycle).catch(() => {})
    }
  }

  readonly #onEnd = (): void => {
    if (this.#disposed) return
    this.#readyEpoch = null
    this.#wakeConnectionWaiters()
    const reason = new Error('subscribeLane: Redis subscriber reconnect attempts exhausted')
    for (const lifecycle of this.#channels.values()) {
      for (const subscription of lifecycle.subscriptions) subscription.failPermanently(reason)
      this.#onChannelRemoved(lifecycle.binding)
    }
    this.#channels.clear()
    this.#channelAcks.clear()
    this.#subscribedChannels.clear()
    this.#subscribedInvalidations.clear()
    this.#invalidationOwners.clear()
  }
}
