export { Subscriptions }
export type { BackendReceiver, BackendSubscription, SourceKey, SubscribingDriver, SubscriptionState }

import { assert } from '../../utils/assert.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import { reportBug } from '../channel/server/channel.js'
import { hostLocal } from '../channel/server/mux.js'
import { assertDriverOrdering } from './driverOrdering.js'
import type { BackendPayload, Subscriber, Unsubscribe } from './driver.js'
import type { OrderingInfo } from '../wire/orderingFrame.js'

type SubscriptionState = 'establishing' | 'ready' | 'lost' | 'closed'

type BackendReceiver<Payload = Uint8Array> = (payload: Payload, ordering: OrderingInfo) => void

/** A consumer's subscription. */
type BackendSubscription = {
  /** Settles when the current establishment does: replaced after a loss, rejected when it ends for good. */
  readonly ready: Promise<void>
  state(): SubscriptionState
  onStateChange(listener: (state: SubscriptionState) => void): () => void
  unsubscribe(): Promise<void>
}

/** The part of a plane's driver that subscribes. */
type SubscribingDriver<Source> = { subscribe(source: Source, subscriber: Subscriber): Unsubscribe }

/** How a consumer's source is shared: subscriptions with one `key` on one host share a driver subscription, which
 *  delivers `delivers`. */
type SourceKey = { key: string; delivers: 'text' | 'binary' }

/** One host's shared subscriptions, by source key. */
type Host = { readonly subscriptions: Map<string, SharedSubscription> }

/** A plane's subscriptions (Broadcast's, Room's): consumers of one source on one host share one driver subscription. */
class Subscriptions<Source> {
  private readonly driver: SubscribingDriver<Source>
  /** Each host's state, found through `hostLocal()`. */
  private readonly hostKey: symbol
  /** The hosts with a subscription, so `dispose()` reaches them all. */
  private readonly hosts = new Set<Host>()
  private readonly cleanups = new Set<Promise<void>>()

  constructor({ driver }: { driver: SubscribingDriver<Source> }) {
    this.driver = driver
    this.hostKey = Symbol('telefunc.subscriptions')
  }

  subscribe(
    source: Source,
    { key, delivers }: SourceKey,
    receiver: BackendReceiver<BackendPayload>,
  ): BackendSubscription {
    const host = hostLocal(this.hostKey, (): Host => ({ subscriptions: new Map() }))
    const existing = host.subscriptions.get(key)
    if (existing !== undefined) return existing.add(receiver)
    const shared = new SharedSubscription({ subscriptions: this, host, key, delivers })
    host.subscriptions.set(key, shared)
    this.hosts.add(host)
    const consumer = shared.add(receiver)
    try {
      shared.start(this.driver, source)
    } catch (error) {
      this.forget(shared)
      throw error
    }
    return consumer
  }

  async dispose(): Promise<void> {
    const all = [...this.hosts].flatMap((host) => [...host.subscriptions.values()])
    await Promise.allSettled([...all.map((shared) => shared.stop()), ...this.cleanups])
  }

  /** Unmaps `shared`, if it is still the one its host maps. */
  forget(shared: SharedSubscription): void {
    const { subscriptions } = shared.host
    if (subscriptions.get(shared.key) === shared) subscriptions.delete(shared.key)
    if (subscriptions.size === 0) this.hosts.delete(shared.host)
  }

  /** Runs a driver's unsubscribe, tracked for `dispose()`. */
  release(unsubscribe: Unsubscribe): Promise<void> {
    // Called now, not deferred: a subscribe in the same tick starts the source's next one after this one let go.
    const cleanup = new Promise<void>((resolve) => resolve(unsubscribe())).catch((error) => console.error(error))
    this.cleanups.add(cleanup)
    void cleanup.finally(() => this.cleanups.delete(cleanup))
    return cleanup
  }
}

/** A message the driver delivered before `ready()`, and the consumers it would have reached then. */
type EarlyMessage = { payload: BackendPayload; ordering: OrderingInfo; targets: readonly Consumer[] }

type SharedSubscriptionOptions = {
  subscriptions: Subscriptions<unknown>
  host: Host
  key: string
  delivers: 'text' | 'binary'
}

/** One driver subscription, shared by the consumers of its source on its host: what the driver tells. */
class SharedSubscription implements Subscriber {
  readonly host: Host
  readonly key: string
  private readonly subscriptions: Subscriptions<unknown>
  private readonly delivers: 'text' | 'binary'
  private readonly attached = new Set<Consumer>()
  /** Taken at a delivery or a state change and dropped when a consumer comes or goes, so each reaches the consumers
   *  it started with. */
  private snapshot: readonly Consumer[] | null = null
  private current: SubscriptionState = 'establishing'
  private readiness: Deferred<void> = createReadiness()
  private unsubscribeDriver: Unsubscribe | null = null
  /** What the driver delivered before its first `ready()`; null from then on, as a message after a loss goes out as it
   *  arrives. Empty, not null, once finished: a message then goes to `keepEarly()`, which drops it, so `message()`
   *  checks this one field. */
  private early: EarlyMessage[] | null = []
  /** It stopped or ended: what the driver says from here on is ignored. */
  private finished = false

  constructor({ subscriptions, host, key, delivers }: SharedSubscriptionOptions) {
    this.subscriptions = subscriptions
    this.host = host
    this.key = key
    this.delivers = delivers
  }

  get consumers(): number {
    return this.attached.size
  }

  get state(): SubscriptionState {
    return this.current
  }

  /** Settles when the current establishment does. */
  get established(): Promise<void> {
    return this.readiness.promise
  }

  start<Source>(driver: SubscribingDriver<Source>, source: Source): void {
    const unsubscribe = driver.subscribe(source, this)
    // A refusal fails it before `subscribe` returns, and core never unsubscribes a failed subscription.
    if (!this.finished) this.unsubscribeDriver = unsubscribe
  }

  add(receiver: BackendReceiver<BackendPayload>): BackendSubscription {
    const consumer = new Consumer(this, receiver)
    this.attached.add(consumer)
    this.snapshot = null
    return consumer
  }

  async remove(consumer: Consumer): Promise<void> {
    this.attached.delete(consumer)
    this.snapshot = null
    if (this.attached.size === 0) await this.stop()
  }

  /** The last consumer left, or the plane is disposed. */
  stop(): Promise<void> {
    if (this.finished) return Promise.resolve()
    const unsubscribe = this.unsubscribeDriver
    this.finish()
    this.readiness.resolve()
    this.enter('closed')
    return unsubscribe === null ? Promise.resolve() : this.subscriptions.release(unsubscribe)
  }

  // What the driver tells.

  message(payload: BackendPayload, ordering: OrderingInfo): void {
    if (this.early !== null) return this.keepEarly(payload, ordering)
    if (!this.checked(payload, ordering)) return
    // The receiver is called here, not through a method of the consumer: one call less on every message.
    for (const consumer of this.targets()) {
      // Consumers are isolated from each other: one that throws is reported.
      try {
        consumer.receiver(payload, ordering)
      } catch (error) {
        console.error(error)
      }
    }
  }

  ready(): void {
    if (this.finished || this.current === 'ready') return
    this.readiness.resolve()
    this.enter('ready')
    const early = this.early
    if (early === null) return
    this.early = null
    for (const { payload, ordering, targets } of early) {
      for (const consumer of targets) {
        // A consumer may leave, or stop the subscription, on one of them.
        if (this.finished || !this.attached.has(consumer)) continue
        try {
          consumer.receiver(payload, ordering)
        } catch (error) {
          console.error(error)
        }
      }
    }
  }

  lost(): void {
    if (this.finished || this.current === 'lost') return
    if (this.current === 'ready') this.readiness = createReadiness()
    this.enter('lost')
  }

  ended(): void {
    if (this.finished) return
    this.close(new Error(`Backend subscription closed: ${this.key}`))
  }

  failed(error: unknown): void {
    if (this.finished) return
    this.close(new Error(`Backend subscription closed: ${this.key}`, { cause: error }))
    // Reported here, once, however many consumers share it.
    void this.readiness.promise.catch(reportBug)
  }

  /** A consumer sees `ready` before any message, so a driver needn't order its acknowledgment before what it delivers:
   *  until then a message waits, with the consumers it would have reached. Once finished, it is dropped. */
  private keepEarly(payload: BackendPayload, ordering: OrderingInfo): void {
    if (!this.finished && this.checked(payload, ordering)) {
      this.early!.push({ payload, ordering, targets: this.targets() })
    }
  }

  /** Checked once for all consumers; one that fails is reported and dropped. */
  private checked(payload: BackendPayload, ordering: OrderingInfo): boolean {
    try {
      assertDriverOrdering(ordering)
      assert((this.delivers === 'text') === (typeof payload === 'string'))
      return true
    } catch (error) {
      console.error(error)
      return false
    }
  }

  /** Its consumers' `ready` rejects with `failure`. */
  private close(failure: Error): void {
    this.finish()
    // A resolved readiness can't carry the failure, so `ready` read from here on is a fresh, rejected one.
    if (this.current === 'ready') this.readiness = createReadiness()
    this.enter('closed')
    this.readiness.reject(failure)
  }

  private finish(): void {
    this.finished = true
    this.early = []
    this.unsubscribeDriver = null
    this.subscriptions.forget(this)
  }

  private enter(state: SubscriptionState): void {
    this.current = state
    for (const consumer of this.targets()) consumer.notify(state)
  }

  private targets(): readonly Consumer[] {
    return (this.snapshot ??= [...this.attached])
  }
}

/** One consumer's subscription on a shared one. */
class Consumer implements BackendSubscription {
  private attached = true
  private readonly listeners = new Set<(state: SubscriptionState) => void>()

  constructor(
    private readonly shared: SharedSubscription,
    readonly receiver: BackendReceiver<BackendPayload>,
  ) {}

  get ready(): Promise<void> {
    return this.attached ? this.shared.established : Promise.resolve()
  }

  state(): SubscriptionState {
    return this.attached ? this.shared.state : 'closed'
  }

  onStateChange(listener: (state: SubscriptionState) => void): () => void {
    assert(this.attached)
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  async unsubscribe(): Promise<void> {
    if (!this.attached) return
    this.attached = false
    if (this.shared.state !== 'closed') this.notify('closed')
    this.listeners.clear()
    await this.shared.remove(this)
  }

  notify(state: SubscriptionState): void {
    for (const listener of this.listeners) {
      try {
        listener(state)
      } catch (error) {
        console.error(error)
      }
    }
  }
}

function createReadiness(): Deferred<void> {
  const readiness = createDeferred()
  void readiness.promise.catch(() => {})
  return readiness
}
