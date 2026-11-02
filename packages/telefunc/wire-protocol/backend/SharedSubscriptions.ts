export { SharedSubscriptions }
export type { BackendSubscription, SourceRules, WaitRule }

import { assert, assertUsage } from '../../utils/assert.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import { getOrCreate } from '../../utils/getOrCreate.js'
import { reportBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { ChannelOverflowError } from '../channel/channelErrors.js'
import { getChannelLimits } from '../../node/server/serverConfig.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { utf8ByteLength } from '../../utils/utf8ByteLength.js'
import { assertDriverOrdering } from './driverOrdering.js'
import type {
  AttemptState,
  BackendListener,
  BackendPayload,
  BackendReceiver,
  DriverAttempt,
  SubscriptionDriver,
} from './driver.js'
import type { OrderingInfo } from '../wire/orderingFrame.js'

/** How long a send waits for the establishing attempts it waits for. */
const ESTABLISH_WAIT_MS = 60_000
/** What a waiting send keeps beyond its payload (130–160 bytes measured), so small ones are bounded too. */
const WAITING_SEND_OVERHEAD_BYTES = 256

/** A consumer's subscription. */
type BackendSubscription = {
  /** Settles when the current establishment does: replaced after a loss, rejected when it ends for good. */
  readonly ready: Promise<void>
  state(): AttemptState
  onStateChange(listener: (state: AttemptState) => void): () => void
  unsubscribe(): Promise<void>
}

/** How a plane's sources share attempts and what they deliver. */
type SourceRules<Source> = {
  /** Subscriptions of one key on one host share an attempt. */
  key(source: Source): string
  /** A string for 'text', bytes for 'binary'. */
  deliveryKind(source: Source): 'text' | 'binary'
  /** What a send to `source` waits for; read only while some attempt establishes or some send waits. */
  sendWaitsFor(source: Source): WaitRule<Source>
}

/** A send waits while one of the attempts on `subscriptions` is establishing, behind the waiting sends ordered with it
 *  (`orderedWith`: what one seq counts). Its bytes count against `countsAgainst`'s `bufferLimit`. */
type WaitRule<Source> = {
  orderedWith: string
  subscriptions: readonly Source[]
  countsAgainst: string
  bufferLimit: 'bufferLimit' | 'bufferLimitBinary'
}

/** The sends on one domain, waiting for its establishing attempts. */
type Wait = { readonly established: Promise<void>; sends: number }

type SharedSubscriptionsOptions<Source> = {
  driver: SubscriptionDriver<Source>
  sources: SourceRules<Source>
  /** What a send is, for its overflow error: 'Broadcast publish', 'Room commit'. */
  sendName: string
}

/** A plane's subscriptions (Broadcast's, Room's): consumers of one source on one host share one driver attempt. */
class SharedSubscriptions<Source> {
  readonly driver: SubscriptionDriver<Source>
  private readonly sources: SourceRules<Source>
  /** By host and source key. */
  private readonly slots = new Map<string, SubscriptionSlot<Source>>()
  private readonly cleanups = new Set<Promise<void>>()
  private readonly sendName: string
  /** By domain. */
  private readonly waits = new Map<string, Wait>()
  /** Bytes waiting, by budget. */
  private readonly waitingBytes = new Map<string, number>()
  /** By source key, the slots never ready yet, unmapped a microtask after: empty, no send has anything to wait for. */
  private readonly establishingSlots = new Map<string, SubscriptionSlot<Source>>()

  constructor({ driver, sources, sendName }: SharedSubscriptionsOptions<Source>) {
    // A process runs one host of an overtaking driver, so a wait has no host to tell apart.
    assert(!(driver.publishOvertakesSubscribe && driver.hostKey))
    this.driver = driver
    this.sources = sources
    this.sendName = sendName
  }

  /** False on the fast path, read before a send builds anything: no attempt establishes and no send waits. */
  get establishing(): boolean {
    return this.establishingSlots.size > 0 || this.waits.size > 0
  }

  /** A send's turn once the attempts on its source are established, at most `ESTABLISH_WAIT_MS` later; a driver
   *  may get a publish before its own subscribe otherwise (Redis's go by two connections). Null when it has nothing to
   *  wait for. The turn gives the send its own copy: the caller reuses its bytes. Throws `ChannelOverflowError` when its
   *  bytes would pass its buffer limit; the config is read only then, as resolving it is costly. */
  waitTurn<Payload extends BackendPayload>(source: Source, payload: Payload): WaitingSend<Payload> | null {
    const { orderedWith, subscriptions, countsAgainst, bufferLimit } = this.sources.sendWaitsFor(source)
    let wait = this.waits.get(orderedWith)
    if (wait === undefined && this.establishingIn(subscriptions).length === 0) return null
    const bytes =
      WAITING_SEND_OVERHEAD_BYTES + (typeof payload === 'string' ? utf8ByteLength(payload) : payload.byteLength)
    const total = (this.waitingBytes.get(countsAgainst) ?? 0) + bytes
    if (total > getChannelLimits()[bufferLimit]) {
      throw new ChannelOverflowError(`${this.sendName} waiting for a new subscription, past ${bufferLimit}`)
    }
    wait ??= this.openWait(subscriptions)
    this.waits.set(orderedWith, wait)
    wait.sends++
    this.waitingBytes.set(countsAgainst, total)
    const owned = (typeof payload === 'string' ? payload : new Uint8Array(payload)) as Payload
    const turn = wait.established.then(() => owned)
    return new WaitingSend({ owner: this, orderedWith, wait, countsAgainst, bytes, turn })
  }

  /** A waiting send went to the driver: the last of its wait's sends unmaps it, so a send made before then still
   *  queues behind the rest. */
  endWait({ orderedWith, wait, countsAgainst, bytes }: WaitingSend<unknown>): void {
    if (--wait.sends === 0) this.waits.delete(orderedWith)
    const left = this.waitingBytes.get(countsAgainst)! - bytes
    if (left === 0) this.waitingBytes.delete(countsAgainst)
    else this.waitingBytes.set(countsAgainst, left)
  }

  subscribe(source: Source, listener: BackendListener): BackendSubscription {
    const key = this.sources.key(source)
    const slotKey = JSON.stringify([this.hostKeyHere(), key])
    const delivers = this.sources.deliveryKind(source)
    const slot = getOrCreate(this.slots, slotKey, () => this.openSlot({ owner: this, source, key, slotKey, delivers }))
    return slot.attach(listener)
  }

  async dispose(): Promise<void> {
    const slots = [...this.slots.values()]
    await Promise.allSettled([...slots.map((slot) => slot.stop()), ...this.cleanups])
  }

  /** Unmaps `slot`, if it is still the one its key maps. */
  unmapSlot(slot: SubscriptionSlot<Source>): void {
    if (this.slots.get(slot.slotKey) === slot) this.slots.delete(slot.slotKey)
  }

  /** Unsubscribes `attempt`, tracked for `dispose()`. */
  releaseAttempt(attempt: DriverAttempt): Promise<void> {
    // Called now, not deferred: a subscribe in the same tick starts the source's next attempt after this one let go.
    const cleanup = new Promise<void>((resolve) => resolve(attempt.unsubscribe())).catch((error) =>
      console.error(error),
    )
    this.cleanups.add(cleanup)
    void cleanup.finally(() => this.cleanups.delete(cleanup))
    return cleanup
  }

  private openSlot(options: SubscriptionSlotOptions<Source>): SubscriptionSlot<Source> {
    const slot = new SubscriptionSlot(options)
    // A driver whose publishes can't overtake its subscribes never makes a send wait.
    if (!this.driver.publishOvertakesSubscribe) return slot
    const { key } = options
    this.establishingSlots.set(key, slot)
    void slot.firstReady.then(() => {
      if (this.establishingSlots.get(key) === slot) this.establishingSlots.delete(key)
    })
    return slot
  }

  /** A wait for the attempts on `subscriptions`, including ones made meanwhile. */
  private openWait(subscriptions: readonly Source[]): Wait {
    let timer!: ReturnType<typeof setTimeout>
    const timeout = new Promise<void>((resolve) => {
      timer = unrefTimer(setTimeout(resolve, ESTABLISH_WAIT_MS))
    })
    const established = Promise.race([this.establishedIn(subscriptions), timeout]).finally(() => clearTimeout(timer))
    return { established, sends: 0 }
  }

  private async establishedIn(subscriptions: readonly Source[]): Promise<void> {
    for (let left = this.establishingIn(subscriptions); left.length > 0; left = this.establishingIn(subscriptions)) {
      await Promise.all(left)
    }
  }

  private establishingIn(subscriptions: readonly Source[]): Promise<void>[] {
    const left: Promise<void>[] = []
    for (const source of subscriptions) {
      const slot = this.establishingSlots.get(this.sources.key(source))
      if (slot?.establishing) left.push(slot.firstReady)
    }
    return left
  }

  private hostKeyHere(): string {
    const hostKey = this.driver.hostKey ? this.driver.hostKey() : ''
    assertUsage(
      hostKey !== null,
      'A Cloudflare subscription delivers to a Telefunc session: subscribe from a telefunction or a channel handler, not from outside a request.',
    )
    return hostKey
  }
}

type WaitingSendOptions<Payload> = {
  owner: SharedSubscriptions<unknown>
  orderedWith: string
  wait: Wait
  countsAgainst: string
  bytes: number
  turn: Promise<Payload>
}

/** A send's place in its domain's wait: `await turn` for its own copy of the payload, send that, then `sent()`. */
class WaitingSend<Payload> {
  readonly turn: Promise<Payload>
  readonly orderedWith: string
  readonly wait: Wait
  readonly countsAgainst: string
  readonly bytes: number
  private readonly owner: SharedSubscriptions<unknown>

  constructor({ owner, orderedWith, wait, countsAgainst, bytes, turn }: WaitingSendOptions<Payload>) {
    this.owner = owner
    this.orderedWith = orderedWith
    this.wait = wait
    this.countsAgainst = countsAgainst
    this.bytes = bytes
    this.turn = turn
  }

  sent(): void {
    this.owner.endWait(this)
  }
}

/** A message the attempt delivered before it was ready, and the consumers it would have reached then. */
type EarlyMessage = { payload: BackendPayload; ordering: OrderingInfo; targets: readonly Consumer[] }

type SubscriptionSlotOptions<Source> = {
  owner: SharedSubscriptions<Source>
  source: Source
  /** The source's key, and the key of the source on its host. */
  key: string
  slotKey: string
  delivers: 'text' | 'binary'
}

/** One driver attempt, shared by the consumers of its source on its host. */
class SubscriptionSlot<Source> implements BackendReceiver {
  readonly slotKey: string
  private readonly owner: SharedSubscriptions<Source>
  private readonly source: Source
  private readonly key: string
  private readonly delivers: 'text' | 'binary'
  private readonly attached = new Set<Consumer>()
  /** Taken at a delivery or a state change and dropped when a consumer comes or goes, so each reaches the consumers
   *  it started with. */
  private snapshot: readonly Consumer[] | null = null
  private current: AttemptState = 'establishing'
  private readiness: Deferred<void> = createReadiness()
  private attempt: DriverAttempt | null = null
  private unobserve: (() => void) | null = null
  /** What the attempt delivered before its first `ready`; null from then on, as a message after a loss goes out as it
   *  arrives. Empty, not null, once finished: a message then goes to `keepEarly()`, which drops it, so `deliver()`
   *  checks this one field. */
  private early: EarlyMessage[] | null = []
  /** It stopped or closed: what the attempt says from here on is ignored. */
  private finished = false
  private readonly firstReadiness = createDeferred()
  /** Settles on its first `ready`, its stop or its close; never rejects. */
  readonly firstReady: Promise<void> = this.firstReadiness.promise
  private wasReady = false

  constructor({ owner, source, key, slotKey, delivers }: SubscriptionSlotOptions<Source>) {
    this.owner = owner
    this.source = source
    this.key = key
    this.slotKey = slotKey
    this.delivers = delivers
  }

  /** It was never ready, and is still open. */
  get establishing(): boolean {
    return !this.wasReady && !this.finished
  }

  get state(): AttemptState {
    return this.current
  }

  /** Settles when the current establishment does. */
  get established(): Promise<void> {
    return this.readiness.promise
  }

  attach(listener: BackendListener): BackendSubscription {
    const consumer = new Consumer({ slot: this, listener })
    this.attached.add(consumer)
    this.snapshot = null
    if (this.attempt === null && !this.finished) this.start()
    return consumer
  }

  async detach(consumer: Consumer): Promise<void> {
    this.attached.delete(consumer)
    this.snapshot = null
    if (this.attached.size === 0) await this.stop()
  }

  /** The last consumer left, or the plane is disposed. */
  stop(): Promise<void> {
    if (this.finished) return Promise.resolve()
    const attempt = this.attempt
    this.finish()
    this.readiness.resolve()
    this.enter('closed')
    return attempt === null ? Promise.resolve() : this.owner.releaseAttempt(attempt)
  }

  deliver(payload: BackendPayload, ordering: OrderingInfo): void {
    if (this.early !== null) return this.keepEarly(payload, ordering)
    if (!this.checked(payload, ordering)) return
    // The listener is called here, not through a method of the consumer: one call less on every message.
    for (const consumer of this.targets()) {
      // Consumers are isolated from each other: one that throws is reported.
      try {
        consumer.listener(payload, ordering)
      } catch (error) {
        console.error(error)
      }
    }
  }

  private start(): void {
    let attempt: DriverAttempt
    try {
      attempt = this.owner.driver.subscribe(this.source, this)
    } catch (error) {
      return this.close(error)
    }
    this.attempt = attempt
    this.unobserve = attempt.onStateChange((state, reason) => this.attemptChanged(state, reason))
    const state = attempt.state()
    // What it said before `subscribe()` returned.
    if (state !== 'establishing') this.attemptChanged(state, undefined)
  }

  private attemptChanged(state: Exclude<AttemptState, 'establishing'>, reason: Error | undefined): void {
    if (this.finished) return
    if (state === 'ready') return this.ready()
    if (state === 'lost') return this.lost()
    this.close(reason)
  }

  private ready(): void {
    if (this.current === 'ready') return
    this.wasReady = true
    this.firstReadiness.resolve()
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
          consumer.listener(payload, ordering)
        } catch (error) {
          console.error(error)
        }
      }
    }
  }

  private lost(): void {
    if (this.current === 'lost') return
    if (this.current === 'ready') this.readiness = createReadiness()
    this.enter('lost')
  }

  /** Its consumers' `ready` rejects. Closed with a `reason`, it failed, which is reported once, however many consumers
   *  share it; without one, the backend ended it on purpose. */
  private close(reason: unknown): void {
    const failure = new Error(`Backend subscription closed: ${this.key}`, reason === undefined ? {} : { cause: reason })
    this.finish()
    // A resolved readiness can't carry the failure, so `ready` read from here on is a fresh, rejected one.
    if (this.current === 'ready') this.readiness = createReadiness()
    this.enter('closed')
    this.readiness.reject(failure)
    if (reason !== undefined) void this.readiness.promise.catch(reportBug)
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

  /** Stops listening first: the attempt's own `closed` on its unsubscribe is no end. */
  private finish(): void {
    this.finished = true
    this.firstReadiness.resolve()
    this.early = []
    this.unobserve?.()
    this.unobserve = null
    this.attempt = null
    this.owner.unmapSlot(this)
  }

  private enter(state: AttemptState): void {
    this.current = state
    for (const consumer of this.targets()) consumer.notify(state)
  }

  private targets(): readonly Consumer[] {
    return (this.snapshot ??= [...this.attached])
  }
}

type ConsumerOptions = { slot: SubscriptionSlot<unknown>; listener: BackendListener }

/** One consumer's subscription on a slot. */
class Consumer implements BackendSubscription {
  readonly listener: BackendListener
  private readonly slot: SubscriptionSlot<unknown>
  private attached = true
  private readonly stateListeners = new Set<(state: AttemptState) => void>()

  constructor({ slot, listener }: ConsumerOptions) {
    this.slot = slot
    this.listener = listener
  }

  get ready(): Promise<void> {
    return this.attached ? this.slot.established : Promise.resolve()
  }

  state(): AttemptState {
    return this.attached ? this.slot.state : 'closed'
  }

  onStateChange(listener: (state: AttemptState) => void): () => void {
    assert(this.attached)
    this.stateListeners.add(listener)
    return () => this.stateListeners.delete(listener)
  }

  async unsubscribe(): Promise<void> {
    if (!this.attached) return
    this.attached = false
    if (this.slot.state !== 'closed') this.notify('closed')
    this.stateListeners.clear()
    await this.slot.detach(this)
  }

  notify(state: AttemptState): void {
    for (const listener of this.stateListeners) {
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
