export { SubscriptionManager }
export type { HoldWeight, SendHold, PlaneSubscriptions }

import { assert } from '../../utils/assert.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import { raceTimeout } from '../../utils/raceTimeout.js'
import { ESTABLISH_HOLD_MS } from '../constants.js'
import { reportBug } from '../channel/server/channel.js'
import { isSourceDroppedError } from './attempt.js'
import { assertDriverOrdering } from './driverOrdering.js'
import { heldSendWeight } from './heldSendWeight.js'
import type { OrderingInfo } from '../wire/orderingFrame.js'
import type {
  BackendPayload,
  BackendReceiver,
  BackendSubscription,
  ReportedState,
  SubscriptionAttempt,
  SubscriptionBinding,
  SubscriptionDriver,
  SubscriptionState,
} from './subscription.js'

type StateListener = (state: SubscriptionState) => void

/** Sends on one key waiting for this instance's establishing subscriptions. */
type Hold = { readonly established: Promise<void>; sends: number }

/** A held send's bytes, read only for a send that is held and counted in `budget`, which sends of several keys can
 *  share; a send its budget can't fit is refused with `overflow()`. */
type HoldWeight = { budget: string; bytes(): number; fits(bytes: number): boolean; overflow(): Error }

/** What a send waits for, read only while some subscription could be establishing; `sources` share a partition. A held
 *  send's `bytes()` count against `budget`'s `kind` buffer limit, and `label` names the overflow. `onHeld` runs once
 *  the send is held, before `afterEstablished` returns. */
type SendHold<Source> = {
  key: string
  sources: readonly [Source, ...Source[]]
  kind: 'text' | 'binary'
  budget: string
  bytes(): number
  label: string
  onHeld?(): void
}

/** What a plane's supervisor gets from `backendPlane()`: its driver's subscriptions, shared by source and checked once
 *  for all their consumers, and the hold of sends on them. */
type PlaneSubscriptions<Source> = Pick<SubscriptionManager<Source>, 'subscribe' | 'afterEstablished' | 'dispose'>

type SubscriptionSlotConfig = {
  binding: SubscriptionBinding
  checkDelivery: (payload: BackendPayload, info: { seq: number; timestamp: number }) => void
  sourceKey: string
  cleanup: (attempt: SubscriptionAttempt) => Promise<void>
  unmap: () => void
}

class SubscriptionManager<Source> {
  /** Slots by driver partition and source key. */
  private readonly slots = new Map<string, SubscriptionSlot>()
  private readonly cleanups = new Set<Promise<void>>()
  private readonly holds = new Map<string, Hold>()
  /** Bytes held per partition and budget. */
  private readonly held = new Map<string, number>()
  /** Slots not yet established, counted down a microtask after: at 0, no send has anything to wait for. */
  private establishing = 0
  private readonly driver: SubscriptionDriver<Source>
  private readonly sourceKey: (source: Source) => string
  private readonly payloadKind: (source: Source) => 'text' | 'binary'

  /** `payloadKind` is what a source carries: a string for 'text', bytes for 'binary'. */
  constructor({
    driver,
    sourceKey,
    payloadKind,
  }: {
    driver: SubscriptionDriver<Source>
    sourceKey: (source: Source) => string
    payloadKind: (source: Source) => 'text' | 'binary'
  }) {
    this.driver = driver
    this.sourceKey = sourceKey
    this.payloadKind = payloadKind
  }

  /** Checks a delivery against the driver contract, once for all its consumers; one it throws for is reported and
   *  dropped. */
  private checkDelivery(source: Source, payload: BackendPayload, info: OrderingInfo): void {
    assertDriverOrdering(info)
    assert((this.payloadKind(source) === 'text') === (typeof payload === 'string'))
  }

  subscribe(source: Source, receiver: BackendReceiver<BackendPayload>): BackendSubscription {
    const binding = this.driver.bind(source)
    const sourceKey = this.sourceKey(source)
    const slotKey = JSON.stringify([binding.partition, sourceKey])
    let slot = this.slots.get(slotKey)
    if (slot === undefined) {
      const created: SubscriptionSlot = new SubscriptionSlot({
        binding,
        checkDelivery: (payload, info) => this.checkDelivery(source, payload, info),
        sourceKey,
        cleanup: (attempt) => this.cleanup(attempt),
        unmap: () => {
          if (this.slots.get(slotKey) === created) this.slots.delete(slotKey)
        },
      })
      this.slots.set(slotKey, (slot = created))
      this.establishing++
      void created.established.then(() => this.establishing--)
    }
    return slot.attach(receiver)
  }

  async dispose(): Promise<void> {
    const slots = [...this.slots.values()]
    this.slots.clear()
    await Promise.allSettled([...slots.map((slot) => slot.stop()), ...this.cleanups])
  }

  private cleanup(attempt: SubscriptionAttempt): Promise<void> {
    // Not deferred: a subscribe in the same tick opens the source's next attempt after this one let go.
    const unsubscribing = new Promise<void>((resolve) => resolve(attempt.unsubscribe()))
    const cleanup = unsubscribing.catch((error) => console.error(error))
    this.cleanups.add(cleanup)
    void cleanup.finally(() => this.cleanups.delete(cleanup))
    return cleanup
  }

  /** Whether a send can be held now: some subscription establishes or some send is held. */
  get holding(): boolean {
    return this.establishing > 0 || this.holds.size > 0
  }

  /** Runs `send` once none of its caller's subscriptions on the hold's `sources` is establishing, waiting at most the
   *  hold time; while the caller holds the hold's `key`, its later sends on it queue behind, in call order.
   *  `describeHold` runs only while `holding`. */
  afterEstablished<T>(send: () => T | Promise<T>, describeHold: () => SendHold<Source>): T | Promise<T> {
    if (!this.holding) return send()
    const { key, sources, kind, budget: budgetKey, bytes: payloadBytes, label, onHeld } = describeHold()
    // Another partition's subscription (another Cloudflare session's) is not ordered before this send.
    const partition = this.driver.partitionHere(sources[0])
    if (partition === null) return send()
    const holdKey = JSON.stringify([partition, key])
    let hold = this.holds.get(holdKey)
    if (hold === undefined) {
      if (this.establishingWaits(sources, partition).length === 0) return send()
      const established = raceTimeout(this.established(sources, partition), ESTABLISH_HOLD_MS, () => {})
      hold = { established, sends: 0 }
    }
    const current = hold
    const weight = heldSendWeight(kind, budgetKey, payloadBytes, label)
    const bytes = weight.bytes()
    const budget = JSON.stringify([partition, weight.budget])
    const held = (this.held.get(budget) ?? 0) + bytes
    if (!weight.fits(held)) return Promise.reject(weight.overflow())
    this.held.set(budget, held)
    onHeld?.()
    this.holds.set(holdKey, current)
    current.sends++
    // Sent inside the reaction, so a send that finds the hold gone can't reach the driver first.
    return current.established.then(() => {
      if (--current.sends === 0) this.holds.delete(holdKey)
      this.unhold(budget, bytes)
      return send()
    })
  }

  private unhold(budget: string, bytes: number): void {
    const held = this.held.get(budget)! - bytes
    if (held === 0) this.held.delete(budget)
    else this.held.set(budget, held)
  }

  /** Resolves once no slot of the partition on `sources`, including ones added meanwhile, is establishing. */
  private async established(sources: readonly Source[], partition: string): Promise<void> {
    for (
      let waits = this.establishingWaits(sources, partition);
      waits.length > 0;
      waits = this.establishingWaits(sources, partition)
    ) {
      await Promise.all(waits)
    }
  }

  /** A slot is establishing until it is first ready, stopped or ended. */
  private establishingWaits(sources: readonly Source[], partition: string): Promise<void>[] {
    return sources.flatMap((source) => {
      const slot = this.slots.get(JSON.stringify([partition, this.sourceKey(source)]))
      return slot?.establishing ? [slot.established] : []
    })
  }
}

class SubscriptionSlot {
  private readonly attachments = new Set<SlotAttachment>()
  /** Taken at a delivery or notify and dropped on a change, so each iterates the attachments it started with. */
  private snapshot: readonly SlotAttachment[] | null = null
  private attempt: SubscriptionAttempt | null = null
  private unobserve: (() => void) | null = null
  private _readiness: Deferred<void> = createReadiness()

  /** The first readiness, settled by the first ready, a stop or an end; a later loss opens a new one. */
  readonly established: Promise<void> = this._readiness.promise.then(
    () => {},
    () => {},
  )

  private wasReady = false
  private _state: SubscriptionState = 'establishing'
  private stopPromise: Promise<void> | null = null

  constructor(private readonly config: SubscriptionSlotConfig) {}

  get establishing(): boolean {
    return this.stopPromise === null && !this.wasReady
  }

  get state(): SubscriptionState {
    return this._state
  }

  get readiness(): Promise<void> {
    return this._readiness.promise
  }

  attach(receiver: BackendReceiver<BackendPayload>): BackendSubscription {
    assert(this.stopPromise === null) // the manager unmaps a slot before stopping it
    const attachment = new SlotAttachment(this, receiver)
    this.attachments.add(attachment)
    this.snapshot = null
    if (this.attempt === null) this.start()
    return attachment
  }

  async detach(attachment: SlotAttachment): Promise<void> {
    this.attachments.delete(attachment)
    this.snapshot = null
    if (this.attachments.size > 0) return
    this.config.unmap()
    await this.stop()
  }

  stop(): Promise<void> {
    if (this.stopPromise !== null) return this.stopPromise
    this.stopPromise = this.release()
    this._readiness.resolve()
    this.transition('closed')
    return this.stopPromise
  }

  private start(): void {
    let attempt: SubscriptionAttempt
    try {
      attempt = this.config.binding.open(
        (payload, info) => this.deliver(payload, info),
        () => this.attachments.size,
      )
    } catch (error) {
      this.terminal(error)
      return
    }
    this.attempt = attempt
    this.unobserve = attempt.onStateChange((state, reason) => this.onStateChange(attempt, state, reason))
    // A refusal known at once throws from open(), so an attempt opens establishing or ready.
    assert(attempt.state() !== 'closed')
    if (attempt.state() === 'ready') this.becameReady()
  }

  private deliver(payload: BackendPayload, info: { seq: number; timestamp: number }): void {
    if (this.stopPromise !== null) return
    try {
      this.config.checkDelivery(payload, info)
    } catch (error) {
      console.error(error)
      return
    }
    for (const attachment of this.targets()) {
      try {
        attachment.receiver(payload, info)
      } catch (error) {
        console.error(error)
      }
    }
  }

  /** An attempt reports a `ReportedState`: ready, lost or closed, never establishing again. */
  private onStateChange(attempt: SubscriptionAttempt, state: ReportedState, reason?: Error): void {
    assert(this.attempt === attempt) // a slot opens one attempt, and unobserves it before cleanup
    if (state === 'ready') return this.becameReady()
    if (state === 'closed') return this.ended(reason)
    this.markLost()
  }

  private becameReady(): void {
    this.wasReady = true
    this._readiness.resolve()
    this.transition('ready')
  }

  /** The driver's reason, if any, is the failure's cause. A dropped source is an end its consumers expect, so only
   *  another end is reported. */
  private ended(reason: Error | undefined): void {
    const failure = new Error(`Backend subscription closed: ${this.config.sourceKey}`, reason && { cause: reason })
    this.terminal(failure, { report: !isSourceDroppedError(reason) })
  }

  private terminal(error: unknown, { report = true }: { report?: boolean } = {}): void {
    const failure = error instanceof Error ? error : new Error(String(error))
    this.config.unmap()
    this.stopPromise = this.release()
    // A resolved readiness cannot carry the failure, so `ready` read from here on is a fresh, rejected one.
    if (this._state === 'ready') this._readiness = createReadiness()
    this.transition('closed')
    this._readiness.reject(failure)
    // Reported here, once, however many consumers share the subscription.
    void this._readiness.promise.catch(report ? reportBug : () => {})
  }

  private markLost(): void {
    if (this._state === 'ready') this._readiness = createReadiness()
    this.transition('lost')
  }

  private transition(state: SubscriptionState): void {
    if (this._state === state) return
    this._state = state
    for (const attachment of this.targets()) attachment.notify(state)
  }

  private targets(): readonly SlotAttachment[] {
    return (this.snapshot ??= [...this.attachments])
  }

  /** Unobserves the attempt first: its closing on cleanup is no end. */
  private release(): Promise<void> {
    const attempt = this.attempt
    this.unobserve?.()
    this.unobserve = null
    this.attempt = null
    return attempt === null ? Promise.resolve() : this.config.cleanup(attempt)
  }
}

/** One consumer's subscription on a slot. */
class SlotAttachment implements BackendSubscription {
  private attached = true
  private readonly listeners = new Set<StateListener>()

  constructor(
    private readonly slot: SubscriptionSlot,
    readonly receiver: BackendReceiver<BackendPayload>,
  ) {}

  get ready(): Promise<void> {
    return this.attached ? this.slot.readiness : Promise.resolve()
  }

  state(): SubscriptionState {
    return this.attached ? this.slot.state : 'closed'
  }

  onStateChange(listener: StateListener): () => void {
    assert(this.attached)
    this.listeners.add(listener)
    return () => this.listeners.delete(listener)
  }

  async unsubscribe(): Promise<void> {
    if (!this.attached) return
    this.attached = false
    if (this.slot.state !== 'closed') this.notify('closed')
    this.listeners.clear()
    await this.slot.detach(this)
  }

  /** Consumer listeners are isolated from each other; one that throws is reported. */
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
