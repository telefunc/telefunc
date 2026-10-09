export { SubscriptionManager, reportSubscriptionEnd }
export type { HoldWeight }

import { assert } from '../../utils/assert.js'
import { createDeferred, type Deferred } from '../../utils/createDeferred.js'
import { raceTimeout } from '../../utils/raceTimeout.js'
import { ESTABLISH_HOLD_MS } from '../constants.js'
import type {
  BackendPayload,
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionBinding,
  SubscriptionDriver,
  SubscriptionState,
} from './subscription.js'

type StateListener = (state: SubscriptionState) => void

// Every consumer of a shared subscription gets its end as one failure object, reported once.
const reportedEnds = new WeakSet<object>()
function reportSubscriptionEnd(error: unknown, report: (error: unknown) => void): void {
  if (typeof error === 'object' && error !== null) {
    if (reportedEnds.has(error)) return
    reportedEnds.add(error)
  }
  report(error)
}

/** Sends on one key waiting for this instance's establishing subscriptions, and the bytes they hold per class. */
type Hold = { readonly established: Promise<void>; sends: number; readonly bytes: Map<string, number> }

/** A held send's bytes, counted in its class and read only for a send that is held; a send its hold can't fit is refused
 *  with `overflow()`. */
type HoldWeight = { class: string; bytes(): number; fits(sends: number, bytes: number): boolean; overflow(): Error }

/** What a send waits for, read only while some subscription could be establishing; `sources` share a partition. */
type SendHold<Source> = { key: string; sources: readonly [Source, ...Source[]]; weight?: HoldWeight }

/** Checks a delivery against the driver contract, once for all its consumers; one it throws for is reported and dropped. */
type DeliveryCheck<Source> = (source: Source, payload: BackendPayload, info: { seq: number; timestamp: number }) => void

type SubscriptionSlotConfig = {
  binding: SubscriptionBinding
  checkDelivery: (payload: BackendPayload, info: { seq: number; timestamp: number }) => void
  reportError: (error: unknown) => void
  sourceKey: string
  cleanup: (attempt: SubscriptionAttempt) => Promise<void>
  unmap: () => void
}

class SubscriptionManager<Source> {
  /** Slots by driver partition and source key. */
  private readonly _slots = new Map<string, SubscriptionSlot>()
  private readonly _cleanups = new Set<Promise<void>>()
  private readonly _holds = new Map<string, Hold>()
  /** Slots not yet established, counted down a microtask after: at 0, no send has anything to wait for. */
  private _establishing = 0

  constructor(
    private readonly _driver: SubscriptionDriver<Source>,
    private readonly _reportError: (error: unknown) => void,
    private readonly _sourceKey: (source: Source) => string,
    private readonly _checkDelivery: DeliveryCheck<Source>,
  ) {}

  subscribe(source: Source, receiver: BackendReceiver<BackendPayload>): BackendSubscription {
    const binding = this._driver.bind(source)
    const sourceKey = this._sourceKey(source)
    const slotKey = JSON.stringify([binding.partition, sourceKey])
    let slot = this._slots.get(slotKey)
    if (slot === undefined) {
      const created: SubscriptionSlot = new SubscriptionSlot({
        binding,
        checkDelivery: (payload, info) => this._checkDelivery(source, payload, info),
        reportError: this._reportError,
        sourceKey,
        cleanup: (attempt) => this._cleanup(attempt),
        unmap: () => {
          if (this._slots.get(slotKey) === created) this._slots.delete(slotKey)
        },
      })
      this._slots.set(slotKey, (slot = created))
      this._establishing++
      void created.established.then(() => this._establishing--)
    }
    return slot.attach(receiver)
  }

  async dispose(): Promise<void> {
    const slots = [...this._slots.values()]
    this._slots.clear()
    await Promise.allSettled([...slots.map((slot) => slot.stop()), ...this._cleanups])
  }

  private _cleanup(attempt: SubscriptionAttempt): Promise<void> {
    // Not deferred: a subscribe in the same tick opens the source's next attempt after this one let go.
    const unsubscribing = new Promise<void>((resolve) => resolve(attempt.unsubscribe()))
    const cleanup = unsubscribing.catch((error) => this._reportError(error))
    this._cleanups.add(cleanup)
    void cleanup.finally(() => this._cleanups.delete(cleanup))
    return cleanup
  }

  /** Runs `send` once none of its caller's subscriptions on the hold's `sources` is establishing, waiting at most the
   *  hold time; while the caller holds the hold's `key`, its later sends on it queue behind, in call order. */
  afterEstablished<T>(send: () => T | Promise<T>, describeHold: () => SendHold<Source>): T | Promise<T> {
    if (this._establishing === 0 && this._holds.size === 0) return send()
    const { key, sources, weight } = describeHold()
    // Another partition's subscription (another Cloudflare session's) is not ordered before this send.
    const partition = this._driver.partitionHere(sources[0])
    if (partition === null) return send()
    const holdKey = JSON.stringify([partition, key])
    let hold = this._holds.get(holdKey)
    if (hold === undefined) {
      if (this._establishingWaits(sources, partition).length === 0) return send()
      const established = raceTimeout(this._established(sources, partition), ESTABLISH_HOLD_MS, () => {})
      hold = { established, sends: 0, bytes: new Map() }
    }
    const current = hold
    const bytes = weight?.bytes() ?? 0
    if (weight !== undefined) {
      const held = (current.bytes.get(weight.class) ?? 0) + bytes
      if (!weight.fits(current.sends + 1, held)) return Promise.reject(weight.overflow())
      current.bytes.set(weight.class, held)
    }
    this._holds.set(holdKey, current)
    current.sends++
    // Sent inside the reaction, so a send that finds the hold gone can't reach the driver first.
    return current.established.then(() => {
      if (--current.sends === 0) this._holds.delete(holdKey)
      if (weight !== undefined) current.bytes.set(weight.class, (current.bytes.get(weight.class) ?? 0) - bytes)
      return send()
    })
  }

  /** Resolves once no slot of the partition on `sources`, including ones added meanwhile, is establishing. */
  private async _established(sources: readonly Source[], partition: string): Promise<void> {
    for (
      let waits = this._establishingWaits(sources, partition);
      waits.length > 0;
      waits = this._establishingWaits(sources, partition)
    )
      await Promise.all(waits)
  }

  /** A slot is establishing until it is first ready, stopped or ended. */
  private _establishingWaits(sources: readonly Source[], partition: string): Promise<void>[] {
    return sources.flatMap((source) => {
      const slot = this._slots.get(JSON.stringify([partition, this._sourceKey(source)]))
      return slot?.establishing ? [slot.established] : []
    })
  }
}

class SubscriptionSlot {
  private readonly _attachments = new Set<SlotAttachment>()
  /** Taken at a delivery or notify and dropped on a change, so each iterates the attachments it started with. */
  private _snapshot: readonly SlotAttachment[] | null = null
  private _attempt: SubscriptionAttempt | null = null
  private _unobserve: (() => void) | null = null
  private _readiness: Deferred<void> = createReadiness()
  /** The first readiness, settled by the first ready, a stop or an end; a later loss opens a new one. */
  readonly established: Promise<void> = this._readiness.promise.then(
    () => {},
    () => {},
  )
  private _wasReady = false
  private _state: SubscriptionState = 'establishing'
  private _stopPromise: Promise<void> | null = null

  constructor(private readonly _config: SubscriptionSlotConfig) {}

  get establishing(): boolean {
    return this._stopPromise === null && !this._wasReady
  }

  get state(): SubscriptionState {
    return this._state
  }

  get readiness(): Promise<void> {
    return this._readiness.promise
  }

  attach(receiver: BackendReceiver<BackendPayload>): BackendSubscription {
    assert(this._stopPromise === null) // the manager unmaps a slot before stopping it
    const attachment = new SlotAttachment(this, receiver)
    this._attachments.add(attachment)
    this._snapshot = null
    if (this._attempt === null) this._start()
    return attachment
  }

  async detach(attachment: SlotAttachment): Promise<void> {
    this._attachments.delete(attachment)
    this._snapshot = null
    if (this._attachments.size > 0) return
    this._config.unmap()
    await this.stop()
  }

  reportError(error: unknown): void {
    this._config.reportError(error)
  }

  stop(): Promise<void> {
    if (this._stopPromise !== null) return this._stopPromise
    this._stopPromise = this._release()
    this._readiness.resolve()
    this._transition('closed')
    return this._stopPromise
  }

  private _start(): void {
    let attempt: SubscriptionAttempt
    try {
      attempt = this._config.binding.open(
        (payload, info) => {
          if (this._stopPromise !== null) return
          try {
            this._config.checkDelivery(payload, info)
          } catch (error) {
            return this._config.reportError(error)
          }
          for (const attachment of this._targets()) {
            try {
              attachment.receiver(payload, info)
            } catch (error) {
              this._config.reportError(error)
            }
          }
        },
        () => this._attachments.size,
      )
    } catch (error) {
      this._terminal(error)
      return
    }
    this._attempt = attempt
    this._unobserve = attempt.onStateChange((state, reason) => this._onStateChange(attempt, state, reason))
    // A refusal known at once throws from open(), so an attempt opens establishing or ready.
    assert(attempt.state() !== 'closed')
    if (attempt.state() === 'ready') this._becameReady()
  }

  private _onStateChange(attempt: SubscriptionAttempt, state: SubscriptionState, reason?: Error): void {
    assert(this._attempt === attempt) // a slot opens one attempt, and unobserves it before cleanup
    if (state === 'ready') return this._becameReady()
    if (state === 'closed') return this._ended(reason)
    this._markUnavailable(state)
  }

  private _becameReady(): void {
    this._wasReady = true
    this._readiness.resolve()
    this._transition('ready')
  }

  /** The driver's reason, if any, is the failure's cause. */
  private _ended(reason: Error | undefined): void {
    this._terminal(new Error(`Backend subscription closed: ${this._config.sourceKey}`, reason && { cause: reason }))
  }

  private _terminal(error: unknown): void {
    const failure = error instanceof Error ? error : new Error(String(error))
    this._config.unmap()
    this._stopPromise = this._release()
    // A resolved readiness cannot carry the failure, so `ready` read from here on is a fresh, rejected one.
    if (this._state === 'ready') this._readiness = createReadiness()
    this._transition('closed')
    this._readiness.reject(failure)
  }

  private _markUnavailable(state: 'establishing' | 'lost'): void {
    if (this._state === 'ready') this._readiness = createReadiness()
    this._transition(state)
  }

  private _transition(state: SubscriptionState): void {
    if (this._state === state) return
    this._state = state
    for (const attachment of this._targets()) attachment.notify(state)
  }

  private _targets(): readonly SlotAttachment[] {
    return (this._snapshot ??= [...this._attachments])
  }

  /** Unobserves the attempt first: its closing on cleanup is no end. */
  private _release(): Promise<void> {
    const attempt = this._attempt
    this._unobserve?.()
    this._unobserve = null
    this._attempt = null
    return attempt === null ? Promise.resolve() : this._config.cleanup(attempt)
  }
}

/** One consumer's subscription on a slot. */
class SlotAttachment implements BackendSubscription {
  private _attached = true
  private readonly _listeners = new Set<StateListener>()

  constructor(
    private readonly _slot: SubscriptionSlot,
    readonly receiver: BackendReceiver<BackendPayload>,
  ) {}

  get ready(): Promise<void> {
    return this._attached ? this._slot.readiness : Promise.resolve()
  }

  state(): SubscriptionState {
    return this._attached ? this._slot.state : 'closed'
  }

  onStateChange(listener: StateListener): () => void {
    assert(this._attached)
    this._listeners.add(listener)
    return () => this._listeners.delete(listener)
  }

  async unsubscribe(): Promise<void> {
    if (!this._attached) return
    this._attached = false
    if (this._slot.state !== 'closed') this.notify('closed')
    this._listeners.clear()
    await this._slot.detach(this)
  }

  /** Consumer listeners are isolated from each other; one that throws is reported. */
  notify(state: SubscriptionState): void {
    for (const listener of [...this._listeners]) {
      if (!this._listeners.has(listener)) continue
      try {
        listener(state)
      } catch (error) {
        this._slot.reportError(error)
      }
    }
  }
}

function createReadiness(): Deferred<void> {
  const readiness = createDeferred()
  void readiness.promise.catch(() => {})
  return readiness
}
