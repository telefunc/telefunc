export type {
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionBinding,
  SubscriptionDriver,
  SubscriptionState,
}

type SubscriptionState = 'establishing' | 'ready' | 'lost' | 'closed'

type BackendSubscription = {
  /** Settles when the current establishment does: replaced after loss, rejected on a terminal failure. */
  readonly ready: Promise<void>
  state(): SubscriptionState
  onStateChange(cb: (state: SubscriptionState) => void): () => void
  unsubscribe(): Promise<void>
}

type BackendReceiver = (payload: Uint8Array, info: { seq: number; timestamp: number }) => void

/** One driver establishment, reporting each state change; an end carries its reason when the driver has one. */
type SubscriptionAttempt = {
  state(): SubscriptionState
  onStateChange(cb: (state: SubscriptionState, reason?: Error) => void): () => void
  /** Settles after the driver's own cleanup, which may leave transport work queued (Redis unsubscribes its channel on
   *  the connection's next sync). */
  unsubscribe(): Promise<void>
}

/** A source bound to its current owner. Bindings with equal `partition` share one attempt. */
type SubscriptionBinding = {
  readonly partition: string
  /** `localReceiverCount` reads how many consumers currently share this attempt. A refusal known at once throws, and
   *  the thrown error is the subscription's failure. */
  open(receiver: BackendReceiver, localReceiverCount: () => number): SubscriptionAttempt
}

/** A driver's subscription edge: bind a source to its owner, then open attempts from the binding. */
type SubscriptionDriver<Source> = {
  bind(source: Source): SubscriptionBinding
}
