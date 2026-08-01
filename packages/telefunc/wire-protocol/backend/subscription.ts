export type {
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionBinding,
  SubscriptionDriver,
  SubscriptionState,
}

type SubscriptionState = 'establishing' | 'ready' | 'lost' | 'closed'

/** Raw-only ownership terminal. Core maps this to public `closed`; consumers never receive it. */
type SubscriptionAttemptState = SubscriptionState | 'terminated'

type BackendSubscription = {
  /** Current readiness generation: replaced after loss, rejected on terminal failure. */
  readonly ready: Promise<void>
  state(): SubscriptionState
  onStateChange(cb: (state: SubscriptionState) => void): () => void
  unsubscribe(): Promise<void>
}

type BackendReceiver = (payload: Uint8Array, info: { seq: number; timestamp: number }) => void

/** One raw backend establishment attempt. Its readiness may remain pending indefinitely. */
type SubscriptionAttempt = {
  readonly ready: Promise<void>
  state(): SubscriptionAttemptState
  onStateChange(cb: (state: SubscriptionAttemptState) => void): () => void
  /** Settles only after the raw registration and transport cleanup are complete. */
  unsubscribe(): Promise<void>
}

/** Captured synchronously under local ownership. `partition` is an opaque value key; `open` is
 * ambient-free and `valid` is a synchronous, side-effect-free, non-throwing ownership read. */
type SubscriptionBinding = {
  readonly partition: string
  valid(): boolean
  /** The callback reads the current live supervised receiver count for this source and partition. */
  open(receiver: BackendReceiver, localReceiverCount: () => number): SubscriptionAttempt
}

/** Sole backend-specific subscription edge: one ownership capture and one raw attempt; core owns
 * identity, stale-attempt rejection, fan-out, ownership checks, and readiness. */
type SubscriptionDriver<Source> = {
  bind(source: Source): SubscriptionBinding
}
