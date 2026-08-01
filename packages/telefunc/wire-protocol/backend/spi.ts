export { BACKEND_SPI_VERSION }
export type {
  BackendDriver,
  BackendSpi,
  BackendSubscriptionSource,
  CellMutation,
  CommitAccepted,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  PublishResult,
  RoomHead,
}
export type {
  BackendReceiver,
  BackendSubscription,
  SubscriptionAttempt,
  SubscriptionAttemptState,
  SubscriptionBinding,
  SubscriptionDriver,
  SubscriptionState,
} from './subscription.js'
export type { BroadcastLane } from './broadcast/contract.js'

import type { BroadcastBackend, BroadcastDriver, BroadcastLane, PublishResult } from './broadcast/contract.js'
import type { RoomBackend, RoomDriver, RoomSubscriptionSource } from './room/contract.js'
import type {
  CellMutation,
  CommitAccepted,
  CommitResult,
  CxResult,
  HeadCx,
  HeadNext,
  LaneId,
  RoomHead,
} from './room/contract.js'
import type { SubscriptionDriver } from './subscription.js'

/** Temporary fused compatibility seam; the sole final version belongs to `BackendDriverPair`. */
const BACKEND_SPI_VERSION = 1 as const

/** Temporary source union retained only for adapters that have not yet split into plane drivers. */
type BackendSubscriptionSource =
  | { kind: 'broadcast'; lane: BroadcastLane }
  | ({ kind: 'durable' } & RoomSubscriptionSource)

/** Temporary fused supervised consumer while core callers migrate to plane projections. */
type BackendSpi = { readonly spiVersion: typeof BACKEND_SPI_VERSION } & Omit<BroadcastBackend, 'dispose'> &
  Omit<RoomBackend, 'dispose'> & {
    dispose(): Promise<void>
  }

/** Temporary fused author contract while Redis, Cloudflare, and memory split into plane drivers. */
type BackendDriver = { readonly spiVersion: typeof BACKEND_SPI_VERSION } & Omit<BroadcastDriver, 'subscriptions'> &
  Omit<RoomDriver, 'subscriptions'> & {
    readonly subscriptions: SubscriptionDriver<BackendSubscriptionSource>
    dispose(): Promise<void>
  }
