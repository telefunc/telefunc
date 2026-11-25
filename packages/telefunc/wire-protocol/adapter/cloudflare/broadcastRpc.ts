/// <reference types="@cloudflare/workers-types" />
export type {
  BroadcastCalls,
  BroadcastForwardRequest,
  BroadcastHoldRequest,
  BroadcastLeaseRequest,
  BroadcastNamespace,
  BroadcastPublishRequest,
  BroadcastReleaseRequest,
  CarriedError,
  DeliveryLoss,
  ErrorMarks,
  ForwardLoss,
  MemberPlacement,
  PublishAnswer,
  TelefuncBroadcastStub,
}

import type { LocationBucket } from './routing.js'
import type { OrderedStubs } from './OrderedStubs.js'
import type { BroadcastPayload, BroadcastRoute, PublishResult } from '../../backend/driver.js'
import type { BroadcastDelivery } from './extensions.js'
import type { LeaseOrder, LeaseRegistered, LeaseRemoved, LeaseRenewed } from './leases.js'

type BroadcastPublishRequest = {
  key: string
  kind: BroadcastRoute['kind']
  payload: BroadcastPayload
}

/** A delivery, handed to one bucket coordinator for the member DOs it names by id. */
type BroadcastForwardRequest = BroadcastDelivery & { members: string[] }
/** What a bucket coordinator lost of a forward: how many member DOs failed to take it, with the routes an owed one had
 *  no receiver for, and why: the first failure's error, else what had no receiver. */
type ForwardLoss = { lost: number; reason: CarriedError }
/** What a delivery lost, of its `total` member DOs, by the time it is answered to its sender. */
type DeliveryLoss = ForwardLoss & { total: number }
/** A key's authority's answer to a publish: its `seq`, or what its delivery lost, which `publish()` rejects with. */
type PublishAnswer = { kind: 'published'; info: PublishResult } | ({ kind: 'lost' } & DeliveryLoss)
/** The properties Workers RPC marks an error it threw across with, which a copy of the error returned across it loses. */
type ErrorMarks = { remote?: boolean; retryable?: boolean; overloaded?: boolean; durableObjectId?: string }
/** An error, as it was caught, to be returned across Workers RPC: its marks travel beside it, and `restoredError()`
 *  puts them back on it where the loss is thrown. */
type CarriedError = { error: unknown; marks: ErrorMarks }

/** A member DO, by id, and the bucket its session was placed in, as it was stored or sent: `forward()` checks each
 *  bucket once. */
type MemberPlacement = { member: string; bucket: string }

/** A member DO's presence on a route, at the key's authority, by DO id and the lease of its subscription there. */
type BroadcastLeaseRequest = { key: string; kind: BroadcastRoute['kind']; member: string; lease: string }
/** Records the presence, in the bucket its member DO was placed in. */
type BroadcastHoldRequest = BroadcastLeaseRequest & { bucket: LocationBucket; order: LeaseOrder }
type BroadcastReleaseRequest = BroadcastLeaseRequest & { order: LeaseOrder }

type TelefuncBroadcastStub = {
  telefuncBroadcastPublish(request: BroadcastPublishRequest): Promise<PublishAnswer>
  /** Resolves to what of the delivery the member DOs it names didn't take, or null where they took it all. */
  telefuncBroadcastForward(request: BroadcastForwardRequest): Promise<ForwardLoss | null>
  /** How many of an owed delivery's routes it had no receiver for. */
  telefuncBroadcastDeliver(delivery: BroadcastDelivery): Promise<number>
  /** A new grant for the member DO, for its writes of presence at this key's authority (`leases.ts`). */
  telefuncBroadcastGrant(member: string): Promise<number>
  telefuncBroadcastHold(request: BroadcastHoldRequest): Promise<LeaseRegistered>
  telefuncBroadcastRenew(request: BroadcastLeaseRequest): Promise<LeaseRenewed>
  telefuncBroadcastRelease(request: BroadcastReleaseRequest): Promise<LeaseRemoved>
}

/** One DO's outgoing Broadcast calls. */
type BroadcastCalls = OrderedStubs<TelefuncBroadcastStub>

type BroadcastNamespace = {
  idFromName(name: string): unknown
  idFromString(id: string): unknown
  get(id: unknown, options?: { locationHint: LocationBucket }): TelefuncBroadcastStub
}
