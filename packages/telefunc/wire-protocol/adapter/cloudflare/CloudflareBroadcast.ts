/// <reference types="@cloudflare/workers-types" />
export { CloudflareBroadcast }

import {
  getBucketCoordinatorScale,
  getDeterministicKeyBucketIndex,
  readLocationBucket,
  type CloudflareScale,
  type LocationBucket,
} from './routing.js'
import type { CallOptions, Invoke } from './OrderedStubs.js'
import type { BroadcastPayload, BroadcastRoute, PublishResult } from '../../backend/driver.js'
import { broadcastRouteKey } from '../../backend/broadcast/routeKey.js'
import { getOrCreate } from '../../../utils/getOrCreate.js'
import { currentCloudflareSession } from './session.js'
import type { BroadcastDelivery, CloudflareRegions } from './extensions.js'
import type {
  BroadcastPublishRequest,
  BroadcastForwardRequest,
  ForwardLoss,
  DeliveryLoss,
  PublishAnswer,
  ErrorMarks,
  CarriedError,
  MemberPlacement,
  TelefuncBroadcastStub,
  BroadcastCalls,
  BroadcastNamespace,
} from './broadcastRpc.js'
import { CloudflareBroadcastMember } from './CloudflareBroadcastMember.js'
import type { CloudflareBroadcastAuthorityState } from './CloudflareBroadcastAuthorityState.js'

type CloudflareBroadcastOptions = {
  baseInstanceName: string
  scale?: CloudflareScale
  locationFallback: LocationBucket
  getNamespace: () => BroadcastNamespace
}

/** The Cloudflare Broadcast driver of the isolate: where each key's authority and each bucket's coordinators live, and
 *  their RPC handlers. Each session DO subscribes through its own `CloudflareBroadcastMember`. */
class CloudflareBroadcast {
  private readonly baseInstanceName: string
  private readonly scale: CloudflareScale | undefined
  private readonly locationFallback: LocationBucket
  private readonly getNamespace: () => BroadcastNamespace

  constructor({ baseInstanceName, scale, locationFallback, getNamespace }: CloudflareBroadcastOptions) {
    this.baseInstanceName = baseInstanceName
    this.scale = scale
    this.locationFallback = locationFallback
    this.getNamespace = getNamespace
  }

  member(id: string, calls: BroadcastCalls): CloudflareBroadcastMember {
    return new CloudflareBroadcastMember({ broadcast: this, id, calls })
  }

  /** From a session DO, through its ordered stubs; from elsewhere, as a cron trigger, through a fresh stub. Async, so
   *  the caller gets a native promise: a stub's RpcPromise is callable, which `isPromise` doesn't take for a
   *  promise. */
  async publish(route: BroadcastRoute, payload: BroadcastPayload): Promise<PublishResult> {
    const name = this.authorityName(route.key)
    const member = currentCloudflareSession()?.broadcast
    const request = {
      key: route.key,
      kind: route.kind,
      payload: typeof payload === 'string' ? payload : new Uint8Array(payload),
    }
    const send = (authority: TelefuncBroadcastStub) => authority.telefuncBroadcastPublish(request)
    const answer =
      member === undefined
        ? await send(this.stubByName(name, null))
        : await this.callByName(member.calls, name, member.bucket, send)
    if (answer.kind === 'lost') throw lostDeliveryError(route.key, answer)
    return answer.info
  }

  /** Through `calls`, to the authority of `key`. */
  callAuthority<T>(
    calls: BroadcastCalls,
    key: string,
    locationHint: LocationBucket | null,
    invoke: Invoke<TelefuncBroadcastStub, T> | Promise<Invoke<TelefuncBroadcastStub, T>>,
    options?: CallOptions,
  ): Promise<T> {
    return calls.call(
      this.authorityName(key),
      () => this.stubByName(this.authorityName(key), locationHint),
      invoke,
      options,
    )
  }

  /** At the key's authority: sequences the publish, reads its presence and forwards it, all in one synchronous turn,
   *  so forwards leave through this DO's ordered stubs in `seq` order. */
  async publishToSubscribers(
    authorityState: CloudflareBroadcastAuthorityState,
    calls: BroadcastCalls,
    request: BroadcastPublishRequest,
  ): Promise<PublishAnswer> {
    const { key, kind, payload } = request
    const info = { seq: authorityState.nextSequence(key), timestamp: Date.now() }
    const presence = authorityState.livePresence(broadcastRouteKey({ key, kind }), info.timestamp)
    const loss = await this.forward(calls, key, { routes: [{ key, kind }], payload, info }, presence)
    return loss === null ? { kind: 'published', info } : { kind: 'lost', ...loss }
  }

  /** Sends `delivery` across to each bucket once, to the coordinator `key` picks there, which hands it to the bucket's
   *  `members`; resolves, once every coordinator answered, to what the delivery lost, or null where every member DO
   *  took it. The forwards leave through `calls` before this returns, so a DO's forwards for one key reach each
   *  coordinator in the order it made them. */
  async forward(
    calls: BroadcastCalls,
    key: string,
    delivery: BroadcastDelivery,
    members: Iterable<MemberPlacement>,
  ): Promise<DeliveryLoss | null> {
    const membersByBucket = new Map<string, string[]>()
    let total = 0
    for (const { member, bucket } of members) {
      getOrCreate(membersByBucket, bucket, () => []).push(member)
      total++
    }
    let lost = 0
    let reason: CarriedError | undefined
    const forwardThrough = async ([name, members]: [string, string[]]) => {
      const bucket = readLocationBucket(name)
      try {
        const loss = await this.callByName(calls, this.coordinatorName(key, bucket), bucket, (coordinator) =>
          coordinator.telefuncBroadcastForward({ ...delivery, members }),
        )
        if (loss === null) return
        lost += loss.lost
        reason ??= loss.reason
      } catch (error) {
        lost += members.length
        reason ??= carriedError(error)
      }
    }
    await Promise.all(Array.from(membersByBucket, forwardThrough))
    return reason === undefined ? null : { lost, total, reason }
  }

  /** Broadcast's regions as an extension's Durable Object sends through them, from the DO owning `calls`: a forward
   *  rejects once a member DO failed to take its delivery. */
  regions(calls: BroadcastCalls): CloudflareRegions {
    return {
      forward: async (key, delivery, members) => {
        const loss = await this.forward(calls, key, delivery, members)
        if (loss !== null) throw lostDeliveryError(key, loss)
      },
    }
  }

  /** At a bucket coordinator: delivers a forwarded delivery to the named member DOs, in arrival order. Returns what they
   *  lost of it, or null where none lost any. */
  async forwardToBucket(
    calls: BroadcastCalls,
    { members, ...delivery }: BroadcastForwardRequest,
  ): Promise<ForwardLoss | null> {
    const deliveries = await Promise.allSettled(
      members.map((member) =>
        calls.call(
          member,
          () => this.stubById(member),
          (stub) => stub.telefuncBroadcastDeliver(delivery),
        ),
      ),
    )
    const keys = delivery.routes.map((route) => route.key).join("', '")
    return reportLostDeliveries(`Cloudflare Broadcast delivery of '${keys}'`, deliveries)
  }

  /** A call from the DO owning `calls`, through its stub for the named instance. */
  private callByName<T>(
    calls: BroadcastCalls,
    name: string,
    locationHint: LocationBucket | null,
    invoke: (stub: TelefuncBroadcastStub) => Promise<T>,
  ): Promise<T> {
    return calls.call(name, () => this.stubByName(name, locationHint), invoke)
  }

  authorityName(key: string): string {
    return `${this.baseInstanceName}:broadcast:authority:${key}`
  }

  private coordinatorName(key: string, locationBucket: LocationBucket): string {
    const bucketShardCount = getBucketCoordinatorScale(this.scale, locationBucket)
    const bucketShardOrdinal = getDeterministicKeyBucketIndex(key, bucketShardCount)
    return `${this.baseInstanceName}:broadcast:${locationBucket}:${bucketShardOrdinal}`
  }

  /** Hinted with `locationFallback` outside a session: Cloudflare places a DO on its first `get()`, for good. */
  private stubByName(name: string, locationHint: LocationBucket | null): TelefuncBroadcastStub {
    const namespace = this.getNamespace()
    return namespace.get(namespace.idFromName(name), { locationHint: locationHint ?? this.locationFallback })
  }

  private stubById(id: string): TelefuncBroadcastStub {
    const namespace = this.getNamespace()
    return namespace.get(namespace.idFromString(id))
  }
}

/** Logs the deliveries that failed, with the first one's reason; returns how many were lost, with the routes a member
 *  DO had no receiver for, and why, or null where none was. */
function reportLostDeliveries(delivery: string, outcomes: PromiseSettledResult<number>[]): ForwardLoss | null {
  let failed = 0
  let missing = 0
  let reason: unknown
  for (const outcome of outcomes) {
    if (outcome.status === 'fulfilled') {
      missing += outcome.value
    } else {
      if (failed === 0) reason = outcome.reason
      failed++
    }
  }
  if (failed > 0) console.error(`${delivery} lost to ${failed}/${outcomes.length} Durable Objects: ${reason}`)
  const lost = failed + missing
  if (lost === 0) return null
  return {
    lost,
    reason: carriedError(
      failed > 0 ? reason : new Error(`${delivery} had no receiver for ${missing} of its owed routes`),
    ),
  }
}

/** The rejection of a delivery of `key` that lost `loss`, with why as its cause, as the error was caught. */
function lostDeliveryError(key: string, { lost, total, reason }: DeliveryLoss): Error {
  return new Error(`Cloudflare Broadcast delivery of '${key}' lost to ${lost}/${total} Durable Objects`, {
    cause: restoredError(reason),
  })
}

function carriedError(error: unknown): CarriedError {
  const marks: ErrorMarks = {}
  if (typeof error === 'object' && error !== null) {
    if ('remote' in error && typeof error.remote === 'boolean') marks.remote = error.remote
    if ('retryable' in error && typeof error.retryable === 'boolean') marks.retryable = error.retryable
    if ('overloaded' in error && typeof error.overloaded === 'boolean') marks.overloaded = error.overloaded
    if ('durableObjectId' in error && typeof error.durableObjectId === 'string') {
      marks.durableObjectId = error.durableObjectId
    }
  }
  return { error, marks }
}

/** The error `carried` holds, with the marks a return across Workers RPC took off it put back. */
function restoredError({ error, marks }: CarriedError): unknown {
  return error instanceof Error ? Object.assign(error, marks) : error
}
