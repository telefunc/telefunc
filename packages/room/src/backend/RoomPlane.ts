export { RoomPlane }
export type { LaneConsumer }

import {
  assertDriverOrdering,
  SharedSubscriptions,
  type BackendPayload,
  type BackendSubscription,
  type OrderingInfo,
  type SourceRules,
} from 'telefunc/__internal'
import type {
  CommitOptions,
  CommitResult,
  CommitTarget,
  EveryMemberLane,
  EveryTrackLane,
  HeadExchangeResult,
  HeadExpectation,
  HeadNext,
  LaneId,
  RetainedFrame,
  RoomDriver,
  RoomSubscriptionSource,
} from './contract.js'
import { roomSubscriptionSourceKey } from './laneKey.js'
import { assert } from '../utils/assert.js'

/** A lane's consumer. */
type LaneConsumer = (payload: Uint8Array, ordering: OrderingInfo) => void

type RoomPlaneOptions = { driver: RoomDriver }

const laneSources: SourceRules<RoomSubscriptionSource> = {
  key: roomSubscriptionSourceKey,
  // A lane carries bytes.
  deliveryKind: () => 'binary',
  waitOf: (target) => {
    const domain = roomSubscriptionSourceKey(target)
    if (target.lane.kind !== 'binary') return { domain, waitsFor: [target], budget: domain, limit: 'text' }
    // A binary commit also reaches its member's every-track lane and its track's every-member lane, and its held bytes
    // count against its member's.
    const { roomId, inc, lane } = target
    const everyTrack = { roomId, inc, lane: { kind: 'binary-all', member: lane.member } } as const
    const everyMember = { roomId, inc, lane: { kind: 'binary-track', track: lane.track } } as const
    const budget = roomSubscriptionSourceKey(everyTrack)
    return { domain, waitsFor: [target, everyTrack, everyMember], budget, limit: 'binary' }
  },
}

/** What Room's server code calls: the driver's writes and orderings checked, its lane subscriptions shared by source
 *  and host. Room reads the rest from `driver` itself. */
class RoomPlane {
  readonly driver: RoomDriver
  private readonly subscriptions: SharedSubscriptions<RoomSubscriptionSource>

  constructor({ driver }: RoomPlaneOptions) {
    this.driver = driver
    this.subscriptions = new SharedSubscriptions({ driver, sources: laneSources, sends: 'Room commit' })
  }

  async compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    assertHeadNextWellFormed(next)
    return this.driver.compareExchangeHead(roomId, expected, next)
  }

  commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    // A close commits under its lease, which would lapse before the wait ends.
    if (!this.subscriptions.establishing || opts?.closingLease !== undefined)
      return this.commitNow(target, payload, opts)
    return this.subscriptions.afterEstablished(target, payload, (owned) => this.commitNow(target, owned, opts))
  }

  async readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    const order = await this.driver.readLaneOrder(roomId, inc, lane)
    if (order !== null) assertDriverOrdering(order)
    return order
  }

  async readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    const retained = await this.driver.readRetained(roomId, inc, lane)
    if (retained !== null) assertDriverOrdering(retained)
    return retained
  }

  subscribeLane(
    roomId: string,
    inc: string,
    lane: LaneId | EveryTrackLane | EveryMemberLane,
    receive: LaneConsumer,
  ): BackendSubscription {
    // The manager checked that a lane delivers bytes.
    return this.subscriptions.subscribe(
      { roomId, inc, lane },
      receive as (payload: BackendPayload, ordering: OrderingInfo) => void,
    )
  }

  dispose(): Promise<void> {
    return this.subscriptions.dispose()
  }

  private async commitNow(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    const result = await this.driver.commitLane(target, payload, opts)
    if (result.kind === 'accepted') assertDriverOrdering(result)
    return result
  }
}

/** Every head write is checked here; drivers only compare-exchange. */
function assertHeadNextWellFormed(next: HeadNext): void {
  const { head, ttlMs } = next
  if (head.state === 'closing') {
    assert(head.closeLease !== undefined, 'head exchange: a head entering closing must carry a close lease')
    const { durationMs } = head.closeLease
    assert(
      Number.isFinite(durationMs) && durationMs > 0,
      `head exchange: close lease durationMs ${durationMs} must be finite and positive`,
    )
  } else {
    assert(head.closeLease === undefined, `head exchange: a '${head.state}' head must not carry a close lease`)
  }
  assert(
    ttlMs === undefined || head.state !== 'closing',
    'head exchange: a closing head lapses with its close lease, not a ttl',
  )
  assert(
    head.state !== 'closed' || head.currentInc === null,
    'head exchange: a closed tombstone must clear currentInc to null',
  )
  assert(
    head.state === 'closed' || head.currentInc !== null,
    `head exchange: a '${head.state}' head must name an incarnation`,
  )
}
