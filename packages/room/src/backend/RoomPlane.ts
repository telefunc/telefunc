export { RoomPlane }

import {
  assertDriverOrdering,
  BackendCall,
  SharedSubscriptions,
  type BackendListener,
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
  OperationOptions,
  OperationResult,
  RetainedFrame,
  RoomDriver,
  RoomOperation,
  RoomSubscriptionSource,
} from './contract.js'
import { roomSubscriptionSourceKey } from './laneKey.js'
import { RoomStore, type RoomReads } from './RoomStore.js'

type RoomPlaneOptions = { driver: RoomDriver }

const laneSources: SourceRules<RoomSubscriptionSource> = {
  key: roomSubscriptionSourceKey,
  // A lane carries bytes.
  deliveryKind: () => 'binary',
}

/** What Room's server code reaches the store by: `call(work)` for a public call, whose writes are counted in its
 *  account, and `direct` for work that tells no answer; and the lane subscriptions, shared by source and host. */
class RoomPlane {
  readonly driver: RoomDriver
  /** The store with no account: background work, and a call whose only write is its last step. */
  readonly direct: RoomStore
  private readonly reads: RoomReads
  private readonly subscriptions: SharedSubscriptions<RoomSubscriptionSource>

  constructor({ driver }: RoomPlaneOptions) {
    this.driver = driver
    this.reads = {
      readHead: (roomId) => driver.readHead(roomId),
      readCells: (roomId, inc, sel) => driver.readCells(roomId, inc, sel),
      readExpiringCells: (roomId, inc, keys) => driver.readExpiringCells(roomId, inc, keys),
      readCellPages: (roomId, inc, prefix) => driver.readCellPages(roomId, inc, prefix),
      listRetained: (roomId, inc, of) => driver.listRetained(roomId, inc, of),
      directoryGet: (roomId) => driver.directoryGet(roomId),
      directoryList: (prefix, cursor) => driver.directoryList(prefix, cursor),
      directoryDue: () => driver.directoryDue(),
      readLaneOrder: async (roomId, inc, lane) => {
        const order = await driver.readLaneOrder(roomId, inc, lane)
        if (order !== null) assertDriverOrdering(order)
        return order
      },
      readRetained: async (roomId, inc, lane) => {
        const retained = await driver.readRetained(roomId, inc, lane)
        if (retained !== null) assertDriverOrdering(retained)
        return retained
      },
    }
    this.direct = new RoomStore(driver, this.reads, null)
    this.subscriptions = new SharedSubscriptions({ driver, sources: laneSources })
  }

  /** The value of `work`, a public call whose steps go through `store`; its answer is read at its close
   *  (`BackendCall.run`). */
  call<T>(work: (store: RoomStore) => Promise<T>): Promise<T> {
    return BackendCall.run((account) => work(new RoomStore(this.driver, this.reads, account)))
  }

  /** The store on `account`, for code that still holds an account rather than a store. */
  storeOf(account: BackendCall): RoomStore {
    return new RoomStore(this.driver, this.reads, account)
  }

  compareExchangeHead(roomId: string, expected: HeadExpectation, next: HeadNext): Promise<HeadExchangeResult> {
    return this.direct.compareExchangeHead(roomId, expected, next)
  }

  commitLane(target: CommitTarget, payload: Uint8Array, opts?: CommitOptions): Promise<CommitResult> {
    return this.direct.commitLane(target, payload, opts)
  }

  runOperation<Args, Value>(
    roomId: string,
    inc: string,
    operation: RoomOperation<Args, Value>,
    args: Args,
    options?: OperationOptions,
  ): Promise<OperationResult<Value>> {
    return this.direct.runOperation(roomId, inc, operation, args, options)
  }

  readLaneOrder(roomId: string, inc: string, lane: LaneId): Promise<OrderingInfo | null> {
    return this.reads.readLaneOrder(roomId, inc, lane)
  }

  readRetained(roomId: string, inc: string, lane: LaneId): Promise<RetainedFrame | null> {
    return this.reads.readRetained(roomId, inc, lane)
  }

  subscribeLane(
    roomId: string,
    inc: string,
    lane: LaneId | EveryTrackLane | EveryMemberLane,
    listener: BackendListener<Uint8Array>,
  ): BackendSubscription {
    // The shared subscriptions checked that a lane delivers bytes.
    return this.subscriptions.subscribe({ roomId, inc, lane }, listener as BackendListener)
  }

  dispose(): void {
    this.subscriptions.dispose()
  }
}
