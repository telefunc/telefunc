export { RoomPlane }

import {
  assertDriverOrdering,
  BackendCall,
  SharedSubscriptions,
  type BackendListener,
  type BackendSubscription,
  type SourceRules,
} from 'telefunc/__internal'
import type { EveryMemberLane, EveryTrackLane, LaneId, RoomDriver, RoomSubscriptionSource } from './contract.js'
import { roomSubscriptionSourceKey } from './laneKey.js'
import { assert } from '../utils/assert.js'
import { CallStore, RoomStore, type RoomReads } from './RoomStore.js'

type RoomPlaneOptions = { driver: RoomDriver }

const laneSources: SourceRules<RoomSubscriptionSource> = {
  key: roomSubscriptionSourceKey,
  // A lane carries bytes.
  deliveryKind: () => 'binary',
}

/** What Room's server code reaches the store by: `call(work)` for a public call, whose writes are counted in its
 *  account, and `direct` for work that tells no answer; and the lane subscriptions, shared by source and host. */
class RoomPlane {
  private readonly driver: RoomDriver
  /** The store with no account: background work and its reads, and a call whose only write is its last step. */
  readonly direct: RoomStore
  private readonly reads: RoomReads
  private readonly subscriptions: SharedSubscriptions<RoomSubscriptionSource>

  constructor({ driver }: RoomPlaneOptions) {
    this.driver = driver
    this.reads = {
      readHead: (roomId) => driver.readHead(roomId),
      readCells: (roomId, inc, sel) => driver.readCells(roomId, inc, sel),
      readExpiringCells: (roomId, inc, keys) => driver.readExpiringCells(roomId, inc, keys),
      readCellPages: (roomId, inc, prefix, beside) => driver.readCellPages(roomId, inc, prefix, beside),
      listRetained: (roomId, inc, of) => driver.listRetained(roomId, inc, of),
      directoryGet: (roomId) => driver.directoryGet(roomId),
      directoryList: (prefix, cursor) => driver.directoryList(prefix, cursor),
      directoryDue: () => driver.directoryDue(),
      readLaneOrder: async (roomId, inc, lane) => {
        const order = await driver.readLaneOrder(roomId, inc, lane)
        if (order !== null)
          assert(Number.isSafeInteger(order.seq) && order.seq > 0, 'A backend driver returned an invalid ordering')
        return order
      },
      readRetained: async (roomId, inc, lane) => {
        const retained = await driver.readRetained(roomId, inc, lane)
        if (retained !== null) assertDriverOrdering(retained)
        return retained
      },
    }
    this.direct = new RoomStore(driver, this.reads)
    this.subscriptions = new SharedSubscriptions({ driver, sources: laneSources })
  }

  /** The value of `work`, a public call whose steps go through `store`; its answer is read at its close
   *  (`BackendCall.run`). */
  call<T>(work: (store: RoomStore) => Promise<T>): Promise<T> {
    return BackendCall.run((account) => work(new CallStore(this.driver, this.reads, account)))
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
