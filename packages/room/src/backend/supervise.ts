export { superviseRoomDriver }

import {
  assertDriverOrdering,
  type BackendPayload,
  type BackendReceiver,
  type Subscriptions,
} from 'telefunc/__internal'
import type { HeadNext, RoomBackend, RoomDriver, RoomSubscriptionSource } from './contract.js'
import { roomSubscriptionSourceKey } from './laneKey.js'
import { assert } from '../utils/assert.js'

/** Checks what the driver is given and returns; its lane subscriptions are shared by source and host. */
function superviseRoomDriver(driver: RoomDriver, subscriptions: Subscriptions<RoomSubscriptionSource>): RoomBackend {
  return {
    readHead: (roomId) => driver.readHead(roomId),
    compareExchangeHead: async (roomId, cx, next) => {
      assertHeadNextWellFormed(next)
      return driver.compareExchangeHead(roomId, cx, next)
    },
    readCells: (roomId, inc, sel) => driver.readCells(roomId, inc, sel),
    readCellPages: (roomId, inc, prefix) => driver.readCellPages(roomId, inc, prefix),
    compareExchangeCells: (roomId, inc, cx) => driver.compareExchangeCells(roomId, inc, cx),
    commitLane: async (target, payload, opts) => {
      const result = await driver.commitLane(target, payload, opts)
      if ('accepted' in result) assertDriverOrdering(result)
      return result
    },
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
    listRetained: (roomId, inc, of) => driver.listRetained(roomId, inc, of),
    deleteRetained: (roomId, inc, lane, opts) => driver.deleteRetained(roomId, inc, lane, opts),
    dropLaneOrders: (roomId, inc, member) => driver.dropLaneOrders(roomId, inc, member),
    subscribeLane: (roomId, inc, lane, receiver) => {
      const source = { roomId, inc, lane }
      const key = { key: roomSubscriptionSourceKey(source), delivers: 'binary' } as const
      return subscriptions.subscribe(source, key, receiver as BackendReceiver<BackendPayload>)
    },
    dropGeneration: (roomId, inc) => driver.dropGeneration(roomId, inc),
    directoryPut: (roomId, incTag, checkAt, minDelayMs) => driver.directoryPut(roomId, incTag, checkAt, minDelayMs),
    directoryDelete: (roomId, incTag) => driver.directoryDelete(roomId, incTag),
    directoryGet: (roomId) => driver.directoryGet(roomId),
    directoryList: (prefix, cursor) => driver.directoryList(prefix, cursor),
    directoryDue: () => driver.directoryDue(),
    dispose: () => subscriptions.dispose(),
  }
}

/** Every head write is checked here; drivers only compare-exchange. */
function assertHeadNextWellFormed(next: HeadNext): void {
  const { head, ttlMs } = next
  if (head.state === 'closing') {
    assert(head.closeLease !== undefined, 'head CX: a head entering closing must carry a close lease')
    const { durationMs } = head.closeLease
    assert(
      Number.isFinite(durationMs) && durationMs > 0,
      `head CX: close lease durationMs ${durationMs} must be finite and positive`,
    )
  } else {
    assert(head.closeLease === undefined, `head CX: a '${head.state}' head must not carry a close lease`)
  }
  assert(
    ttlMs === undefined || head.state !== 'closing',
    'head CX: a closing head lapses with its close lease, not a ttl',
  )
  assert(
    head.state !== 'closed' || head.currentInc === null,
    'head CX: a closed tombstone must clear currentInc to null',
  )
  assert(
    head.state === 'closed' || head.currentInc !== null,
    `head CX: a '${head.state}' head must name an incarnation`,
  )
}
