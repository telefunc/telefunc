export { superviseRoomDriver }

import { assert } from '../../../utils/assert.js'
import { assertHeadNextWellFormed } from './head.js'
import { SubscriptionManager } from '../subscription-manager.js'
import type { HeadCx, HeadNext, RoomBackend, RoomDriver } from './contract.js'
import { roomSubscriptionSourceKey } from './lane-key.js'
import { assertDriverPosition } from '../driver-position.js'

/** Owns the Room subscription manager and durable head/drop supervision. */
function superviseRoomDriver(driver: RoomDriver): RoomBackend {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, roomSubscriptionSourceKey)
  let disposal: Promise<void> | undefined

  return {
    readHead: (roomId) => driver.readHead(roomId),
    compareExchangeHead: async (roomId: string, cx: HeadCx, next: HeadNext) => {
      assertHeadNextWellFormed(next)
      return driver.compareExchangeHead(roomId, cx, next)
    },
    readCells: (roomId, inc, sel) => driver.readCells(roomId, inc, sel),
    compareExchangeCells: (roomId, inc, revision, mutations) =>
      driver.compareExchangeCells(roomId, inc, revision, mutations),
    commitLane: async (roomId, inc, lane, payload, opts) => {
      const result = await driver.commitLane(roomId, inc, lane, payload, opts)
      if ('accepted' in result) assertDriverPosition(result)
      return result
    },
    readRetained: async (roomId, inc, lane) => {
      const retained = await driver.readRetained(roomId, inc, lane)
      if (retained !== null) assertDriverPosition(retained)
      return retained
    },
    listRetained: (roomId, inc) => driver.listRetained(roomId, inc),
    deleteRetained: (roomId, inc, lane, opts) => driver.deleteRetained(roomId, inc, lane, opts),
    subscribeLane: (roomId, inc, lane, receiver) =>
      subscriptions.subscribe({ roomId, inc, lane }, (payload, info) => {
        assertDriverPosition(info)
        return receiver(payload, info)
      }),
    dropGeneration: async (roomId, inc) => {
      // Core drops only finalized incarnations; a random `inc` never comes back as current, so no atomic check is needed.
      assert((await driver.readHead(roomId))?.currentInc !== inc, 'Dropping the current incarnation')
      await driver.dropGeneration(roomId, inc)
      subscriptions.terminate((source) => source.roomId === roomId && source.inc === inc)
    },
    directoryPut: (roomId, incTag) => driver.directoryPut(roomId, incTag),
    directoryDelete: (roomId, incTag) => driver.directoryDelete(roomId, incTag),
    directoryList: (prefix, cursor) => driver.directoryList(prefix, cursor),
    dispose: () => (disposal ??= subscriptions.dispose()),
  }
}
