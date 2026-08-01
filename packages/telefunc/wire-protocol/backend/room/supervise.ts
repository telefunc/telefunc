export { superviseRoomDriver }

import { assertHeadNextWellFormed } from './head-transitions.js'
import { SubscriptionManager } from '../subscription-manager.js'
import type { HeadCx, HeadNext, RoomBackend, RoomDriver } from './contract.js'
import { roomSubscriptionSourceKey } from './lane-key.js'

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
    commitLane: (roomId, inc, lane, payload, opts) => driver.commitLane(roomId, inc, lane, payload, opts),
    readRetained: (roomId, inc, lane) => driver.readRetained(roomId, inc, lane),
    listRetained: (roomId, inc) => driver.listRetained(roomId, inc),
    deleteRetained: (roomId, inc, lane, opts) => driver.deleteRetained(roomId, inc, lane, opts),
    subscribeLane: (roomId, inc, lane, receiver) => subscriptions.subscribe({ roomId, inc, lane }, receiver),
    dropGeneration: async (roomId, inc) => {
      await driver.dropGeneration(roomId, inc)
      subscriptions.terminate((source) => source.roomId === roomId && source.inc === inc)
    },
    directoryPut: (roomId, incTag) => driver.directoryPut(roomId, incTag),
    directoryDelete: (roomId, incTag) => driver.directoryDelete(roomId, incTag),
    directoryList: (prefix, cursor) => driver.directoryList(prefix, cursor),
    dispose: () => (disposal ??= subscriptions.dispose()),
  }
}
