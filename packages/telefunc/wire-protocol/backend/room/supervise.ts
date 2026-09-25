export { superviseRoomDriver }

import { assertHeadNextWellFormed } from './head.js'
import { SubscriptionManager } from '../subscription-manager.js'
import type { HeadCx, HeadNext, RoomBackend, RoomDriver, RoomSubscriptionSource } from './contract.js'
import { roomSubscriptionSourceKey } from './lane-key.js'
import { assertDriverPosition } from '../driver-position.js'
import { raceTimeout } from '../../../utils/raceTimeout.js'
import { ROOM_HORIZON_MS } from '../../room/constants.js'

/** Owns the Room subscription manager and durable head/drop supervision. */
function superviseRoomDriver(driver: RoomDriver): RoomBackend {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, roomSubscriptionSourceKey)
  let disposal: Promise<void> | undefined
  // While this instance's subscription on a lane is establishing, commits on it wait (within the horizon) and later
  // ones queue behind them, so a publish right after a subscribe on the same connection reaches it.
  const held = new Map<string, { established: Promise<void>; waiters: number }>()
  const holdFor = (source: RoomSubscriptionSource): Promise<void> | null => {
    const key = roomSubscriptionSourceKey(source)
    let hold = held.get(key)
    if (hold === undefined) {
      if (!subscriptions.hasEstablishing(source)) return null
      hold = { established: raceTimeout(subscriptions.established(source), ROOM_HORIZON_MS, () => {}), waiters: 0 }
      held.set(key, hold)
    }
    const current = hold
    current.waiters++
    return current.established.then(() => {
      if (--current.waiters === 0) held.delete(key)
    })
  }

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
      const hold = holdFor({ roomId, inc, lane })
      if (hold !== null) await hold
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
    dropGeneration: (roomId, inc) => driver.dropGeneration(roomId, inc),
    directoryPut: (roomId, incTag) => driver.directoryPut(roomId, incTag),
    directoryDelete: (roomId, incTag) => driver.directoryDelete(roomId, incTag),
    directoryList: (prefix, cursor) => driver.directoryList(prefix, cursor),
    dispose: () => (disposal ??= subscriptions.dispose()),
  }
}
