export { superviseBackend }

import type { BroadcastDriver } from './broadcast/contract.js'
import { superviseBroadcastDriver } from './broadcast/supervise.js'
import type { RoomDriver } from './room/contract.js'
import { superviseRoomDriver } from './room/supervise.js'
import type { BackendDriver, BackendSpi } from './spi.js'

/** Temporary fused composition delegate while concrete drivers migrate to the two plane contracts. */
function superviseBackend(driver: BackendDriver): BackendSpi {
  const broadcastDriver: BroadcastDriver = {
    publish: (lane, payload) => driver.publish(lane, payload),
    subscriptions: {
      bind: (lane) => driver.subscriptions.bind({ kind: 'broadcast', lane }),
    },
  }
  const roomDriver: RoomDriver = {
    readHead: (roomId) => driver.readHead(roomId),
    compareExchangeHead: (roomId, cx, next) => driver.compareExchangeHead(roomId, cx, next),
    readCells: (roomId, inc, sel) => driver.readCells(roomId, inc, sel),
    compareExchangeCells: (roomId, inc, revision, mutations) =>
      driver.compareExchangeCells(roomId, inc, revision, mutations),
    commitLane: (roomId, inc, lane, payload, opts) => driver.commitLane(roomId, inc, lane, payload, opts),
    readRetained: (roomId, inc, lane) => driver.readRetained(roomId, inc, lane),
    listRetained: (roomId, inc) => driver.listRetained(roomId, inc),
    deleteRetained: (roomId, inc, lane, opts) => driver.deleteRetained(roomId, inc, lane, opts),
    dropGeneration: (roomId, inc) => driver.dropGeneration(roomId, inc),
    directoryPut: (roomId, incTag) => driver.directoryPut(roomId, incTag),
    directoryDelete: (roomId, incTag) => driver.directoryDelete(roomId, incTag),
    directoryList: (prefix, cursor) => driver.directoryList(prefix, cursor),
    subscriptions: {
      bind: (source) => driver.subscriptions.bind({ kind: 'durable', ...source }),
    },
  }

  const { dispose: disposeBroadcast, ...broadcast } = superviseBroadcastDriver(broadcastDriver)
  const { dispose: disposeRoom, ...room } = superviseRoomDriver(roomDriver)
  let disposal: Promise<void> | undefined

  return {
    spiVersion: driver.spiVersion,
    ...broadcast,
    ...room,
    dispose: () =>
      (disposal ??= Promise.all([disposeBroadcast(), disposeRoom()]).then(() => {
        return driver.dispose()
      })),
  }
}
