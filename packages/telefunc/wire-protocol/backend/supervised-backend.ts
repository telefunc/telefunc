export { superviseBackend }

import { subscriptionSourceKey } from './subscription-source.js'
import { SubscriptionManager } from './subscriptions.js'
import { assertHeadNextWellFormed } from './head-transitions.js'
import type { BackendDriver, BackendSpi, HeadCx, HeadNext } from './spi.js'

/** The sole raw-driver-to-consumer composition path and owner of subscription supervision. */
function superviseBackend(
  driver: BackendDriver,
  disposeDriver: () => Promise<void> = () => driver.dispose(),
): BackendSpi {
  const subscriptions = new SubscriptionManager(driver.subscriptions, console.error, subscriptionSourceKey)
  let disposal: Promise<void> | undefined

  return {
    spiVersion: driver.spiVersion,

    publish: (lane, payload) =>
      subscriptions.publish({ kind: 'broadcast', lane }, payload, (ownedPayload) => driver.publish(lane, ownedPayload)),
    subscribe: (lane, receiver) => subscriptions.subscribe({ kind: 'broadcast', lane }, receiver),

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

    subscribeLane: (roomId, inc, lane, receiver) =>
      subscriptions.subscribe({ kind: 'durable', roomId, inc, lane }, receiver),

    listGenerations: (roomId) => driver.listGenerations(roomId),
    dropGeneration: async (roomId, inc) => {
      // Normative ordering: a refused or failed durable drop cannot close live sources. Termination is
      // the local consequence of a successful destruction, never an optimistic precursor to it.
      await driver.dropGeneration(roomId, inc)
      subscriptions.terminate((source) => source.kind === 'durable' && source.roomId === roomId && source.inc === inc)
    },

    directoryPut: (roomId, incTag) => driver.directoryPut(roomId, incTag),
    directoryDelete: (roomId, incTag) => driver.directoryDelete(roomId, incTag),
    directoryList: (prefix, cursor) => driver.directoryList(prefix, cursor),

    // Detach raw attempts before closing the resources their cleanup uses.
    dispose: () => (disposal ??= subscriptions.dispose().then(disposeDriver)),
  }
}
