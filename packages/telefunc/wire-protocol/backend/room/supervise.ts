export { superviseRoomDriver }

import { SubscriptionManager } from '../subscription-manager.js'
import type { HeadCx, HeadNext, RoomBackend, RoomDriver } from './contract.js'
import { roomSubscriptionSourceKey } from './lane-key.js'
import { assertDriverPosition } from '../driver-position.js'
import { assert } from '../../../utils/assert.js'

/** Owns the Room subscription manager, holds a lane's commits while this instance's subscription on it establishes,
 *  and checks what the driver is given and returns. */
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
      const source = { roomId, inc, lane }
      const commit = () => driver.commitLane(roomId, inc, lane, payload, opts)
      // A close commits under its lease, which would lapse before the hold ends.
      const result = await (opts?.closingLease === undefined
        ? subscriptions.afterEstablished(roomSubscriptionSourceKey(source), [source], commit)
        : commit())
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
    ttlMs === undefined || head.state === 'closed',
    `head CX: ttlMs is only valid for a 'closed' tombstone, got '${head.state}'`,
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
