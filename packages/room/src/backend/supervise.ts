export { superviseRoomDriver }

import {
  SubscriptionManager,
  assertDriverPosition,
  heldSendWeight,
  type BackendPayload,
  type BackendReceiver,
  type OrderingInfo,
  type SendHold,
} from 'telefunc/__internal'
import type { CommitTarget, HeadNext, RoomBackend, RoomDriver, RoomSubscriptionSource } from './contract.js'
import { roomSubscriptionSourceKey } from './laneKey.js'
import { assert } from '../utils/assert.js'

/** A lane carries bytes. */
function checkDelivery(_source: RoomSubscriptionSource, payload: BackendPayload, info: OrderingInfo): void {
  assertDriverPosition(info)
  assert(typeof payload !== 'string')
}

/** What a commit holds while this instance's subscriptions on its lanes establish. A binary commit also reaches its
 *  member's every-track lane and its track's every-member lane, and its held bytes count against its member's. */
function describeCommitHold(target: CommitTarget, payload: Uint8Array): SendHold<RoomSubscriptionSource> {
  const { roomId, inc, lane } = target
  const key = roomSubscriptionSourceKey(target)
  if (lane.kind !== 'binary') {
    return { key, sources: [target], weight: heldSendWeight('text', key, () => payload.byteLength, 'Room') }
  }
  const everyTrack = { roomId, inc, lane: { kind: 'binary-all', member: lane.member } } as const
  const everyMember = { roomId, inc, lane: { kind: 'binary-track', track: lane.track } } as const
  return {
    key,
    sources: [target, everyTrack, everyMember],
    weight: heldSendWeight('binary', roomSubscriptionSourceKey(everyTrack), () => payload.byteLength, 'Room'),
  }
}

/** Owns the Room subscription manager, holds a lane's commits while this instance's subscription on it establishes,
 *  and checks what the driver is given and returns. */
function superviseRoomDriver(driver: RoomDriver): RoomBackend {
  const subscriptions = new SubscriptionManager({
    driver: driver.subscriptions,
    sourceKey: roomSubscriptionSourceKey,
    checkDelivery,
  })

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
      const commit = () => driver.commitLane(target, payload, opts)
      // A close commits under its lease, which would lapse before the hold ends.
      const result =
        opts?.closingLease === undefined
          ? await subscriptions.afterEstablished(commit, () => describeCommitHold(target, payload))
          : await commit()
      if ('accepted' in result) assertDriverPosition(result)
      return result
    },
    readLaneOrder: async (roomId, inc, lane) => {
      const order = await driver.readLaneOrder(roomId, inc, lane)
      if (order !== null) assertDriverPosition(order)
      return order
    },
    readRetained: async (roomId, inc, lane) => {
      const retained = await driver.readRetained(roomId, inc, lane)
      if (retained !== null) assertDriverPosition(retained)
      return retained
    },
    listRetained: (roomId, inc, of) => driver.listRetained(roomId, inc, of),
    deleteRetained: (roomId, inc, lane, opts) => driver.deleteRetained(roomId, inc, lane, opts),
    dropLaneOrders: (roomId, inc, member) => driver.dropLaneOrders(roomId, inc, member),
    // The manager hands a lane's consumers only the deliveries checkDelivery passed.
    subscribeLane: (roomId, inc, lane, receiver) =>
      subscriptions.subscribe({ roomId, inc, lane }, receiver as BackendReceiver<BackendPayload>),
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
