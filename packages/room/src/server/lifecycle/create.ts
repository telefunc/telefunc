export { tryCreateRoom }

import { assert, assertUsage } from '../../utils/assert.js'
import { isObject } from 'telefunc/__internal'
import { getRoomPlane } from '../../backend/plane.js'
import type { HeadExpectation, HeadNext, RoomHead } from '../../backend/contract.js'
import { assertKnownOptions, isRecord, ownMetadata } from '../../shared/arguments.js'
import type { RoomConfigRecord, RoomIncarnation } from '../protocol.js'
import type { RoomMeta, RoomOptions } from '../../types.js'
import { encodeRoomRecord } from '../lanes/lanes.js'
import type { ServerRoom } from '../room.js'
import { RoomReplica } from '../RoomReplica.js'
import { metaEntriesAt } from '../../shared/RoomMetaState.js'
import { EXCHANGE_CONFLICT, retryCompareExchange } from '../store/retryCompareExchange.js'
import { cleanupFinalizedIncarnation, takeOverClose } from './close.js'
import { newHoldConfig, newRoomHold, scheduleNewRoomClose } from './hold.js'
import { listRoom, sweepLapsedRoomsSoon } from '../store/directory.js'
import { assertRoomTimeout, getRoomConfig } from '../config.js'

// A create reads the head until it may replace it, and replaces it in one compare-exchange, which counts the new
// incarnation's holds and sets its lapse; a head that changed under the exchange sends it back to the read. Then it
// lists the room. They run in one async attempt.

type TryCreateRoomResult = { kind: 'created'; room: ServerRoom } | { kind: 'exists' } | { kind: 'closing' }
type Refusal = { kind: 'exists' } | { kind: 'closing' }

async function tryCreateRoom(id: string, options: RoomOptions | undefined): Promise<TryCreateRoomResult> {
  const { meta, emptyTimeout, departureTimeout } = normalizeRoomOptions(options)
  const plane = getRoomPlane()
  return await retryCompareExchange(async () => {
    // The head a create may replace: a closing one taken over, a closed one cleaned up.
    let current = await plane.driver.readHead(id)
    if (current?.state === 'closing') {
      if (!(await takeOverClose(plane, id, current))) return { kind: 'closing' }
      current = await plane.driver.readHead(id)
    }
    if (current?.state === 'closed') await cleanupFinalizedIncarnation(plane, id, current)
    if (current !== null && current.state !== 'closed') return { kind: 'exists' }
    const incarnation: RoomIncarnation = {
      id,
      inc: crypto.randomUUID(),
      hold: newHoldConfig(emptyTimeout, departureTimeout),
    }
    const created: RoomConfigRecord = { meta, inc: incarnation.inc, hold: incarnation.hold }
    // An incarnation that lapsed is still listed, and its data still stored: dropped as `Room.list()` would.
    const lapsedInc = current === null ? await plane.driver.directoryGet(id) : undefined
    // A creator that stops past its head leaves a listing the sweep finds: the lapsed incarnation's, or this one.
    if (lapsedInc === undefined) await listRoom(plane, incarnation, undefined)
    const next = { head: openHead(created), ...newRoomHold(incarnation.hold) }
    const result = await plane.compareExchangeHead(id, replacing(current), next)
    if (result.kind === 'conflict') return refusalOrRetry(result.current)
    scheduleNewRoomClose(incarnation)
    // The lapsed incarnation's data goes before the listing that names it is replaced.
    if (lapsedInc !== undefined) await plane.driver.dropIncarnation(id, lapsedInc)
    await listRoom(plane, incarnation, result.head.expiresAt)
    sweepLapsedRoomsSoon()
    return { kind: 'created', room: seededRoom(id, created) }
  })
}

/** What the head exchange expects of the head the create read: still absent, or still at its revision. */
function replacing(current: RoomHead | null): HeadExpectation {
  return current === null ? { form: 'absent' } : { form: 'rev', rev: current.rev }
}

function openHead(created: RoomConfigRecord): HeadNext['head'] {
  return { currentInc: created.inc, state: 'open', config: encodeRoomRecord(created) }
}

/** A head that went away (a lapsed tombstone) or closed meanwhile still allows the create: try again. */
function refusalOrRetry(conflicting: RoomHead | null): Refusal | typeof EXCHANGE_CONFLICT {
  if (conflicting === null || conflicting.state === 'closed') return EXCHANGE_CONFLICT
  return conflicting.state === 'closing' ? { kind: 'closing' } : { kind: 'exists' }
}

/** This process's view of the new incarnation: its meta at seq 0, no members. */
function seededRoom(id: string, created: RoomConfigRecord): ServerRoom {
  const meta = { entries: metaEntriesAt(created.meta, 0), seq: 0 }
  return RoomReplica.seeded(id, created, meta, { members: [] }).handle()
}

function normalizeRoomOptions(options: RoomOptions | undefined): {
  meta: RoomMeta
  emptyTimeout: number
  departureTimeout: number
} {
  assertUsage(options === undefined || isObject(options), 'Room options should be an object')
  assertKnownOptions(options, ['meta', 'emptyTimeout', 'departureTimeout'], 'Room')
  const meta = options?.meta ?? {}
  assertUsage(isRecord(meta), 'options.meta should be an object')
  const defaults = getRoomConfig()
  const emptyTimeout = options?.emptyTimeout ?? defaults.emptyTimeout
  const departureTimeout = options?.departureTimeout ?? defaults.departureTimeout
  assertRoomTimeout(emptyTimeout, 'options.emptyTimeout')
  assertRoomTimeout(departureTimeout, 'options.departureTimeout')
  return { meta: ownMetadata(meta), emptyTimeout, departureTimeout }
}
