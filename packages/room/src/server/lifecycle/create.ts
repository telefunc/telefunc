export { tryCreateRoom }

import { assertUsage } from '../../utils/assert.js'
import { isObject } from 'telefunc/__internal'
import type { RoomStore } from '../../backend/RoomStore.js'
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

type TryCreateRoomResult = { kind: 'created'; room: ServerRoom } | Refusal
/** `head`: the open head it found. */
type Refusal = { kind: 'exists'; head: RoomHead } | { kind: 'closing' }

/** Its reads and writes are steps of the call `store` is of. */
async function tryCreateRoom(
  store: RoomStore,
  id: string,
  options: RoomOptions | undefined,
): Promise<TryCreateRoomResult> {
  const { meta, emptyTimeout, departureTimeout } = normalizeRoomOptions(options)
  return await retryCompareExchange(async () => {
    // The head a create may replace: a closing one taken over, a closed one cleaned up.
    let current = await store.reads.readHead(id)
    if (current?.state === 'closing') {
      const closing = current
      if (!(await takeOverClose(store, id, closing))) return { kind: 'closing' }
      current = await store.reads.readHead(id)
    }
    if (current?.state === 'closed') {
      const closed = current
      await cleanupFinalizedIncarnation(store, id, closed)
    }
    if (current !== null && current.state !== 'closed') return refusalOrRetry(current)
    const incarnation: RoomIncarnation = {
      id,
      inc: crypto.randomUUID(),
      hold: newHoldConfig(emptyTimeout, departureTimeout),
    }
    const created: RoomConfigRecord = { meta, inc: incarnation.inc, hold: incarnation.hold }
    // An incarnation that lapsed is still listed, and its data still stored: dropped as `Room.list()` would.
    const lapsedInc = current === null ? (await store.reads.directoryGet(id))?.incTag : undefined
    // A creator that stops past its head leaves a listing the sweep finds: the lapsed incarnation's, or this one.
    if (lapsedInc === undefined) await listRoom(store, incarnation, undefined)
    const next = { head: openHead(created), ...newRoomHold(incarnation.hold) }
    const expected = replacing(current)
    const result = await store.compareExchangeHead(id, expected, next)
    if (result.kind === 'conflict') return refusalOrRetry(result.current)
    scheduleNewRoomClose(incarnation)
    // The lapsed incarnation's data goes before the listing that names it is replaced.
    if (lapsedInc !== undefined) await store.dropIncarnation(id, lapsedInc)
    await listRoom(store, incarnation, result.head.expiresAt)
    sweepLapsedRoomsSoon()
    return { kind: 'created', room: seededRoom(id, created) }
  })
}

/** What the head exchange expects of the head the create read: still absent, or still at its revision. */
function replacing(current: RoomHead | null): HeadExpectation {
  return current === null ? { form: 'absent' } : { form: 'rev', rev: current.rev }
}

function openHead(created: RoomConfigRecord): HeadNext['head'] {
  return { rev: crypto.randomUUID(), currentInc: created.inc, state: 'open', config: encodeRoomRecord(created) }
}

/** A head that went away (a lapsed tombstone) or closed meanwhile still allows the create: try again. */
function refusalOrRetry(conflicting: RoomHead | null): Refusal | typeof EXCHANGE_CONFLICT {
  if (conflicting === null || conflicting.state === 'closed') return EXCHANGE_CONFLICT
  return conflicting.state === 'closing' ? { kind: 'closing' } : { kind: 'exists', head: conflicting }
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
