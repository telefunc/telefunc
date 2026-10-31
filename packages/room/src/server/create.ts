export { tryCreateRoom }

import { assert, assertUsage } from '../utils/assert.js'
import { isObject } from '../utils/isObject.js'
import { getRoomBackend } from '../backend/plane.js'
import type { RoomBackend, RoomHead } from '../backend/contract.js'
import { assertKnownOptions, assertRoomTimeout, isRecord, ownMetadata } from '../model.js'
import type { RoomConfigRecord } from './protocol.js'
import type { RoomMeta, RoomOptions } from '../types.js'
import { encodeRoomRecord } from './lanes.js'
import { ServerRoom } from './room.js'
import { RoomReplica } from './replica.js'
import { metaEntriesAt } from '../metaState.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { cleanupFinalizedIncarnation, takeOverClose } from './close.js'
import { createHold, lapseNewRoom, newHoldConfig, type RoomIncarnation } from './lifecycle.js'
import { listRoom, sweepLapsedRoomsSoon } from './directory.js'
import { getRoomConfig } from './config.js'

// A create reads the head until it may replace it, replaces it in one compare-exchange, and only then sets up the new
// incarnation; a head that changed under the exchange sends it back to the read.

type TryCreateRoomResult = { kind: 'created'; room: ServerRoom } | { kind: 'exists' } | { kind: 'closing' }
type Refusal = { kind: 'exists' } | { kind: 'closing' }

async function tryCreateRoom(id: string, options: RoomOptions | undefined): Promise<TryCreateRoomResult> {
  const { meta, emptyTimeout, departureTimeout } = normalizeRoomOptions(options)
  const backend = getRoomBackend()
  return await retryCompareExchange(async () => {
    const current = await resolveHeadForCreate(backend, id)
    if (current !== null && 'kind' in current) return current
    const incarnation: RoomIncarnation = {
      id,
      inc: crypto.randomUUID(),
      hold: newHoldConfig(emptyTimeout, departureTimeout),
    }
    const created: RoomConfigRecord = { meta, inc: incarnation.inc, hold: incarnation.hold }
    // An incarnation that lapsed is still listed, and its data still stored: dropped as `Room.list()` would.
    const lapsedInc = current === null ? await backend.directoryGet(id) : undefined
    // A creator that stops past its head leaves a listing the sweep finds: the lapsed incarnation's, or this one.
    if (lapsedInc === undefined) await listRoom(backend, incarnation, undefined)
    const committed = await commitNewHead(backend, id, current, created)
    if (committed !== true) return committed
    return { kind: 'created', room: await initNewIncarnation(backend, incarnation, created, lapsedInc) }
  })
}

/** The head a create may replace (null: none), once a closing one was taken over and a closed one cleaned up; or why it
 *  may not. */
async function resolveHeadForCreate(backend: RoomBackend, id: string): Promise<RoomHead | null | Refusal> {
  let current = await backend.readHead(id)
  if (current?.state === 'closing') {
    if (!(await takeOverClose(backend, id, current))) return { kind: 'closing' }
    current = await backend.readHead(id)
  }
  if (current?.state === 'closed') await cleanupFinalizedIncarnation(backend, id, current)
  if (current !== null && current.state !== 'closed') return { kind: 'exists' }
  return current
}

/** Replaces `current` with the new incarnation's open head; true once it did. */
async function commitNewHead(
  backend: RoomBackend,
  id: string,
  current: RoomHead | null,
  created: RoomConfigRecord,
): Promise<true | Refusal | typeof CX_CONFLICT> {
  const result = await backend.compareExchangeHead(
    id,
    current === null ? { form: 'absent' } : { form: 'rev', rev: current.rev },
    { head: { currentInc: created.inc, state: 'open', config: encodeRoomRecord(created) } },
  )
  if ('conflict' in result) {
    // A head that went away (a lapsed tombstone) or closed meanwhile still allows the create: try again.
    if (result.current === null || result.current.state === 'closed') return CX_CONFLICT
    return result.current.state === 'closing' ? { kind: 'closing' } : { kind: 'exists' }
  }
  assert('head' in result)
  return true
}

/** Sets up an incarnation whose head was just written: its hold, its lapse, its listing; drops the lapsed one it
 *  replaced, and seeds this process's view. */
async function initNewIncarnation(
  backend: RoomBackend,
  incarnation: RoomIncarnation,
  created: RoomConfigRecord,
  lapsedInc: string | undefined,
): Promise<ServerRoom> {
  const { id } = incarnation
  // The head lapses only once the counters that keep it ahead are written.
  await createHold(incarnation)
  const lapsing = await lapseNewRoom(incarnation)
  if (lapsedInc !== undefined) await backend.dropGeneration(id, lapsedInc)
  await listRoom(backend, incarnation, lapsing?.expiresAt)
  sweepLapsedRoomsSoon()
  const meta = { entries: metaEntriesAt(created.meta, 0), seq: 0 }
  return new ServerRoom(RoomReplica.seeded(id, created, meta, { members: [] }))
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
