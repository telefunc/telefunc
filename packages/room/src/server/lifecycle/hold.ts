export { newHoldConfig, newRoomHold, holdDelta, afterHoldChange, scheduleNewRoomClose, maintainHold, cancelClose }
export type { HoldDelta }

// An unheld room closes on its own: the server whose write leaves it unheld closes it on a timer, and the head's lapse,
// which every heartbeat on the room keeps ahead, removes it if no server is left to.

import { unrefTimer } from 'telefunc/__internal'
import { assert } from '../../utils/assert.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getRoomPlane } from '../../backend/plane.js'
import type { CellCount, CellCounter, HeadNext, RoomHead } from '../../backend/contract.js'
import { decodeCellCounter, isOpenIncarnation } from '../../backend/semantics.js'
import { TIMER_DELAY_MAX_MS } from '../../shared/constants.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_LAPSE_AFTER_CLOSE_MS, ROOM_LEASE_TTL_MS } from '../constants.js'
import type { HoldConfig, RoomIncarnation } from '../protocol.js'
import { HOLDS_CELL_KEY, JOINS_CELL_KEY } from '../store/cells.js'
import { closeIncarnation } from './close.js'
import { EXCHANGE_CONFLICT, retryCompareExchange } from '../store/retryCompareExchange.js'
import { reportRoomError } from '../reportRoomError.js'
assertIsNotBrowser()

/** What holds a room (its non-hidden members and pages' views), and how many joins it had. */
type HoldCounts = { holds: number; joins: number }

/** `closesIn` is set while nothing holds the room: how long after `readAt` it closes, its unheld timeout counted from
 *  the last hold change. `readAt` is on the authority's clock, which an app server's may differ from. */
type HoldRecord = HoldConfig & HoldCounts & { closesIn?: number; readAt: number }

type HoldChange = { before: HoldCounts; after: HoldRecord }

/** A write's change to the hold counts: the counts it commits, and the change they made once committed. */
type HoldDelta = { counts: CellCount[]; after(counters: ReadonlyMap<string, CellCounter>): HoldChange | null }

function newHoldConfig(emptyTimeout: number, departureTimeout: number): HoldConfig | null {
  if (emptyTimeout === Infinity && departureTimeout === Infinity) return null
  return { emptyTimeout, departureTimeout }
}

/** Adds `delta` holds, and one join when `joining`; each count creates a counter it lacks, and the commit returns
 *  them. A room without hold timeouts counts nothing. */
function holdDelta(config: HoldConfig | null, delta: number, opts?: { joining?: boolean }): HoldDelta {
  const joins = opts?.joining ? 1 : 0
  if (config === null) return { counts: [], after: () => null }
  return {
    counts: [
      { key: HOLDS_CELL_KEY, delta, create: true },
      { key: JOINS_CELL_KEY, delta: joins, create: true },
    ],
    after(counters) {
      const after = holdRecord(config, counters)
      return { before: { holds: after.holds - delta, joins: after.joins - joins }, after }
    },
  }
}

/** The record from both counters, which every count creates. Without `readAt` (after an exchange), the record is as of
 *  the counters' last change. */
function holdRecord(config: HoldConfig, counters: ReadonlyMap<string, CellCounter>, readAt?: number): HoldRecord {
  const holds = counters.get(HOLDS_CELL_KEY)
  const joins = counters.get(JOINS_CELL_KEY)
  assert(holds !== undefined && joins !== undefined)
  const at = readAt ?? Math.max(holds.changedAt, joins.changedAt)
  const { emptyTimeout, departureTimeout } = config
  const record: HoldRecord = { emptyTimeout, departureTimeout, holds: holds.value, joins: joins.value, readAt: at }
  const timeout = unheldTimeout(record)
  if (record.holds <= 0 && timeout !== Infinity) record.closesIn = holds.changedAt + timeout - at
  return record
}

/** Null once the incarnation is gone, and for a room without hold timeouts, which counts nothing. */
async function readHoldRecord({ id, inc, hold }: RoomIncarnation): Promise<HoldRecord | null> {
  if (hold === null) return null
  const read = await getRoomPlane().driver.readCells(id, inc, { keys: [HOLDS_CELL_KEY, JOINS_CELL_KEY] })
  if (read.kind === 'stale-incarnation' || read.cells.size === 0) return null
  const counters = new Map<string, CellCounter>()
  for (const [key, raw] of read.cells) counters.set(key, decodeCellCounter(raw))
  return holdRecord(hold, counters, read.now)
}

function unheldTimeout({ joins, emptyTimeout, departureTimeout }: HoldConfig & HoldCounts): number {
  return joins > 0 ? departureTimeout : emptyTimeout
}

/** After a committed hold change: an unheld room gets its close timer, a newly held one its lapse pushed out. */
function afterHoldChange(room: RoomIncarnation, change: HoldChange | null): void {
  if (change === null) return
  const { before, after } = change
  if (after.closesIn !== undefined) return scheduleClose(room, after.closesIn)
  if (before.holds === 0 || before.joins > 0 !== after.joins > 0) {
    void extendLapse(room, after, { firstHold: before.holds === 0 }).catch(reportRoomError)
  }
}

/** A new room's hold, which the exchange creating its head writes with it: both counters, and the head's lapse just
 *  after the room would close, as nothing holds it yet. */
function newRoomHold(hold: HoldConfig | null): Pick<HeadNext, 'counts' | 'ttlMs'> {
  if (hold === null) return {}
  const { counts } = holdDelta(hold, 0)
  return hold.emptyTimeout === Infinity ? { counts } : { counts, ttlMs: hold.emptyTimeout + ROOM_LAPSE_AFTER_CLOSE_MS }
}

/** A new room nothing holds yet closes `emptyTimeout` after its creation. */
function scheduleNewRoomClose(room: RoomIncarnation): void {
  if (room.hold !== null && room.hold.emptyTimeout !== Infinity) scheduleClose(room, room.hold.emptyTimeout)
}

/** Each heartbeat, after it reaped lapsed leases: keeps a held room's lapse ahead, or arms the close of
 *  an unheld one, whose timer's server may be gone. `head` is the head the heartbeat read. */
async function maintainHold(room: RoomIncarnation, head: RoomHead): Promise<void> {
  const record = await readHoldRecord(room)
  if (record === null) return
  if (record.closesIn !== undefined) scheduleClose(room, record.closesIn)
  else await extendLapse(room, record, { head })
}

/** A held room's head lapses once its holders' leases would have lapsed and its timeout passed, plus a heartbeat. Each
 *  heartbeat pushes it out when less than that is left. A first hold always writes: the head lapses as a new
 *  room's until then. A `head` already read saves the first read; if it changed since, the exchange reads it again. */
async function extendLapse(
  { id, inc }: RoomIncarnation,
  record: HoldRecord,
  opts?: { firstHold?: boolean; head?: RoomHead },
): Promise<void> {
  const firstHold = opts?.firstHold === true
  let known = opts?.head
  const timeout = unheldTimeout(record)
  const ttlMs = timeout === Infinity ? undefined : ROOM_LEASE_TTL_MS + timeout + 2 * ROOM_HEARTBEAT_INTERVAL_MS
  const plane = getRoomPlane()
  await retryCompareExchange(async () => {
    const head = known ?? (await plane.driver.readHead(id))
    known = undefined
    if (!isOpenIncarnation(head, inc)) return
    const { expiresAt } = head
    if (!firstHold && ttlMs === undefined && expiresAt === undefined) return
    const bothLapse = ttlMs !== undefined && expiresAt !== undefined
    if (!firstHold && bothLapse && expiresAt >= record.readAt + ttlMs - ROOM_HEARTBEAT_INTERVAL_MS) return
    const result = await plane.compareExchangeHead(
      id,
      { form: 'rev', rev: head.rev },
      { head: { currentInc: inc, state: 'open', config: head.config }, ttlMs },
    )
    return result.kind === 'conflict' ? EXCHANGE_CONFLICT : undefined
  })
}

/** One close timer per room in this process, with the incarnation it closes; it checks the record again when it
 *  fires. */
const { closeTimers } = getGlobalObject('server/lifecycle/hold.ts', () => ({
  closeTimers: new Map<string, { inc: string; timer: ReturnType<typeof setTimeout> }>(),
}))

function scheduleClose(room: RoomIncarnation, closesIn: number): void {
  clearTimeout(closeTimers.get(room.id)?.timer)
  const delay = Math.min(Math.max(closesIn, 0), TIMER_DELAY_MAX_MS)
  const timer = unrefTimer(
    setTimeout(() => {
      if (closeTimers.get(room.id)?.timer === timer) closeTimers.delete(room.id)
      void closeIfUnheld(room).catch(reportRoomError)
    }, delay),
  )
  closeTimers.set(room.id, { inc: room.inc, timer })
}

/** Incarnation `inc` closed: its close timer goes, a recreated room's stays. */
function cancelClose(id: string, inc: string): void {
  const scheduled = closeTimers.get(id)
  if (scheduled?.inc !== inc) return
  clearTimeout(scheduled.timer)
  closeTimers.delete(id)
}

async function closeIfUnheld(room: RoomIncarnation): Promise<void> {
  const record = await readHoldRecord(room)
  if (record?.closesIn === undefined) return // held again
  if (record.closesIn > 0) return scheduleClose(room, record.closesIn)
  await closeIncarnation(room.id, room.inc)
}
