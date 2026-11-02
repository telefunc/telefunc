export { newHoldConfig, holdDelta, afterHoldChange, createHold, maintainHold, lapseNewRoom, cancelClose }
export type { HoldDelta }

// An unheld room closes on its own: the server whose write leaves it unheld closes it on a timer, and the head's lapse,
// which every heartbeat on the room keeps ahead, removes it if no server is left to.

import { assert } from '../../utils/assert.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { getGlobalObject } from '../../utils/getGlobalObject.js'
import { getRoomPlane } from '../../backend/plane.js'
import type { CellCount, CellCounter, RoomHead } from '../../backend/contract.js'
import { decodeCellCounter, isOpenIncarnation } from '../../backend/semantics.js'
import { TIMER_DELAY_MAX_MS } from '../../shared/constants.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_LAPSE_AFTER_CLOSE_MS, ROOM_LEASE_TTL_MS } from '../constants.js'
import type { HoldConfig, RoomIncarnation } from '../protocol.js'
import { roomClosedError } from '../../shared/errors.js'
import { HOLDS_CELL_KEY, JOINS_CELL_KEY } from '../store/cells.js'
import { closeIncarnation } from './close.js'
import { EXCHANGE_CONFLICT, retryCompareExchange } from '../store/retryCompareExchange.js'
import { reportRoomError } from '../reportRoomError.js'
assertIsNotBrowser()

/** What holds a room (its non-hidden members and pages' views), and how many joins it had. */
type HoldCounts = { holds: number; joins: number }

/** `closesIn` is set while nothing holds the room: the time from `readAt` until its unheld timeout after the hold count
 *  last changed. `readAt` is the authority time of the read or exchange that gave the record; app servers' clocks may
 *  be off it. */
type HoldRecord = HoldConfig & HoldCounts & { closesIn?: number; readAt: number }

type HoldChange = { before: HoldCounts; after: HoldRecord }

/** A write's change to the hold counts: the counts it commits, and the change they made once committed. */
type HoldDelta = { counts: CellCount[]; after(counters: ReadonlyMap<string, CellCounter>): HoldChange | null }

function newHoldConfig(emptyTimeout: number, departureTimeout: number): HoldConfig | null {
  if (emptyTimeout === Infinity && departureTimeout === Infinity) return null
  return { emptyTimeout, departureTimeout }
}

/** Adds `delta` holds, and a join with `joining`: whichever comes first, the room's creation or its first hold, creates
 *  both counters, and both come back with the commit. No counts in a room without hold timeouts. */
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

/** Every count creates both counters, so a room with timeouts that was counted has both. An exchange's time is taken
 *  as its counters' latest change, which is its own once it changed one. */
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

/** Null once the incarnation is gone, or while nothing counted it: it has no hold timeouts, or its creation's count
 *  failed and nothing has held it since. */
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

/** Counts a new room's creation, which a first hold may have counted before it. */
async function createHold(room: RoomIncarnation): Promise<void> {
  const { hold } = room
  if (hold === null) return
  const result = await getRoomPlane().driver.compareExchangeCells(room.id, room.inc, {
    expect: [],
    mutations: [],
    counts: holdDelta(hold, 0).counts,
  })
  if (result.kind === 'stale-incarnation') throw roomClosedError(room.id)
  assert(result.kind === 'committed')
  const record = holdRecord(hold, result.counters)
  if (record.closesIn !== undefined) scheduleClose(room, record.closesIn)
}

/** Each heartbeat, after it reaped lapsed leases: keeps a held room's lapse ahead, or arms the close of
 *  an unheld one, whose timer's server may be gone. */
async function maintainHold(room: RoomIncarnation): Promise<void> {
  const record = await readHoldRecord(room)
  if (record === null) return
  if (record.closesIn !== undefined) scheduleClose(room, record.closesIn)
  else await extendLapse(room, record)
}

/** Once its creation is counted, a new room nothing holds yet lapses just past its close. The record is read under
 *  the head's revision, which the lapse write of a first hold moves, so that hold's lapse wins: first the head the
 *  create wrote, then the one each conflict returns. Returns the head it leaves, while that is the room's open head. */
async function lapseNewRoom(room: RoomIncarnation, created: RoomHead): Promise<RoomHead | null> {
  if (room.hold === null) return null
  const { id, inc } = room
  const plane = getRoomPlane()
  let head: RoomHead | null = created
  return await retryCompareExchange(async () => {
    if (!isOpenIncarnation(head, inc)) return null
    const record = await readHoldRecord(room)
    if (record?.closesIn === undefined) return head
    const result = await plane.compareExchangeHead(
      id,
      { form: 'rev', rev: head.rev },
      {
        head: { currentInc: inc, state: 'open', config: head.config },
        ttlMs: record.closesIn + ROOM_LAPSE_AFTER_CLOSE_MS,
      },
    )
    if (result.kind === 'committed') return result.head
    head = result.current
    return EXCHANGE_CONFLICT
  })
}

/** A held room's head lapses once its holders' records would have lapsed and its timeout passed, with a heartbeat to
 *  spare: pushed out whenever less than that is left, so a holder's every heartbeat keeps it ahead. A first hold
 *  always writes, which fences a new room's lapse (`lapseNewRoom`) it raced. */
async function extendLapse(
  { id, inc }: RoomIncarnation,
  record: HoldRecord,
  opts?: { firstHold?: boolean },
): Promise<void> {
  const firstHold = opts?.firstHold === true
  const timeout = unheldTimeout(record)
  const ttlMs = timeout === Infinity ? undefined : ROOM_LEASE_TTL_MS + timeout + 2 * ROOM_HEARTBEAT_INTERVAL_MS
  const plane = getRoomPlane()
  await retryCompareExchange(async () => {
    const head = await plane.driver.readHead(id)
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
