export {
  newHoldConfig,
  holdCells,
  holdConfigOf,
  holdCounts,
  holdRecord,
  holdChange,
  afterHoldChange,
  scheduleClose,
  maintainHold,
  lapseNewRoom,
}
export type { HoldConfig }

// An unheld room closes on its own: the server whose write leaves it unheld closes it on a timer, and the head's lapse,
// which every heartbeat on the room keeps ahead, removes it if no server is left to.

import { assert } from '../../../utils/assert.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getRoomBackend } from '../../backend/install.js'
import type { CellCount, CellCounter, CellMutation } from '../../backend/room/contract.js'
import { decodeCellCounter } from '../../backend/room/semantics.js'
import { TIMER_DELAY_MAX_MS } from '../../constants.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_MEMBER_TTL_MS } from '../constants.js'
import { HOLD_CELL_KEY, HOLDS_CELL_KEY, JOINS_CELL_KEY } from './cells.js'
import { closeIncarnation } from './close.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { reportRoomError } from './errors.js'
import { decodeRoomRecord, encodeRoomRecord } from './lanes.js'
assertIsNotBrowser()

/** How long past an unheld room's close its head lapses: the close that server's timer starts lands well before. */
const ROOM_LAPSE_AFTER_CLOSE_MS = 5_000

/** A room's hold timeouts; a room that never closes on its own has none. */
type HoldConfig = { emptyTimeout: number; departureTimeout: number }

/** What holds a room (its non-hidden members and pages' views), and how many joins it had. */
type HoldCounts = { holds: number; joins: number }

/** `closesAt` is set while nothing holds the room: its unheld timeout after the hold count last changed. */
type HoldRecord = HoldConfig & HoldCounts & { closesAt?: number }

type HoldChange = { before: HoldCounts; after: HoldRecord }

function newHoldConfig(emptyTimeout: number, departureTimeout: number): HoldConfig | null {
  if (emptyTimeout === Infinity && departureTimeout === Infinity) return null
  return { emptyTimeout, departureTimeout }
}

/** A new room's hold timeouts, and its counters created at zero. */
function holdCells(config: HoldConfig): { mutations: CellMutation[]; counts: CellCount[] } {
  return {
    mutations: [{ key: HOLD_CELL_KEY, bytes: encodeRoomRecord(config) }],
    counts: [
      { key: HOLDS_CELL_KEY, delta: 0, create: true },
      { key: JOINS_CELL_KEY, delta: 0, create: true },
    ],
  }
}

function holdConfigOf(cells: ReadonlyMap<string, Uint8Array>): HoldConfig | null {
  const raw = cells.get(HOLD_CELL_KEY)
  return raw === undefined ? null : decodeRoomRecord<HoldConfig>(raw)
}

/** The counts that add `delta` holds, and a join with `joining`: both counters come back with the commit. None in a
 *  room without hold timeouts. */
function holdCounts(config: HoldConfig | null, delta: number, joining = false): CellCount[] {
  if (config === null) return []
  return [
    { key: HOLDS_CELL_KEY, delta },
    { key: JOINS_CELL_KEY, delta: joining ? 1 : 0 },
  ]
}

/** The counters are written with the timeouts, so a room with timeouts has both. */
function holdRecord(config: HoldConfig, counters: ReadonlyMap<string, CellCounter>): HoldRecord {
  const holds = counters.get(HOLDS_CELL_KEY)
  const joins = counters.get(JOINS_CELL_KEY)
  assert(holds !== undefined && joins !== undefined)
  const record: HoldRecord = { ...config, holds: holds.value, joins: joins.value }
  const timeout = unheldTimeout(record)
  return record.holds > 0 || timeout === Infinity ? record : { ...record, closesAt: holds.changedAt + timeout }
}

/** The hold record a commit of `holdCounts(config, delta, joining)` left, and the counts before it. */
function holdChange(
  config: HoldConfig | null,
  counters: ReadonlyMap<string, CellCounter>,
  delta: number,
  joining = false,
): HoldChange | null {
  if (config === null) return null
  const after = holdRecord(config, counters)
  return { before: { holds: after.holds - delta, joins: after.joins - (joining ? 1 : 0) }, after }
}

async function readHoldRecord(roomId: string, inc: string): Promise<HoldRecord | null> {
  const keys = [HOLD_CELL_KEY, HOLDS_CELL_KEY, JOINS_CELL_KEY]
  const read = await getRoomBackend().readCells(roomId, inc, { keys })
  if ('staleInc' in read) return null
  const config = holdConfigOf(read.cells)
  if (config === null) return null
  const counters = new Map<string, CellCounter>()
  for (const [key, raw] of read.cells) if (key !== HOLD_CELL_KEY) counters.set(key, decodeCellCounter(raw))
  return holdRecord(config, counters)
}

function unheldTimeout({ joins, emptyTimeout, departureTimeout }: HoldConfig & HoldCounts): number {
  return joins > 0 ? departureTimeout : emptyTimeout
}

/** After a committed hold change: an unheld room gets its close timer, a newly held one its lapse pushed out. */
function afterHoldChange(roomId: string, inc: string, change: HoldChange | null): void {
  if (change === null) return
  const { before, after } = change
  if (after.closesAt !== undefined) return scheduleClose(roomId, inc, after.closesAt)
  if (before.holds === 0 || before.joins > 0 !== after.joins > 0)
    void extendLapse(roomId, inc, after, before.holds === 0).catch(reportRoomError)
}

/** Each heartbeat, after its roster read reaped lapsed holds: keeps a held room's lapse ahead, or arms the close of
 *  an unheld one, whose timer's server may be gone. */
async function maintainHold(roomId: string, inc: string): Promise<void> {
  const record = await readHoldRecord(roomId, inc)
  if (record === null) return
  if (record.closesAt !== undefined) scheduleClose(roomId, inc, record.closesAt)
  else await extendLapse(roomId, inc, record)
}

/** Once its hold record is written, a new room nothing holds yet lapses just past its close. The record is read under
 *  the head's revision, which a first hold's lapse write moves, so that hold's lapse wins. */
async function lapseNewRoom(roomId: string, inc: string): Promise<void> {
  const backend = getRoomBackend()
  await retryCompareExchange(async () => {
    const head = await backend.readHead(roomId)
    if (head?.state !== 'open' || head.currentInc !== inc) return
    const closesAt = (await readHoldRecord(roomId, inc))?.closesAt
    if (closesAt === undefined) return
    const result = await backend.compareExchangeHead(
      roomId,
      { form: 'rev', rev: head.rev },
      {
        head: { currentInc: inc, state: 'open', config: head.config },
        ttlMs: closesAt - Date.now() + ROOM_LAPSE_AFTER_CLOSE_MS,
      },
    )
    return 'conflict' in result ? CX_CONFLICT : undefined
  })
}

/** A held room's head lapses once its holders' records would have lapsed and its timeout passed, with a heartbeat to
 *  spare: pushed out whenever less than that is left, so a holder's every heartbeat keeps it ahead. A first hold
 *  always writes, which fences a new room's lapse (`lapseNewRoom`) it raced. */
async function extendLapse(roomId: string, inc: string, record: HoldRecord, firstHold = false): Promise<void> {
  const timeout = unheldTimeout(record)
  const ttlMs = timeout === Infinity ? undefined : ROOM_MEMBER_TTL_MS + timeout + 2 * ROOM_HEARTBEAT_INTERVAL_MS
  const backend = getRoomBackend()
  await retryCompareExchange(async () => {
    const head = await backend.readHead(roomId)
    if (head?.state !== 'open' || head.currentInc !== inc) return
    const { expiresAt } = head
    const ahead =
      ttlMs === undefined
        ? expiresAt === undefined
        : expiresAt !== undefined && expiresAt >= Date.now() + ttlMs - ROOM_HEARTBEAT_INTERVAL_MS
    if (ahead && !firstHold) return
    const result = await backend.compareExchangeHead(
      roomId,
      { form: 'rev', rev: head.rev },
      { head: { currentInc: inc, state: 'open', config: head.config }, ...(ttlMs === undefined ? {} : { ttlMs }) },
    )
    return 'conflict' in result ? CX_CONFLICT : undefined
  })
}

/** One close timer per room in this process; it checks the record again when it fires. */
const closeTimers = new Map<string, ReturnType<typeof setTimeout>>()

function scheduleClose(roomId: string, inc: string, closesAt: number): void {
  clearTimeout(closeTimers.get(roomId))
  const delay = Math.min(Math.max(closesAt - Date.now(), 0), TIMER_DELAY_MAX_MS)
  const timer = unrefTimer(
    setTimeout(() => {
      if (closeTimers.get(roomId) === timer) closeTimers.delete(roomId)
      void closeIfUnheld(roomId, inc).catch(reportRoomError)
    }, delay),
  )
  closeTimers.set(roomId, timer)
}

async function closeIfUnheld(roomId: string, inc: string): Promise<void> {
  const closesAt = (await readHoldRecord(roomId, inc))?.closesAt
  if (closesAt === undefined) return // held again
  if (closesAt > Date.now()) return scheduleClose(roomId, inc, closesAt)
  await closeIncarnation(roomId, inc)
}
