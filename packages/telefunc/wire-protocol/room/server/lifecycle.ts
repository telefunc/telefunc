export { newHoldConfig, holdCounts, holdRecord, holdChange, afterHoldChange, scheduleClose, maintainHold, lapseNewRoom }

// An unheld room closes on its own: the server whose write leaves it unheld closes it on a timer, and the head's lapse,
// which every heartbeat on the room keeps ahead, removes it if no server is left to.

import { assert } from '../../../utils/assert.js'
import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getRoomBackend } from '../../backend/install.js'
import type { CellCount, CellCounter } from '../../backend/room/contract.js'
import { decodeCellCounter } from '../../backend/room/semantics.js'
import { TIMER_DELAY_MAX_MS } from '../../constants.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_MEMBER_TTL_MS } from '../constants.js'
import type { HoldConfig } from '../protocol.js'
import { HOLDS_CELL_KEY, JOINS_CELL_KEY } from './cells.js'
import { closeIncarnation } from './close.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { reportRoomError } from './errors.js'
assertIsNotBrowser()

/** How long past an unheld room's close its head lapses: the close that server's timer starts lands well before. */
const ROOM_LAPSE_AFTER_CLOSE_MS = 5_000

/** What holds a room (its non-hidden members and pages' views), and how many joins it had. */
type HoldCounts = { holds: number; joins: number }

/** `closesAt` is set while nothing holds the room: its unheld timeout after the hold count last changed. */
type HoldRecord = HoldConfig & HoldCounts & { closesAt?: number }

type HoldChange = { config: HoldConfig; before: HoldCounts; after: HoldRecord }

function newHoldConfig(emptyTimeout: number, departureTimeout: number): HoldConfig | null {
  if (emptyTimeout === Infinity && departureTimeout === Infinity) return null
  return { emptyTimeout, departureTimeout }
}

/** The counts that add `delta` holds, and a join with `joining`: whichever comes first, the room's creation or its
 *  first hold, creates both counters, and both come back with the commit. None in a room without hold timeouts. */
function holdCounts(config: HoldConfig | null, delta: number, joining = false): CellCount[] {
  if (config === null) return []
  return [
    { key: HOLDS_CELL_KEY, delta, create: true },
    { key: JOINS_CELL_KEY, delta: joining ? 1 : 0, create: true },
  ]
}

/** Every count creates both counters, so a room with timeouts that was counted has both. */
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
  return { config, before: { holds: after.holds - delta, joins: after.joins - (joining ? 1 : 0) }, after }
}

/** Null while nothing counted the room: its creation's count failed and nothing has held it since. */
async function readHoldRecord(roomId: string, inc: string, config: HoldConfig): Promise<HoldRecord | null> {
  const read = await getRoomBackend().readCells(roomId, inc, { keys: [HOLDS_CELL_KEY, JOINS_CELL_KEY] })
  if ('staleInc' in read || read.cells.size === 0) return null
  const counters = new Map<string, CellCounter>()
  for (const [key, raw] of read.cells) counters.set(key, decodeCellCounter(raw))
  return holdRecord(config, counters)
}

function unheldTimeout({ joins, emptyTimeout, departureTimeout }: HoldConfig & HoldCounts): number {
  return joins > 0 ? departureTimeout : emptyTimeout
}

/** After a committed hold change: an unheld room gets its close timer, a newly held one its lapse pushed out. */
function afterHoldChange(roomId: string, inc: string, change: HoldChange | null): void {
  if (change === null) return
  const { config, before, after } = change
  if (after.closesAt !== undefined) return scheduleClose(roomId, inc, config, after.closesAt)
  if (before.holds === 0 || before.joins > 0 !== after.joins > 0)
    void extendLapse(roomId, inc, after, before.holds === 0).catch(reportRoomError)
}

/** Each heartbeat, after its roster read reaped lapsed holds: keeps a held room's lapse ahead, or arms the close of
 *  an unheld one, whose timer's server may be gone. */
async function maintainHold(roomId: string, inc: string, config: HoldConfig | null): Promise<void> {
  if (config === null) return
  const record = await readHoldRecord(roomId, inc, config)
  if (record === null) return
  if (record.closesAt !== undefined) scheduleClose(roomId, inc, config, record.closesAt)
  else await extendLapse(roomId, inc, record)
}

/** Once its creation is counted, a new room nothing holds yet lapses just past its close. The record is read under
 *  the head's revision, which a first hold's lapse write moves, so that hold's lapse wins. */
async function lapseNewRoom(roomId: string, inc: string, config: HoldConfig): Promise<void> {
  const backend = getRoomBackend()
  await retryCompareExchange(async () => {
    const head = await backend.readHead(roomId)
    if (head?.state !== 'open' || head.currentInc !== inc) return
    const closesAt = (await readHoldRecord(roomId, inc, config))?.closesAt
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

function scheduleClose(roomId: string, inc: string, config: HoldConfig, closesAt: number): void {
  clearTimeout(closeTimers.get(roomId))
  const delay = Math.min(Math.max(closesAt - Date.now(), 0), TIMER_DELAY_MAX_MS)
  const timer = unrefTimer(
    setTimeout(() => {
      if (closeTimers.get(roomId) === timer) closeTimers.delete(roomId)
      void closeIfUnheld(roomId, inc, config).catch(reportRoomError)
    }, delay),
  )
  closeTimers.set(roomId, timer)
}

async function closeIfUnheld(roomId: string, inc: string, config: HoldConfig): Promise<void> {
  const closesAt = (await readHoldRecord(roomId, inc, config))?.closesAt
  if (closesAt === undefined) return // held again
  if (closesAt > Date.now()) return scheduleClose(roomId, inc, config, closesAt)
  await closeIncarnation(roomId, inc)
}
