export { newHoldRecord, countHold, afterHoldChange, scheduleClose, maintainHold, lapseNewRoom }
export type { HoldRecord, HoldChange }

// An unheld room closes on its own: the server whose write leaves it unheld closes it on a timer, and the head's lapse,
// which every heartbeat on the room keeps ahead, removes it if no server is left to.

import { assertIsNotBrowser } from '../../../utils/assertIsNotBrowser.js'
import { unrefTimer } from '../../../utils/unrefTimer.js'
import { getRoomBackend } from '../../backend/install.js'
import type { CellMutation } from '../../backend/room/contract.js'
import { TIMER_DELAY_MAX_MS } from '../../constants.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_MEMBER_TTL_MS } from '../constants.js'
import { HOLD_CELL_KEY } from './cells.js'
import { closeIncarnation } from './close.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { reportRoomError } from './errors.js'
import { decodeRoomRecord, encodeRoomRecord } from './lanes.js'
assertIsNotBrowser()

/** How long past an unheld room's close its head lapses: the close that server's timer starts lands well before. */
const ROOM_LAPSE_AFTER_CLOSE_MS = 5_000

/** What holds a room (its non-hidden members and pages' views), counted in the compare-exchange that adds or removes
 *  one; `closesAt` is set while nothing holds it. A room that never closes on its own has no record. */
type HoldRecord = {
  emptyTimeout: number
  departureTimeout: number
  holds: number
  joined: boolean
  closesAt?: number
}

type HoldChange = { mutation: CellMutation; before: HoldRecord; after: HoldRecord }

function newHoldRecord(emptyTimeout: number, departureTimeout: number): HoldRecord | null {
  if (emptyTimeout === Infinity && departureTimeout === Infinity) return null
  return closingIfUnheld({ emptyTimeout, departureTimeout, holds: 0, joined: false })
}

/** The hold record with one hold more or less, or null for a room without one. */
function countHold(cells: ReadonlyMap<string, Uint8Array>, delta: 1 | -1, joining = false): HoldChange | null {
  const raw = cells.get(HOLD_CELL_KEY)
  if (raw === undefined) return null
  const before = decodeRoomRecord<HoldRecord>(raw)
  const { closesAt: _, ...held } = before
  const after = closingIfUnheld({ ...held, holds: held.holds + delta, joined: held.joined || joining })
  return { mutation: { key: HOLD_CELL_KEY, bytes: encodeRoomRecord(after) }, before, after }
}

function closingIfUnheld(record: HoldRecord): HoldRecord {
  const timeout = unheldTimeout(record)
  return record.holds > 0 || timeout === Infinity ? record : { ...record, closesAt: Date.now() + timeout }
}

function unheldTimeout({ joined, emptyTimeout, departureTimeout }: HoldRecord): number {
  return joined ? departureTimeout : emptyTimeout
}

/** After a committed hold change: an unheld room gets its close timer, a newly held one its lapse pushed out. */
function afterHoldChange(roomId: string, inc: string, change: HoldChange | null): void {
  if (change === null) return
  const { before, after } = change
  if (after.closesAt !== undefined) return scheduleClose(roomId, inc, after.closesAt)
  if (before.holds === 0 || before.joined !== after.joined)
    void extendLapse(roomId, inc, after, before.holds === 0).catch(reportRoomError)
}

/** Each heartbeat, after its roster read reaped lapsed holds: keeps a held room's lapse ahead, or arms the close of
 *  an unheld one, whose timer's server may be gone. */
async function maintainHold(roomId: string, inc: string): Promise<void> {
  const read = await getRoomBackend().readCells(roomId, inc, { keys: [HOLD_CELL_KEY] })
  const raw = 'staleInc' in read ? undefined : read.cells.get(HOLD_CELL_KEY)
  if (raw === undefined) return
  const record = decodeRoomRecord<HoldRecord>(raw)
  if (record.closesAt !== undefined) scheduleClose(roomId, inc, record.closesAt)
  else await extendLapse(roomId, inc, record)
}

/** Once its hold record is written, a new room nothing holds yet lapses just past its close. The record is read under
 *  the head's revision, which a first hold's lapse write moves, so that hold's lapse wins. */
async function lapseNewRoom(roomId: string, inc: string): Promise<void> {
  const backend = getRoomBackend()
  await retryCompareExchange(roomId, async () => {
    const head = await backend.readHead(roomId)
    if (head?.state !== 'open' || head.currentInc !== inc) return
    const read = await backend.readCells(roomId, inc, { keys: [HOLD_CELL_KEY] })
    const raw = 'staleInc' in read ? undefined : read.cells.get(HOLD_CELL_KEY)
    const closesAt = raw === undefined ? undefined : decodeRoomRecord<HoldRecord>(raw).closesAt
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
  await retryCompareExchange(roomId, async () => {
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
  const read = await getRoomBackend().readCells(roomId, inc, { keys: [HOLD_CELL_KEY] })
  const raw = 'staleInc' in read ? undefined : read.cells.get(HOLD_CELL_KEY)
  const closesAt = raw === undefined ? undefined : decodeRoomRecord<HoldRecord>(raw).closesAt
  if (closesAt === undefined) return // held again
  if (closesAt > Date.now()) return scheduleClose(roomId, inc, closesAt)
  await closeIncarnation(roomId, inc)
}
