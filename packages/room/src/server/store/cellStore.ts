export { readPrefix, readPrefixAt, readOpenCells, eachInFlight, deleteCells }
export type { CellsAt }

import { getRoomPlane } from '../../backend/plane.js'
import type { CellsRead } from '../../backend/contract.js'
import { Slice } from '../../shared/Slice.js'
import { roomClosedError } from '../../shared/errors.js'
import { ROOM_BACKEND_CALLS_IN_FLIGHT_MAX } from '../constants.js'

// A room's cells, read and deleted by the incarnation's server code: a read that throws once the incarnation closed.

/** The cells under `prefix`, a page and a `ROOM_PLAN_SLICE_MS` slice at a time. */
async function readPrefix(roomId: string, inc: string, prefix: string): Promise<Map<string, Uint8Array>> {
  return (await readPrefixAt(roomId, inc, prefix)).cells
}

/** Cells read, and the authority time of the last page's read. */
type CellsAt = { cells: Map<string, Uint8Array>; now: number }

async function readPrefixAt(roomId: string, inc: string, prefix: string): Promise<CellsAt> {
  const cells = new Map<string, Uint8Array>()
  let now = 0
  const slice = new Slice()
  for await (const page of getRoomPlane().driver.readCellPages(roomId, inc, prefix)) {
    if (page.kind === 'stale-incarnation') throw roomClosedError(roomId)
    for (const [key, raw] of page.cells) cells.set(key, raw)
    now = page.now
    if (slice.over) await slice.next()
  }
  return { cells, now }
}

async function readOpenCells(
  roomId: string,
  inc: string,
  keys: string[],
): Promise<Extract<CellsRead, { kind: 'read' }>> {
  const read = await getRoomPlane().driver.readCells(roomId, inc, { keys })
  if (read.kind === 'stale-incarnation') throw roomClosedError(roomId)
  return read
}

/** Runs `task` on each item in turn, `ROOM_BACKEND_CALLS_IN_FLIGHT_MAX` at a time and a `ROOM_PLAN_SLICE_MS` slice per
 *  turn; throws the first failure once every task ran. */
async function eachInFlight<T>(items: Iterable<T>, task: (item: T) => Promise<unknown>): Promise<void> {
  const pending = items[Symbol.iterator]()
  const slice = new Slice()
  let turn: Promise<void> | null = null
  let failure = null as { error: unknown } | null
  const nextTurn = async () => {
    await slice.next()
    turn = null
  }
  const worker = async () => {
    for (let next = pending.next(); next.done !== true; next = pending.next()) {
      if (slice.over) await (turn ??= nextTurn())
      try {
        await task(next.value)
      } catch (error) {
        failure ??= { error }
      }
    }
  }
  await Promise.all(Array.from({ length: ROOM_BACKEND_CALLS_IN_FLIGHT_MAX }, worker))
  if (failure) throw failure.error
}

/** Deletes `keys` in one exchange; false once the incarnation is stale. */
async function deleteCells(roomId: string, inc: string, keys: Iterable<string>): Promise<boolean> {
  const mutations = Array.from(keys, (key) => ({ key, bytes: null }))
  const result = await getRoomPlane().driver.compareExchangeCells(roomId, inc, { expect: [], mutations })
  return result.kind !== 'stale-incarnation'
}
