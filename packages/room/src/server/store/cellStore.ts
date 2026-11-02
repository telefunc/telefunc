export { readPrefix, readPrefixAt, readOpenCells, mutateCells, eachInFlight, dropCells, deleteCells }
export type { CellPlan, CellCommit, CellsAt }

import { getRoomPlane } from '../../backend/plane.js'
import type { CellCount, CellCounter, CellMutation, CellsRead } from '../../backend/contract.js'
import { Slice } from '../../shared/Slice.js'
import { roomClosedError } from '../../shared/errors.js'
import { ROOM_MEMBER_WRITES_IN_FLIGHT_MAX } from '../constants.js'
import { EXCHANGE_CONFLICT, retryCompareExchange } from './retryCompareExchange.js'

// A room's cells, read and written by the incarnation's server code: a read that throws once the incarnation closed, a
// compare-exchange retried until it commits, and paged drops.

type CellPlan<T> = { value: T; mutations: CellMutation[]; counts?: CellCount[] }
type CellCommit<T> = { value: T; counters: ReadonlyMap<string, CellCounter> }

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
  for await (const page of getRoomPlane().readCellPages(roomId, inc, prefix)) {
    if (page.kind === 'stale-incarnation') throw roomClosedError(roomId)
    for (const [key, raw] of page.cells) cells.set(key, raw)
    now = page.now
    if (slice.over) await slice.next()
  }
  return { cells, now }
}

/** Reads `keys`, plans from them and the read's authority time, and commits only while each is still as read. */
async function mutateCells<T>(
  roomId: string,
  inc: string,
  keys: string[],
  plan: (cells: ReadonlyMap<string, Uint8Array>, now: number) => CellPlan<T>,
): Promise<CellCommit<T>> {
  const plane = getRoomPlane()
  return await retryCompareExchange(async () => {
    const read = await plane.readCells(roomId, inc, { keys })
    if (read.kind === 'stale-incarnation') throw roomClosedError(roomId)
    const { value, mutations, counts = [] } = plan(read.cells, read.now)
    if (mutations.length === 0 && counts.length === 0) return { value, counters: new Map() }
    const expect = keys.map((key) => ({ key, version: read.versions.get(key) ?? null }))
    const result = await plane.compareExchangeCells(roomId, inc, { expect, mutations, counts })
    if (result.kind === 'stale-incarnation') throw roomClosedError(roomId)
    return result.kind === 'conflict' ? EXCHANGE_CONFLICT : { value, counters: result.counters }
  })
}

async function readOpenCells(
  roomId: string,
  inc: string,
  keys: string[],
): Promise<Extract<CellsRead, { kind: 'read' }>> {
  const read = await getRoomPlane().readCells(roomId, inc, { keys })
  if (read.kind === 'stale-incarnation') throw roomClosedError(roomId)
  return read
}

/** Runs `task` on each item in turn, `ROOM_MEMBER_WRITES_IN_FLIGHT_MAX` at a time and a `ROOM_PLAN_SLICE_MS` slice per
 *  turn; throws the first failure once every task ran. */
async function eachInFlight<T>(items: Iterable<T>, task: (item: T) => Promise<unknown>): Promise<void> {
  const pending = items[Symbol.iterator]()
  const slice = new Slice()
  let turn: Promise<void> | null = null
  let failure = null as { error: unknown } | null
  const worker = async () => {
    for (let next = pending.next(); next.done !== true; next = pending.next()) {
      if (slice.over) {
        await (turn ??= slice.next().then(() => {
          turn = null
        }))
      }
      try {
        await task(next.value)
      } catch (error) {
        failure ??= { error }
      }
    }
  }
  await Promise.all(Array.from({ length: ROOM_MEMBER_WRITES_IN_FLIGHT_MAX }, worker))
  if (failure) throw failure.error
}

/** Deletes the cells under `prefix`, a page per exchange and a `ROOM_PLAN_SLICE_MS` slice per turn; `mark` goes with
 *  the last page. False once the incarnation is stale. */
async function dropCells(roomId: string, inc: string, prefix: string, mark?: string): Promise<boolean> {
  let held: string[] = []
  const slice = new Slice()
  for await (const page of getRoomPlane().readCellPages(roomId, inc, prefix)) {
    if (page.kind === 'stale-incarnation') return false
    // Without a mark, each page goes as it is read; with one, a page waits for the next, so the last goes with it.
    if (mark === undefined) {
      if (!(await deleteCells(roomId, inc, page.cells.keys()))) return false
    } else {
      if (held.length > 0 && !(await deleteCells(roomId, inc, held))) return false
      held = [...page.cells.keys()]
    }
    if (slice.over) await slice.next()
  }
  if (mark === undefined) return true
  held.push(mark)
  return await deleteCells(roomId, inc, held)
}

/** Deletes `keys` in one exchange; false once the incarnation is stale. */
async function deleteCells(roomId: string, inc: string, keys: Iterable<string>): Promise<boolean> {
  const mutations = Array.from(keys, (key) => ({ key, bytes: null }))
  const result = await getRoomPlane().compareExchangeCells(roomId, inc, { expect: [], mutations })
  return result.kind !== 'stale-incarnation'
}
