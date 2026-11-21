export { readPrefix, readPrefixAt, readOpenCells, deleteCells }
export type { CellsAt }

import type { CellsRead } from '../../backend/contract.js'
import type { RoomStore } from '../../backend/RoomStore.js'
import { Slice } from '../../shared/Slice.js'
import { roomClosedError } from '../../shared/errors.js'

// A room's cells, read and deleted by the incarnation's server code: a read that throws once the incarnation closed.

/** The cells under `prefix`, a page and a `ROOM_PLAN_SLICE_MS` slice at a time. */
async function readPrefix(
  store: RoomStore,
  roomId: string,
  inc: string,
  prefix: string,
): Promise<Map<string, Uint8Array>> {
  return (await readPrefixAt(store, roomId, inc, prefix)).cells
}

/** Cells read, and the authority time of the last page's read. */
type CellsAt = { cells: Map<string, Uint8Array>; now: number }

async function readPrefixAt(store: RoomStore, roomId: string, inc: string, prefix: string): Promise<CellsAt> {
  const cells = new Map<string, Uint8Array>()
  let now = 0
  const slice = new Slice()
  for await (const page of store.reads.readCellPages(roomId, inc, prefix)) {
    if (page.kind === 'stale-incarnation') throw roomClosedError(roomId)
    for (const [key, raw] of page.cells) cells.set(key, raw)
    now = page.now
    if (slice.over) await slice.next()
  }
  return { cells, now }
}

async function readOpenCells(
  store: RoomStore,
  roomId: string,
  inc: string,
  keys: string[],
): Promise<Extract<CellsRead, { kind: 'read' }>> {
  const read = await store.reads.readCells(roomId, inc, { keys })
  if (read.kind === 'stale-incarnation') throw roomClosedError(roomId)
  return read
}

/** Deletes `keys` in one exchange; false once the incarnation is stale. */
async function deleteCells(store: RoomStore, roomId: string, inc: string, keys: Iterable<string>): Promise<boolean> {
  const mutations = Array.from(keys, (key) => ({ key, bytes: null }))
  const result = await store.compareExchangeCells(roomId, inc, { expect: [], mutations })
  return result.kind !== 'stale-incarnation'
}
