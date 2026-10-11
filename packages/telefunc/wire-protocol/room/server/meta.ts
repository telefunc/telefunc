export { readRoomMeta, writeRoomMeta }
export type { StoredRoomMeta, RoomMetaWrite }

import { getRoomBackend } from '../../backend/install.js'
import type { CellMutation, CellsCx, CellsRead } from '../../backend/room/contract.js'
import { decodeCellCounter } from '../../backend/room/semantics.js'
import { nextTurnOf } from '../../flow-control/macrotask-yield.js'
import { ROOM_PLAN_SLICE_MS } from '../constants.js'
import { roomClosedError } from '../errors.js'
import { mergeAttributes } from '../model.js'
import type { RoomConfigRecord } from '../protocol.js'
import type { RoomMeta } from '../types.js'
import {
  META_CELL_PREFIX,
  META_GEN_CELL_KEY,
  META_SEQ_CELL_KEY,
  metaKeyCellKey,
  metaKeyCellPrefix,
  metaKeyOfCellKey,
} from './cells.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { decodeRoomRecord, encodeRoomRecord, publishCtrl } from './lanes.js'

/** `seq` counts the writes that made `meta`, so it orders `update` events. */
type StoredRoomMeta = { meta: RoomMeta; seq: number }
type RoomMetaWrite = { replace: RoomMeta } | { merge: RoomMeta }
/** A key's place: the `meta:seq` the write that added it read, which every later write reads higher, and its index in
 *  that write. Keys that concurrent writes added tie, and go by key. */
type MetaKeyCell = { value: unknown; place: [seq: number, index: number] }
type MetaEntry = MetaKeyCell & { key: string }

/** Paged reads that a write landing in invalidates before the read takes the meta in one call. */
const META_PAGED_READS = 2

/** Null once the incarnation is gone. Until its first write, the meta is the one the room was created with. A read
 *  pages its generation's keys between two reads of `meta:seq`, which every write counts, and keeps the pages only
 *  when no write landed between. */
async function readRoomMeta(roomId: string, { inc, meta }: RoomConfigRecord): Promise<StoredRoomMeta | null> {
  const backend = getRoomBackend()
  for (let attempt = 0; attempt < META_PAGED_READS; attempt++) {
    const before = await backend.readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY, META_GEN_CELL_KEY] })
    if ('staleInc' in before) return null
    const seq = before.cells.get(META_SEQ_CELL_KEY)
    if (seq === undefined) return { meta, seq: 0 }
    const gen = decodeRoomRecord<number>(before.cells.get(META_GEN_CELL_KEY)!)
    const entries: MetaEntry[] = []
    let deadline = performance.now() + ROOM_PLAN_SLICE_MS
    for await (const page of backend.readCellPages(roomId, inc, metaKeyCellPrefix(gen))) {
      if ('staleInc' in page) return null
      decodeMetaCells(page.cells, gen, entries)
      if (performance.now() < deadline) continue
      await nextTurnOf()
      deadline = performance.now() + ROOM_PLAN_SLICE_MS
    }
    const after = await backend.readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY] })
    if ('staleInc' in after) return null
    if (after.versions.get(META_SEQ_CELL_KEY) === before.versions.get(META_SEQ_CELL_KEY))
      return storedMeta(entries, seq)
  }
  const read = await backend.readCells(roomId, inc, { prefix: META_CELL_PREFIX })
  if ('staleInc' in read) return null
  const gen = decodeRoomRecord<number>(read.cells.get(META_GEN_CELL_KEY)!)
  const prefix = metaKeyCellPrefix(gen)
  const cells = new Map([...read.cells].filter(([key]) => key.startsWith(prefix)))
  return storedMeta(decodeMetaCells(cells, gen, []), read.cells.get(META_SEQ_CELL_KEY)!)
}

function decodeMetaCells(cells: ReadonlyMap<string, Uint8Array>, gen: number, into: MetaEntry[]): MetaEntry[] {
  for (const [key, raw] of cells) into.push({ key: metaKeyOfCellKey(gen, key), ...decodeRoomRecord<MetaKeyCell>(raw) })
  return into
}

function storedMeta(entries: MetaEntry[], seq: Uint8Array): StoredRoomMeta {
  entries.sort((a, b) => a.place[0] - b.place[0] || a.place[1] - b.place[1] || (a.key < b.key ? -1 : 1))
  return { meta: Object.fromEntries(entries.map(({ key, value }) => [key, value])), seq: decodeCellCounter(seq).value }
}

/** Once the room's meta was first written, a write is one exchange that expects only `meta:gen`, which only a replace
 *  writes, so concurrent merges never conflict; the first write stores the whole meta, and only the writes racing it
 *  conflict, once. A merge reads its keys with `meta:seq`, so a key already there keeps its place, and retries once a
 *  replace landed. A replace writes its keys into a new generation and drops the old one's a page at a time; one that
 *  finds a replace landed since its read is ordered just before it, so it is overwritten whole and writes nothing. A
 *  generation a crashed replace left goes with its incarnation. */
async function writeRoomMeta(roomId: string, config: RoomConfigRecord, write: RoomMetaWrite): Promise<void> {
  const { inc } = config
  const backend = getRoomBackend()
  const replaced = await retryCompareExchange(async () => {
    const read = await readMetaCells(roomId, inc, [META_SEQ_CELL_KEY, META_GEN_CELL_KEY])
    const seq = read.cells.get(META_SEQ_CELL_KEY)
    if (seq === undefined) {
      const meta = 'replace' in write ? write.replace : mergeAttributes(config.meta, write.merge)
      const result = await backend.compareExchangeCells(roomId, inc, {
        expect: [{ key: META_SEQ_CELL_KEY, version: null }],
        mutations: [genMutation(0), ...metaMutations(meta, 0, 0)],
        counts: [{ key: META_SEQ_CELL_KEY, delta: 1, create: true }],
      })
      if (result === 'stale-inc') throw roomClosedError(roomId)
      return result === 'conflict' ? CX_CONFLICT : null
    }
    const counts = [{ key: META_SEQ_CELL_KEY, delta: 1 }]
    const expect = [{ key: META_GEN_CELL_KEY, version: read.versions.get(META_GEN_CELL_KEY)! }]
    const count = decodeCellCounter(seq).value
    const gen = decodeRoomRecord<number>(read.cells.get(META_GEN_CELL_KEY)!)
    let cx: CellsCx
    if ('replace' in write) {
      cx = { expect, mutations: [genMutation(count), ...metaMutations(write.replace, count, count)], counts }
    } else {
      const keys = Object.keys(write.merge).map((key) => metaKeyCellKey(gen, key))
      const merging = keys.length === 0 ? new Map() : (await readMetaCells(roomId, inc, keys)).cells
      cx = { expect, mutations: metaMutations(write.merge, gen, count, merging), counts }
    }
    const result = await backend.compareExchangeCells(roomId, inc, cx)
    if (result === 'stale-inc') throw roomClosedError(roomId)
    if (result !== 'conflict') return 'replace' in write ? gen : null
    return 'replace' in write ? null : CX_CONFLICT
  })
  // Read after the write, so the newest of the concurrent writes publishes the newest meta.
  const stored = await readRoomMeta(roomId, config)
  if (stored === null) throw roomClosedError(roomId)
  await publishCtrl(roomId, inc, { __r: 'update', ...stored })
  if (replaced !== null) await dropMetaGeneration(roomId, inc, replaced)
}

async function readMetaCells(
  roomId: string,
  inc: string,
  keys: string[],
): Promise<Exclude<CellsRead, { staleInc: true }>> {
  const read = await getRoomBackend().readCells(roomId, inc, { keys })
  if ('staleInc' in read) throw roomClosedError(roomId)
  return read
}

/** The generation a replace left, a page per exchange and a `ROOM_PLAN_SLICE_MS` slice per turn. */
async function dropMetaGeneration(roomId: string, inc: string, gen: number): Promise<void> {
  const backend = getRoomBackend()
  let deadline = performance.now() + ROOM_PLAN_SLICE_MS
  for await (const page of backend.readCellPages(roomId, inc, metaKeyCellPrefix(gen))) {
    if ('staleInc' in page) return
    const mutations = [...page.cells.keys()].map((key) => ({ key, bytes: null }))
    if ((await backend.compareExchangeCells(roomId, inc, { expect: [], mutations })) === 'stale-inc') return
    if (performance.now() < deadline) continue
    await nextTurnOf()
    deadline = performance.now() + ROOM_PLAN_SLICE_MS
  }
}

function genMutation(gen: number): CellMutation {
  return { key: META_GEN_CELL_KEY, bytes: encodeRoomRecord(gen) }
}

/** A new key's place is `seq` and its index here. With the `merging` cells read, a key found there keeps its place,
 *  and an `undefined` value deletes its key, as in `mergeAttributes`. */
function metaMutations(
  meta: RoomMeta,
  gen: number,
  seq: number,
  merging?: ReadonlyMap<string, Uint8Array>,
): CellMutation[] {
  return Object.entries(meta).map(([key, value], index) => {
    const cellKey = metaKeyCellKey(gen, key)
    if (merging !== undefined && value === undefined) return { key: cellKey, bytes: null }
    const found = merging?.get(cellKey)
    const place: MetaKeyCell['place'] = found === undefined ? [seq, index] : decodeRoomRecord<MetaKeyCell>(found).place
    return { key: cellKey, bytes: encodeRoomRecord({ value, place } satisfies MetaKeyCell) }
  })
}
