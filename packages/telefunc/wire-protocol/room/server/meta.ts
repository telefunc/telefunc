export { readRoomMeta, writeRoomMeta }
export type { StoredRoomMeta, RoomMetaWrite }

import { getRoomBackend } from '../../backend/install.js'
import type { CellMutation, CellsRead, CxResult } from '../../backend/room/contract.js'
import { decodeCellCounter } from '../../backend/room/semantics.js'
import { nextTurnOf } from '../../flow-control/macrotask-yield.js'
import { ROOM_PLAN_SLICE_MS } from '../constants.js'
import { roomClosedError } from '../errors.js'
import { compareMetaEntries, metaEntriesAt } from '../meta-state.js'
import { mergeAttributes } from '../model.js'
import type { RoomConfigRecord, RoomMetaEntry, RoomMetaUpdate } from '../protocol.js'
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

/** `entries` in key order; `seq` counts the writes that made them, so it orders `update` events. */
type StoredRoomMeta = { entries: RoomMetaEntry[]; seq: number }
type RoomMetaWrite = { replace: RoomMeta } | { merge: RoomMeta }
/** A key's place: the `meta:seq` the write that added it read, which every later write reads higher, and its index in
 *  that write. Keys that concurrent writes added tie, and go by key. */
type MetaKeyCell = { value: unknown; place: [seq: number, index: number] }

/** Paged reads that a write landing in invalidates before the read takes the meta in one call. */
const META_PAGED_READS = 2
/** The keys sorted in one run of a whole read, as many as a page holds. */
const META_RUN_KEYS = 1024

/** Null once the incarnation is gone. Until its first write, the meta is the one the room was created with. A read
 *  pages its generation's keys between two reads of `meta:seq`, which every write counts, and keeps the pages only
 *  when no write landed between. */
async function readRoomMeta(roomId: string, { inc, meta }: RoomConfigRecord): Promise<StoredRoomMeta | null> {
  const backend = getRoomBackend()
  for (let attempt = 0; attempt < META_PAGED_READS; attempt++) {
    const before = await backend.readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY, META_GEN_CELL_KEY] })
    if ('staleInc' in before) return null
    const seq = before.cells.get(META_SEQ_CELL_KEY)
    if (seq === undefined) return { entries: metaEntriesAt(meta, 0), seq: 0 }
    const gen = decodeRoomRecord<number>(before.cells.get(META_GEN_CELL_KEY)!)
    const runs: RoomMetaEntry[][] = []
    let deadline = performance.now() + ROOM_PLAN_SLICE_MS
    for await (const page of backend.readCellPages(roomId, inc, metaKeyCellPrefix(gen))) {
      if ('staleInc' in page) return null
      runs.push(decodeMetaCells(page.cells, gen).sort(compareMetaEntries))
      if (performance.now() < deadline) continue
      await nextTurnOf()
      deadline = performance.now() + ROOM_PLAN_SLICE_MS
    }
    const after = await backend.readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY] })
    if ('staleInc' in after) return null
    if (after.versions.get(META_SEQ_CELL_KEY) === before.versions.get(META_SEQ_CELL_KEY))
      return { entries: await mergeRuns(runs), seq: decodeCellCounter(seq).value }
  }
  const read = await backend.readCells(roomId, inc, { prefix: META_CELL_PREFIX })
  if ('staleInc' in read) return null
  const gen = decodeRoomRecord<number>(read.cells.get(META_GEN_CELL_KEY)!)
  const prefix = metaKeyCellPrefix(gen)
  const runs: RoomMetaEntry[][] = []
  let run = new Map<string, Uint8Array>()
  let deadline = performance.now() + ROOM_PLAN_SLICE_MS
  for (const [key, raw] of read.cells) {
    if (key.startsWith(prefix)) run.set(key, raw)
    if (run.size < META_RUN_KEYS) continue
    runs.push(decodeMetaCells(run, gen).sort(compareMetaEntries))
    run = new Map()
    if (performance.now() < deadline) continue
    await nextTurnOf()
    deadline = performance.now() + ROOM_PLAN_SLICE_MS
  }
  runs.push(decodeMetaCells(run, gen).sort(compareMetaEntries))
  return { entries: await mergeRuns(runs), seq: decodeCellCounter(read.cells.get(META_SEQ_CELL_KEY)!).value }
}

function decodeMetaCells(cells: ReadonlyMap<string, Uint8Array>, gen: number): RoomMetaEntry[] {
  return Array.from(cells, ([key, raw]) => {
    const { value, place } = decodeRoomRecord<MetaKeyCell>(raw)
    return [metaKeyOfCellKey(gen, key), value, place[0], place[1]]
  })
}

/** The sorted runs as one, merged two at a time, a `ROOM_PLAN_SLICE_MS` slice per turn. */
async function mergeRuns(runs: RoomMetaEntry[][]): Promise<RoomMetaEntry[]> {
  let deadline = performance.now() + ROOM_PLAN_SLICE_MS
  while (runs.length > 1) {
    const merged: RoomMetaEntry[][] = []
    for (let r = 0; r < runs.length; r += 2) {
      const a = runs[r]!
      const b = runs[r + 1]
      if (b === undefined) {
        merged.push(a)
        continue
      }
      const out: RoomMetaEntry[] = []
      let i = 0
      let j = 0
      while (i < a.length && j < b.length) {
        out.push(compareMetaEntries(a[i]!, b[j]!) < 0 ? a[i++]! : b[j++]!)
        if (out.length % META_RUN_KEYS !== 0 || performance.now() < deadline) continue
        await nextTurnOf()
        deadline = performance.now() + ROOM_PLAN_SLICE_MS
      }
      while (i < a.length) out.push(a[i++]!)
      while (j < b.length) out.push(b[j++]!)
      merged.push(out)
    }
    runs = merged
  }
  return runs[0] ?? []
}

/** Once the room's meta was first written, a write is one exchange that expects only `meta:gen`, which only a replace
 *  writes, so concurrent merges never conflict; the first write stores the whole meta, and only the writes racing it
 *  conflict, once. A merge reads its keys with `meta:seq`, so a key already there keeps its place, and retries once a
 *  replace landed. A replace writes its keys into a new generation and drops the old one's a page at a time; one that
 *  finds a replace landed since its read is ordered just before it, so it is overwritten whole and writes nothing. A
 *  generation a crashed replace left goes with its incarnation. Each write that commits publishes what it changed with
 *  the `meta:seq` it left, so a view applies it in what it changed. */
async function writeRoomMeta(roomId: string, config: RoomConfigRecord, write: RoomMetaWrite): Promise<void> {
  const { inc } = config
  const backend = getRoomBackend()
  let replaced: number | null = null
  const update = await retryCompareExchange(async (): Promise<RoomMetaUpdate | null | typeof CX_CONFLICT> => {
    const read = await readMetaCells(roomId, inc, [META_SEQ_CELL_KEY, META_GEN_CELL_KEY])
    const seq = read.cells.get(META_SEQ_CELL_KEY)
    if (seq === undefined) {
      const meta = metaEntriesAt('replace' in write ? write.replace : mergeAttributes(config.meta, write.merge), 0)
      const result = await backend.compareExchangeCells(roomId, inc, {
        expect: [{ key: META_SEQ_CELL_KEY, version: null }],
        mutations: [genMutation(0), ...meta.map((entry) => keyMutation(0, entry))],
        counts: [{ key: META_SEQ_CELL_KEY, delta: 1, create: true }],
      })
      return committed(roomId, result, (seq) => ({ __r: 'update', seq, meta }))
    }
    const counts = [{ key: META_SEQ_CELL_KEY, delta: 1 }]
    const expect = [{ key: META_GEN_CELL_KEY, version: read.versions.get(META_GEN_CELL_KEY)! }]
    const count = decodeCellCounter(seq).value
    const gen = decodeRoomRecord<number>(read.cells.get(META_GEN_CELL_KEY)!)
    if ('replace' in write) {
      const meta = metaEntriesAt(write.replace, count)
      const mutations = [genMutation(count), ...meta.map((entry) => keyMutation(count, entry))]
      const result = await backend.compareExchangeCells(roomId, inc, { expect, mutations, counts })
      if (result === 'conflict') return null
      replaced = gen
      return committed(roomId, result, (seq) => ({ __r: 'update', seq, meta }))
    }
    const keys = Object.keys(write.merge).map((key) => metaKeyCellKey(gen, key))
    const merging = keys.length === 0 ? new Map() : (await readMetaCells(roomId, inc, keys)).cells
    const changes = metaChanges(write.merge, gen, count, merging)
    const mutations = changes.map((change) =>
      change.length === 1 ? { key: metaKeyCellKey(gen, change[0]), bytes: null } : keyMutation(gen, change),
    )
    const result = await backend.compareExchangeCells(roomId, inc, { expect, mutations, counts })
    return committed(roomId, result, (seq) => ({ __r: 'update', seq, changes }))
  })
  if (update === null) return
  await publishCtrl(roomId, inc, update)
  if (replaced !== null) await dropMetaGeneration(roomId, inc, replaced)
}

/** The update a committed exchange made, at the `meta:seq` it left. */
function committed(
  roomId: string,
  result: CxResult,
  update: (seq: number) => RoomMetaUpdate,
): RoomMetaUpdate | typeof CX_CONFLICT {
  if (result === 'stale-inc') throw roomClosedError(roomId)
  if (result === 'conflict') return CX_CONFLICT
  return update(result.committed.get(META_SEQ_CELL_KEY)!.value)
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

function keyMutation(gen: number, [key, value, placeSeq, placeIndex]: RoomMetaEntry): CellMutation {
  const cell: MetaKeyCell = { value, place: [placeSeq, placeIndex] }
  return { key: metaKeyCellKey(gen, key), bytes: encodeRoomRecord(cell) }
}

/** A merge's keys: one found in the `merging` cells read keeps its place, a new one is placed at `seq` by its index,
 *  and an `undefined` value removes its key, as in `mergeAttributes`. */
function metaChanges(
  attrs: RoomMeta,
  gen: number,
  seq: number,
  merging: ReadonlyMap<string, Uint8Array>,
): (RoomMetaEntry | [key: string])[] {
  return Object.entries(attrs).map(([key, value], index) => {
    if (value === undefined) return [key]
    const found = merging.get(metaKeyCellKey(gen, key))
    const [placeSeq, placeIndex] = found === undefined ? [seq, index] : decodeRoomRecord<MetaKeyCell>(found).place
    return [key, value, placeSeq, placeIndex]
  })
}
