export {
  readRoomMeta,
  readRoomMetaSeq,
  writeRoomMeta,
  dropReplacedMetaGenerations,
  memberMetaMutations,
  writeMemberMeta,
  dropMemberMetaGeneration,
  dropMemberMeta,
  readMembersMetaCells,
  memberMetaOf,
  dropCells,
  readOpenCells,
}
export type { StoredRoomMeta, RoomMetaWrite, MemberMetaWrite, MemberMetaCells }

import { getRoomBackend } from '../../backend/install.js'
import type { CellMutation, CellsRead, CxResult } from '../../backend/room/contract.js'
import { decodeCellCounter } from '../../backend/room/semantics.js'
import { Slice } from '../slice.js'
import { participantGoneError, roomClosedError } from '../errors.js'
import { compareMetaEntries, metaEntriesAt } from '../meta-state.js'
import { mergeAttributes } from '../model.js'
import type { MemberMetaUpdate, RoomMetaEntry, RoomMetaUpdate } from '../protocol.js'
import type { RoomConfigRecord, RoomMemberRecord } from './protocol.js'
import type { ParticipantMeta, RoomMeta } from '../types.js'
import {
  MEMBER_META_CELL_PREFIX,
  META_CELL_PREFIX,
  META_DROP_CELL_PREFIX,
  META_GEN_CELL_KEY,
  META_SEQ_CELL_KEY,
  memberCellKey,
  memberIdOfMetaCellKey,
  memberMetaCellPrefix,
  memberMetaGenPrefix,
  metaDropCellKey,
  metaKeyCellKey,
  metaKeyOfCellKey,
  roomMetaGenPrefix,
} from './cells.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { commitControlOrThrow, decodeRoomRecord, encodeRoomRecord } from './lanes.js'
import type { RoomIncarnation } from './lifecycle.js'

// A meta, the room's or a member's, is a cell per top-level key under its generation's prefix, each holding the
// key's value and place. A replace writes a new generation; a merge writes its keys into the current one.

/** `entries` in key order; `seq` counts the writes that made them, so it orders `update` events. */
type StoredRoomMeta = { entries: RoomMetaEntry[]; seq: number }
type RoomMetaWrite = { replace: RoomMeta } | { merge: RoomMeta }
type MemberMetaWrite = { replace: ParticipantMeta } | { merge: ParticipantMeta }
/** A member's meta cells as read, by cell key. */
type MemberMetaCells = Array<readonly [string, Uint8Array]>
/** A key's place: the seq the write that added it was placed at, which every later write's is higher than, and its
 *  index in that write. Keys that concurrent writes added tie, and go by key. */
type MetaKeyCell = { value: unknown; place: [seq: number, index: number] }

/** Paged reads that a write landing in invalidates before the read takes the meta in one call. */
const META_PAGED_READS = 2
/** The keys sorted in one run of a whole read, as many as a page holds. */
const META_RUN_KEYS = 1024

function metaKeyMutation(prefix: string, [key, value, placeSeq, placeIndex]: RoomMetaEntry): CellMutation {
  const cell: MetaKeyCell = { value, place: [placeSeq, placeIndex] }
  return { key: metaKeyCellKey(prefix, key), bytes: encodeRoomRecord(cell) }
}

/** A merge's keys: one found in the `merging` cells read keeps its place, a new one is placed at `seq` by its index,
 *  and an `undefined` value removes its key, as in `mergeAttributes`. */
function metaChanges(
  attrs: RoomMeta,
  prefix: string,
  seq: number,
  merging: ReadonlyMap<string, Uint8Array>,
): (RoomMetaEntry | [key: string])[] {
  return Object.entries(attrs).map(([key, value], index) => {
    if (value === undefined) return [key]
    const found = merging.get(metaKeyCellKey(prefix, key))
    const [placeSeq, placeIndex] = found === undefined ? [seq, index] : decodeRoomRecord<MetaKeyCell>(found).place
    return [key, value, placeSeq, placeIndex]
  })
}

function changeMutation(prefix: string, change: RoomMetaEntry | [key: string]): CellMutation {
  return change.length === 1 ? { key: metaKeyCellKey(prefix, change[0]), bytes: null } : metaKeyMutation(prefix, change)
}

/** The keys of `cells` in the generation at `prefix`, sorted; another generation's cells are skipped. */
function metaRun(cells: Iterable<readonly [string, Uint8Array]>, prefix: string): RoomMetaEntry[] {
  const run: RoomMetaEntry[] = []
  for (const [cellKey, raw] of cells) {
    const key = metaKeyOfCellKey(prefix, cellKey)
    if (key === null) continue
    const { value, place } = decodeRoomRecord<MetaKeyCell>(raw)
    run.push([key, value, place[0], place[1]])
  }
  return run.sort(compareMetaEntries)
}

/** The sorted runs as one, merged two at a time, a `ROOM_PLAN_SLICE_MS` slice per turn. */
async function mergeRuns(runs: RoomMetaEntry[][]): Promise<RoomMetaEntry[]> {
  const slice = new Slice()
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
        if (out.length % META_RUN_KEYS === 0 && slice.over) await slice.next()
      }
      while (i < a.length) out.push(a[i++]!)
      while (j < b.length) out.push(b[j++]!)
      merged.push(out)
    }
    runs = merged
  }
  return runs[0] ?? []
}

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
    const prefix = roomMetaGenPrefix(decodeRoomRecord<number>(before.cells.get(META_GEN_CELL_KEY)!))
    const runs: RoomMetaEntry[][] = []
    const slice = new Slice()
    for await (const page of backend.readCellPages(roomId, inc, prefix)) {
      if ('staleInc' in page) return null
      runs.push(metaRun(page.cells, prefix))
      if (slice.over) await slice.next()
    }
    const after = await backend.readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY] })
    if ('staleInc' in after) return null
    if (after.versions.get(META_SEQ_CELL_KEY) === before.versions.get(META_SEQ_CELL_KEY))
      return { entries: await mergeRuns(runs), seq: decodeCellCounter(seq).value }
  }
  const read = await backend.readCells(roomId, inc, { prefix: META_CELL_PREFIX })
  if ('staleInc' in read) return null
  const prefix = roomMetaGenPrefix(decodeRoomRecord<number>(read.cells.get(META_GEN_CELL_KEY)!))
  const runs: RoomMetaEntry[][] = []
  let run = new Map<string, Uint8Array>()
  const slice = new Slice()
  for (const [key, raw] of read.cells) {
    if (key.startsWith(prefix)) run.set(key, raw)
    if (run.size < META_RUN_KEYS) continue
    runs.push(metaRun(run, prefix))
    run = new Map()
    if (slice.over) await slice.next()
  }
  runs.push(metaRun(run, prefix))
  return { entries: await mergeRuns(runs), seq: decodeCellCounter(read.cells.get(META_SEQ_CELL_KEY)!).value }
}

/** How many writes the stored meta reflects; null once the incarnation is gone. */
async function readRoomMetaSeq(roomId: string, inc: string): Promise<number | null> {
  const read = await getRoomBackend().readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY] })
  if ('staleInc' in read) return null
  const seq = read.cells.get(META_SEQ_CELL_KEY)
  return seq === undefined ? 0 : decodeCellCounter(seq).value
}

/** Once the room's meta was first written, a write is one exchange that expects only `meta:gen`, which only a replace
 *  writes, so concurrent merges never conflict; the first write stores the whole meta, and only the writes racing it
 *  conflict, once. A merge reads its keys with `meta:seq`, so a key already there keeps its place, and retries once a
 *  replace landed. A replace writes its keys into a new generation and marks the old one to drop; one that finds a
 *  replace landed since its read is ordered just before it, so it is overwritten whole and writes nothing. Each write
 *  that commits publishes what it changed with the `meta:seq` it left, so a view applies it in what it changed. New
 *  keys are placed at the `meta:seq` the write read. */
async function writeRoomMeta(roomId: string, config: RoomConfigRecord, write: RoomMetaWrite): Promise<void> {
  const { inc } = config
  const backend = getRoomBackend()
  const update = await retryCompareExchange(async (): Promise<RoomMetaUpdate | null | typeof CX_CONFLICT> => {
    const read = await readOpenCells(roomId, inc, [META_SEQ_CELL_KEY, META_GEN_CELL_KEY])
    const seq = read.cells.get(META_SEQ_CELL_KEY)
    if (seq === undefined) {
      const meta = metaEntriesAt('replace' in write ? write.replace : mergeAttributes(config.meta, write.merge), 0)
      const prefix = roomMetaGenPrefix(0)
      const result = await backend.compareExchangeCells(roomId, inc, {
        expect: [{ key: META_SEQ_CELL_KEY, version: null }],
        mutations: [genMutation(0), ...meta.map((entry) => metaKeyMutation(prefix, entry))],
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
      const prefix = roomMetaGenPrefix(count)
      const drop = { key: metaDropCellKey(gen), bytes: encodeRoomRecord(gen) }
      const mutations = [genMutation(count), drop, ...meta.map((entry) => metaKeyMutation(prefix, entry))]
      const result = await backend.compareExchangeCells(roomId, inc, { expect, mutations, counts })
      return result === 'conflict' ? null : committed(roomId, result, (seq) => ({ __r: 'update', seq, meta }))
    }
    const prefix = roomMetaGenPrefix(gen)
    const keys = Object.keys(write.merge).map((key) => metaKeyCellKey(prefix, key))
    const merging = keys.length === 0 ? new Map() : (await readOpenCells(roomId, inc, keys)).cells
    const changes = metaChanges(write.merge, prefix, count, merging)
    const mutations = changes.map((change) => changeMutation(prefix, change))
    const result = await backend.compareExchangeCells(roomId, inc, { expect, mutations, counts })
    return committed(roomId, result, (seq) => ({ __r: 'update', seq, changes }))
  })
  if (update === null) return
  await commitControlOrThrow(roomId, inc, update)
  if ('replace' in write) await dropReplacedMetaGenerations(roomId, inc)
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

async function readOpenCells(
  roomId: string,
  inc: string,
  keys: string[],
): Promise<Exclude<CellsRead, { staleInc: true }>> {
  const read = await getRoomBackend().readCells(roomId, inc, { keys })
  if ('staleInc' in read) throw roomClosedError(roomId)
  return read
}

/** Each generation a replace marked, a page per exchange and a `ROOM_PLAN_SLICE_MS` slice per turn, its mark going with
 *  its last page. A marked generation stopped being `meta:gen` in the exchange that marked it, so whoever finishes a
 *  mark, a later replace or the heartbeat, drops no live key. */
async function dropReplacedMetaGenerations(roomId: string, inc: string): Promise<void> {
  const backend = getRoomBackend()
  const marks = await backend.readCells(roomId, inc, { prefix: META_DROP_CELL_PREFIX })
  if ('staleInc' in marks) return
  const drop = async (mutations: CellMutation[]) =>
    (await backend.compareExchangeCells(roomId, inc, { expect: [], mutations })) !== 'stale-inc'
  const slice = new Slice()
  for (const [mark, raw] of marks.cells) {
    let held: CellMutation[] = []
    for await (const page of backend.readCellPages(roomId, inc, roomMetaGenPrefix(decodeRoomRecord<number>(raw)))) {
      if ('staleInc' in page || (held.length > 0 && !(await drop(held)))) return
      held = [...page.cells.keys()].map((key) => ({ key, bytes: null }))
      if (slice.over) await slice.next()
    }
    if (!(await drop([...held, { key: mark, bytes: null }]))) return
  }
}

function genMutation(gen: number): CellMutation {
  return { key: META_GEN_CELL_KEY, bytes: encodeRoomRecord(gen) }
}

/** A joining member's meta: generation 0, its keys placed at seq 0. */
function memberMetaMutations(id: string, meta: ParticipantMeta): CellMutation[] {
  const prefix = memberMetaGenPrefix(id, 0)
  return metaEntriesAt(meta, 0).map((entry) => metaKeyMutation(prefix, entry))
}

/** One exchange that expects the member's record, which counts each meta write in `metaSeq`.
 *  A merge writes its keys, a key already there keeping its place; a replace writes its keys into a new generation and
 *  returns the one it `replaced`, for `dropMemberMetaGeneration`. New keys are placed at the write's own `metaSeq`. Each
 *  costs the keys it was given. */
async function writeMemberMeta(
  room: RoomIncarnation,
  id: string,
  write: MemberMetaWrite,
): Promise<{ update: MemberMetaUpdate; hidden: boolean; replaced: number | null }> {
  const backend = getRoomBackend()
  const memberKey = memberCellKey(id)
  return await retryCompareExchange(async () => {
    const read = await readOpenCells(room.id, room.inc, [memberKey])
    const raw = read.cells.get(memberKey)
    if (raw === undefined) throw participantGoneError(id)
    const record = decodeRoomRecord<RoomMemberRecord>(raw)
    const seq = record.metaSeq + 1
    const gen = 'replace' in write ? seq : record.metaGen
    const prefix = memberMetaGenPrefix(id, gen)
    let mutations: CellMutation[]
    if ('replace' in write) mutations = metaEntriesAt(write.replace, seq).map((entry) => metaKeyMutation(prefix, entry))
    else {
      const keys = Object.keys(write.merge).map((key) => metaKeyCellKey(prefix, key))
      const merging = keys.length === 0 ? new Map() : (await readOpenCells(room.id, room.inc, keys)).cells
      mutations = metaChanges(write.merge, prefix, seq, merging).map((change) => changeMutation(prefix, change))
    }
    const next: RoomMemberRecord = { ...record, metaSeq: seq, metaGen: gen }
    mutations.push({ key: memberKey, bytes: encodeRoomRecord(next) })
    const expect = [{ key: memberKey, version: read.versions.get(memberKey)! }]
    const result = await backend.compareExchangeCells(room.id, room.inc, { expect, mutations })
    if (result === 'stale-inc') throw roomClosedError(room.id)
    if (result === 'conflict') return CX_CONFLICT
    const update: MemberMetaUpdate = 'replace' in write ? { seq, meta: write.replace } : { seq, attrs: write.merge }
    return { update, hidden: record.hidden === true, replaced: gen === record.metaGen ? null : record.metaGen }
  })
}

/** A generation a replace left, which stopped being the record's `metaGen` in its exchange. */
async function dropMemberMetaGeneration(room: RoomIncarnation, id: string, gen: number): Promise<void> {
  await dropCells(room.id, room.inc, memberMetaGenPrefix(id, gen))
}

/** Every generation of a member's meta, once its eviction removed the record. */
async function dropMemberMeta(roomId: string, inc: string, id: string): Promise<void> {
  await dropCells(roomId, inc, memberMetaCellPrefix(id))
}

/** Deletes the cells under `prefix`, a page per exchange and a `ROOM_PLAN_SLICE_MS` slice per turn. */
async function dropCells(roomId: string, inc: string, prefix: string): Promise<void> {
  const backend = getRoomBackend()
  const slice = new Slice()
  for await (const page of backend.readCellPages(roomId, inc, prefix)) {
    if ('staleInc' in page) return
    const mutations = [...page.cells.keys()].map((key) => ({ key, bytes: null }))
    if ((await backend.compareExchangeCells(roomId, inc, { expect: [], mutations })) === 'stale-inc') return
    if (slice.over) await slice.next()
  }
}

/** The meta cells of every member, or of member `id`, by member, a page and a `ROOM_PLAN_SLICE_MS` slice at a time. */
async function readMembersMetaCells(room: RoomIncarnation, id?: string): Promise<Map<string, MemberMetaCells>> {
  const byMember = new Map<string, MemberMetaCells>()
  const prefix = id === undefined ? MEMBER_META_CELL_PREFIX : memberMetaCellPrefix(id)
  const slice = new Slice()
  for await (const page of getRoomBackend().readCellPages(room.id, room.inc, prefix)) {
    if ('staleInc' in page) throw roomClosedError(room.id)
    for (const cell of page.cells) {
      const id = memberIdOfMetaCellKey(cell[0])
      const cells = byMember.get(id)
      if (cells === undefined) byMember.set(id, [cell])
      else cells.push(cell)
    }
    if (slice.over) await slice.next()
  }
  return byMember
}

/** The member's meta from its generation's cells, sorted a run at a time, then merged and built a slice per turn. */
async function memberMetaOf(id: string, gen: number, cells: MemberMetaCells): Promise<ParticipantMeta> {
  const prefix = memberMetaGenPrefix(id, gen)
  const runs: RoomMetaEntry[][] = []
  const slice = new Slice()
  for (let at = 0; at < cells.length; at += META_RUN_KEYS) {
    runs.push(metaRun(cells.slice(at, at + META_RUN_KEYS), prefix))
    if (slice.over) await slice.next()
  }
  const entries = await mergeRuns(runs)
  const meta: ParticipantMeta = {}
  for (let i = 0; i < entries.length; i++) {
    const [key, value] = entries[i]!
    Object.defineProperty(meta, key, { value, enumerable: true, configurable: true, writable: true })
    if (i % META_RUN_KEYS === 0 && slice.over) await slice.next()
  }
  return Object.freeze(meta)
}
