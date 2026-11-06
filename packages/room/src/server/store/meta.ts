export {
  readRoomMeta,
  readRoomMetaSeq,
  writeRoomMeta,
  dropReplacedMetaGenerations,
  memberMetaMutations,
  SET_MEMBER_META,
  SET_MEMBER_CONNECTION,
  setMemberMeta,
  setMemberConnection,
  readMembersMetaCells,
  memberMetaOf,
}
export type { StoredRoomMeta, RoomMetaWrite, MemberMetaWrite, MemberMetaCells }

import { getRoomPlane } from '../../backend/plane.js'
import { getOrCreate } from 'telefunc/__internal'
import {
  CELL_PAGE_SIZE,
  type CellMutation,
  type CellsExchangeResult,
  type RoomOperation,
} from '../../backend/contract.js'
import { decodeCellCounter } from '../../backend/semantics.js'
import { Slice } from '../../shared/Slice.js'
import { participantGoneError, roomClosedError } from '../../shared/errors.js'
import { compareMetaEntries, metaEntriesAt } from '../../shared/RoomMetaState.js'
import { mergeAttributes } from '../../shared/arguments.js'
import { freezeMeta } from '../../shared/freezeMeta.js'
import type { MemberMetaUpdate, RoomCtrlEnvelope, RoomMetaEntry, RoomMetaUpdate } from '../../shared/protocol.js'
import type { RoomConfigRecord, RoomMemberRecord, RoomIncarnation } from '../protocol.js'
import type { ParticipantMeta, RoomMeta } from '../../types.js'
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
  memberMetaGenOfCellKey,
  metaDropCellKey,
  metaKeyCellKey,
  metaKeyOfCellKey,
  roomMetaGenPrefix,
} from './cells.js'
import { EXCHANGE_CONFLICT, retryCompareExchange } from './retryCompareExchange.js'
import { deleteCells, eachInFlight, readOpenCells } from './cellStore.js'
import { runOperation } from './operations.js'
import { acceptedOrThrow, commitLane, controlLane, decodeRoomRecord, encodeRoomRecord } from '../lanes/lanes.js'

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
      merged.push(b === undefined ? a : await mergePair(a, b, slice))
    }
    runs = merged
  }
  return runs[0] ?? []
}

/** Two sorted runs as one, within `slice`. */
async function mergePair(a: RoomMetaEntry[], b: RoomMetaEntry[], slice: Slice): Promise<RoomMetaEntry[]> {
  const out: RoomMetaEntry[] = []
  let i = 0
  let j = 0
  while (i < a.length && j < b.length) {
    out.push(compareMetaEntries(a[i]!, b[j]!) < 0 ? a[i++]! : b[j++]!)
    if (out.length % CELL_PAGE_SIZE === 0 && slice.over) await slice.next()
  }
  while (i < a.length) out.push(a[i++]!)
  while (j < b.length) out.push(b[j++]!)
  return out
}

/** Null once the incarnation is gone. Until its first write, the meta is the one the room was created with. A read
 *  pages its generation's keys between two reads of `meta:seq`, which every write counts, and keeps the pages only
 *  when no write landed between. */
async function readRoomMeta(roomId: string, config: RoomConfigRecord): Promise<StoredRoomMeta | null> {
  for (let attempt = 0; attempt < META_PAGED_READS; attempt++) {
    const read = await readMetaPaged(roomId, config)
    if (read !== 'raced') return read
  }
  return await readMetaWhole(roomId, config.inc)
}

/** The meta paged between two reads of `meta:seq`; `'raced'` when a write landed between. */
async function readMetaPaged(
  roomId: string,
  { inc, meta }: RoomConfigRecord,
): Promise<StoredRoomMeta | null | 'raced'> {
  const plane = getRoomPlane()
  const before = await plane.driver.readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY, META_GEN_CELL_KEY] })
  if (before.kind === 'stale-incarnation') return null
  const seq = before.cells.get(META_SEQ_CELL_KEY)
  if (seq === undefined) return { entries: metaEntriesAt(meta, 0), seq: 0 }
  const prefix = roomMetaGenPrefix(decodeRoomRecord<number>(before.cells.get(META_GEN_CELL_KEY)!))
  const runs: RoomMetaEntry[][] = []
  const slice = new Slice()
  for await (const page of plane.driver.readCellPages(roomId, inc, prefix)) {
    if (page.kind === 'stale-incarnation') return null
    runs.push(metaRun(page.cells, prefix))
    if (slice.over) await slice.next()
  }
  const after = await plane.driver.readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY] })
  if (after.kind === 'stale-incarnation') return null
  if (after.versions.get(META_SEQ_CELL_KEY) !== before.versions.get(META_SEQ_CELL_KEY)) return 'raced'
  return { entries: await mergeRuns(runs), seq: decodeCellCounter(seq).value }
}

/** The whole meta in one read, cut into runs of `CELL_PAGE_SIZE` as pages would be. */
async function readMetaWhole(roomId: string, inc: string): Promise<StoredRoomMeta | null> {
  const read = await getRoomPlane().driver.readCells(roomId, inc, { prefix: META_CELL_PREFIX })
  if (read.kind === 'stale-incarnation') return null
  const prefix = roomMetaGenPrefix(decodeRoomRecord<number>(read.cells.get(META_GEN_CELL_KEY)!))
  const runs: RoomMetaEntry[][] = []
  let run = new Map<string, Uint8Array>()
  const slice = new Slice()
  for (const [key, raw] of read.cells) {
    if (key.startsWith(prefix)) run.set(key, raw)
    if (run.size < CELL_PAGE_SIZE) continue
    runs.push(metaRun(run, prefix))
    run = new Map()
    if (slice.over) await slice.next()
  }
  runs.push(metaRun(run, prefix))
  return { entries: await mergeRuns(runs), seq: decodeCellCounter(read.cells.get(META_SEQ_CELL_KEY)!).value }
}

/** How many writes the stored meta reflects; null once the incarnation is gone. */
async function readRoomMetaSeq(roomId: string, inc: string): Promise<number | null> {
  const read = await getRoomPlane().driver.readCells(roomId, inc, { keys: [META_SEQ_CELL_KEY] })
  if (read.kind === 'stale-incarnation') return null
  const seq = read.cells.get(META_SEQ_CELL_KEY)
  return seq === undefined ? 0 : decodeCellCounter(seq).value
}

/** Writes the room's meta in one compare-exchange. Each commit adds one to `meta:seq` and publishes what it changed at
 *  that seq, and views apply the changes in seq order.
 *  - First write (no `meta:seq` yet): stores the whole meta as generation 0, the replace, or the merge applied to the
 *    meta the room was created with. It expects no `meta:seq`, so of racing first writes one commits and the others
 *    retry as a merge or a replace.
 *  - Merge: writes only its keys, in the current generation. It expects `meta:gen` unchanged, which only a replace
 *    changes, so merges never conflict with each other (a key two of them write ends as the later commit wrote it), and
 *    one that a replace overtook retries on the new meta. A key already there keeps its place; a new one is placed at
 *    the `meta:seq` the merge read.
 *  - Replace: writes the whole meta as a new generation and marks the old one to drop. It also expects `meta:gen`
 *    unchanged: one that another replace overtook counts as the earlier of the two, and writes nothing. */
async function writeRoomMeta(roomId: string, config: RoomConfigRecord, write: RoomMetaWrite): Promise<void> {
  const { inc } = config
  const plane = getRoomPlane()
  const update = await retryCompareExchange(async (): Promise<RoomMetaUpdate | null | typeof EXCHANGE_CONFLICT> => {
    const read = await readOpenCells(roomId, inc, [META_SEQ_CELL_KEY, META_GEN_CELL_KEY])
    const seq = read.cells.get(META_SEQ_CELL_KEY)
    if (seq === undefined) {
      const meta = metaEntriesAt('replace' in write ? write.replace : mergeAttributes(config.meta, write.merge), 0)
      const prefix = roomMetaGenPrefix(0)
      const result = await plane.driver.compareExchangeCells(roomId, inc, {
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
      const result = await plane.driver.compareExchangeCells(roomId, inc, { expect, mutations, counts })
      return result.kind === 'conflict' ? null : committed(roomId, result, (seq) => ({ __r: 'update', seq, meta }))
    }
    const prefix = roomMetaGenPrefix(gen)
    const keys = Object.keys(write.merge).map((key) => metaKeyCellKey(prefix, key))
    const merging = keys.length === 0 ? new Map() : (await readOpenCells(roomId, inc, keys)).cells
    const changes = metaChanges(write.merge, prefix, count, merging)
    const mutations = changes.map((change) => changeMutation(prefix, change))
    const result = await plane.driver.compareExchangeCells(roomId, inc, { expect, mutations, counts })
    return committed(roomId, result, (seq) => ({ __r: 'update', seq, changes }))
  })
  if (update === null) return
  acceptedOrThrow(roomId, await commitLane(controlLane(roomId, inc), encodeRoomRecord(update)))
  if ('replace' in write) await dropReplacedMetaGenerations(roomId, inc)
}

/** The update a committed exchange made, at the `meta:seq` it left. */
function committed(
  roomId: string,
  result: CellsExchangeResult,
  update: (seq: number) => RoomMetaUpdate,
): RoomMetaUpdate | typeof EXCHANGE_CONFLICT {
  if (result.kind === 'stale-incarnation') throw roomClosedError(roomId)
  if (result.kind === 'conflict') return EXCHANGE_CONFLICT
  return update(result.counters.get(META_SEQ_CELL_KEY)!.value)
}

/** Each generation a replace marked, a page per exchange and a `ROOM_PLAN_SLICE_MS` slice per turn, its mark going with
 *  its last page. A marked generation stopped being `meta:gen` in the exchange that marked it, so whoever finishes a
 *  mark, a later replace or the heartbeat, drops no live key. */
async function dropReplacedMetaGenerations(roomId: string, inc: string): Promise<void> {
  const plane = getRoomPlane()
  const marks = await plane.driver.readCells(roomId, inc, { prefix: META_DROP_CELL_PREFIX })
  if (marks.kind === 'stale-incarnation') return
  const slice = new Slice()
  for (const [mark, raw] of marks.cells) {
    let held: string[] = []
    for await (const page of plane.driver.readCellPages(
      roomId,
      inc,
      roomMetaGenPrefix(decodeRoomRecord<number>(raw)),
    )) {
      if (page.kind === 'stale-incarnation' || (held.length > 0 && !(await deleteCells(roomId, inc, held)))) return
      held = [...page.cells.keys()]
      if (slice.over) await slice.next()
    }
    held.push(mark)
    if (!(await deleteCells(roomId, inc, held))) return
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

type SetMemberMetaArgs = { id: string; write: MemberMetaWrite }
type MemberMetaWritten = { update: MemberMetaUpdate; event: Extract<RoomCtrlEnvelope, { __r: 'p-meta' }> }

/** A member's meta write and its event, counted in the record's `metaSeq`; `'gone'` once the member left. A merge
 *  writes its keys, a key already there keeping its place; a replace writes its keys and its mark into a new
 *  generation, which the old one's cells leave with. New keys are placed at the write's own `metaSeq`. */
const SET_MEMBER_META: RoomOperation<SetMemberMetaArgs, MemberMetaWritten | 'gone'> = {
  name: 'setMemberMeta',
  plan(tx, { id, write }) {
    const memberKey = memberCellKey(id)
    const raw = tx.cell(memberKey)
    if (raw === undefined) return 'gone'
    const record = decodeRoomRecord<RoomMemberRecord>(raw)
    const seq = record.metaSeq + 1
    const gen = 'replace' in write ? seq : record.metaGen
    const prefix = memberMetaGenPrefix(id, gen)
    let update: MemberMetaUpdate
    if ('replace' in write) {
      tx.dropPrefix(memberMetaGenPrefix(id, record.metaGen))
      for (const entry of metaEntriesAt(write.replace, seq)) tx.write(metaKeyMutation(prefix, entry))
      tx.write({ key: prefix, bytes: new Uint8Array() })
      update = { seq, meta: write.replace }
      if (record.disconnected) update.disconnected = true
    } else {
      const merging = new Map<string, Uint8Array>()
      for (const key of Object.keys(write.merge)) {
        const cellKey = metaKeyCellKey(prefix, key)
        const cell = tx.cell(cellKey)
        if (cell !== undefined) merging.set(cellKey, cell)
      }
      for (const change of metaChanges(write.merge, prefix, seq, merging)) tx.write(changeMutation(prefix, change))
      update = { seq, attrs: write.merge }
    }
    tx.write({
      key: memberKey,
      bytes: encodeRoomRecord({ ...record, metaSeq: seq, metaGen: gen } satisfies RoomMemberRecord),
    })
    const event: MemberMetaWritten['event'] = { __r: 'p-meta', id, ...update }
    if (record.hidden) event.hidden = true
    tx.commit(encodeRoomRecord(event))
    return { update, event }
  },
}

/** Writes member `id`'s meta with its event. */
async function setMemberMeta(room: RoomIncarnation, id: string, write: MemberMetaWrite): Promise<MemberMetaWritten> {
  const { value } = await runOperation(room, SET_MEMBER_META, { id, write })
  if (value === 'gone') throw participantGoneError(id)
  return value
}

type SetMemberConnectionArgs = { id: string; connected: boolean }

/** The connection of the page holding member `id`, at the next `metaSeq`, and its event; null when the record already
 *  has it, `'gone'` once the member left. */
const SET_MEMBER_CONNECTION: RoomOperation<SetMemberConnectionArgs, MemberMetaWritten | null | 'gone'> = {
  name: 'setMemberConnection',
  plan(tx, { id, connected }) {
    const memberKey = memberCellKey(id)
    const raw = tx.cell(memberKey)
    if (raw === undefined) return 'gone'
    const { disconnected, ...record } = decodeRoomRecord<RoomMemberRecord>(raw)
    if (connected === (disconnected !== true)) return null
    const seq = record.metaSeq + 1
    const next: RoomMemberRecord = { ...record, metaSeq: seq }
    if (!connected) next.disconnected = true
    tx.write({ key: memberKey, bytes: encodeRoomRecord(next) })
    const update: MemberMetaUpdate = { seq, connected }
    const event: MemberMetaWritten['event'] = { __r: 'p-meta', id, ...update }
    if (record.hidden) event.hidden = true
    tx.commit(encodeRoomRecord(event))
    return { update, event }
  },
}

/** Writes the connection of member `id`'s page with its event; null when the record already has it. */
async function setMemberConnection(
  room: RoomIncarnation,
  id: string,
  connected: boolean,
): Promise<MemberMetaWritten | null> {
  const { value } = await runOperation(room, SET_MEMBER_CONNECTION, { id, connected })
  if (value === 'gone') throw participantGoneError(id)
  return value
}

/** The meta cells of every member, or of member `id`, by member, read after their `records` (by cell key). A page is
 *  one read that holds a member's cells together, so a member's cells within a page are as one write left them; the
 *  member a page boundary cuts is settled by `settleMemberMetaCells`. */
async function readMembersMetaCells(
  room: RoomIncarnation,
  records: ReadonlyMap<string, Uint8Array>,
  id?: string,
): Promise<Map<string, MemberMetaCells>> {
  const { byMember, cut } = await readMetaCellPages(
    room,
    id === undefined ? MEMBER_META_CELL_PREFIX : memberMetaCellPrefix(id),
  )
  await eachInFlight(cut, (member) => settleCutMember(room, records, byMember, member))
  return byMember
}

async function settleCutMember(
  room: RoomIncarnation,
  records: ReadonlyMap<string, Uint8Array>,
  byMember: Map<string, MemberMetaCells>,
  member: string,
): Promise<void> {
  const seq = metaSeqOf(records.get(memberCellKey(member)))
  byMember.set(member, await settleMemberMetaCells(room, member, seq, byMember.get(member)!))
}

/** The meta cells under `prefix` by member, a page and a `ROOM_PLAN_SLICE_MS` slice at a time, and the members that
 *  held a page's last cell with a page after it. */
async function readMetaCellPages(
  room: RoomIncarnation,
  prefix: string,
): Promise<{ byMember: Map<string, MemberMetaCells>; cut: Set<string> }> {
  const byMember = new Map<string, MemberMetaCells>()
  const cut = new Set<string>()
  let last: string | undefined
  const slice = new Slice()
  for await (const page of getRoomPlane().driver.readCellPages(room.id, room.inc, prefix)) {
    if (page.kind === 'stale-incarnation') throw roomClosedError(room.id)
    if (last !== undefined) cut.add(last)
    for (const cell of page.cells) {
      last = memberIdOfMetaCellKey(cell[0])
      getOrCreate(byMember, last, () => []).push(cell)
    }
    if (slice.over) await slice.next()
  }
  return { byMember, cut }
}

/** Member `id`'s `cells`, which a page boundary cut, read after its record was at `seq`: kept while the record is still
 *  at it, as every meta write moves it, else read again, up to `META_PAGED_READS` times, then in one call. */
async function settleMemberMetaCells(
  room: RoomIncarnation,
  id: string,
  seq: number | undefined,
  cells: MemberMetaCells,
): Promise<MemberMetaCells> {
  const memberKey = memberCellKey(id)
  for (let reads = 0; ; reads++) {
    const now = metaSeqOf((await readOpenCells(room.id, room.inc, [memberKey])).cells.get(memberKey))
    if (now === seq) return cells
    if (reads === META_PAGED_READS) break
    seq = now
    const read = await readMetaCellPages(room, memberMetaCellPrefix(id))
    cells = read.byMember.get(id) ?? []
    if (read.cut.size === 0) return cells
  }
  const read = await getRoomPlane().driver.readCells(room.id, room.inc, { prefix: memberMetaCellPrefix(id) })
  if (read.kind === 'stale-incarnation') throw roomClosedError(room.id)
  return Array.from(read.cells)
}

function metaSeqOf(record: Uint8Array | undefined): number | undefined {
  return record === undefined ? undefined : decodeRoomRecord<RoomMemberRecord>(record).metaSeq
}

/** The member's meta from its generation's cells, sorted a run at a time, then merged and built a slice per turn. A
 *  newer generation among the cells is a replace that committed after the record was read, which may have dropped the
 *  record's: the meta is read from it. */
async function memberMetaOf(id: string, recordGen: number, cells: MemberMetaCells): Promise<ParticipantMeta> {
  let gen = recordGen
  for (const [cellKey] of cells) gen = Math.max(gen, memberMetaGenOfCellKey(id, cellKey))
  const prefix = memberMetaGenPrefix(id, gen)
  const runs: RoomMetaEntry[][] = []
  const slice = new Slice()
  for (let at = 0; at < cells.length; at += CELL_PAGE_SIZE) {
    runs.push(metaRun(cells.slice(at, at + CELL_PAGE_SIZE), prefix))
    if (slice.over) await slice.next()
  }
  const entries = await mergeRuns(runs)
  const meta: ParticipantMeta = {}
  for (let i = 0; i < entries.length; i++) {
    const [key, value] = entries[i]!
    Object.defineProperty(meta, key, { value, enumerable: true, configurable: true, writable: true })
    if (i % CELL_PAGE_SIZE === 0 && slice.over) await slice.next()
  }
  return freezeMeta(meta)
}
