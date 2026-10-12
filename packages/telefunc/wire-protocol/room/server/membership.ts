export {
  holdViews,
  removeView,
  createMember,
  writeMemberMeta,
  dropMemberMetaGeneration,
  renewMemberLeases,
  evictMember,
  reapAndReadRoster,
  reapAndReadMembersById,
  reapAndCountPresence,
  reapAndResolveIdentity,
}

import { assert } from '../../../utils/assert.js'
import { getRoomBackend } from '../../backend/install.js'
import { Slice } from '../slice.js'
import type { CellCount, CellCounter, CellMutation } from '../../backend/room/contract.js'
import { ROOM_MEMBER_TTL_MS, ROOM_MEMBER_WRITES_IN_FLIGHT_MAX } from '../constants.js'
import { compareMetaEntries } from '../meta-state.js'
import { participantGoneError, roomClosedError } from '../errors.js'
import {
  leaveCauseToWire,
  type HoldConfig,
  type MemberMetaUpdate,
  type MemberSnapshot,
  type RoomMetaEntry,
  type RoomDataEnvelope,
  type RoomMemberRecord,
  type WireLeaveCause,
} from '../protocol.js'
import type { LeaveCause, ParticipantMeta } from '../types.js'
import { META_RUN_KEYS, mergeRuns, type MetaKeyCell } from './meta.js'
import { SEMANTIC_LANE, decodeRoomRecord, encodeRoomRecord, publishCtrl } from './lanes.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { afterHoldChange, holdChange, holdCounts, type RoomIncarnation } from './lifecycle.js'
import {
  CLEANUP_CELL_PREFIX,
  MEMBER_CELL_PREFIX,
  MEMBER_META_CELL_PREFIX,
  memberIdOfMetaCellKey,
  memberMetaCellKey,
  memberMetaCellPrefix,
  memberMetaGenPrefix,
  memberMetaKeyOfCellKey,
  VIEW_CELL_PREFIX,
  cleanupCellKey,
  identityCellKey,
  identityCellPrefix,
  memberCellKey,
  memberIdOfCellKey,
  memberIdOfCleanupKey,
  viewCellKey,
  viewIdOfCellKey,
} from './cells.js'

type CellPlan<T> = { value: T; mutations: CellMutation[]; counts?: CellCount[] }
type MemberMetaWrite = { replace: ParticipantMeta } | { merge: ParticipantMeta }
/** A member's meta cells as read, by cell key. */
type MemberMetaCells = Array<readonly [string, Uint8Array]>
type CellCommit<T> = { value: T; counters: ReadonlyMap<string, CellCounter> }
type PendingMemberCleanup = { cause: WireLeaveCause; hidden?: true }
/** A page's view of the room, renewed like a member record. */
type ViewRecord = { seenAt: number }

function isLapsed(record: { seenAt: number }): boolean {
  return Date.now() - record.seenAt > ROOM_MEMBER_TTL_MS
}

async function readCells(roomId: string, inc: string, keys: string[]): Promise<Map<string, Uint8Array>> {
  const result = await getRoomBackend().readCells(roomId, inc, { keys })
  if ('staleInc' in result) throw roomClosedError(roomId)
  return result.cells
}

/** The cells under `prefix`, a page and a `ROOM_PLAN_SLICE_MS` slice at a time. */
async function readPrefix(roomId: string, inc: string, prefix: string): Promise<Map<string, Uint8Array>> {
  const cells = new Map<string, Uint8Array>()
  const slice = new Slice()
  for await (const page of getRoomBackend().readCellPages(roomId, inc, prefix)) {
    if ('staleInc' in page) throw roomClosedError(roomId)
    for (const [key, raw] of page.cells) cells.set(key, raw)
    if (slice.over) await slice.next()
  }
  return cells
}

/** Reads `keys`, plans, and commits only while each of them is still as read. */
async function mutateCells<T>(
  roomId: string,
  inc: string,
  keys: string[],
  plan: (cells: ReadonlyMap<string, Uint8Array>) => CellPlan<T>,
): Promise<CellCommit<T>> {
  const backend = getRoomBackend()
  return await retryCompareExchange(async () => {
    const read = await backend.readCells(roomId, inc, { keys })
    if ('staleInc' in read) throw roomClosedError(roomId)
    const { value, mutations, counts = [] } = plan(read.cells)
    if (mutations.length === 0 && counts.length === 0) return { value, counters: new Map() }
    const expect = keys.map((key) => ({ key, version: read.versions.get(key) ?? null }))
    const result = await backend.compareExchangeCells(roomId, inc, { expect, mutations, counts })
    if (result === 'stale-inc') throw roomClosedError(roomId)
    return result === 'conflict' ? CX_CONFLICT : { value, counters: result.committed }
  })
}

/** Persist a join's member record, its meta's keys and identity marker; a non-hidden member holds the room. */
async function createMember(
  room: RoomIncarnation,
  id: string,
  record: RoomMemberRecord,
  meta: ParticipantMeta,
): Promise<void> {
  const mutations: CellMutation[] = [
    { key: memberCellKey(id), bytes: encodeRoomRecord(record) },
    ...Object.entries(meta).map(([key, value], index) => metaKeyMutation(id, 0, key, value, [0, index])),
  ]
  if (record.identity !== undefined)
    mutations.push({ key: identityCellKey(record.identity, id), bytes: new Uint8Array() })
  const config = record.hidden ? null : room.hold
  const keys = mutations.map(({ key }) => key)
  const { counters } = await mutateCells(room.id, room.inc, keys, () => ({
    value: undefined,
    mutations,
    counts: holdCounts(config, 1, true),
  }))
  afterHoldChange(room, holdChange(config, counters, 1, true))
}

/** Writes or renews pages' views in one write; each holds the room, and a room without hold timeouts keeps none. A
 *  renewal also restores a view whose write failed or that was reaped while its page held on. */
async function holdViews(room: RoomIncarnation, ids: string[]): Promise<void> {
  const { hold } = room
  if (hold === null) return
  const keys = ids.map(viewCellKey)
  const { value: added, counters } = await mutateCells(room.id, room.inc, keys, (cells) => {
    const added = keys.filter((key) => !cells.has(key)).length
    const view = encodeRoomRecord({ seenAt: Date.now() } satisfies ViewRecord)
    const views = keys.map((key) => ({ key, bytes: view }))
    return { value: added, mutations: views, counts: holdCounts(hold, added) }
  })
  if (added > 0) afterHoldChange(room, holdChange(hold, counters, added))
}

/** With `onlyIfLapsed`, a view renewed meanwhile stays. */
async function removeView(room: RoomIncarnation, id: string, opts?: { onlyIfLapsed: true }): Promise<void> {
  const key = viewCellKey(id)
  const { value: config, counters } = await mutateCells(room.id, room.inc, [key], (cells) => {
    const raw = cells.get(key)
    if (raw === undefined || (opts?.onlyIfLapsed && !isLapsed(decodeRoomRecord<ViewRecord>(raw))))
      return { value: null, mutations: [] }
    return { value: room.hold, mutations: [{ key, bytes: null }], counts: holdCounts(room.hold, -1) }
  })
  afterHoldChange(room, holdChange(config, counters, -1))
}

/** Read-modify-write one member record; a returned `next` is stored with a renewed lease. */
async function mutateMember<T>(
  roomId: string,
  inc: string,
  id: string,
  update: (record: RoomMemberRecord | null) => { value: T; next?: RoomMemberRecord },
): Promise<T> {
  const key = memberCellKey(id)
  const { value } = await mutateCells(roomId, inc, [key], (cells) => {
    const raw = cells.get(key)
    const { value, next } = update(raw === undefined ? null : decodeRoomRecord<RoomMemberRecord>(raw))
    if (next === undefined) return { value, mutations: [] }
    return { value, mutations: [{ key, bytes: encodeRoomRecord({ ...next, seenAt: Date.now() }) }] }
  })
  return value
}

/** One exchange that expects the member's record, which each meta write counts in and each renewal rewrites. A merge
 *  writes its keys, a key already there keeping its place; a replace writes its keys into a new generation and returns
 *  the one it `replaced`, for `dropMemberMetaGeneration`. Each costs the keys it was given. */
async function writeMemberMeta(
  room: RoomIncarnation,
  id: string,
  write: MemberMetaWrite,
): Promise<{ update: MemberMetaUpdate; hidden: boolean; replaced: number | null }> {
  const backend = getRoomBackend()
  const memberKey = memberCellKey(id)
  return await retryCompareExchange(async () => {
    const read = await backend.readCells(room.id, room.inc, { keys: [memberKey] })
    if ('staleInc' in read) throw roomClosedError(room.id)
    const raw = read.cells.get(memberKey)
    if (raw === undefined) throw participantGoneError(id)
    const record = decodeRoomRecord<RoomMemberRecord>(raw)
    const seq = record.metaSeq + 1
    const gen = 'replace' in write ? seq : record.metaGen
    let mutations: CellMutation[]
    if ('replace' in write)
      mutations = Object.entries(write.replace).map(([key, value], index) =>
        metaKeyMutation(id, gen, key, value, [seq, index]),
      )
    else {
      const keys = Object.keys(write.merge).map((key) => memberMetaCellKey(id, gen, key))
      const merging = keys.length === 0 ? new Map<string, Uint8Array>() : await readCells(room.id, room.inc, keys)
      mutations = Object.entries(write.merge).map(([key, value], index) => {
        const cellKey = memberMetaCellKey(id, gen, key)
        if (value === undefined) return { key: cellKey, bytes: null }
        const found = merging.get(cellKey)
        const place = found === undefined ? ([seq, index] as const) : decodeRoomRecord<MetaKeyCell>(found).place
        return metaKeyMutation(id, gen, key, value, place)
      })
    }
    const next: RoomMemberRecord = { ...record, metaSeq: seq, metaGen: gen, seenAt: Date.now() }
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

function metaKeyMutation(
  id: string,
  gen: number,
  key: string,
  value: unknown,
  place: readonly [number, number],
): CellMutation {
  const cell: MetaKeyCell = { value, place: [place[0], place[1]] }
  return { key: memberMetaCellKey(id, gen, key), bytes: encodeRoomRecord(cell) }
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

/** Every member's meta cells, by member, a page and a `ROOM_PLAN_SLICE_MS` slice at a time. */
async function readMembersMetaCells(room: RoomIncarnation): Promise<Map<string, MemberMetaCells>> {
  const byMember = new Map<string, Array<readonly [string, Uint8Array]>>()
  const slice = new Slice()
  for await (const page of getRoomBackend().readCellPages(room.id, room.inc, MEMBER_META_CELL_PREFIX)) {
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
  const runs: RoomMetaEntry[][] = []
  let run: RoomMetaEntry[] = []
  const slice = new Slice()
  for (const [cellKey, raw] of cells) {
    const key = memberMetaKeyOfCellKey(id, gen, cellKey)
    if (key === null) continue
    const { value, place } = decodeRoomRecord<MetaKeyCell>(raw)
    run.push([key, value, place[0], place[1]])
    if (run.length < META_RUN_KEYS) continue
    runs.push(run.sort(compareMetaEntries))
    run = []
    if (slice.over) await slice.next()
  }
  runs.push(run.sort(compareMetaEntries))
  const entries = runs.length === 1 ? runs[0]! : await mergeRuns(runs)
  const meta: ParticipantMeta = {}
  for (let i = 0; i < entries.length; i++) {
    const [key, value] = entries[i]!
    Object.defineProperty(meta, key, { value, enumerable: true, configurable: true, writable: true })
    if (i % META_RUN_KEYS === 0 && slice.over) await slice.next()
  }
  return Object.freeze(meta)
}

/** Each renewal is its own compare-exchange; a failed one fails the call once all are tried. */
async function renewMemberLeases(roomId: string, inc: string, ids: Iterable<string>): Promise<void> {
  await eachInFlight(ids, (id) =>
    mutateMember(roomId, inc, id, (record) =>
      record === null ? { value: undefined } : { value: undefined, next: record },
    ),
  )
}

/** Runs `task` on each item in turn, `ROOM_MEMBER_WRITES_IN_FLIGHT_MAX` at a time and a `ROOM_PLAN_SLICE_MS` slice per
 *  turn; throws the first failure once every task ran. */
async function eachInFlight<T>(items: Iterable<T>, task: (item: T) => Promise<unknown>): Promise<void> {
  const pending = items[Symbol.iterator]()
  const slice = new Slice()
  let turn: Promise<void> | null = null
  let failure = null as { error: unknown } | null
  const worker = async (first: T) => {
    for (let next: IteratorResult<T> = { value: first }; next.done !== true; next = pending.next()) {
      if (slice.over)
        await (turn ??= slice.next().then(() => {
          turn = null
        }))
      try {
        await task(next.value)
      } catch (error) {
        failure ??= { error }
      }
    }
  }
  const workers: Promise<void>[] = []
  for (let next = pending.next(); next.done !== true; next = pending.next()) {
    workers.push(worker(next.value))
    if (workers.length === ROOM_MEMBER_WRITES_IN_FLIGHT_MAX) break
  }
  await Promise.all(workers)
  if (failure) throw failure.error
}

/** Remove a member and record the cleanup in one write, then finish the cleanup. With `onlyIfLapsed`, a member whose
 *  lease was renewed meanwhile stays, and its record is returned. */
async function evictMember(
  room: RoomIncarnation,
  id: string,
  identity: string | null,
  cause: LeaveCause,
  opts?: { onlyIfLapsed: true },
): Promise<RoomMemberRecord | null> {
  try {
    return await removeMemberCells(room, id, identity, cause, opts)
  } catch (error) {
    // A room that closed under the eviction ended the member with everyone else.
    const head = await getRoomBackend().readHead(room.id)
    if (head?.state !== 'open' || head.currentInc !== room.inc) return null
    throw error
  }
}

async function removeMemberCells(
  room: RoomIncarnation,
  id: string,
  identity: string | null,
  cause: LeaveCause,
  opts?: { onlyIfLapsed: true },
): Promise<RoomMemberRecord | null> {
  const memberKey = memberCellKey(id)
  const cleanupKey = cleanupCellKey(id)
  const removedKeys = identity === null ? [memberKey] : [memberKey, identityCellKey(identity, id)]
  const { value: outcome, counters } = await mutateCells<
    { live: RoomMemberRecord } | { cleanup: boolean; config: HoldConfig | null }
  >(room.id, room.inc, [...removedKeys, cleanupKey], (cells) => {
    const pending = cells.has(cleanupKey)
    const raw = cells.get(memberKey)
    const record = raw === undefined ? null : decodeRoomRecord<RoomMemberRecord>(raw)
    if (record !== null && opts?.onlyIfLapsed && !isLapsed(record)) return { value: { live: record }, mutations: [] }
    if (record === null) return { value: { cleanup: pending, config: null }, mutations: [] }
    const cleanup: PendingMemberCleanup = {
      cause: leaveCauseToWire(cause),
      ...(record.hidden ? { hidden: true } : {}),
    }
    const config = record.hidden ? null : room.hold
    return {
      value: { cleanup: true, config },
      mutations: [
        ...removedKeys.map((key) => ({ key, bytes: null })),
        ...(pending ? [] : [{ key: cleanupKey, bytes: encodeRoomRecord(cleanup) }]),
      ],
      counts: holdCounts(config, -1),
    }
  })
  if ('live' in outcome) return outcome.live
  afterHoldChange(room, holdChange(outcome.config, counters, -1))
  if (outcome.cleanup) await finishPendingMemberCleanup(room.id, room.inc, id)
  return null
}

/** Live members, and departing ones whose eviction the read completes; lapsed members and views are reaped on the way. */
async function reapAndReadRoster(
  room: RoomIncarnation,
): Promise<{ members: MemberSnapshot[]; departing: Set<string> }> {
  const cells = await readPrefix(room.id, room.inc, MEMBER_CELL_PREFIX)
  // After the member read, so an eviction committing in between shows up as departing.
  const cleanups = await readPrefix(room.id, room.inc, CLEANUP_CELL_PREFIX)
  const departing = new Set([...cleanups.keys()].map(memberIdOfCleanupKey))
  await eachInFlight(cleanups, ([key, raw]) => completeCleanup(room.id, room.inc, memberIdOfCleanupKey(key), raw))
  const views = await readPrefix(room.id, room.inc, VIEW_CELL_PREFIX)
  await eachInFlight(views, async ([key, raw]) => {
    if (isLapsed(decodeRoomRecord<ViewRecord>(raw)))
      await removeView(room, viewIdOfCellKey(key), { onlyIfLapsed: true })
  })
  // After the member read, so each member's cells are as new as its record or newer, and the updates after the record's
  // seq bring a view to them.
  const metaCells = await readMembersMetaCells(room)
  const members = await liveMembers(room, stayingMembers(cells, departing), metaCells)
  return { members, departing }
}

function* stayingMembers(
  cells: ReadonlyMap<string, Uint8Array>,
  departing: ReadonlySet<string>,
): Generator<readonly [string, Uint8Array]> {
  for (const [key, raw] of cells) {
    const id = memberIdOfCellKey(key)
    if (!departing.has(id)) yield [id, raw]
  }
}

async function reapAndReadMembersById(room: RoomIncarnation, ids: string[]): Promise<MemberSnapshot[]> {
  const cells = await readCells(room.id, room.inc, ids.map(memberCellKey))
  const metaCells = new Map<string, MemberMetaCells>()
  const entries = ids.flatMap((id) => {
    const raw = cells.get(memberCellKey(id))
    return raw === undefined ? [] : [[id, raw] as const]
  })
  await eachInFlight(entries, async ([id]) => {
    metaCells.set(id, [...(await readPrefix(room.id, room.inc, memberMetaCellPrefix(id)))])
  })
  return await liveMembers(room, entries, metaCells)
}

/** In the order of `entries`. */
async function liveMembers(
  room: RoomIncarnation,
  entries: Iterable<readonly [string, Uint8Array]>,
  metaCells: ReadonlyMap<string, MemberMetaCells>,
): Promise<MemberSnapshot[]> {
  const members: (MemberSnapshot | null)[] = []
  await eachInFlight(numbered(entries), async ([i, [id, raw]]) => {
    const candidate = decodeRoomRecord<RoomMemberRecord>(raw)
    const record = isLapsed(candidate)
      ? await evictMember(room, id, candidate.identity ?? null, { type: 'disconnected' }, { onlyIfLapsed: true })
      : candidate
    const cells = metaCells.get(id)
    const meta = record && cells !== undefined ? await memberMetaOf(id, record.metaGen, cells) : {}
    members[i] = record && memberSnapshot(id, record, meta)
  })
  return members.filter((member) => member !== null)
}

function* numbered<T>(items: Iterable<T>): Generator<readonly [number, T]> {
  let i = 0
  for (const item of items) yield [i++, item]
}

function memberSnapshot(id: string, record: RoomMemberRecord, meta: ParticipantMeta): MemberSnapshot {
  return {
    id,
    meta,
    joinedAt: record.joinedAt,
    metaSeq: record.metaSeq,
    identity: record.identity ?? null,
    ...(record.hidden ? { hidden: true } : {}),
  }
}

async function reapAndCountPresence(room: RoomIncarnation): Promise<number> {
  return (await reapAndReadRoster(room)).members.filter((member) => !member.hidden).length
}

async function reapAndResolveIdentity(room: RoomIncarnation, identity: string): Promise<MemberSnapshot[]> {
  const prefix = identityCellPrefix(identity)
  const markers = await readPrefix(room.id, room.inc, prefix)
  const ids = [...markers.keys()].map((key) => key.slice(prefix.length))
  const members = await reapAndReadMembersById(room, ids)
  // A member and its identity marker are written and removed in one compare-exchange.
  assert(members.every((member) => member.identity === identity))
  return members
}

async function dropRetainedOwnedBy(roomId: string, inc: string, memberId: string): Promise<void> {
  const backend = getRoomBackend()
  for (const lane of await backend.listRetained(roomId, inc, { member: memberId }))
    await backend.deleteRetained(roomId, inc, lane)
  const text = await backend.readRetained(roomId, inc, SEMANTIC_LANE)
  if (text !== null && decodeRoomRecord<RoomDataEnvelope>(text.payload).from === memberId)
    await backend.deleteRetained(roomId, inc, SEMANTIC_LANE, { ifSeq: text.seq })
}

async function finishPendingMemberCleanup(roomId: string, inc: string, memberId: string): Promise<void> {
  const key = cleanupCellKey(memberId)
  const raw = (await readCells(roomId, inc, [key])).get(key)
  if (raw !== undefined) await completeCleanup(roomId, inc, memberId, raw)
}

async function completeCleanup(roomId: string, inc: string, memberId: string, raw: Uint8Array): Promise<void> {
  const key = cleanupCellKey(memberId)
  const cleanup = decodeRoomRecord<PendingMemberCleanup>(raw)
  await dropRetainedOwnedBy(roomId, inc, memberId)
  // Every commit to the member's lanes requires its cell, which the eviction removed.
  await getRoomBackend().dropLaneOrders(roomId, inc, memberId)
  await dropCells(roomId, inc, memberMetaCellPrefix(memberId))
  await publishCtrl(roomId, inc, {
    __r: 'leave',
    id: memberId,
    ...cleanup.cause,
    ...(cleanup.hidden ? { hidden: true } : {}),
  })
  await mutateCells(roomId, inc, [key], (cells) => ({
    value: undefined,
    mutations: cells.has(key) ? [{ key, bytes: null }] : [],
  }))
}
