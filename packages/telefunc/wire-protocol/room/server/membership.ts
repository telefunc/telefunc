export {
  holdViews,
  removeView,
  createMember,
  renewSession,
  finishSessionCleanups,
  evictMember,
  reapLapsedSessions,
  reapAndReadRoster,
  reapAndReadMembersById,
  reapAndCountPresence,
  reapAndResolveIdentity,
  storeDmReply,
  takeDmReply,
  takeDmReplies,
  dropDmReply,
}

import { assert } from '../../../utils/assert.js'
import { getRoomBackend } from '../../backend/install.js'
import { Slice } from '../Slice.js'
import type { CellCount, CellCounter, CellMutation } from '../../backend/room/contract.js'
import { isOpenIncarnation } from '../../backend/room/semantics.js'
import { ROOM_MEMBER_WRITES_IN_FLIGHT_MAX } from './constants.js'
import { roomClosedError } from '../errors.js'
import {
  leaveCauseToWire,
  type RoomResult,
  type MemberSnapshot,
  type RoomCtrlEnvelope,
  type WireLeaveCause,
} from '../protocol.js'
import type { HoldConfig, RoomDataEnvelope, RoomMemberRecord } from './protocol.js'
import type { LeaveCause, ParticipantMeta } from '../types.js'
import {
  dropCells,
  dropMemberMeta,
  memberMetaMutations,
  memberMetaOf,
  readMembersMetaCells,
  readOpenCells,
  deleteCells,
  type MemberMetaCells,
} from './meta.js'
import { SEMANTIC_LANE, commitControlOrThrow, decodeRoomRecord, decodeRoomText, encodeRoomRecord } from './lanes.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { afterHoldChange, holdChange, holdCounts, type RoomIncarnation } from './lifecycle.js'
import {
  CLEANUP_CELL_PREFIX,
  MEMBER_CELL_PREFIX,
  SESSION_CELL_PREFIX,
  ackIdOfDmReplyCellKey,
  cleanupCellKey,
  dmReplyCellKey,
  dmReplyCellPrefix,
  identityCellKey,
  identityCellPrefix,
  memberCellKey,
  memberIdOfCellKey,
  memberIdOfCleanupKey,
  memberIdOfSessionMemberKey,
  sessionCellKey,
  sessionMemberCellKey,
  sessionMemberCellPrefix,
  sessionViewCellPrefix,
  viewCellKey,
} from './cells.js'
import { decodeSessionRecord, sessionRecordBytes, type RoomSession } from './session.js'

type CellPlan<T> = { value: T; mutations: CellMutation[]; counts?: CellCount[] }
type CellCommit<T> = { value: T; counters: ReadonlyMap<string, CellCounter> }
/** `session` hosted the member: its mark goes with the cleanup. */
type PendingMemberCleanup = { cause: WireLeaveCause; session: string; hidden?: true }
/** An exchange hosting something under a session found it lost. */
const SESSION_LOST: unique symbol = Symbol.for('telefunc.RoomSessionLost')
const identityTextEncoder = new TextEncoder()

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
  for await (const page of getRoomBackend().readCellPages(roomId, inc, prefix)) {
    if ('staleInc' in page) throw roomClosedError(roomId)
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
  const backend = getRoomBackend()
  return await retryCompareExchange(async () => {
    const read = await backend.readCells(roomId, inc, { keys })
    if ('staleInc' in read) throw roomClosedError(roomId)
    const { value, mutations, counts = [] } = plan(read.cells, read.now)
    if (mutations.length === 0 && counts.length === 0) return { value, counters: new Map() }
    const expect = keys.map((key) => ({ key, version: read.versions.get(key) ?? null }))
    const result = await backend.compareExchangeCells(roomId, inc, { expect, mutations, counts })
    if (result === 'stale-inc') throw roomClosedError(roomId)
    return result === 'conflict' ? CX_CONFLICT : { value, counters: result.committed }
  })
}

/** Commits what `writesOf` plans under the session in one exchange with its cell, which it only expects while live: a
 *  session found lost is replaced, and the write planned again under the new one. */
async function mutateHosted<T>(
  room: RoomIncarnation,
  session: RoomSession,
  writesOf: (id: string) => { keys: string[]; plan: (cells: ReadonlyMap<string, Uint8Array>) => CellPlan<T> },
): Promise<CellCommit<T>> {
  for (;;) {
    if (!session.created) await createSession(room, session)
    const id = session.id
    const { keys, plan } = writesOf(id)
    const committed = await mutateCells<T | typeof SESSION_LOST>(
      room.id,
      room.inc,
      [sessionCellKey(id), ...keys],
      (cells, now) => {
        const lease = session.hosting(id, cells, now)
        if (lease === null) return { value: SESSION_LOST, mutations: [] }
        const planned = plan(cells)
        return lease.length === 0 ? planned : { ...planned, mutations: [...lease, ...planned.mutations] }
      },
    )
    if (committed.value !== SESSION_LOST) {
      session.hosted(id)
      return committed as CellCommit<T>
    }
    session.lost(id)
  }
}

/** Writes the session's cell alone, once however many writes wait to host something under it, so they don't conflict
 *  on it. */
function createSession(room: RoomIncarnation, session: RoomSession): Promise<void> {
  const id = session.id
  const key = sessionCellKey(id)
  session.creating ??= mutateCells(room.id, room.inc, [key], (cells, now) => {
    const lease = session.hosting(id, cells, now)
    return { value: lease !== null, mutations: lease ?? [] }
  })
    .then(({ value: hosted }) => {
      if (hosted) session.hosted(id)
      else session.lost(id)
    })
    .finally(() => {
      session.creating = null
    })
  return session.creating
}

/** Persist a join's member record under the session, its mark, its meta's keys and identity marker; a non-hidden
 *  member holds the room. */
async function createMember(
  room: RoomIncarnation,
  session: RoomSession,
  id: string,
  record: Omit<RoomMemberRecord, 'session'>,
  meta: ParticipantMeta,
): Promise<void> {
  const config = record.hidden ? null : room.hold
  const { counters } = await mutateHosted(room, session, (hostId) => {
    const mutations: CellMutation[] = [
      { key: memberCellKey(id), bytes: encodeRoomRecord({ ...record, session: hostId } satisfies RoomMemberRecord) },
      { key: sessionMemberCellKey(hostId, id), bytes: identityTextEncoder.encode(record.identity ?? '') },
      ...memberMetaMutations(id, meta),
    ]
    if (record.identity !== undefined) {
      mutations.push({ key: identityCellKey(record.identity, id), bytes: new Uint8Array() })
    }
    const plan = () => ({ value: undefined, mutations, counts: holdCounts(config, 1, { joining: true }) })
    return { keys: mutations.map(({ key }) => key), plan }
  })
  afterHoldChange(room, holdChange(config, counters, 1, { joining: true }))
}

/** Writes pages' views under the session in one write, and returns their cells' keys; each holds the room, and a room
 *  without hold timeouts keeps none. Writing views already held changes nothing. */
async function holdViews(room: RoomIncarnation, session: RoomSession, ids: string[]): Promise<string[]> {
  const { hold } = room
  if (hold === null) return []
  const { value, counters } = await mutateHosted(room, session, (hostId) => {
    const keys = ids.map((id) => viewCellKey(hostId, id))
    const plan = (cells: ReadonlyMap<string, Uint8Array>) => {
      const added = keys.filter((key) => !cells.has(key))
      const views = added.map((key) => ({ key, bytes: new Uint8Array() }))
      return { value: { keys, added: added.length }, mutations: views, counts: holdCounts(hold, added.length) }
    }
    return { keys, plan }
  })
  if (value.added > 0) afterHoldChange(room, holdChange(hold, counters, value.added))
  return value.keys
}

/** Removes a view by the key `holdViews` returned. */
async function removeView(room: RoomIncarnation, key: string): Promise<void> {
  const { value: config, counters } = await mutateCells(room.id, room.inc, [key], (cells) => {
    if (!cells.has(key)) return { value: null, mutations: [] }
    return { value: room.hold, mutations: [{ key, bytes: null }], counts: holdCounts(room.hold, -1) }
  })
  afterHoldChange(room, holdChange(config, counters, -1))
}

/** Renews the session's lease once a write created it; false once the session was lost. */
async function renewSession(room: RoomIncarnation, session: RoomSession): Promise<boolean> {
  if (!session.created) return true
  const id = session.id
  const key = sessionCellKey(id)
  const { value: renewed } = await mutateCells(room.id, room.inc, [key], (cells, now) => {
    const raw = cells.get(key)
    if (raw === undefined || decodeSessionRecord(raw).cleaner !== undefined) return { value: false, mutations: [] }
    return { value: true, mutations: [{ key, bytes: sessionRecordBytes(now) }] }
  })
  if (!renewed) session.lost(id)
  return renewed
}

/** Finishes the cleanups of the session's members that were pending at the last call too: their evicting server, this
 *  one or another, stopped before finishing them. Returns the ones pending now, for the next call. */
async function finishSessionCleanups(
  room: RoomIncarnation,
  session: RoomSession,
  pendingBefore: ReadonlySet<string>,
): Promise<Set<string>> {
  const pending = new Set<string>()
  if (!session.created) return pending
  const cleanups = await readPrefix(room.id, room.inc, CLEANUP_CELL_PREFIX)
  await eachInFlight(cleanups, async ([key, raw]) => {
    if (decodeRoomRecord<PendingMemberCleanup>(raw).session !== session.id) return
    if (pendingBefore.has(key)) await completeCleanup(room.id, room.inc, memberIdOfCleanupKey(key), raw)
    else pending.add(key)
  })
  return pending
}

/** Claims each lapsed session and removes what it hosted: its members, each leaving `'disconnected'`, their cleanups
 *  its server left unfinished, and its pages' views; then the session. A claim lapses as a session does, so a claimant
 *  that stops leaves the rest to the next. */
async function reapLapsedSessions(room: RoomIncarnation): Promise<void> {
  await reapSessions(room, await readPrefixAt(room.id, room.inc, SESSION_CELL_PREFIX))
}

/** False once none of the sessions read had lapsed. */
async function reapSessions(room: RoomIncarnation, { cells, now }: CellsAt): Promise<boolean> {
  const cleaner = crypto.randomUUID()
  let lapsed = false
  for (const [key, raw] of cells) {
    if (decodeSessionRecord(raw).until >= now) continue
    lapsed = true
    const { value: claimed } = await mutateCells(room.id, room.inc, [key], (current, at) => {
      const latest = current.get(key)
      if (latest === undefined || decodeSessionRecord(latest).until >= at) return { value: false, mutations: [] }
      return { value: true, mutations: [{ key, bytes: sessionRecordBytes(at, cleaner) }] }
    })
    if (claimed) await cleanSession(room, key.slice(SESSION_CELL_PREFIX.length), cleaner)
  }
  return lapsed
}

async function cleanSession(room: RoomIncarnation, session: string, cleaner: string): Promise<void> {
  const marks = await readPrefix(room.id, room.inc, sessionMemberCellPrefix(session))
  await eachInFlight(marks, ([key, raw]) =>
    finishSessionMember(room, session, memberIdOfSessionMemberKey(key), raw.length > 0 ? decodeRoomText(raw) : null),
  )
  const views = await readPrefix(room.id, room.inc, sessionViewCellPrefix(session))
  await eachInFlight(views.keys(), (key) => removeView(room, key))
  const key = sessionCellKey(session)
  await mutateCells(room.id, room.inc, [key], (cells) => {
    const raw = cells.get(key)
    const ours = raw !== undefined && decodeSessionRecord(raw).cleaner === cleaner
    return { value: undefined, mutations: ours ? [{ key, bytes: null }] : [] }
  })
}

/** A member of a lapsed session leaves, or finishes leaving; its mark names its identity. */
async function finishSessionMember(
  room: RoomIncarnation,
  session: string,
  id: string,
  identity: string | null,
): Promise<void> {
  if (await evictMember(room, id, identity, { type: 'disconnected' })) return
  const mark = sessionMemberCellKey(session, id)
  await mutateCells(room.id, room.inc, [mark], (current) => ({
    value: undefined,
    mutations: current.has(mark) ? [{ key: mark, bytes: null }] : [],
  }))
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

/** Remove a member and record the cleanup in one write, then finish the cleanup; false once neither the member nor
 *  its cleanup was there. */
async function evictMember(
  room: RoomIncarnation,
  id: string,
  identity: string | null,
  cause: LeaveCause,
): Promise<boolean> {
  try {
    return await removeMemberCells(room, id, identity, cause)
  } catch (error) {
    // A room that closed under the eviction ended the member with everyone else.
    if (!isOpenIncarnation(await getRoomBackend().readHead(room.id), room.inc)) return true
    throw error
  }
}

async function removeMemberCells(
  room: RoomIncarnation,
  id: string,
  identity: string | null,
  cause: LeaveCause,
): Promise<boolean> {
  const memberKey = memberCellKey(id)
  const cleanupKey = cleanupCellKey(id)
  const removedKeys = identity === null ? [memberKey] : [memberKey, identityCellKey(identity, id)]
  const { value: outcome, counters } = await mutateCells<{ cleanup: boolean; config: HoldConfig | null }>(
    room.id,
    room.inc,
    [...removedKeys, cleanupKey],
    (cells) => {
      const pending = cells.has(cleanupKey)
      const raw = cells.get(memberKey)
      const record = raw === undefined ? null : decodeRoomRecord<RoomMemberRecord>(raw)
      if (record === null) return { value: { cleanup: pending, config: null }, mutations: [] }
      const cleanup: PendingMemberCleanup = { cause: leaveCauseToWire(cause), session: record.session }
      if (record.hidden) cleanup.hidden = true
      const config = record.hidden ? null : room.hold
      const mutations: CellMutation[] = removedKeys.map((key) => ({ key, bytes: null }))
      if (!pending) mutations.push({ key: cleanupKey, bytes: encodeRoomRecord(cleanup) })
      return { value: { cleanup: true, config }, mutations, counts: holdCounts(config, -1) }
    },
  )
  afterHoldChange(room, holdChange(outcome.config, counters, -1))
  if (outcome.cleanup) await finishPendingMemberCleanup(room.id, room.inc, id)
  return outcome.cleanup
}

type Roster = { members: MemberSnapshot[]; departing: Set<string> }

/** Live members, and departing ones, whose leave their evicting server finishes; read again after a lapsed session's
 *  were removed. */
async function reapAndReadRoster(room: RoomIncarnation): Promise<Roster> {
  const [sessions, roster] = await Promise.all([readPrefixAt(room.id, room.inc, SESSION_CELL_PREFIX), readRoster(room)])
  return (await reapSessions(room, sessions)) ? await readRoster(room) : roster
}

async function readRoster(room: RoomIncarnation): Promise<Roster> {
  const cells = await readPrefix(room.id, room.inc, MEMBER_CELL_PREFIX)
  // After the member read, so an eviction committing in between shows up as departing.
  const cleanups = await readPrefix(room.id, room.inc, CLEANUP_CELL_PREFIX)
  const departing = new Set(Array.from(cleanups.keys(), memberIdOfCleanupKey))
  // After the member read, so each member's cells are as new as its record or newer, and the updates after the record's
  // seq bring a view to them.
  const metaCells = await readMembersMetaCells(room, cells)
  const members = await liveMembers(stayingMembers(cells, departing), metaCells)
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

/** Read again after a lapsed session's members were removed. */
async function reapAndReadMembersById(room: RoomIncarnation, ids: string[]): Promise<MemberSnapshot[]> {
  const [sessions, members] = await Promise.all([
    readPrefixAt(room.id, room.inc, SESSION_CELL_PREFIX),
    readMembersById(room, ids),
  ])
  return (await reapSessions(room, sessions)) ? await readMembersById(room, ids) : members
}

async function readMembersById(room: RoomIncarnation, ids: string[]): Promise<MemberSnapshot[]> {
  const { cells } = await readOpenCells(room.id, room.inc, ids.map(memberCellKey))
  const metaCells = new Map<string, MemberMetaCells>()
  const entries = ids.flatMap((id) => {
    const raw = cells.get(memberCellKey(id))
    return raw === undefined ? [] : [[id, raw] as const]
  })
  await eachInFlight(entries, async ([id]) => {
    metaCells.set(id, (await readMembersMetaCells(room, cells, id)).get(id) ?? [])
  })
  return await liveMembers(entries, metaCells)
}

/** In the order of `entries`. */
async function liveMembers(
  entries: Iterable<readonly [string, Uint8Array]>,
  metaCells: ReadonlyMap<string, MemberMetaCells>,
): Promise<MemberSnapshot[]> {
  const members: MemberSnapshot[] = []
  await eachInFlight(numbered(entries), async ([i, [id, raw]]) => {
    const record = decodeRoomRecord<RoomMemberRecord>(raw)
    const cells = metaCells.get(id)
    const meta = cells !== undefined ? await memberMetaOf(id, record.metaGen, cells) : {}
    members[i] = memberSnapshot(id, record, meta)
  })
  return members
}

function* numbered<T>(items: Iterable<T>): Generator<readonly [number, T]> {
  let i = 0
  for (const item of items) yield [i++, item]
}

function memberSnapshot(id: string, record: RoomMemberRecord, meta: ParticipantMeta): MemberSnapshot {
  const snapshot: MemberSnapshot = {
    id,
    meta,
    joinedAt: record.joinedAt,
    metaSeq: record.metaSeq,
    identity: record.identity ?? null,
  }
  if (record.hidden) snapshot.hidden = true
  if (record.disconnected) snapshot.disconnected = true
  return snapshot
}

async function reapAndCountPresence(room: RoomIncarnation): Promise<number> {
  return (await reapAndReadRoster(room)).members.filter((member) => !member.hidden).length
}

async function reapAndResolveIdentity(room: RoomIncarnation, identity: string): Promise<MemberSnapshot[]> {
  const prefix = identityCellPrefix(identity)
  const markers = await readPrefix(room.id, room.inc, prefix)
  const ids = Array.from(markers.keys(), (key) => key.slice(prefix.length))
  const members = await reapAndReadMembersById(room, ids)
  // A member and its identity marker are written and removed in one compare-exchange.
  assert(members.every((member) => member.identity === identity))
  return members
}

async function dropRetainedOwnedBy(roomId: string, inc: string, memberId: string): Promise<void> {
  const backend = getRoomBackend()
  for (const lane of await backend.listRetained(roomId, inc, { member: memberId })) {
    await backend.deleteRetained(roomId, inc, lane)
  }
  const text = await backend.readRetained(roomId, inc, SEMANTIC_LANE)
  if (text !== null && decodeRoomRecord<RoomDataEnvelope>(text.payload).from === memberId) {
    await backend.deleteRetained(roomId, inc, SEMANTIC_LANE, { ifSeq: text.seq })
  }
}

async function finishPendingMemberCleanup(roomId: string, inc: string, memberId: string): Promise<void> {
  const key = cleanupCellKey(memberId)
  const raw = (await readOpenCells(roomId, inc, [key])).cells.get(key)
  if (raw !== undefined) await completeCleanup(roomId, inc, memberId, raw)
}

async function completeCleanup(roomId: string, inc: string, memberId: string, raw: Uint8Array): Promise<void> {
  const key = cleanupCellKey(memberId)
  const cleanup = decodeRoomRecord<PendingMemberCleanup>(raw)
  await dropRetainedOwnedBy(roomId, inc, memberId)
  // Every commit to the member's lanes requires its cell, which the eviction removed.
  await getRoomBackend().dropLaneOrders(roomId, inc, memberId)
  await dropMemberMeta(roomId, inc, memberId)
  await dropCells(roomId, inc, dmReplyCellPrefix(memberId))
  const leave: Extract<RoomCtrlEnvelope, { __r: 'leave' }> = { __r: 'leave', id: memberId, ...cleanup.cause }
  if (cleanup.hidden) leave.hidden = true
  await commitControlOrThrow(roomId, inc, leave)
  // The mark goes last, so a session's cleaner finds the member until its leave is out.
  const mark = sessionMemberCellKey(cleanup.session, memberId)
  await mutateCells(roomId, inc, [key, mark], (cells) => ({
    value: undefined,
    mutations: [key, mark].filter((at) => cells.has(at)).map((at) => ({ key: at, bytes: null })),
  }))
}

/** Kept only while its sender is a member, whose leave drops what it never settled. */
async function storeDmReply(
  roomId: string,
  inc: string,
  sender: string,
  ackId: string,
  reply: RoomResult,
): Promise<void> {
  const member = memberCellKey(sender)
  await mutateCells(roomId, inc, [member], (cells) => ({
    value: undefined,
    mutations: cells.has(member) ? [{ key: dmReplyCellKey(sender, ackId), bytes: encodeRoomRecord(reply) }] : [],
  }))
}

/** The reply stored for `ackId`, deleted as it is read, or null if none is. */
async function takeDmReply(roomId: string, inc: string, sender: string, ackId: string): Promise<RoomResult | null> {
  const key = dmReplyCellKey(sender, ackId)
  const raw = (await readOpenCells(roomId, inc, [key])).cells.get(key)
  if (raw === undefined) return null
  await dropDmReply(roomId, inc, sender, ackId)
  return decodeRoomRecord<RoomResult>(raw)
}

/** Every reply stored for `sender`, by `ackId`, deleted a page at a time as it is read. */
async function takeDmReplies(roomId: string, inc: string, sender: string): Promise<Map<string, RoomResult>> {
  const backend = getRoomBackend()
  const replies = new Map<string, RoomResult>()
  const slice = new Slice()
  for await (const page of backend.readCellPages(roomId, inc, dmReplyCellPrefix(sender))) {
    if ('staleInc' in page) throw roomClosedError(roomId)
    for (const [key, raw] of page.cells) replies.set(ackIdOfDmReplyCellKey(key), decodeRoomRecord<RoomResult>(raw))
    if (!(await deleteCells(roomId, inc, page.cells.keys()))) throw roomClosedError(roomId)
    if (slice.over) await slice.next()
  }
  return replies
}

async function dropDmReply(roomId: string, inc: string, sender: string, ackId: string): Promise<void> {
  if (!(await deleteCells(roomId, inc, [dmReplyCellKey(sender, ackId)]))) throw roomClosedError(roomId)
}
