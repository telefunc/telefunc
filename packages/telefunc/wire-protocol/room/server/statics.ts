export { Room }
export type { RoomGuards }

import { assert, assertUsage } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { getRoomBackend } from '../../backend/install.js'
import type { RoomBackend, RoomHead } from '../../backend/room/contract.js'
import { RoomError } from '../errors.js'
import { mergeAttributes, ownMetadata } from '../model.js'
import type { MemberSnapshot, RoomConfigRecord, RoomCtrlEnvelope, RoomDmEnvelope, RoomEnvelope } from '../protocol.js'
import type {
  AfterJoinHook,
  AfterPublishHook,
  AfterSendHook,
  JoinGuard,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  ParticipantRef,
  ParticipantSnapshotView,
  PublishGuard,
  Room as RoomInstance,
  RoomGetOptions,
  RoomInfo,
  RoomMeta,
  RoomOptions,
  RoomSendReceipt,
  SendGuard,
} from '../types.js'
import {
  evictMember,
  memberCellKey,
  presenceCount,
  readAllMembers,
  readMembersById,
  resolveIdentityMembers,
} from './membership.js'
import {
  CONTROL_LANE,
  SEMANTIC_LANE,
  commitRoomLane,
  configFromHead,
  encodeRoomConfig,
  encodeRoomRecord,
  publishCtrl,
  staleCommitError,
} from './lanes.js'
import { ServerRoom } from './room.js'

type Room<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = RoomInstance<
  M,
  P,
  Pub
>

type RoomStatic = {
  create<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: RoomOptions<M>,
  ): Promise<Room<M, P, Pub>>
  get<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: RoomGetOptions,
  ): Promise<Room<M, P, Pub>>
  getOrCreate<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: RoomOptions<M>,
  ): Promise<Room<M, P, Pub>>
  guard<M extends RoomMeta, P extends ParticipantMeta, Pub = unknown>(
    room: Room<M, P, Pub>,
    guards: {
      onBeforeJoin?: JoinGuard<P>
      onAfterJoin?: AfterJoinHook<P>
      onBeforeSend?: SendGuard<P>
      onAfterSend?: AfterSendHook<P>
      onBeforePublish?: PublishGuard<P>
      onAfterPublish?: AfterPublishHook<P>
    },
  ): void
  join<P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: JoinOptions<P>,
  ): Promise<LocalParticipant<P, Pub>>
  list<M extends RoomMeta = RoomMeta>(options?: { prefix?: string }): Promise<RoomInfo<M>[]>
  setMeta(id: string, meta: RoomMeta): Promise<void>
  setAttributes(id: string, attributes: RoomMeta): Promise<void>
  close(id: string): Promise<void>
  removeParticipant(id: string, target: ParticipantRef & { reason?: unknown }): Promise<void>
  announce(id: string, data: unknown): Promise<RoomSendReceipt>
  send(id: string, target: ParticipantRef, data: unknown): Promise<void>
  getParticipants<P extends ParticipantMeta = ParticipantMeta>(
    id: string,
    target?: { identity: string },
  ): Promise<ParticipantSnapshotView<P>[]>
}

const Room: RoomStatic = {
  create: createRoom as RoomStatic['create'],
  get: getRoom as RoomStatic['get'],
  getOrCreate: getOrCreateRoom as RoomStatic['getOrCreate'],
  guard: guardRoom as RoomStatic['guard'],
  join: joinRoom as RoomStatic['join'],
  list: listRooms as RoomStatic['list'],
  setMeta: setRoomMeta,
  setAttributes: setRoomAttributes,
  close: closeRoom,
  removeParticipant,
  announce: announceToRoom,
  send: sendToParticipant,
  getParticipants: getRoomParticipants as RoomStatic['getParticipants'],
}

const ROOM_TOMBSTONE_TTL_MS = 60_000
const ROOM_CLOSE_LEASE_MS = 15_000
const ROOM_CX_ATTEMPTS = 16
let _writerId: string | undefined

function writerId(): string {
  _writerId ??= crypto.randomUUID()
  return _writerId
}

function assertRoomId(id: unknown): asserts id is string {
  assertUsage(typeof id === 'string' && id.length > 0, 'The room ID should be a non-empty string')
  assertUsage(id.isWellFormed(), 'The room ID should be a well-formed string')
}

async function requireRoom(id: string): Promise<RoomConfigRecord> {
  assertRoomId(id)
  const current = await getRoomBackend().readHead(id)
  if (current === null || current.head.state !== 'open' || current.head.currentInc === null) {
    throw new RoomError(`Room not found: ${id}`)
  }
  return configFromHead(current.head)
}

async function repairRoomIndex(id: string, listedInc: string | null, liveInc: string | null): Promise<void> {
  if (liveInc === null) return await getRoomBackend().directoryDelete(id, listedInc!)
  if (liveInc !== listedInc) await getRoomBackend().directoryPut(id, liveInc)
}
type TryCreateRoomResult = { kind: 'created'; room: Room } | { kind: 'exists' } | { kind: 'closing' }

async function tryCreateRoom(id: string, options: RoomOptions | undefined): Promise<TryCreateRoomResult> {
  const { meta } = normalizeOptions(options)
  const backend = getRoomBackend()
  let current = await backend.readHead(id)
  if (current?.head.state === 'closing') {
    const closing = await acquireClosingLease(backend, id, current.head)
    if (closing === null || !(await finishClose(backend, id, closing))) return { kind: 'closing' }
    current = await backend.readHead(id)
  }
  if (current?.head.state === 'closed') await cleanupFinalizedGeneration(backend, id, current.head)
  if (current !== null && current.head.state !== 'closed') return { kind: 'exists' }
  const created: RoomConfigRecord = {
    meta,
    at: Date.now(),
    by: writerId(),
    inc: crypto.randomUUID(),
  }
  const result = await backend.compareExchangeHead(
    id,
    current === null ? { expect: 'absent' } : { expect: { rev: current.head.rev } },
    { head: { currentInc: created.inc, state: 'open', config: encodeRoomConfig(created) } },
  )
  if ('conflict' in result) {
    return result.current?.state === 'closing' ? { kind: 'closing' } : { kind: 'exists' }
  }
  assert('head' in result)
  await backend.directoryPut(id, created.inc)
  return { kind: 'created', room: new ServerRoom(id, created, { members: [] }) }
}

async function createRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const result = await tryCreateRoom(id, options)
  if (result.kind !== 'created') throw new RoomError(`Room already exists: ${id}`)
  return result.room
}

async function getRoom(id: string, options?: RoomGetOptions): Promise<Room> {
  const config = await requireRoom(id)
  const room = new ServerRoom(id, config, { count: await presenceCount(id, config.inc) })
  if (options?.tail === true) room._startTail()
  return room
}

async function getOrCreateRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const result = await tryCreateRoom(id, options)
  if (result.kind === 'created') return result.room
  if (result.kind === 'closing') throw new RoomError(`Room is closing: ${id}`)
  const room = await getRoom(id)
  assert(ServerRoom.isServerRoom(room))
  await getRoomBackend().directoryPut(id, room._inc)
  return room
}

const ROOM_GUARD_KEYS = [
  'onBeforeJoin',
  'onAfterJoin',
  'onBeforeSend',
  'onAfterSend',
  'onBeforePublish',
  'onAfterPublish',
] as const

type RoomGuards = {
  onBeforeJoin: JoinGuard | null
  onAfterJoin: AfterJoinHook | null
  onBeforeSend: SendGuard | null
  onAfterSend: AfterSendHook | null
  onBeforePublish: PublishGuard | null
  onAfterPublish: AfterPublishHook | null
}

function guardRoom(room: Room, guards: Partial<Record<(typeof ROOM_GUARD_KEYS)[number], unknown>>): void {
  assertUsage(ServerRoom.isServerRoom(room), 'Room.guard() expects a room obtained from Room.get()/Room.create()')
  assertUsage(isObject(guards), 'Room.guard() guards should be an object')
  for (const key of ROOM_GUARD_KEYS) {
    assertUsage(
      guards[key] === undefined || typeof guards[key] === 'function',
      `Room.guard() ${key} should be a function`,
    )
  }
  room._setGuards({
    onBeforeJoin: (guards.onBeforeJoin as JoinGuard) ?? null,
    onAfterJoin: (guards.onAfterJoin as AfterJoinHook) ?? null,
    onBeforeSend: (guards.onBeforeSend as SendGuard) ?? null,
    onAfterSend: (guards.onAfterSend as AfterSendHook) ?? null,
    onBeforePublish: (guards.onBeforePublish as PublishGuard) ?? null,
    onAfterPublish: (guards.onAfterPublish as AfterPublishHook) ?? null,
  })
}

async function joinRoom(id: string, options?: JoinOptions): Promise<LocalParticipant> {
  const config = await requireRoom(id)
  return await new ServerRoom(id, config, { count: 0 }).join(options)
}

async function listRooms(options?: { prefix?: string }): Promise<RoomInfo[]> {
  assertUsage(
    options === undefined ||
      (isObject(options) && (options.prefix === undefined || typeof options.prefix === 'string')),
    'Room.list() options.prefix should be a string',
  )
  const backend = getRoomBackend()
  const rooms: RoomInfo[] = []
  let cursor: string | undefined
  do {
    const page = await backend.directoryList(options?.prefix ?? '', cursor)
    cursor = page.cursor
    for (const { roomId, incTag } of page.entries) {
      const current = await backend.readHead(roomId)
      if (current === null || current.head.state !== 'open' || current.head.currentInc === null) {
        await repairRoomIndex(roomId, incTag, null)
        continue
      }
      const config = configFromHead(current.head)
      await repairRoomIndex(roomId, incTag, current.head.currentInc)
      const count = await presenceCount(roomId, config.inc)
      rooms.push({ id: roomId, meta: config.meta, count, isEmpty: count === 0 })
    }
  } while (cursor !== undefined)
  return rooms
}

async function setRoomMeta(id: string, meta: RoomMeta): Promise<void> {
  assertUsage(isObject(meta), 'Room.setMeta() meta should be an object')
  const owned = ownMetadata(meta)
  const config = await requireRoom(id)
  await writeRoomConfig(id, config, () => owned)
}

async function setRoomAttributes(id: string, attributes: RoomMeta): Promise<void> {
  assertUsage(isObject(attributes), 'Room.setAttributes() attributes should be an object')
  const owned = ownMetadata(attributes)
  const config = await requireRoom(id)
  await writeRoomConfig(id, config, (current) => mergeAttributes(current, owned))
}

async function writeRoomConfig(
  id: string,
  config: RoomConfigRecord,
  computeMeta: (current: RoomMeta) => RoomMeta,
): Promise<void> {
  const by = writerId()
  const backend = getRoomBackend()
  for (let attempt = 0; attempt < ROOM_CX_ATTEMPTS; attempt++) {
    const current = await backend.readHead(id)
    if (current === null || current.head.state !== 'open' || current.head.currentInc !== config.inc) {
      throw new RoomError(`Room is closed: ${id}`)
    }
    const currentConfig = configFromHead(current.head)
    const at = Math.max(Date.now(), currentConfig.at + 1)
    const meta = computeMeta(currentConfig.meta)
    const nextConfig = { meta, at, by, inc: config.inc }
    const result = await backend.compareExchangeHead(
      id,
      { expect: { rev: current.head.rev } },
      { head: { currentInc: config.inc, state: 'open', config: encodeRoomConfig(nextConfig) } },
    )
    if ('conflict' in result) continue
    await publishCtrl(id, config.inc, { __r: 'update', meta, at, by })
    return
  }
  throw new RoomError(`Room update contention: ${id}`)
}

async function closeRoom(id: string): Promise<void> {
  assertRoomId(id)
  const backend = getRoomBackend()
  for (;;) {
    const current = await backend.readHead(id)
    if (current === null) return
    if (current.head.state === 'closed') {
      await cleanupFinalizedGeneration(backend, id, current.head)
      return
    }
    const closing = await acquireClosingLease(backend, id, current.head)
    if (closing !== null && (await finishClose(backend, id, closing))) return
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

async function acquireClosingLease(backend: RoomBackend, roomId: string, current: RoomHead): Promise<RoomHead | null> {
  assert(current.currentInc !== null) // only open and closing heads reach here; both name an incarnation
  const closeLease = { id: crypto.randomUUID(), durationMs: ROOM_CLOSE_LEASE_MS }
  const result = await backend.compareExchangeHead(
    roomId,
    current.state === 'open'
      ? { expect: { rev: current.rev } }
      : { expect: { rev: current.rev, closingLeaseExpired: true } },
    {
      head: {
        currentInc: current.currentInc,
        state: 'closing',
        config: encodeRoomConfig(configFromHead(current)),
        closeLease,
      },
    },
  )
  if ('conflict' in result) return null
  assert('head' in result)
  return result.head
}

async function finishClose(backend: RoomBackend, roomId: string, closing: RoomHead): Promise<boolean> {
  const inc = closing.currentInc
  const lease = closing.closeLease
  assert(inc !== null && lease !== undefined) // the closing head acquireClosingLease just wrote
  const closedEvent = await commitRoomLane(
    roomId,
    inc,
    CONTROL_LANE,
    encodeRoomRecord({ __r: 'closed' } satisfies RoomCtrlEnvelope),
    { closingLease: lease.id },
  )
  if ('stale' in closedEvent) return false
  const config = configFromHead(closing)
  const finalized = await backend.compareExchangeHead(
    roomId,
    { expect: { rev: closing.rev, closingLease: lease.id } },
    {
      head: { currentInc: null, state: 'closed', config: encodeRoomConfig(config) },
      ttlMs: ROOM_TOMBSTONE_TTL_MS,
    },
  )
  if ('conflict' in finalized) return false
  assert('head' in finalized)
  await cleanupFinalizedGeneration(backend, roomId, finalized.head)
  return true
}

async function cleanupFinalizedGeneration(backend: RoomBackend, roomId: string, closed: RoomHead): Promise<void> {
  const inc = configFromHead(closed).inc
  await backend.dropGeneration(roomId, inc)
  await backend.directoryDelete(roomId, inc)
}

async function resolveParticipantRef(roomId: string, inc: string, target: ParticipantRef): Promise<MemberSnapshot[]> {
  if ('id' in target) {
    assertUsage(
      typeof target.id === 'string' && target.id.length > 0,
      'The participant { id } should be a non-empty string',
    )
    const members = await readMembersById(roomId, inc, [target.id])
    if (members.length === 0) throw new RoomError(`Participant not found: ${target.id}`)
    return members
  }
  assertUsage(
    isObject(target) && typeof target.identity === 'string' && target.identity.length > 0,
    'The participant ref should be { id } or { identity }',
  )
  return await resolveIdentityMembers(roomId, inc, target.identity)
}

async function removeParticipant(id: string, target: ParticipantRef & { reason?: unknown }): Promise<void> {
  const cause: LeaveCause =
    target.reason === undefined ? { type: 'removed' } : { type: 'removed', reason: target.reason }
  const config = await requireRoom(id)
  for (const member of await resolveParticipantRef(id, config.inc, target)) {
    await evictMember(id, config.inc, member.id, member.identity ?? null, cause)
  }
}

async function getRoomParticipants(id: string, target?: { identity: string }): Promise<ParticipantSnapshotView[]> {
  const config = await requireRoom(id)
  let members: MemberSnapshot[]
  if (target === undefined) {
    members = await readAllMembers(id, config.inc)
  } else {
    assertUsage(
      isObject(target) && typeof target.identity === 'string' && target.identity.length > 0,
      'Room.getParticipants() target should be { identity }',
    )
    members = await resolveIdentityMembers(id, config.inc, target.identity)
  }
  return members
    .filter((member) => !member.hidden)
    .map((member) => ({
      id: member.id,
      identity: member.identity ?? null,
      meta: member.meta,
      joinedAt: member.joinedAt,
    }))
}

async function announceToRoom(id: string, data: unknown): Promise<RoomSendReceipt> {
  const config = await requireRoom(id)
  const commit = await commitRoomLane(
    id,
    config.inc,
    SEMANTIC_LANE,
    encodeRoomRecord({ __r: 'announce', data } satisfies RoomEnvelope),
  )
  if ('stale' in commit) throw staleCommitError(id, commit)
  return { seq: commit.seq, timestamp: commit.timestamp }
}

async function sendToParticipant(id: string, target: ParticipantRef, data: unknown): Promise<void> {
  const config = await requireRoom(id)
  const exact = 'id' in target
  for (const member of await resolveParticipantRef(id, config.inc, target)) {
    if (!(await sendServerDm(id, config.inc, member.id, data)) && exact) {
      throw new RoomError(`Participant not found (left?): ${member.id}`)
    }
  }
}

async function sendServerDm(roomId: string, inc: string, memberId: string, data: unknown): Promise<boolean> {
  const envelope: RoomDmEnvelope = { __r: 'dm', to: memberId, from: '', fromMeta: null, data }
  const committed = await commitRoomLane(roomId, inc, { kind: 'inbox', member: memberId }, encodeRoomRecord(envelope), {
    requiredCellKeys: [memberCellKey(memberId)],
  })
  if (!('stale' in committed)) return true
  if (committed.stale === 'cell') return false
  throw staleCommitError(roomId, committed)
}

function normalizeOptions(options: RoomOptions | undefined): { meta: RoomMeta } {
  assertUsage(options === undefined || isObject(options), 'Room options should be an object')
  const meta = options?.meta ?? {}
  assertUsage(isObject(meta), 'options.meta should be an object')
  return { meta: ownMetadata(meta) }
}
