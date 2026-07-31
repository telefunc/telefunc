export { Room }
export type { RoomGuards }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { getBackend } from '../../backend/install.js'
import type { BackendSpi, RoomHead } from '../../backend/spi.js'
import { RoomError } from '../errors.js'
import { roomMemberKvKey } from '../keys.js'
import { mergeAttributes, ownMetadata } from '../model.js'
import {
  type MemberSnapshot,
  type RoomConfigRecord,
  type RoomCtrlEnvelope,
  type RoomDmEnvelope,
  type RoomEnvelope,
  type RoomMemberRecord,
} from '../protocol.js'
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
  assertRoomId,
  evictMember,
  presenceCount,
  readCell,
  readMembers,
  requireRoom,
  resolveIdentityMembers,
} from './membership.js'
import {
  CONTROL_LANE,
  SEMANTIC_LANE,
  commitRoomLane,
  configFromHead,
  decodeRoomText,
  encodeRoomConfig,
  encodeRoomText,
  publishCtrl,
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

async function registerRoomIndex(id: string, inc: string): Promise<void> {
  await getBackend().directoryPut(id, inc)
}

type TryCreateRoomResult = { kind: 'created'; room: Room } | { kind: 'exists' } | { kind: 'closing' }

async function tryCreateRoom(id: string, options: RoomOptions | undefined): Promise<TryCreateRoomResult> {
  const { meta } = normalizeOptions(options)
  const backend = getBackend()
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
  try {
    await registerRoomIndex(id, created.inc)
  } catch (error) {
    await closeRoom(id)
    throw error
  }
  return { kind: 'created', room: new ServerRoom(id, created, { members: [] }) }
}

async function createRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const result = await tryCreateRoom(id, options)
  if (result.kind !== 'created') throw new RoomError(`Room already exists: ${id}`)
  return result.room
}

async function getRoom(id: string, options?: RoomGetOptions): Promise<Room> {
  const { config } = await requireRoom(id)
  const room = new ServerRoom(id, config, { count: await presenceCount(id, config.inc) })
  if (options?.tail === true) room._startTail()
  return room
}

async function getOrCreateRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const deadline = Date.now() + ROOM_CLOSE_LEASE_MS * 2
  for (;;) {
    const result = await tryCreateRoom(id, options)
    if (result.kind === 'created') return result.room
    if (result.kind === 'exists') {
      try {
        const room = await getRoom(id)
        assert(ServerRoom.isServerRoom(room))
        await registerRoomIndex(id, room._inc)
        return room
      } catch (error) {
        const current = await getBackend().readHead(id)
        if (current?.head.state === 'open') throw error
      }
    }
    if (Date.now() >= deadline) throw new RoomError(`Room lifecycle contention: ${id}`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
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
  const { config } = await requireRoom(id)
  return await new ServerRoom(id, config, { count: 0 }).join(options)
}

async function listRooms(options?: { prefix?: string }): Promise<RoomInfo[]> {
  assertUsage(
    options === undefined ||
      (isObject(options) && (options.prefix === undefined || typeof options.prefix === 'string')),
    'Room.list() options.prefix should be a string',
  )
  const backend = getBackend()
  const rooms: RoomInfo[] = []
  let cursor: string | undefined
  do {
    const page = await backend.directoryList(options?.prefix ?? '', cursor)
    cursor = page.cursor
    for (const { roomId, incTag } of page.entries) {
      const current = await backend.readHead(roomId)
      if (current === null || current.head.state !== 'open' || current.head.currentInc === null) {
        await backend.directoryDelete(roomId, incTag)
        continue
      }
      const config = configFromHead(current.head)
      if (current.head.currentInc !== incTag) await backend.directoryPut(roomId, current.head.currentInc)
      const count = await presenceCount(roomId, config.inc)
      rooms.push({ id: roomId, meta: config.meta, count, isEmpty: count === 0 })
    }
  } while (cursor !== undefined)
  return rooms
}

async function setRoomMeta(id: string, meta: RoomMeta): Promise<void> {
  assertUsage(isObject(meta), 'Room.setMeta() meta should be an object')
  const owned = ownMetadata(meta)
  const { config } = await requireRoom(id)
  await writeRoomConfig(id, config, () => owned)
}

async function setRoomAttributes(id: string, attributes: RoomMeta): Promise<void> {
  assertUsage(isObject(attributes), 'Room.setAttributes() attributes should be an object')
  const owned = ownMetadata(attributes)
  const { config } = await requireRoom(id)
  await writeRoomConfig(id, config, (current) => mergeAttributes(current, owned))
}

async function writeRoomConfig(
  id: string,
  config: RoomConfigRecord,
  computeMeta: (current: RoomMeta) => RoomMeta,
): Promise<void> {
  const by = writerId()
  const backend = getBackend()
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
  const backend = getBackend()
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

async function acquireClosingLease(backend: BackendSpi, roomId: string, current: RoomHead): Promise<RoomHead | null> {
  if (current.currentInc === null) return null
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
  return 'conflict' in result || !('head' in result) ? null : result.head
}

async function finishClose(backend: BackendSpi, roomId: string, closing: RoomHead): Promise<boolean> {
  const inc = closing.currentInc
  const lease = closing.closeLease
  if (inc === null || lease === undefined) return false
  const closedEvent = await commitRoomLane(
    roomId,
    inc,
    CONTROL_LANE,
    encodeRoomText(stringify({ __r: 'closed' } satisfies RoomCtrlEnvelope)),
    { closingLease: lease.id },
  )
  if (closedEvent === null) return false
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

async function cleanupFinalizedGeneration(backend: BackendSpi, roomId: string, closed: RoomHead): Promise<void> {
  const inc = configFromHead(closed).inc
  await backend.dropGeneration(roomId, inc)
  await backend.directoryDelete(roomId, inc)
}

async function resolveParticipantRef(
  roomId: string,
  inc: string,
  target: ParticipantRef,
): Promise<{ memberId: string; identity: string | undefined }[]> {
  if ('id' in target) {
    assertUsage(
      typeof target.id === 'string' && target.id.length > 0,
      'The participant { id } should be a non-empty string',
    )
    const raw = await readCell(roomId, inc, roomMemberKvKey(roomId, target.id))
    if (raw === null) throw new RoomError(`Participant not found: ${target.id}`)
    return [{ memberId: target.id, identity: (parse(decodeRoomText(raw)) as RoomMemberRecord).identity }]
  }
  assertUsage(
    isObject(target) && typeof target.identity === 'string' && target.identity.length > 0,
    'The participant ref should be { id } or { identity }',
  )
  const { identity } = target
  return (await resolveIdentityMembers(roomId, inc, identity)).map((memberId) => ({ memberId, identity }))
}

async function removeParticipant(id: string, target: ParticipantRef & { reason?: unknown }): Promise<void> {
  const cause: LeaveCause =
    target.reason === undefined ? { type: 'removed' } : { type: 'removed', reason: target.reason }
  const { config } = await requireRoom(id)
  for (const { memberId, identity } of await resolveParticipantRef(id, config.inc, target)) {
    await evictMember(id, config.inc, memberId, identity, cause)
  }
}

async function getRoomParticipants(id: string, target?: { identity: string }): Promise<ParticipantSnapshotView[]> {
  const { config } = await requireRoom(id)
  let members: MemberSnapshot[]
  if (target === undefined) {
    members = await readMembers(id, config.inc)
  } else {
    assertUsage(
      isObject(target) && typeof target.identity === 'string' && target.identity.length > 0,
      'Room.getParticipants() target should be { identity }',
    )
    members = await readMembers(id, config.inc, await resolveIdentityMembers(id, config.inc, target.identity))
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
  const { config } = await requireRoom(id)
  const commit = await commitRoomLane(
    id,
    config.inc,
    SEMANTIC_LANE,
    encodeRoomText(stringify({ __r: 'announce', data } satisfies RoomEnvelope)),
  )
  if (commit === null) throw new RoomError(`Room is closed: ${id}`)
  return { seq: commit.seq, timestamp: commit.timestamp }
}

async function sendToParticipant(id: string, target: ParticipantRef, data: unknown): Promise<void> {
  const { config } = await requireRoom(id)
  const exact = 'id' in target
  for (const { memberId } of await resolveParticipantRef(id, config.inc, target)) {
    if (!(await sendServerDm(id, config.inc, memberId, data)) && exact) {
      throw new RoomError(`Participant not found (left?): ${memberId}`)
    }
  }
}

async function sendServerDm(roomId: string, inc: string, memberId: string, data: unknown): Promise<boolean> {
  const envelope: RoomDmEnvelope = { __r: 'dm', to: memberId, from: '', fromMeta: null, data }
  const committed = await commitRoomLane(
    roomId,
    inc,
    { kind: 'inbox', member: memberId },
    encodeRoomText(stringify(envelope)),
    { requiredCellKeys: [roomMemberKvKey(roomId, memberId)] },
  )
  if (committed !== null) return true
  const current = await getBackend().readHead(roomId)
  if (
    current?.head.state === 'open' &&
    current.head.currentInc === inc &&
    (await readCell(roomId, inc, roomMemberKvKey(roomId, memberId))) === null
  ) {
    return false
  }
  throw new RoomError(`Room is closed: ${roomId}`)
}

function normalizeOptions(options: RoomOptions | undefined): { meta: RoomMeta } {
  assertUsage(options === undefined || isObject(options), 'Room options should be an object')
  const meta = options?.meta ?? {}
  assertUsage(isObject(meta), 'options.meta should be an object')
  return { meta: ownMetadata(meta) }
}
