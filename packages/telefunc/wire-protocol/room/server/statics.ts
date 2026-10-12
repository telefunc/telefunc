export { Room }
export type { RoomGuardHooks }

import { assert, assertUsage } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { getRoomBackend } from '../../backend/install.js'
import type { RoomHead } from '../../backend/room/contract.js'
import { RoomError, isRoomError, participantGoneError, roomClosedError } from '../errors.js'
import {
  assertKnownOptions,
  assertParticipantIdentity,
  assertRoomTimeout,
  isRecord,
  ownMessage,
  ownMetadata,
  removedCause,
} from '../model.js'
import type { MemberSnapshot, RoomConfigRecord, RoomEnvelope } from '../protocol.js'
import type {
  AfterJoinHook,
  AfterPublishHook,
  AfterSendHook,
  JoinGuard,
  JoinOptions,
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
  reapAndCountPresence,
  reapAndReadMembersById,
  reapAndReadRoster,
  reapAndResolveIdentity,
} from './membership.js'
import { SEMANTIC_LANE, commitToInbox, commitRoomLaneOrThrow, openConfig, encodeRoomRecord } from './lanes.js'
import { ServerRoom } from './room.js'
import { readRoomMeta, writeRoomMeta, type StoredRoomMeta } from './meta.js'
import { metaEntriesAt, metaObjectOf } from '../meta-state.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'
import { acquireClosingLease, cleanupFinalizedIncarnation, closeIncarnation, finishClose } from './close.js'
import { createHold, lapseNewRoom, newHoldConfig, type RoomIncarnation } from './lifecycle.js'
import { directoryCheckAt, repairRoomIndex, sweepLapsedRoomsSoon } from './directory.js'
import { getServerConfig } from '../../../node/server/serverConfig.js'

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
    guards: Partial<RoomGuardHooks<P>>,
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

function assertRoomId(id: unknown): asserts id is string {
  assertUsage(typeof id === 'string' && id.length > 0, 'The room ID should be a non-empty string')
  assertUsage(id.isWellFormed(), 'The room ID should be a well-formed string')
}

/** The room's config, or null if the room is missing or closed. */
async function findRoom(id: string): Promise<RoomConfigRecord | null> {
  assertRoomId(id)
  return openConfig(await getRoomBackend().readHead(id))
}

async function requireRoom(id: string): Promise<RoomConfigRecord> {
  const config = await findRoom(id)
  if (config === null) throw roomNotFoundError(id)
  return config
}

function roomNotFoundError(id: string): RoomError {
  return new RoomError(`Room not found: ${id}`)
}

type TryCreateRoomResult = { kind: 'created'; room: Room } | { kind: 'exists'; head: RoomHead } | { kind: 'closing' }

async function tryCreateRoom(id: string, options: RoomOptions | undefined): Promise<TryCreateRoomResult> {
  const { meta, emptyTimeout, departureTimeout } = normalizeRoomOptions(options)
  const backend = getRoomBackend()
  return await retryCompareExchange(async () => {
    let current = await backend.readHead(id)
    if (current?.state === 'closing') {
      const closing = await acquireClosingLease(backend, id, current)
      if (closing === null || !(await finishClose(backend, id, closing))) return { kind: 'closing' }
      current = await backend.readHead(id)
    }
    if (current?.state === 'closed') await cleanupFinalizedIncarnation(backend, id, current)
    if (current !== null && current.state !== 'closed') return { kind: 'exists', head: current }
    const created: RoomConfigRecord = {
      meta,
      inc: crypto.randomUUID(),
      hold: newHoldConfig(emptyTimeout, departureTimeout),
    }
    const result = await backend.compareExchangeHead(
      id,
      current === null ? { form: 'absent' } : { form: 'rev', rev: current.rev },
      { head: { currentInc: created.inc, state: 'open', config: encodeRoomRecord(created) } },
    )
    if ('conflict' in result) {
      // A head that went away (a lapsed tombstone) or closed meanwhile still allows the create: try again.
      if (result.current === null || result.current.state === 'closed') return CX_CONFLICT
      return result.current.state === 'closing' ? { kind: 'closing' } : { kind: 'exists', head: result.current }
    }
    assert('head' in result)
    // The head lapses only once the counters that keep it ahead are written.
    await createHold({ id, ...created })
    const lapsing = await lapseNewRoom({ id, ...created })
    // An incarnation that lapsed is still listed, and its data still stored: dropped as `Room.list()` would.
    const lapsed = current === null ? (await backend.directoryList(id)).entries.find((e) => e.roomId === id) : undefined
    if (lapsed !== undefined) await backend.dropGeneration(id, lapsed.incTag)
    await backend.directoryPut(id, created.inc, directoryCheckAt(created.hold, lapsing?.expiresAt))
    sweepLapsedRoomsSoon()
    return {
      kind: 'created',
      room: new ServerRoom(id, created, { entries: metaEntriesAt(meta, 0), seq: 0 }, { members: [] }),
    }
  })
}

async function createRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const result = await tryCreateRoom(id, options)
  if (result.kind !== 'created') throw new RoomError(`Room already exists: ${id}`)
  return result.room
}

async function getRoom(id: string, options?: RoomGetOptions): Promise<Room> {
  assertKnownOptions(options, ['tail'], 'Room.get()')
  const room = await openRoom(id)
  if (options?.tail === true) await room._startTail()
  return room
}

async function openRoom(id: string): Promise<ServerRoom> {
  const config = await requireRoom(id)
  const count = await reapAndCountPresence({ id, ...config })
  return new ServerRoom(id, config, await requireRoomMeta(id, config), { count })
}

async function requireRoomMeta(id: string, config: RoomConfigRecord): Promise<StoredRoomMeta> {
  const stored = await readRoomMeta(id, config)
  if (stored === null) throw roomClosedError(id)
  return stored
}

async function getOrCreateRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const result = await tryCreateRoom(id, options)
  if (result.kind === 'created') return result.room
  if (result.kind === 'closing') throw new RoomError(`Room is closing: ${id}`)
  const room = await openRoom(id)
  // Repairs the listing of a creator that crashed between its head and directory writes.
  const { head } = result
  const expiresAt = head.currentInc === room._inc ? head.expiresAt : undefined
  await getRoomBackend().directoryPut(id, room._inc, directoryCheckAt(room._incarnation.hold, expiresAt))
  return room
}

/** A room's guards and after-hooks, typed by its participants' meta. */
type RoomGuardHooks<P extends ParticipantMeta = ParticipantMeta> = {
  onBeforeJoin: JoinGuard<P>
  onAfterJoin: AfterJoinHook<P>
  onBeforeSend: SendGuard<P>
  onAfterSend: AfterSendHook<P>
  onBeforePublish: PublishGuard<P>
  onAfterPublish: AfterPublishHook<P>
}

const ROOM_GUARD_KEYS = Object.keys({
  onBeforeJoin: true,
  onAfterJoin: true,
  onBeforeSend: true,
  onAfterSend: true,
  onBeforePublish: true,
  onAfterPublish: true,
} satisfies Record<keyof RoomGuardHooks, true>) as (keyof RoomGuardHooks)[]

function guardRoom(room: Room, guards: Partial<Record<keyof RoomGuardHooks, unknown>>): void {
  assertUsage(ServerRoom.isServerRoom(room), 'Room.guard() expects a room obtained from Room.get()/Room.create()')
  assertUsage(isObject(guards), 'Room.guard() guards should be an object')
  assertKnownOptions(guards, ROOM_GUARD_KEYS, 'Room.guard()')
  for (const key of ROOM_GUARD_KEYS) {
    assertUsage(
      guards[key] === undefined || typeof guards[key] === 'function',
      `Room.guard() ${key} should be a function`,
    )
  }
  room._setGuards(Object.fromEntries(ROOM_GUARD_KEYS.map((key) => [key, guards[key]])) as Partial<RoomGuardHooks>)
}

async function joinRoom(id: string, options?: JoinOptions): Promise<LocalParticipant> {
  const config = await requireRoom(id)
  const room = new ServerRoom(id, config, await requireRoomMeta(id, config), { count: 0 })
  return await room.join(options)
}

async function listRooms(options?: { prefix?: string }): Promise<RoomInfo[]> {
  assertUsage(
    options === undefined ||
      (isObject(options) && (options.prefix === undefined || typeof options.prefix === 'string')),
    'Room.list() options.prefix should be a string',
  )
  assertKnownOptions(options, ['prefix'], 'Room.list()')
  const backend = getRoomBackend()
  const rooms: RoomInfo[] = []
  let cursor: string | undefined
  do {
    const page = await backend.directoryList(options?.prefix ?? '', cursor)
    cursor = page.cursor
    for (const { roomId, incTag } of page.entries) {
      const head = await backend.readHead(roomId)
      await repairRoomIndex(backend, roomId, incTag, head)
      const config = openConfig(head)
      if (config === null) continue
      // A room that began closing after its head read is no longer listed.
      const count = await reapAndCountPresence({ id: roomId, ...config }).catch(async (error: unknown) => {
        if (isRoomError(error) && openConfig(await backend.readHead(roomId), config.inc) === null) return null
        throw error
      })
      if (count === null) continue
      const stored = await readRoomMeta(roomId, config)
      if (stored === null) continue
      rooms.push({ id: roomId, meta: metaObjectOf(stored.entries), count, isEmpty: count === 0 })
    }
  } while (cursor !== undefined)
  return rooms
}

async function setRoomMeta(id: string, meta: RoomMeta): Promise<void> {
  assertUsage(isRecord(meta), 'Room.setMeta() meta should be an object')
  const owned = ownMetadata(meta)
  const config = await requireRoom(id)
  await writeRoomMeta(id, config, { replace: owned })
}

async function setRoomAttributes(id: string, attributes: RoomMeta): Promise<void> {
  assertUsage(isRecord(attributes), 'Room.setAttributes() attributes should be an object')
  const owned = ownMetadata(attributes)
  const config = await requireRoom(id)
  await writeRoomMeta(id, config, { merge: owned })
}

async function closeRoom(id: string): Promise<void> {
  assertRoomId(id)
  await closeIncarnation(id)
}

async function resolveParticipantRef(incarnation: RoomIncarnation, target: ParticipantRef): Promise<MemberSnapshot[]> {
  assertUsage(isObject(target), 'The participant ref should be { id } or { identity }')
  if ('id' in target) {
    assertUsage(
      typeof target.id === 'string' && target.id.length > 0,
      'The participant { id } should be a non-empty string',
    )
    const members = await reapAndReadMembersById(incarnation, [target.id])
    if (members.length === 0) throw participantGoneError(target.id)
    return members
  }
  assertParticipantIdentity(target.identity, 'The participant ref { identity }')
  return await reapAndResolveIdentity(incarnation, target.identity)
}

async function removeParticipant(id: string, target: ParticipantRef & { reason?: unknown }): Promise<void> {
  const config = await findRoom(id)
  if (config === null) {
    // An `{ identity }` matches nobody in a room that is missing or closed; an `{ id }` names one who isn't there.
    if (!isObject(target) || !('identity' in target)) throw roomNotFoundError(id)
    assertParticipantIdentity(target.identity, 'The participant ref { identity }')
    return
  }
  const incarnation = { id, ...config }
  const members = await resolveParticipantRef(incarnation, target)
  const cause = removedCause(target.reason)
  for (const member of members) await evictMember(incarnation, member.id, member.identity ?? null, cause)
}

async function getRoomParticipants(id: string, target?: { identity: string }): Promise<ParticipantSnapshotView[]> {
  const incarnation = { id, ...(await requireRoom(id)) }
  let members: MemberSnapshot[]
  if (target === undefined) {
    members = (await reapAndReadRoster(incarnation)).members
  } else {
    assertUsage(isObject(target), 'Room.getParticipants() target should be { identity }')
    assertParticipantIdentity(target.identity, 'Room.getParticipants() target identity')
    members = await reapAndResolveIdentity(incarnation, target.identity)
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
  const record = encodeRoomRecord({ __r: 'announce', data } satisfies RoomEnvelope)
  const config = await requireRoom(id)
  const commit = await commitRoomLaneOrThrow(id, config.inc, SEMANTIC_LANE, record)
  return { seq: commit.seq, timestamp: commit.timestamp }
}

async function sendToParticipant(id: string, target: ParticipantRef, data: unknown): Promise<void> {
  const message = ownMessage(data)
  const config = await requireRoom(id)
  const members = await resolveParticipantRef({ id, ...config }, target)
  const exact = 'id' in target
  for (const member of members) {
    const envelope = { __r: 'dm', to: member.id, from: '', fromMeta: null, data: message } as const
    if ('gone' in (await commitToInbox(id, config.inc, envelope)) && exact) throw participantGoneError(member.id)
  }
}

function normalizeRoomOptions(options: RoomOptions | undefined): {
  meta: RoomMeta
  emptyTimeout: number
  departureTimeout: number
} {
  assertUsage(options === undefined || isObject(options), 'Room options should be an object')
  assertKnownOptions(options, ['meta', 'emptyTimeout', 'departureTimeout'], 'Room')
  const meta = options?.meta ?? {}
  assertUsage(isRecord(meta), 'options.meta should be an object')
  const defaults = getServerConfig().room
  const emptyTimeout = options?.emptyTimeout ?? defaults.emptyTimeout
  const departureTimeout = options?.departureTimeout ?? defaults.departureTimeout
  assertRoomTimeout(emptyTimeout, 'options.emptyTimeout')
  assertRoomTimeout(departureTimeout, 'options.departureTimeout')
  return { meta: ownMetadata(meta), emptyTimeout, departureTimeout }
}
