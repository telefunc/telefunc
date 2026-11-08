export { roomStatics }
export type { RoomGuardHooks, RoomGuards, RoomStatic }

import { assertUsage } from '../utils/assert.js'
import { isObject } from 'telefunc/__internal'
import { getRoomPlane } from '../backend/plane.js'
import { DIRECTORY_PAGE_SIZE } from '../backend/contract.js'
import type { RoomPlane } from '../backend/RoomPlane.js'
import { RoomError, isRoomError, messageLostError, participantGoneError, roomClosedError } from '../shared/errors.js'
import {
  assertKnownOptions,
  assertParticipantIdentity,
  isRecord,
  ownMessage,
  ownMetadata,
  removedCause,
} from '../shared/arguments.js'
import type { MemberSnapshot, RoomEnvelope } from '../shared/protocol.js'
import type { RoomConfigRecord, RoomIncarnation } from './protocol.js'
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
  Room,
  RoomGetOptions,
  RoomInfo,
  RoomMeta,
  RoomOptions,
  RoomSendReceipt,
  SendGuard,
} from '../types.js'
import { memberCellKey } from './store/cells.js'
import { tryCreateRoom } from './lifecycle/create.js'
import { eachInFlight } from './store/cellStore.js'
import { evictMember } from './store/members.js'
import {
  reapAndCountPresence,
  reapAndReadMembersById,
  reapAndReadRoster,
  reapAndResolveIdentity,
} from './store/roster.js'
import {
  SEMANTIC_LANE,
  acceptedOrThrow,
  commitLane,
  configFromHead,
  inboxOf,
  openConfig,
  encodeRoomRecord,
} from './lanes/lanes.js'
import { ServerRoom, handleOf } from './room.js'
import { RoomReplica } from './RoomReplica.js'
import { readRoomMeta, writeRoomMeta, type StoredRoomMeta } from './store/meta.js'
import { metaObject } from '../shared/RoomMetaState.js'
import { closeIncarnation } from './lifecycle/close.js'
import { cancelClose } from './lifecycle/hold.js'
import { repairRoomIndex } from './store/directory.js'
import { ROOM_GUARD_USAGE } from './constants.js'

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
    options?: RoomOptions<M> & RoomGetOptions,
  ): Promise<Room<M, P, Pub>>
  guard<M extends RoomMeta, P extends ParticipantMeta, Pub = unknown>(
    room: Room<M, P, Pub>,
    guards: RoomGuards<P>,
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

const roomStatics: RoomStatic = {
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

/** The room's incarnation and config, or null if the room is missing or closed. */
async function findRoom(id: string): Promise<(RoomIncarnation & RoomConfigRecord) | null> {
  assertRoomId(id)
  const config = openConfig(await getRoomPlane().driver.readHead(id))
  return config === null ? null : { id, ...config }
}

async function requireRoom(id: string): Promise<RoomIncarnation & RoomConfigRecord> {
  const room = await findRoom(id)
  if (room === null) throw roomNotFoundError(id)
  return room
}

function roomNotFoundError(id: string): RoomError {
  return new RoomError(`Room not found: ${id}`)
}

async function createRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const result = await tryCreateRoom(id, options)
  if (result.kind === 'closing') throw new RoomError(`Room is closing: ${id}`)
  if (result.kind !== 'created') throw new RoomError(`Room already exists: ${id}`)
  return result.room
}

async function getRoom(id: string, options?: RoomGetOptions): Promise<Room> {
  assertKnownOptions(options, ['tail', 'whenBehind'], 'Room.get()')
  assertWhenBehind(options?.whenBehind, 'Room.get()')
  const room = await openRoom(await requireRoom(id))
  await applyGetOptions(room, options)
  return room
}

async function applyGetOptions(
  room: ServerRoom,
  { tail, whenBehind = 'missOldest' }: RoomGetOptions = {},
): Promise<void> {
  handleOf(room).whenBehind = whenBehind
  if (tail === true) await handleOf(room).startTail()
}

function assertWhenBehind(whenBehind: unknown, api: string): void {
  assertUsage(
    whenBehind === undefined || whenBehind === 'missOldest' || whenBehind === 'close',
    `${api}'s whenBehind should be 'missOldest' or 'close'`,
  )
}

/** A handle on this process's view of the room: a held view catches up unless it is current; a new one reads the member
 *  count and the meta. */
async function openRoom(incarnation: RoomIncarnation & RoomConfigRecord): Promise<ServerRoom> {
  const { id } = incarnation
  const held = RoomReplica.of(incarnation.inc)
  if (held !== undefined) return await held.sharedHandle()
  const [count, meta] = await Promise.all([reapAndCountPresence(incarnation), requireRoomMeta(id, incarnation)])
  const replica = RoomReplica.seeded(id, incarnation, meta, { count })
  return replica.handle()
}

async function requireRoomMeta(id: string, config: RoomConfigRecord): Promise<StoredRoomMeta> {
  const stored = await readRoomMeta(id, config)
  if (stored === null) throw roomClosedError(id)
  return stored
}

async function getOrCreateRoom(id: string, options?: RoomOptions & RoomGetOptions): Promise<Room> {
  assertRoomId(id)
  const { tail, whenBehind, ...roomOptions } = options ?? {}
  assertWhenBehind(whenBehind, 'Room.getOrCreate()')
  const result = await tryCreateRoom(id, roomOptions)
  if (result.kind === 'closing') throw new RoomError(`Room is closing: ${id}`)
  const room = result.kind === 'created' ? result.room : await openRoom({ id, ...configFromHead(result.head) })
  await applyGetOptions(room, { tail, whenBehind })
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

/** What `Room.guard()` takes: the hooks, and `anyoneCanJoin`, which lets any page join without an `onBeforeJoin`. */
type RoomGuards<P extends ParticipantMeta = ParticipantMeta> = Partial<RoomGuardHooks<P>> & { anyoneCanJoin?: boolean }

const ROOM_GUARD_KEYS = Object.keys({
  onBeforeJoin: true,
  onAfterJoin: true,
  onBeforeSend: true,
  onAfterSend: true,
  onBeforePublish: true,
  onAfterPublish: true,
} satisfies Record<keyof RoomGuardHooks, true>) as (keyof RoomGuardHooks)[]

function guardRoom(
  room: Room,
  guards: Partial<Record<keyof RoomGuardHooks, unknown>> & { anyoneCanJoin?: unknown },
): void {
  assertUsage(ServerRoom.isServerRoom(room), ROOM_GUARD_USAGE)
  assertUsage(isObject(guards), 'Room.guard() guards should be an object')
  assertKnownOptions(guards, [...ROOM_GUARD_KEYS, 'anyoneCanJoin'], 'Room.guard()')
  const { anyoneCanJoin } = guards
  assertUsage(
    anyoneCanJoin === undefined || typeof anyoneCanJoin === 'boolean',
    'Room.guard() anyoneCanJoin should be a boolean',
  )
  for (const key of ROOM_GUARD_KEYS) {
    assertUsage(
      guards[key] === undefined || typeof guards[key] === 'function',
      `Room.guard() ${key} should be a function`,
    )
  }
  const hooks = Object.fromEntries(ROOM_GUARD_KEYS.map((key) => [key, guards[key]])) as Partial<RoomGuardHooks>
  handleOf(room).setGuards({ ...hooks, anyoneCanJoin: anyoneCanJoin === true })
}

async function joinRoom(id: string, options?: JoinOptions): Promise<LocalParticipant> {
  const config = await requireRoom(id)
  // A view made for a join alone counts no member until a handle reads its roster.
  const replica =
    RoomReplica.of(config.inc) ?? RoomReplica.seeded(id, config, await requireRoomMeta(id, config), { count: 0 })
  return await handleOf(replica.handle()).join(options)
}

async function listRooms(options?: { prefix?: string }): Promise<RoomInfo[]> {
  assertUsage(
    options === undefined ||
      (isObject(options) && (options.prefix === undefined || typeof options.prefix === 'string')),
    'Room.list() options.prefix should be a string',
  )
  assertKnownOptions(options, ['prefix'], 'Room.list()')
  const plane = getRoomPlane()
  const rooms: RoomInfo[] = []
  let cursor: string | undefined
  do {
    const page = await plane.driver.directoryList(options?.prefix ?? '', cursor)
    cursor = page.cursor
    // `DIRECTORY_PAGE_SIZE` reads at once, each at its room's authority: a Cloudflare page holds a page per region.
    for (let at = 0; at < page.entries.length; at += DIRECTORY_PAGE_SIZE) {
      const chunk = page.entries.slice(at, at + DIRECTORY_PAGE_SIZE)
      const listed = await Promise.all(chunk.map(({ roomId, incTag }) => listedRoom(plane, roomId, incTag)))
      for (const info of listed) if (info !== null) rooms.push(info)
    }
  } while (cursor !== undefined)
  return rooms
}

/** A directory entry as `Room.list()` lists it, after repairing its listing; null once the room isn't open. */
async function listedRoom(plane: RoomPlane, roomId: string, incTag: string): Promise<RoomInfo | null> {
  const head = await plane.driver.readHead(roomId)
  await repairRoomIndex(plane, roomId, incTag, head)
  const config = openConfig(head)
  if (config === null) return null
  const [count, stored] = await Promise.all([countListedRoom(plane, roomId, config), readRoomMeta(roomId, config)])
  if (count === null || stored === null) return null
  return { id: roomId, meta: metaObject(stored.entries), count, isEmpty: count === 0 }
}

/** Null once the room began closing after its head read, as it is then no longer listed. */
async function countListedRoom(plane: RoomPlane, roomId: string, config: RoomConfigRecord): Promise<number | null> {
  try {
    return await reapAndCountPresence({ id: roomId, ...config })
  } catch (error) {
    if (isRoomError(error) && openConfig(await plane.driver.readHead(roomId), config.inc) === null) return null
    throw error
  }
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
  const closed = await closeIncarnation(id)
  if (closed !== null) cancelClose(id, closed)
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
  const incarnation = await findRoom(id)
  if (incarnation === null) {
    // An `{ identity }` matches nobody in a room that is missing or closed; an `{ id }` names one who isn't there.
    if (!isObject(target) || !('identity' in target)) throw roomNotFoundError(id)
    assertParticipantIdentity(target.identity, 'The participant ref { identity }')
    return
  }
  const members = await resolveParticipantRef(incarnation, target)
  const cause = removedCause(target.reason)
  await eachInFlight(members, (member) => evictMember(incarnation, member.id, cause))
}

async function getRoomParticipants(id: string, target?: { identity: string }): Promise<ParticipantSnapshotView[]> {
  const incarnation = await requireRoom(id)
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
      isConnected: member.disconnected !== true,
    }))
}

async function announceToRoom(id: string, data: unknown): Promise<RoomSendReceipt> {
  const record = encodeRoomRecord({ __r: 'announce', data } satisfies RoomEnvelope)
  const config = await requireRoom(id)
  const commit = acceptedOrThrow(id, await commitLane({ roomId: id, inc: config.inc, lane: SEMANTIC_LANE }, record))
  return { seq: commit.seq, timestamp: commit.timestamp }
}

async function sendToParticipant(id: string, target: ParticipantRef, data: unknown): Promise<void> {
  const message = ownMessage(data)
  const incarnation = await requireRoom(id)
  const members = await resolveParticipantRef(incarnation, target)
  const exact = 'id' in target
  await eachInFlight(members, (member) => sendDm(id, incarnation.inc, member.id, message, exact))
}

async function sendDm(id: string, inc: string, member: string, message: unknown, exact: boolean): Promise<void> {
  const envelope = { __r: 'dm', to: member, from: '', fromMeta: null, data: message } as const
  const requiredCellKeys = [memberCellKey(member)]
  const receipt = await commitLane(inboxOf(id, inc, member), encodeRoomRecord(envelope), { requiredCellKeys })
  if (receipt.kind === 'stale-incarnation') throw roomClosedError(id)
  if (receipt.kind === 'gone' && exact) throw participantGoneError(member)
  if (receipt.kind === 'accepted' && receipt.lost !== undefined) throw messageLostError()
}
