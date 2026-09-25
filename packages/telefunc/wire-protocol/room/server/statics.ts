export { Room }
export type { RoomGuards }

import { assert, assertUsage } from '../../../utils/assert.js'
import { isObject } from '../../../utils/isObject.js'
import { getRoomBackend } from '../../backend/install.js'
import type { RoomBackend, RoomHead } from '../../backend/room/contract.js'
import { RoomError, isRoomError, participantGoneError, roomClosedError } from '../errors.js'
import {
  assertKnownOptions,
  assertParticipantIdentity,
  isRecord,
  mergeAttributes,
  ownMetadata,
  removedCause,
} from '../model.js'
import type { MemberSnapshot, RoomConfigRecord, RoomCtrlEnvelope, RoomDmEnvelope, RoomEnvelope } from '../protocol.js'
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
import { evictMember, presenceCount, readMembersById, readRoster, resolveIdentityMembers } from './membership.js'
import { memberCellKey } from './cells.js'
import {
  CONTROL_LANE,
  SEMANTIC_LANE,
  commitRoomLane,
  commitRoomLaneOrThrow,
  configFromHead,
  openConfig,
  encodeRoomRecord,
  ownMessage,
  publishCtrl,
  staleCommitError,
} from './lanes.js'
import { ServerRoom } from './room.js'
import { CX_CONFLICT, retryCompareExchange } from './cx.js'

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

const ROOM_TOMBSTONE_TTL_MS = 60_000
const ROOM_CLOSE_LEASE_MS = 15_000
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
  const config = openConfig(await getRoomBackend().readHead(id))
  if (config === null) throw new RoomError(`Room not found: ${id}`)
  return config
}

/** A listed incarnation no head names as current has no owner left (its close finished, or was interrupted and its
 *  tombstone lapsed), so it is dropped with its listing. */
async function repairRoomIndex(
  backend: RoomBackend,
  id: string,
  listedInc: string,
  head: RoomHead | null,
): Promise<void> {
  if (head?.currentInc === listedInc) return
  await backend.dropGeneration(id, listedInc)
  const live = openConfig(head)
  if (live) await backend.directoryPut(id, live.inc)
  else await backend.directoryDelete(id, listedInc)
}
type TryCreateRoomResult = { kind: 'created'; room: Room } | { kind: 'exists' } | { kind: 'closing' }

async function tryCreateRoom(id: string, options: RoomOptions | undefined): Promise<TryCreateRoomResult> {
  const { meta } = normalizeOptions(options)
  const backend = getRoomBackend()
  return await retryCompareExchange(id, async () => {
    let current = await backend.readHead(id)
    if (current?.state === 'closing') {
      const closing = await acquireClosingLease(backend, id, current)
      if (closing === null || !(await finishClose(backend, id, closing))) return { kind: 'closing' }
      current = await backend.readHead(id)
    }
    if (current?.state === 'closed') await cleanupFinalizedIncarnation(backend, id, current)
    if (current !== null && current.state !== 'closed') return { kind: 'exists' }
    const created: RoomConfigRecord = {
      meta,
      at: Date.now(),
      by: writerId(),
      inc: crypto.randomUUID(),
    }
    const result = await backend.compareExchangeHead(
      id,
      current === null ? { form: 'absent' } : { form: 'rev', rev: current.rev },
      { head: { currentInc: created.inc, state: 'open', config: encodeRoomRecord(created) } },
    )
    if ('conflict' in result) {
      // A head that went away (a lapsed tombstone) or closed meanwhile still allows the create: try again.
      if (result.current === null || result.current.state === 'closed') return CX_CONFLICT
      return result.current.state === 'closing' ? { kind: 'closing' } : { kind: 'exists' }
    }
    assert('head' in result)
    await backend.directoryPut(id, created.inc)
    return { kind: 'created', room: new ServerRoom(id, created, { members: [] }) }
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
  return new ServerRoom(id, config, { count: await presenceCount(id, config.inc) })
}

async function getOrCreateRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const result = await tryCreateRoom(id, options)
  if (result.kind === 'created') return result.room
  if (result.kind === 'closing') throw new RoomError(`Room is closing: ${id}`)
  const room = await openRoom(id)
  // Repairs the listing of a creator that crashed between its head and directory writes.
  await getRoomBackend().directoryPut(id, room._inc)
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
type RoomGuards = { [K in keyof RoomGuardHooks]: RoomGuardHooks[K] | null }

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
  room._setGuards(Object.fromEntries(ROOM_GUARD_KEYS.map((key) => [key, guards[key] ?? null])) as RoomGuards)
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
      const count = await presenceCount(roomId, config.inc).catch(async (error: unknown) => {
        if (isRoomError(error) && openConfig(await backend.readHead(roomId), config.inc) === null) return null
        throw error
      })
      if (count === null) continue
      rooms.push({ id: roomId, meta: config.meta, count, isEmpty: count === 0 })
    }
  } while (cursor !== undefined)
  return rooms
}

async function setRoomMeta(id: string, meta: RoomMeta): Promise<void> {
  assertUsage(isRecord(meta), 'Room.setMeta() meta should be an object')
  const owned = ownMetadata(meta)
  const config = await requireRoom(id)
  await writeRoomConfig(id, config, () => owned)
}

async function setRoomAttributes(id: string, attributes: RoomMeta): Promise<void> {
  assertUsage(isRecord(attributes), 'Room.setAttributes() attributes should be an object')
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
  const update = await retryCompareExchange(id, async () => {
    const current = await backend.readHead(id)
    const currentConfig = openConfig(current, config.inc)
    if (current === null || currentConfig === null) throw roomClosedError(id)
    const next = { meta: computeMeta(currentConfig.meta), at: Math.max(Date.now(), currentConfig.at + 1), by }
    const result = await backend.compareExchangeHead(
      id,
      { form: 'rev', rev: current.rev },
      { head: { currentInc: config.inc, state: 'open', config: encodeRoomRecord({ ...next, inc: config.inc }) } },
    )
    return 'conflict' in result ? CX_CONFLICT : next
  })
  await publishCtrl(id, config.inc, { __r: 'update', ...update })
}

async function closeRoom(id: string): Promise<void> {
  assertRoomId(id)
  const backend = getRoomBackend()
  for (;;) {
    const current = await backend.readHead(id)
    if (current === null) return
    if (current.state === 'closed') {
      await cleanupFinalizedIncarnation(backend, id, current)
      return
    }
    const closing = await acquireClosingLease(backend, id, current)
    if (closing !== null && (await finishClose(backend, id, closing))) return
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

async function acquireClosingLease(backend: RoomBackend, roomId: string, current: RoomHead): Promise<RoomHead | null> {
  assert(current.currentInc !== null) // only open and closing heads reach here; both name an incarnation
  const closeLease = { id: crypto.randomUUID(), durationMs: ROOM_CLOSE_LEASE_MS }
  const result = await backend.compareExchangeHead(
    roomId,
    current.state === 'open' ? { form: 'rev', rev: current.rev } : { form: 'takeover', rev: current.rev },
    {
      head: {
        currentInc: current.currentInc,
        state: 'closing',
        config: current.config,
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
  const finalized = await backend.compareExchangeHead(
    roomId,
    { form: 'finalize', rev: closing.rev, lease: lease.id },
    {
      head: { currentInc: null, state: 'closed', config: closing.config },
      ttlMs: ROOM_TOMBSTONE_TTL_MS,
    },
  )
  if ('conflict' in finalized) return false
  assert('head' in finalized)
  await cleanupFinalizedIncarnation(backend, roomId, finalized.head)
  return true
}

async function cleanupFinalizedIncarnation(backend: RoomBackend, roomId: string, closed: RoomHead): Promise<void> {
  // The only drop: a closed tombstone's incarnation, which a random `inc` never makes current again.
  assert(closed.state === 'closed' && closed.currentInc === null, 'Dropping the current incarnation')
  const inc = configFromHead(closed).inc
  await backend.dropGeneration(roomId, inc)
  await backend.directoryDelete(roomId, inc)
}

async function resolveParticipantRef(roomId: string, inc: string, target: ParticipantRef): Promise<MemberSnapshot[]> {
  assertUsage(isObject(target), 'The participant ref should be { id } or { identity }')
  if ('id' in target) {
    assertUsage(
      typeof target.id === 'string' && target.id.length > 0,
      'The participant { id } should be a non-empty string',
    )
    const members = await readMembersById(roomId, inc, [target.id])
    if (members.length === 0) throw participantGoneError(target.id)
    return members
  }
  assertParticipantIdentity(target.identity, 'The participant ref { identity }')
  return await resolveIdentityMembers(roomId, inc, target.identity)
}

async function removeParticipant(id: string, target: ParticipantRef & { reason?: unknown }): Promise<void> {
  const config = await requireRoom(id)
  const members = await resolveParticipantRef(id, config.inc, target)
  const cause = removedCause(target.reason)
  for (const member of members) await evictMember(id, config.inc, member.id, member.identity ?? null, cause)
}

async function getRoomParticipants(id: string, target?: { identity: string }): Promise<ParticipantSnapshotView[]> {
  const config = await requireRoom(id)
  let members: MemberSnapshot[]
  if (target === undefined) {
    members = (await readRoster(id, config.inc)).members
  } else {
    assertUsage(isObject(target), 'Room.getParticipants() target should be { identity }')
    assertParticipantIdentity(target.identity, 'Room.getParticipants() target identity')
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
  const record = encodeRoomRecord({ __r: 'announce', data } satisfies RoomEnvelope)
  const config = await requireRoom(id)
  const commit = await commitRoomLaneOrThrow(id, config.inc, SEMANTIC_LANE, record)
  return { seq: commit.seq, timestamp: commit.timestamp }
}

async function sendToParticipant(id: string, target: ParticipantRef, data: unknown): Promise<void> {
  const message = ownMessage(data)
  const config = await requireRoom(id)
  const members = await resolveParticipantRef(id, config.inc, target)
  const exact = 'id' in target
  for (const member of members) {
    if (!(await sendServerDm(id, config.inc, member.id, message)) && exact) throw participantGoneError(member.id)
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
  assertKnownOptions(options, ['meta'], 'Room')
  const meta = options?.meta ?? {}
  assertUsage(isRecord(meta), 'options.meta should be an object')
  return { meta: ownMetadata(meta) }
}
