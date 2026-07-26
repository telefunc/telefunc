export { Room, ServerRoom, ServerLocalParticipant, reportRoomError }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { ShieldValidationError } from '../../shared/ShieldValidationError.js'
import type { ShieldValidator } from '../../node/server/shield.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'
import { assert, assertUsage } from '../../utils/assert.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { isObject } from '../../utils/isObject.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { makePublishInfo, type ChannelPublishAck, type ChannelPublishInfo } from '../channel.js'
import {
  ROOM_DM_ACK_TIMEOUT_MS,
  ROOM_HEARTBEAT_INTERVAL_MS,
  ROOM_ID_MAX_BYTES,
  ROOM_MEMBER_KV_TTL_MS,
  ROOM_MEMBER_TTL_MS,
  ROOM_TAIL_ATTACH_TIMEOUT_MS,
  ROOM_TRACKS_PER_MEMBER_MAX,
} from '../constants.js'
import { getRoomBackend } from '../backend/install.js'
import type {
  CellMutation,
  CommitAccepted,
  LaneId,
  LaneSubscription,
  RoomBackendSpi,
  RoomHead,
} from '../backend/spi.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../shared-ws.js'
import {
  RoomError,
  roomFailureError,
  leaveCauseFromWire,
  leaveCauseToWire,
  frameWithMemberId,
  binaryFrameSender,
  hasRoomTag,
  mergeAttributes,
  normalizeJoinOptions,
  roomMemberKvKey,
  roomMemberKvPrefix,
  roomHiddenMemberKvKey,
  roomHiddenMemberKvPrefix,
  roomIdentityMemberKvKey,
  roomIdentityKvPrefix,
  unframeMemberId,
  uuidToBytes,
  DEFAULT_TRACK,
  emptyTrackWants,
  mergeTrackWants,
  binaryWantsCovers,
  sanitizeBinaryWants,
  pushBoundedTail,
  wantsAnyBinary,
  type BinaryWants,
  type MemberWants,
  type TrackWants,
  type MemberSnapshot,
  type RoomConfigRecord,
  type RoomCtrlEnvelope,
  type RoomDataEnvelope,
  type RoomOrder,
  type RoomDataPublish,
  type RoomDmEnvelope,
  type RoomDmAckEnvelope,
  type DmReply,
  type RoomEnvelope,
  type RoomMemberRecord,
  type RoomStubRequest,
} from './protocol.js'
import { RoomState } from './state.js'
import { RoomDemand } from './demand.js'
import { ParticipantBase, type InboxMessage } from './participant.js'
import type { RoomStubChannel } from './stubs.js'
import type {
  BinaryPublishOptions,
  RoomBinaryListener,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  ParticipantRef,
  ParticipantSnapshotView,
  PublishOptions,
  RemoteParticipant,
  Room as RoomInstance,
  RoomInfo,
  RoomMeta,
  RoomOptions,
  RoomGetOptions,
  RoomSendReceipt,
  RoomAckReceipt,
  RoomSnapshotView,
  JoinGuard,
  PublishGuard,
  SendGuard,
  AfterJoinHook,
  AfterPublishHook,
  AfterSendHook,
  Sender,
} from './types.js'
assertIsNotBrowser()

/** This process's identity as an LWW writer — breaks `Room.setMeta()` timestamp ties.
 *  Minted lazily: Cloudflare Workers forbid crypto RNG in module scope (this module loads at
 *  worker startup via the serializer registry), and inside a request it's always available. */
let _writerId: string | undefined
function writerId(): string {
  _writerId ??= crypto.randomUUID()
  return _writerId
}

/** `Room` is one identifier with two meanings, like the built-in `Date`: the statics object
 *  below (value) and the instance type from ./types.js — re-established locally so the two
 *  merge into a single export. */
type Room<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = RoomInstance<
  M,
  P,
  Pub
>

// ---------------------------------------------------------------------------
// `Room` entry point
// ---------------------------------------------------------------------------

/** Meta type parameters are caller assertions (like `querySelector<T>`): metadata is data, so
 *  the types you pass declare what your app stores — nothing re-validates them at runtime. */
type RoomStatic = {
  /** Create a new room. Throws if it already exists. `Pub` (3rd arg) types what members
   *  `publish()`/`subscribe()` here — omit it for `unknown`. */
  create<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: RoomOptions<M>,
  ): Promise<Room<M, P, Pub>>
  /** Get an existing room. Throws if it doesn't exist. Pass `{ tail: true }` to start relaying
   *  live messages at serialization time so a history read in the same telefunction misses
   *  nothing (see `RoomGetOptions`). */
  get<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: RoomGetOptions,
  ): Promise<Room<M, P, Pub>>
  /** Get the room, creating it if it doesn't exist. Concurrent callers converge: one creates,
   *  the others get. `options` apply only when this call is the one that creates. */
  getOrCreate<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: RoomOptions<M>,
  ): Promise<Room<M, P, Pub>>
  /** Guard every membership granted through `room` — server-side and client-side `join()`s alike.
   *  Each operation has a pre-commit guard (`onBefore*`, throw to reject the caller before anything
   *  is written) and a post-commit hook (`onAfter*`, runs once the operation lands, with its
   *  receipt): `onBeforeJoin`/`onAfterJoin` around admission, `onBeforeSend`/`onAfterSend` around a
   *  private `send()`, `onBeforePublish`/`onAfterPublish` around each `publish()`/`publishBinary()`.
   *  Persist in `onAfterPublish` — its receipt carries the authoritative `seq`/`timestamp`. Declared
   *  in the granting telefunction (close over `getContext()`); one `Room.guard()` per instance. */
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
  /** Shorthand for `(await Room.get(id)).join(options)`. */
  join<P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    options?: JoinOptions<P>,
  ): Promise<LocalParticipant<P, Pub>>
  /** List all rooms — optionally only those whose ID starts with `prefix`. `M` types the returned
   *  `meta` (`Room.list<MatchMeta>()`), replacing a `r.meta as MatchMeta` cast at the call site. */
  list<M extends RoomMeta = RoomMeta>(options?: { prefix?: string }): Promise<RoomInfo<M>[]>
  /** Admin: replace the room's metadata wholesale. The room-level counterpart to
   *  `LocalParticipant.setMeta`. */
  setMeta(id: string, meta: RoomMeta): Promise<void>
  /** Admin: merge the room's metadata per key — provided keys replace, omitted keys keep their
   *  value, a key set to `undefined` is removed. The room-level counterpart to
   *  `LocalParticipant.setAttributes`. */
  setAttributes(id: string, attributes: RoomMeta): Promise<void>
  /** Admin: close the room — disconnects all participants and removes the room. */
  close(id: string): Promise<void>
  /** Admin: remove a participant — one membership by `{ id }` (throws when unknown), or every
   *  membership of an app identity at once (`{ identity }`, an idempotent sweep: kicking a user removes
   *  all their tabs/connections, and 0 matches is fine). `reason` rides in the same descriptor and
   *  travels with the removal — the kicked participant's `onLeave` receives `{ type: 'removed', reason }`,
   *  so "why" never races the removal. */
  removeParticipant(id: string, target: ParticipantRef & { reason?: unknown }): Promise<void>
  /** Publish a room-authored message — no sender, delivered to `onAnnounce()` (e.g. system notices).
   *  Returns the message's room-wide order (`{ seq, timestamp }`), the same clock `publish()` stamps. */
  announce(id: string, data: unknown): Promise<RoomSendReceipt>
  /** Send a server-authored private message — arrives on `listen()` with `from: null`. Target one
   *  participant by `{ id }` (throws when unknown), or every membership of an app identity at once
   *  (`{ identity }`, resolved from the identity index; 0 matches is a no-op — a signed-out user). */
  send(id: string, target: ParticipantRef, data: unknown): Promise<void>
  /** Server-side snapshot of a room's participants — a point-in-time read with no live view or
   *  subscription (unlike the instance `room.getParticipants()`). Omit `target` for the whole roster;
   *  pass `{ identity }` to read one app identity's memberships (its open tabs/connections) in
   *  O(memberships) via the identity index — the cheap "is this user present / what's their status"
   *  read that doesn't load the roster. Returns `[]` for an absent identity. */
  getParticipants<P extends ParticipantMeta = ParticipantMeta>(
    id: string,
    target?: { identity: string },
  ): Promise<ParticipantSnapshotView<P>[]>
}

/**
 * Multi-party rooms with presence, membership, and admin controls. Server-side entry point —
 * clients receive `Room` and `LocalParticipant` objects by returning them from telefunctions.
 *
 * ```ts
 * import { Room } from 'telefunc'
 *
 * await Room.create('lobby', { meta: { topic: 'general' } })
 * const lobby = await Room.get('lobby')
 * const me = await lobby.join({ meta: { name: 'Alice' } })
 * await me.publish({ text: 'hello' })
 * ```
 */
// The generic signatures are caller assertions over the runtime-typed implementations —
// same relationship as `document.querySelector<T>` to its untyped DOM lookup.
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

/** How long a `closed` tombstone lingers before it's reaped. It marks the id closed for the window so
 *  a create that races or follows a close takes it over cleanly with a fresh incarnation id; once it
 *  lapses, the id is simply absent and a fresh create starts clean. */
const ROOM_TOMBSTONE_TTL_MS = 60_000

/** How long a fenced `closing` config is leased before its expiry reaps it. Bounds an abandoned close
 *  — a node that flipped open→closing then died before finalizing — so once the lease lapses the stuck
 *  `closing` config vanishes and the id is recreatable, rather than wedged forever. A normal close
 *  finalizes to the `closed` tombstone well within it; the fresh incarnation ignores any leftovers the
 *  abandoned sweep left behind (they carry the old id and TTL-reap on their own). */
const ROOM_CLOSE_LEASE_MS = 15_000
const ROOM_CX_ATTEMPTS = 16

/** The retained value stored per (member, track) binary lane: the base64 frame plus the publish
 *  receipt it was assigned, so a late subscriber replays it in the lane's real order (never a fresh
 *  `seq:0`/`Date.now()` stamp). */
const roomTextEncoder = new TextEncoder()
const roomTextDecoder = new TextDecoder()
const SEMANTIC_LANE = { kind: 'semantic' } as const satisfies LaneId
const CONTROL_LANE = { kind: 'control' } as const satisfies LaneId

function encodeRoomText(value: string): Uint8Array {
  return roomTextEncoder.encode(value)
}

function decodeRoomText(value: Uint8Array): string {
  return roomTextDecoder.decode(value)
}

function encodeRoomConfig(config: RoomConfigRecord): Uint8Array {
  return encodeRoomText(stringify(config))
}

function configFromHead(head: RoomHead): RoomConfigRecord {
  const stored = parse(decodeRoomText(head.config)) as RoomConfigRecord
  return {
    ...stored,
    status: head.state,
    ...(head.currentInc === null ? {} : { inc: head.currentInc }),
  }
}

/** (Re)register a room in the backend's cross-room directory. Idempotent, so it also repairs a create
 *  that wrote the authoritative head but stopped before registering the incarnation. */
async function registerRoomIndex(id: string, inc: string): Promise<void> {
  await getRoomBackend().directoryPut(id, inc)
}

/** Commit one opaque lane payload under the incarnation fence, then await its single handoff attempt. */
async function commitRoomLane(
  id: string,
  inc: string,
  lane: LaneId,
  payload: Uint8Array,
  opts?: { retain?: boolean; closingLease?: string },
): Promise<CommitAccepted | null> {
  const result = await getRoomBackend().commitLane(id, inc, lane, payload, opts)
  if ('stale' in result) return null
  await result.delivery
  return result
}

/** Atomically create a room, returning the fresh room if this call won the create, or `null` if a
 *  live room already owns the id. One authority compare-and-set decides it: a fresh id (or a `closed`
 *  tombstone left by an earlier incarnation) is taken at the next generation; an `open`/`closing`
 *  record means the create lost. Race-free — exactly one of any number of concurrent callers writes. */
async function tryCreateRoom(id: string, options: RoomOptions | undefined): Promise<Room | null> {
  const { meta } = normalizeOptions(options)
  const backend = getRoomBackend()
  let current = await backend.readHead(id)
  if (current?.head.state === 'closing') {
    const closing = await acquireClosingLease(backend, id, current.head)
    if (closing === null || !(await finishClose(backend, id, closing))) return null
    current = await backend.readHead(id)
  }
  if (current !== null && current.head.state !== 'closed') return null
  const created: RoomConfigRecord = {
    meta,
    at: Date.now(),
    by: writerId(),
    inc: crypto.randomUUID(),
    status: 'open',
  }
  const result = await backend.compareExchangeHead(
    id,
    current === null ? { expect: 'absent' } : { expect: { rev: current.head.rev } },
    { head: { currentInc: created.inc, state: 'open', config: encodeRoomConfig(created) } },
  )
  if ('conflict' in result) return null
  assert('head' in result)
  await registerRoomIndex(id, created.inc)
  const room = new ServerRoom(id, created, { members: [] }) // fresh room — the roster is known: empty
  return room
}

async function createRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const room = await tryCreateRoom(id, options)
  if (room === null) throw new RoomError(`Room already exists: ${id}`)
  return room
}

async function getRoom(id: string, options?: RoomGetOptions): Promise<Room> {
  const { config } = await requireRoom(id)
  // Scan-only count — no per-member reads: the roster itself loads lazily, on the first observation
  // that needs it. Hidden members are excluded via their marker index (see presenceCount).
  const count = await presenceCount(id, config.inc)
  const room = new ServerRoom(id, config, { count })
  if (options?.tail === true) room._startTail()
  return room
}

async function getOrCreateRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  // Won the atomic create → the fresh room. Lost it (already exists) → load it and idempotently
  // repair the directory index, in case an earlier create wrote the config but crashed before the
  // separate index write left it unlistable.
  const created = await tryCreateRoom(id, options)
  if (created !== null) return created
  const room = await getRoom(id)
  assert(ServerRoom.isServerRoom(room))
  await registerRoomIndex(id, room._inc)
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

/** The resolved guard/hook set an instance holds — each slot `null` when not declared. */
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
  // A pure joiner only wants its own participant handle — it never reads the roster, so it
  // skips even the count scan `Room.get()` pays: config read, join, done.
  const { config } = await requireRoom(id)
  return await new ServerRoom(id, config, { count: 0 }).join(options)
}

async function listRooms(options?: { prefix?: string }): Promise<RoomInfo[]> {
  assertUsage(
    options === undefined ||
      (isObject(options) && (options.prefix === undefined || typeof options.prefix === 'string')),
    'Room.list() options.prefix should be a string',
  )
  // The room enumeration is cross-room, so it reads from the unscoped (listable) store; each room's
  // config and count then read from that room's own authority.
  const backend = getRoomBackend()
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
  const { config } = await requireRoom(id)
  await writeRoomConfig(id, config, () => meta)
}

/** Merge into the room's metadata per key — provided keys replace, omitted keys keep their value,
 *  a key set to `undefined` is removed. The admin counterpart to `LocalParticipant.setAttributes`:
 *  one changed field is one small write, not a whole-`meta` resend. */
async function setRoomAttributes(id: string, attributes: RoomMeta): Promise<void> {
  assertUsage(isObject(attributes), 'Room.setAttributes() attributes should be an object')
  const { config } = await requireRoom(id)
  // The merge is a mutator, not a value: `writeRoomConfig` runs it inside the compare-and-set against
  // the config actually present, so a concurrent room `setAttributes` merges onto the latest, not the
  // snapshot read above — no lost key on a race (mirrors `_writeMemberMeta`).
  await writeRoomConfig(id, config, (current) => mergeAttributes(current, attributes))
}

/** Commit a room-config change and converge it. Both the new value (`computeMeta` — a replace for
 *  `setMeta()`, a per-key merge for `setAttributes()`) and its hybrid-clock stamp are derived inside
 *  the compare-and-set, against the config actually present — mirroring `_writeMemberMeta`. A concurrent
 *  write that landed first re-runs the mutator on that fresh config, so a merge never loses a key and the
 *  stamp is always strictly after what it overwrites (the write lands, never dropped for staleness). One
 *  writer's back-to-back writes order; cross-writer ties break wall-clock last-writer-wins. `prev` isn't
 *  shipped — each node derives its own local pre-value when it applies the `update` (see `applyRoomUpdate`). */
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
    const nextConfig = { meta, at, by, inc: config.inc, status: 'open' as const }
    const result = await backend.compareExchangeHead(
      id,
      { expect: { rev: current.head.rev } },
      {
        head: { currentInc: config.inc, state: 'open', config: encodeRoomConfig(nextConfig) },
      },
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
  const current = await backend.readHead(id)
  if (current === null || current.head.state === 'closed') return
  const closing = await acquireClosingLease(backend, id, current.head)
  if (closing === null) return
  await finishClose(backend, id, closing)
}

async function acquireClosingLease(
  backend: RoomBackendSpi,
  roomId: string,
  current: RoomHead,
): Promise<RoomHead | null> {
  if (current.currentInc === null) return null
  const nextConfig = { ...configFromHead(current), status: 'closing' as const }
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
        config: encodeRoomConfig(nextConfig),
        closeLease,
      },
    },
  )
  return 'conflict' in result || !('head' in result) ? null : result.head
}

async function finishClose(backend: RoomBackendSpi, roomId: string, closing: RoomHead): Promise<boolean> {
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

  const config = { ...configFromHead(closing), status: 'closed' as const }
  const finalized = await backend.compareExchangeHead(
    roomId,
    { expect: { rev: closing.rev, closingLease: lease.id } },
    {
      head: { currentInc: null, state: 'closed', config: encodeRoomConfig(config) },
      ttlMs: ROOM_TOMBSTONE_TTL_MS,
    },
  )
  if ('conflict' in finalized) return false
  await backend.dropGeneration(roomId, inc)
  await backend.directoryDelete(roomId, inc)
  return true
}

/** The `(memberId, identity)` pairs a `ParticipantRef` addresses — shared by `Room.send()` and
 *  `Room.removeParticipant()`. `{ id }` is one membership and must exist (throws otherwise); `{ identity }`
 *  is every membership of that identity, resolved from the identity index in O(memberships) rather than a
 *  full-roster scan (0 matches is fine — an idempotent sweep, a no-op DM). */
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

/** Server-side snapshot read of a room's participants (`Room.getParticipants`) — no live view, no
 *  subscription, unlike the instance `room.getParticipants()`. Omit `target` for the whole roster;
 *  pass `{ identity }` to read one identity's memberships in O(memberships) via the identity index —
 *  the cheap "is this user present / what's their status" read without loading the roster. */
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
    .filter((m) => !m.hidden) // hidden participants aren't presence participants
    .map((m) => ({ id: m.id, identity: m.identity ?? null, meta: m.meta, joinedAt: m.joinedAt }))
}

async function announceToRoom(id: string, data: unknown): Promise<RoomSendReceipt> {
  const { config } = await requireRoom(id)
  // One commit on the room's semantic lane, the same one `publish()` draws, so an announcement is
  // ordered relative to participant text and reaches every observer.
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
  // `{ id }` is one participant; `{ identity }` fans out to every membership (tabs, connections).
  for (const { memberId } of await resolveParticipantRef(id, config.inc, target)) {
    await sendServerDm(id, config.inc, memberId, data)
  }
}

/** Publish a server-authored DM to one member's inbox. An empty `from` marks it server-authored —
 *  clients can't spoof it (their DMs are validated against members joined through their own stub). */
async function sendServerDm(roomId: string, inc: string, memberId: string, data: unknown): Promise<void> {
  const envelope: RoomDmEnvelope = { __r: 'dm', to: memberId, from: '', fromMeta: null, data }
  const committed = await commitRoomLane(
    roomId,
    inc,
    { kind: 'inbox', member: memberId },
    encodeRoomText(stringify(envelope)),
  )
  if (committed === null) throw new RoomError(`Room is closed: ${roomId}`)
}

// ---------------------------------------------------------------------------
// ServerRoom
// ---------------------------------------------------------------------------

const SERVER_ROOM_BRAND: unique symbol = Symbol.for('telefunc.ServerRoom')

/**
 * Server-side `Room`.
 *
 * Not a channel itself: serializing a `ServerRoom` attaches a fresh `RoomStubChannel` per
 * response (see `roomReplacer`), so the same instance can be serialized any number of times
 * and `room.id` stays the room ID (wire channel IDs must be globally unique).
 *
 * State changes flow through backend lanes: the node causing a change applies it locally and commits
 * it; every observing node — including the committer's own echo — applies the subscribed payload.
 * Application is idempotent, so the overlap is harmless.
 */
class ServerRoom implements Room {
  readonly [SERVER_ROOM_BRAND] = true
  /** Phantom: the publish shield rides the type only (see `RoomShield`), never a runtime field. */
  declare readonly [TELEFUNC_SHIELDS]: { data: unknown }

  /** @internal — the room incarnation id (`RoomConfigRecord.inc`) this handle is bound to. Every
   *  authority-side legality check (join, mutate, close) and every member record this handle writes
   *  carries it, so an operation from a previous incarnation is rejected after a close/recreate. */
  readonly _inc: string
  /** @internal — tail mode (`Room.get(id, { tail: true })`): this node ingests and holds the room's
   *  text from the moment of `Room.get`, so a history read done before the room is serialized misses
   *  no live message. Cleared when a stub attaches (the hold moves onto the stub, which keeps it until
   *  the client's first `subscribe()`) or when the safety timer tears an unserialized tail down. */
  _tail = false
  /** Text held between `Room.get({ tail })` and the stub attaching; handed to the stub on attach (see
   *  `_attachStub`). Bounded drop-oldest, so a fetched-with-tail room that is never serialized (misuse)
   *  can't grow it without limit. */
  private readonly _tailHold: Array<{ serialized: string; ord: RoomOrder; from: string }> = []
  private _tailTimer: ReturnType<typeof setTimeout> | null = null
  /** In-flight `send(…, { ack: true })`s awaiting the recipient's reply, keyed by `ackId`. `to` is
   *  the recipient, so a leave/close can fail the ones it strands. Empty at steady state. */
  private readonly _pendingDmAcks = new Map<string, { to: string; settle: (reply: DmReply) => void }>()
  private _guards: RoomGuards | null = null
  /** @internal */ readonly _state: RoomState
  private readonly _stubs = new Set<RoomStubChannel>()
  private readonly _localParticipants = new Map<string, ServerLocalParticipant>()

  private readonly _ctrlSub = new SubSlot()
  private readonly _textSub = new SubSlot()
  /** Upstream subscriptions keyed by their policy identity. */
  private readonly _binaryKeyUnsubs = new Map<string, LaneSubscription>()
  private readonly _dmUnsubs = new Map<string, LaneSubscription>()
  /** (member, track) pairs this instance has already announced — first publish pays the
   *  KV append + ctrl event, every further frame is a Set lookup. */
  private readonly _announcedTracks = new Map<string, Set<string>>()
  /** Cross-node binary-demand aggregation (`onDemand`) — constructed once `roomId` and the
   *  ownership/delivery callbacks are available (see the constructor). */
  private readonly _demand: RoomDemand
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private _heartbeatBusy = false
  private _pendingRefresh: Promise<void> | null = null

  constructor(roomId: string, config: RoomConfigRecord, seed: { members: MemberSnapshot[] } | { count: number }) {
    this._inc = config.inc
    this._state = new RoomState({
      roomId,
      meta: config.meta,
      seed,
      updateStamp: { at: config.at, by: config.by },
      onListenersChanged: () => this._syncSubs(),
      onCallbackError: reportRoomError,
    })
    this._state._owner = this
    this._demand = new RoomDemand(
      (event) => void publishCtrl(roomId, config.inc, { __r: 'want', ...event }).catch(reportRoomError),
      (id) => this._ownsMember(id),
      (member, track, wanted) => this._deliverDemand(member, track, wanted),
    )
  }

  static isServerRoom(value: unknown): value is ServerRoom {
    return value !== null && typeof value === 'object' && SERVER_ROOM_BRAND in value
  }

  /** @internal — see `Room.guard()`. One declaration per instance keeps the grant declarative. */
  _setGuards(guards: RoomGuards): void {
    assertUsage(
      this._guards === null,
      'Room.guard() was already called for this room instance — declare all guards in one call',
    )
    this._guards = guards
  }

  // ── Room API ──

  get id(): string {
    return this._state.roomId
  }
  get meta(): RoomMeta {
    return this._state.meta
  }
  get count(): number {
    return this._state.count
  }
  get isEmpty(): boolean {
    return this._state.count === 0
  }
  get isClosed(): boolean {
    return this._state.closed
  }

  async join(options?: JoinOptions): Promise<LocalParticipant> {
    const { meta, selfDelivery } = normalizeJoinOptions(options)
    const identity = normalizeIdentity(options)
    const hidden = normalizeHidden(options)
    let participant!: ServerLocalParticipant
    await this._admitMember(
      meta,
      identity,
      (id, joinedAt) => {
        participant = new ServerLocalParticipant(this, id, meta, joinedAt, selfDelivery, identity)
        this._localParticipants.set(id, participant)
      },
      hidden,
    )
    return participant
  }

  async getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant[]> {
    await this._ensureRoster()
    return options?.hidden ? this._state.listHidden() : this._state.listRemotes()
  }

  async getParticipant(id: string): Promise<RemoteParticipant | null> {
    await this._ensureRoster()
    return this._state.getRemote(id)
  }

  subscribe(callback: (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this._state.subscribe(callback)
  }
  subscribeBinary(callback: RoomBinaryListener, options?: { track?: string }): () => void {
    return this._state.subscribeBinary(callback, options)
  }
  onJoin(callback: (member: RemoteParticipant) => void): () => void {
    return this._state.onJoin(callback)
  }
  onLeave(callback: (member: RemoteParticipant, cause?: LeaveCause) => void): () => void {
    return this._state.onLeave(callback)
  }
  onParticipantUpdate(
    callback: (member: RemoteParticipant, meta: ParticipantMeta, prev: ParticipantMeta) => void,
  ): () => void {
    return this._state.onParticipantUpdate(callback)
  }
  onUpdate(callback: (meta: RoomMeta, prev: RoomMeta) => void): () => void {
    return this._state.onUpdate(callback)
  }
  onEmpty(callback: () => void): () => void {
    return this._state.onEmpty(callback)
  }
  onClose(callback: () => void): () => void {
    return this._state.onClose(callback)
  }
  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this._state.onAnnounce(callback)
  }

  // Arrow-valued, not prototype methods: the documented `useSyncExternalStore(room.onChange, room.snapshot)`
  // passes both detached (React calls them with no receiver), so they must stay bound to survive it.
  onChange = (callback: () => void): (() => void) => this._state.onChange(callback)

  snapshot = (): RoomSnapshotView => {
    // Snapshot consumers want the member view — load it (need-driven, single-flight); the
    // arrival lands as an onChange, and the next snapshot() is complete.
    if (!this._state.rosterKnown) void this._ensureRoster().catch(reportRoomError)
    return this._state.snapshot()
  }

  // ── Membership operations (shared by local participants and stub requests) ──

  /** Join choreography shared by local `join()` and stub `req-join`. `track` registers the
   *  holder first — the member must count as owned before `_syncSubs()` brings up its inbox
   *  subscription and heartbeat, and before its join is announced. */
  private async _admitMember(
    meta: ParticipantMeta,
    identity: string | null,
    track: (id: string, joinedAt: number) => void,
    hidden = false,
  ): Promise<{ id: string; joinedAt: number }> {
    const id = crypto.randomUUID()
    // A hidden participant is not a party seeking admission — it's a server/bot/recorder — so it
    // bypasses the admission ceremony: no `onBeforeJoin` policy and no `onAfterJoin` side effects. But
    // its join IS announced on the control lane (flagged), so observers already connected learn of it
    // live — the presence callbacks (`onJoin`/`count`) stay suppressed via the flag in `applyJoin`.
    // Admission policy runs first, on the definitive member ID — a rejected join writes nothing.
    const onBeforeJoin = this._guards?.onBeforeJoin
    if (!hidden && onBeforeJoin) await onBeforeJoin({ id, meta, identity })
    const joinedAt = await this._createMember(id, meta, identity, hidden)
    track(id, joinedAt)
    this._syncSubs()
    this._state.applyJoin(id, meta, joinedAt, identity, hidden)
    await publishCtrl(this.id, this._inc, {
      __r: 'join',
      id,
      meta,
      joinedAt,
      ...(identity === null ? {} : { identity }),
      ...(hidden ? { hidden: true } : {}),
    })
    if (hidden) return { id, joinedAt } // announced above; a hidden participant has no post-join hook
    // Post-commit: the member exists and its join is announced — the place for side effects.
    const onAfterJoin = this._guards?.onAfterJoin
    if (onAfterJoin) await onAfterJoin({ id, meta, identity }, { joinedAt })
    return { id, joinedAt }
  }

  /** Persist the member cells for a join, guarding against a concurrent `Room.close()`. */
  private async _createMember(
    id: string,
    meta: ParticipantMeta,
    identity: string | null,
    hidden = false,
  ): Promise<number> {
    await this._assertOpen()
    const joinedAt = Date.now()
    const record: RoomMemberRecord = {
      meta,
      joinedAt,
      seenAt: joinedAt,
      inc: this._inc,
      metaSeq: 0,
      ...(identity === null ? {} : { identity }),
      ...(hidden ? { hidden: true } : {}),
    }
    const memberKey = roomMemberKvKey(this.id, id)
    const keys = [memberKey]
    if (identity !== null) keys.push(roomIdentityMemberKvKey(this.id, identity, id))
    if (hidden) keys.push(roomHiddenMemberKvKey(this.id, id))
    await mutateCells(this.id, this._inc, { keys }, () => ({
      value: undefined,
      mutations: keys.map((key) => ({
        key,
        set: {
          bytes: key === memberKey ? encodeRoomText(stringify(record)) : new Uint8Array(),
          ttlMs: ROOM_MEMBER_KV_TTL_MS,
        },
      })),
    }))
    return joinedAt
  }

  /** @internal */
  async _removeMember(id: string, cause: LeaveCause): Promise<void> {
    if (this._state.closed) return // close() already removed everyone
    const identity = this._state.getRemote(id)?.identity ?? null
    const hidden = this._state.isHidden(id)
    const keys = [roomMemberKvKey(this.id, id)]
    if (identity !== null) keys.push(roomIdentityMemberKvKey(this.id, identity, id))
    if (hidden) keys.push(roomHiddenMemberKvKey(this.id, id))
    await mutateCells(this.id, this._inc, { keys }, () => ({
      value: undefined,
      mutations: keys.map((key) => ({ key })),
    }))
    // A leaving member's per-track streams end, so their retained frames go too — binary per
    // (member, track), and the room's one retained-text slot if this member still owns it. Room
    // close drops the whole generation, including anything a leave races past.
    await this._dropRetainedBinary(id)
    await dropRetainedTextOwnedBy(this.id, this._inc, id)
    this._applyLeave(id, cause)
    await publishCtrl(this.id, this._inc, { __r: 'leave', id, ...leaveCauseToWire(cause) })
  }

  /** Delete a member's retained binary frames — every track, wherever stored. The backend's retained
   *  lane inventory is authoritative, so a frame retained on another node is still cleaned up.
   *  Deleting an absent lane is a no-op, so a member that retained nothing just pays one empty scan. */
  private async _dropRetainedBinary(id: string): Promise<void> {
    const backend = getRoomBackend()
    for (const lane of await backend.listRetained(this.id, this._inc)) {
      if (lane.kind === 'binary' && lane.member === id) await backend.deleteRetained(this.id, this._inc, lane)
    }
  }

  /** @internal — full replace (`setMeta`). */
  async _setMemberMeta(id: string, meta: ParticipantMeta): Promise<void> {
    assertUsage(isObject(meta), 'setMeta() meta should be an object')
    await this._writeMemberMeta(id, () => meta)
  }

  /** @internal — per-key merge (`setAttributes`); an `undefined` value deletes the key. */
  async _mergeMemberMeta(id: string, attrs: ParticipantMeta): Promise<void> {
    assertUsage(isObject(attrs), 'setAttributes() attributes should be an object')
    await this._writeMemberMeta(id, (current) => mergeAttributes(current, attrs))
  }

  private async _writeMemberMeta(
    id: string,
    computeMeta: (current: ParticipantMeta) => ParticipantMeta,
  ): Promise<void> {
    await this._assertOpen()
    const key = roomMemberKvKey(this.id, id)
    const { prev, meta, seq } = await mutateCells(this.id, this._inc, { keys: [key] }, (cells) => {
      const raw = cells.get(key)
      if (raw === undefined) throw new RoomError(`Participant not found (left?): ${id}`)
      const record = parse(decodeRoomText(raw)) as RoomMemberRecord
      const prev = this._state.getRemote(id)?.meta ?? record.meta
      const meta = computeMeta(record.meta)
      const seq = record.metaSeq + 1
      const next = { ...record, meta, metaSeq: seq, seenAt: Date.now() } satisfies RoomMemberRecord
      return {
        value: { prev, meta, seq },
        mutations: [{ key, set: { bytes: encodeRoomText(stringify(next)), ttlMs: ROOM_MEMBER_KV_TTL_MS } }],
      }
    })
    this._state.applyParticipantMeta(id, meta, prev, seq)
    await publishCtrl(this.id, this._inc, { __r: 'p-meta', id, meta, prev, seq })
  }

  /** @internal — publish a member's text message. The sender's verified meta/identity are stamped
   *  into the envelope here — never client-supplied. Text rides the room's semantic lane. `retain`
   *  stores the message as the room's one retained-text slot (MQTT-style), replayed to any later
   *  text subscriber (see `_replayRetainedText`). */
  async _publishText(from: string, data: unknown, retain = false): Promise<ChannelPublishAck> {
    const sender = await this._admitPublish(from, data)
    const envelope: RoomDataEnvelope = {
      __r: 'data',
      from,
      fromMeta: sender.meta,
      ...(sender.identity === null ? {} : { fromIdentity: sender.identity }),
      data,
    }
    // One atomic commit assigns the room-wide order, stores the retained frame (if any), and publishes
    // — the assigned order rides the lane frame, so the retained copy, the live frame, and the
    // receipt all carry the one pair with no separate allocate, and no subscriber sees a gap between
    // the store and the publish. Text and `announce()` share this clock, so they totally order.
    const commit = await commitRoomLane(this.id, this._inc, SEMANTIC_LANE, encodeRoomText(stringify(envelope)), {
      retain,
    })
    if (commit === null) throw new RoomError(`Room is closed: ${this.id}`)
    return this._finishPublish(sender, data, commit)
  }

  /** @internal — publish a member's binary frame (`[16-byte member ID][flags][…]`, validated at
   *  its entry point — the unframe cannot fail). Binary rides per-publisher lanes — per
   *  (publisher, track) for named tracks: that's what makes delivery track-selective at the
   *  source, so `receivers: 0` on the ack truthfully means "nobody anywhere wants this track". */
  async _publishBinaryFramed(from: string, framed: Uint8Array): Promise<ChannelPublishAck> {
    // The single validating unframe of the publish path: a locally-built frame always parses; a
    // hand-crafted one that doesn't (truncated, over-long track/meta, malformed meta JSON) is rejected
    // here, cleanly, rather than crashing the relay or corrupting a lane.
    const frame = unframeMemberId(framed)
    if (!frame) throw new RoomError('Malformed binary frame')
    // The guard sees exactly what a subscriber would: the payload, without the wire frame.
    const sender = await this._admitPublish(from, frame.payload)
    if (frame.track !== null) await this._ensureTrackAnnounced(from, frame.track)
    const result = await commitRoomLane(
      this.id,
      this._inc,
      { kind: 'binary', member: from, track: frame.track ?? DEFAULT_TRACK },
      framed,
      { retain: frame.retain },
    )
    if (result === null) throw new RoomError(`Room is closed: ${this.id}`)
    const ack = await this._finishPublish(sender, frame.payload, result)
    return ack
  }

  /** Shared publish prologue: open check + `onBeforePublish` guard, on the verified sender. */
  private async _admitPublish(from: string, payload: unknown): Promise<Sender> {
    if (this._state.closed) throw new RoomError(`Room is closed: ${this.id}`)
    const sender = this._memberSender(from)
    const onBeforePublish = this._guards?.onBeforePublish
    if (onBeforePublish) await onBeforePublish(sender, payload)
    return sender
  }

  /** Shared publish epilogue: the receipt (with `receivers`) plus the `onAfterPublish` hook, which
   *  sees the same `payload` the guard did and the authoritative `seq`/`timestamp` — the place to
   *  persist for history. Awaited, so a throw rejects the publisher (the message is already out). */
  private async _finishPublish(
    sender: Sender,
    payload: unknown,
    // Semantic and binary lanes each carry their backend-assigned domain order.
    info: { seq: number; timestamp: number; receivers?: number; meta?: Record<string, unknown> },
  ): Promise<ChannelPublishAck> {
    const ack = Object.assign(makePublishInfo(this.id, info.seq, info.timestamp), {
      meta: info.meta,
      ...(info.receivers === undefined ? {} : { receivers: info.receivers }),
    })
    const onAfterPublish = this._guards?.onAfterPublish
    if (onAfterPublish) {
      await onAfterPublish(sender, payload, {
        seq: ack.seq,
        timestamp: ack.timestamp,
        ...(ack.receivers === undefined ? {} : { receivers: ack.receivers }),
      })
    }
    return ack
  }

  /** First frame on a new (member, track): record the track on the member's cell (late observers
   *  discover it from the roster) and announce it on the control lane (live all-track subscribers
   *  bring the lane subscription up) — both strictly before the frame. Idempotent across owner
   *  incarnations via the cell record; O(1) per further frame via `_announcedTracks`. */
  private async _ensureTrackAnnounced(from: string, track: string): Promise<void> {
    let announced = this._announcedTracks.get(from)
    if (announced?.has(track)) return
    if (!announced) {
      announced = new Set()
      this._announcedTracks.set(from, announced)
    }
    // Atomic append: record the track on the member's record unless it's already there (a previous
    // owner incarnation recorded it). The compare-and-set re-runs on a concurrent record write, so the
    // append is never clobbered. Announce on the control lane only when this call actually added it.
    const key = roomMemberKvKey(this.id, from)
    const appended = await mutateCells(this.id, this._inc, { keys: [key] }, (cells) => {
      const raw = cells.get(key)
      if (raw === undefined) throw new RoomError(`Participant not found (left?): ${from}`)
      const record = parse(decodeRoomText(raw)) as RoomMemberRecord
      const tracks = record.tracks ?? []
      if (tracks.includes(track)) {
        return { value: false, mutations: [] }
      }
      // Bound named tracks per participant: authoritative here (the record is the cross-node source
      // of truth), so a hostile publisher can't spray distinct track names to multiply KV slots,
      // announcements, retained frames, and subscriptions. The default lane is unnamed, never counted.
      if (tracks.length >= ROOM_TRACKS_PER_MEMBER_MAX) {
        throw new RoomError(`A participant may announce at most ${ROOM_TRACKS_PER_MEMBER_MAX} tracks`)
      }
      const next = { ...record, tracks: [...tracks, track], seenAt: Date.now() } satisfies RoomMemberRecord
      return {
        value: true,
        mutations: [{ key, set: { bytes: encodeRoomText(stringify(next)), ttlMs: ROOM_MEMBER_KV_TTL_MS } }],
      }
    })
    if (appended) await publishCtrl(this.id, this._inc, { __r: 'track', id: from, track })
    this._state.applyTrack(from, track)
    announced.add(track)
  }

  /** The verified sender as this node knows it — own participants first (freshest), then the
   *  view. The one place sender identity is assembled; guards and envelopes both consume it. */
  private _memberSender(from: string): Sender {
    const local = this._localParticipants.get(from)
    if (local) return { id: from, meta: local.meta, identity: local.identity }
    const remote = this._state.getRemote(from)
    return { id: from, meta: remote?.meta ?? {}, identity: remote?.identity ?? null }
  }

  /** @internal — send a private message: published on the target's inbox key, which only
   *  the target's owning node subscribes to (see `_onDm`). The sender's verified meta rides
   *  the envelope so every receiver can surface a rich sender. */
  async _sendDm(from: string, to: string, data: unknown): Promise<RoomSendReceipt> {
    return this._publishDm(from, to, data)
  }

  /** @internal — `send(…, { ack: true })`: publish the DM tagged with an `ackId`, then wait for the
   *  recipient's node to route the handler's reply back on our own inbox (`_onDm` → `_resolveDmAck`).
   *  Returns the send receipt plus the recipient's `DmReply` — a success value, or a failure the
   *  caller surfaces (the handler's `Abort`, the recipient leaving, the room closing). The outbound
   *  publish itself still throws (an invalid send never reaches a recipient to reply). */
  async _sendDmAck(from: string, to: string, data: unknown): Promise<{ receipt: RoomSendReceipt; reply: DmReply }> {
    const ackId = crypto.randomUUID()
    let timer: ReturnType<typeof setTimeout> | undefined
    const reply = new Promise<DmReply>((settle) => {
      this._pendingDmAcks.set(ackId, { to, settle })
      // The recipient replying/leaving/overflowing settles this promptly; this bounds the one case
      // none of those cover — a recipient that joined but never listens and never leaves.
      timer = setTimeout(() => {
        if (this._pendingDmAcks.delete(ackId))
          settle({ ok: false, err: 'send({ ack: true }) timed out — the recipient never handled the message' })
      }, ROOM_DM_ACK_TIMEOUT_MS)
    })
    let receipt: RoomSendReceipt
    try {
      receipt = await this._publishDm(from, to, data, ackId)
    } catch (err) {
      this._pendingDmAcks.delete(ackId)
      clearTimeout(timer)
      throw err
    }
    const settled = await reply
    clearTimeout(timer)
    return { receipt, reply: settled }
  }

  /** Shared DM publish: validate the target, run the send guards, stamp the verified sender, and
   *  publish on the target's inbox key. `ackId` rides the envelope when a reply is awaited. */
  private async _publishDm(from: string, to: string, data: unknown, ackId?: string): Promise<RoomSendReceipt> {
    if (this._state.closed) throw new RoomError(`Room is closed: ${this.id}`)
    const target = await this._resolveMember(to)
    if (!target) throw new RoomError(`Participant not found: ${to}`)
    const sender = this._memberSender(from)
    const onBeforeSend = this._guards?.onBeforeSend
    if (onBeforeSend) await onBeforeSend(sender, target, data)
    const envelope: RoomDmEnvelope = {
      __r: 'dm',
      to,
      from,
      fromMeta: sender.meta,
      ...(sender.identity === null ? {} : { fromIdentity: sender.identity }),
      data,
      ...(ackId ? { ackId } : {}),
    }
    const receipt = await commitRoomLane(
      this.id,
      this._inc,
      { kind: 'inbox', member: to },
      encodeRoomText(stringify(envelope)),
    )
    if (receipt === null) throw new RoomError(`Room is closed: ${this.id}`)
    const info: RoomSendReceipt = { seq: receipt.seq, timestamp: receipt.timestamp }
    const onAfterSend = this._guards?.onAfterSend
    if (onAfterSend) await onAfterSend(sender, target, data, info)
    return info
  }

  /** @internal — publish an `{ ack: true }` reply back to the sender's inbox (`to` is the sender). */
  private async _publishDmAck(to: string, ackId: string, reply: DmReply): Promise<void> {
    const envelope: RoomDmAckEnvelope = { __r: 'dm-ack', to, ackId, ...reply }
    const committed = await commitRoomLane(
      this.id,
      this._inc,
      { kind: 'inbox', member: to },
      encodeRoomText(stringify(envelope)),
    )
    if (committed === null) throw new RoomError(`Room is closed: ${this.id}`)
  }

  /** @internal — the recipient replied: settle the matching pending `send(…, { ack: true })` with
   *  its `DmReply`, verbatim — the sender's caller reconstructs success or failure from it. */
  private _resolveDmAck(envelope: RoomDmAckEnvelope): void {
    const pending = this._pendingDmAcks.get(envelope.ackId)
    if (!pending) return
    this._pendingDmAcks.delete(envelope.ackId)
    pending.settle(envelope)
  }

  /** @internal — fail any pending ack whose recipient just left (or, on close, all of them). This
   *  is an expected operational outcome, not a bug, so it carries a visible reason (`err`). */
  private _rejectDmAcks(message: string, to?: string): void {
    for (const [ackId, pending] of this._pendingDmAcks) {
      if (to !== undefined && pending.to !== to) continue
      this._pendingDmAcks.delete(ackId)
      pending.settle({ ok: false, err: message })
    }
  }

  /** The member's live view — falling back to the authoritative backend cell, since the local
   *  view lags while unobserved (and briefly after the observe transition, until the cell
   *  resync lands). */
  private async _resolveMember(id: string): Promise<Sender | null> {
    const remote = this._state.getRemote(id)
    if (remote) return remote
    const raw = await readCell(this.id, this._inc, roomMemberKvKey(this.id, id))
    if (raw === null) return null
    const record = parse(decodeRoomText(raw)) as RoomMemberRecord
    return { id, meta: record.meta, identity: record.identity ?? null }
  }

  /** The room's authoritative config iff it is `open` at this handle's incarnation — the legality
   *  oracle for every mutation. A `null` result means closed/closing/gone or another incarnation. */
  private async _openConfig(): Promise<RoomConfigRecord | null> {
    const current = await getRoomBackend().readHead(this.id)
    if (current === null || current.head.state !== 'open' || current.head.currentInc !== this._inc) return null
    return configFromHead(current.head)
  }

  private async _assertOpen(): Promise<void> {
    if (this._state.closed || (await this._openConfig()) === null) {
      throw new RoomError(`Room is closed: ${this.id}`)
    }
  }

  // ── Event stream (backend lane callbacks) ──

  /** The control lane: presence and lifecycle events — relayed to every stub
   *  unconditionally, since a client's live view is only correct if it sees every one. */
  private _onCtrlMessage(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the control lane
    }
    if (!hasRoomTag(envelope)) return
    const event = envelope as RoomEnvelope
    if (event.__r === 'data') return // data never travels on the control key
    if (event.__r === 'want') {
      this._demand.applyWant(event) // demand gossip — node-to-node only, never relayed to clients
      return
    }
    const wasClosed = this._state.closed
    // A hidden member's presence events (join/leave/meta/track) are server-only — decide before
    // applying, since `leave` removes the member from state (see `_hidesFromClients`).
    const serverOnly = this._hidesFromClients(event)

    if (event.__r === 'announce') return
    this._applyCtrl(event)

    if (this._stubs.size > 0 && !serverOnly) {
      // Presence/lifecycle events are ordered by the control lane; the receipt rides the frame.
      const wireText = encodePublishText(serialized, rawInfo)
      for (const stub of this._stubs) stub._relayPublishText(wireText)
    }

    if (this._state.closed && !wasClosed) this._teardown()
  }

  /** Whether a control event concerns a hidden (server-only) member and so must not be relayed to
   *  clients. The event still rides the control lane between servers — that is how every server's
   *  projection converges on a hidden member — but it stops at the stub boundary, so a hidden member
   *  never enters a client's roster or its presence narration (see `getParticipants({ hidden })`). */
  private _hidesFromClients(event: RoomEnvelope): boolean {
    switch (event.__r) {
      case 'join':
        return event.hidden === true
      case 'leave':
      case 'p-meta':
      case 'track':
        return this._state.isHidden(event.id)
      default:
        return false // room-level events (update/closed) always reach clients
    }
  }

  /** The text data lane — relayed per stub, skipping the sender's own holder when it opted out. */
  private _onTextData(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the semantic lane
    }
    if (!hasRoomTag(envelope)) return
    if (envelope.__r === 'announce') {
      const announce = envelope as Extract<RoomEnvelope, { __r: 'announce' }>
      const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
      this._state.applyAnnounce(announce.data, info)
      if (this._stubs.size > 0) {
        const wireText = encodePublishText(serialized, rawInfo)
        for (const stub of this._stubs) stub._relayPublishText(wireText)
      }
      return
    }
    if (envelope.__r !== 'data') return
    const event = envelope as RoomDataEnvelope
    // The semantic-lane order rides on the transport frame, never in the payload.
    const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
    this._state.applyData(
      event.from,
      event.fromMeta,
      event.fromIdentity ?? null,
      event.data,
      info,
      this._suppress(event.from),
    )
    this._healUnknownSender(event.from)

    if (this._stubs.size > 0) {
      const wireText = encodePublishText(serialized, rawInfo)
      for (const stub of this._stubs) {
        // A stub still awaiting its client's first text want holds the message server-side (bounded,
        // drop-oldest) instead of relaying — flushed selectively once the want declares the selector.
        if (stub._tailPending !== null) {
          stub._holdTail(serialized, rawInfo, event.from)
          continue
        }
        if (!stub._wantsTextFrom(event.from)) continue
        if (stub._selfSuppressed.has(event.from)) continue
        stub._relayTextLive(wireText, event.from, rawInfo)
      }
    } else if (this._tail) {
      // Tail, pre-attach: no stub yet, but `Room.get({ tail })` opened ingestion — hold the message so
      // the stub inherits it on attach. Bounded by count and total size; the client dedupes any overlap.
      pushBoundedTail(this._tailHold, { serialized, ord: rawInfo, from: event.from })
    }
  }

  private _onBinary(framed: Uint8Array, rawInfo: WirePublishInfo): void {
    const unframed = unframeMemberId(framed)
    if (!unframed) return // junk on the binary lane
    const info = makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp)
    this._state.applyBinary(
      unframed.from,
      unframed.payload,
      unframed.track,
      unframed.meta,
      info,
      this._suppress(unframed.from),
    )
    this._healUnknownSender(unframed.from)

    if (this._stubs.size > 0) {
      const wireData = encodePublishBinary(framed, rawInfo)
      const track = unframed.track ?? DEFAULT_TRACK
      for (const stub of this._stubs) {
        if (!stub._wantsBinary(unframed.from, track)) continue
        if (stub._selfSuppressed.has(unframed.from)) continue
        stub._relayBinaryLive(wireData, unframed.from, track, rawInfo)
      }
    }
  }

  /** A message on the inbox key of a member this instance owns — route it to the holder:
   *  a server-side participant's listeners, or the one client stub the member joined through. */
  private _onDm(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the inbox lane
    }
    if (!hasRoomTag(envelope)) return
    // A reply to one of our own `send(…, { ack: true })`s, riding our inbox back home.
    if (envelope.__r === 'dm-ack') return this._resolveDmAck(envelope as RoomDmAckEnvelope)
    if (envelope.__r !== 'dm') return
    const dm = envelope as RoomDmEnvelope
    const msg: InboxMessage = {
      from: dm.from,
      fromMeta: dm.fromMeta,
      fromIdentity: dm.fromIdentity ?? null,
      data: dm.data,
      ...(dm.ackId ? { ackId: dm.ackId } : {}),
    }
    const local = this._localParticipants.get(dm.to)
    if (local) {
      // A server-side participant, or one a client holds (its forwarder replies). Either way,
      // for an ack DM we route the handler's reply back to the sender's inbox.
      if (dm.ackId) {
        void local
          ._deliverMessageAck(msg)
          .then((reply) => this._publishDmAck(dm.from, dm.ackId!, reply))
          .catch(reportRoomError)
      } else {
        local._deliverMessage(msg)
      }
      return
    }
    // A client held through a room stub — relay the DM (its `ackId` rides along); the client
    // replies with `dm-reply`, which `_handleStubRequest` turns into the `dm-ack` above.
    const wireText = encodePublishText(serialized, rawInfo)
    for (const stub of this._stubs) {
      if (!stub._stubMembers.has(dm.to)) continue
      if (dm.ackId) stub._recordAckDm(dm.ackId, dm.from, dm.to)
      stub._relayPublishText(wireText)
    }
  }

  private _applyCtrl(event: RoomCtrlEnvelope): void {
    switch (event.__r) {
      case 'join':
        this._state.applyJoin(event.id, event.meta, event.joinedAt, event.identity ?? null, event.hidden)
        this._syncSubs() // a new member means a new per-member key candidate
        return
      case 'track':
        this._state.applyTrack(event.id, event.track)
        this._syncSubs() // all-track subscribers need the new (member, track) key
        return
      case 'leave':
        this._applyLeave(event.id, leaveCauseFromWire(event))
        return
      case 'p-meta': {
        this._state.applyParticipantMeta(event.id, event.meta, event.prev, event.seq)
        const local = this._localParticipants.get(event.id)
        if (local) local._meta = event.meta
        return
      }
      case 'update':
        this._state.applyRoomUpdate(event.meta, event.at, event.by)
        return
      case 'closed':
        this._state.applyClosed()
    }
  }

  private _applyLeave(id: string, cause?: LeaveCause): void {
    this._state.applyLeave(id, cause)
    this._announcedTracks.delete(id)
    this._rejectDmAcks('Recipient left the room before replying', id) // strand no waiter on a gone member
    const local = this._localParticipants.get(id)
    if (local) {
      this._localParticipants.delete(id)
      // A live-heartbeating owner can't be reaped (heartbeats outpace the TTL by 4x), so a
      // vanished record with no observed event means the member was removed.
      local._onLeft(cause ?? { type: 'removed' })
    }
    for (const stub of this._stubs) {
      stub._stubMembers.delete(id)
      stub._selfSuppressed.delete(id)
      stub._forgetMember(id) // drop the departed member's retained-replay watermarks (bounded state)
    }
    this._syncSubs()
  }

  /** The room closed — runs once, after the `closed` event has been applied and relayed. */
  private _teardown(): void {
    this._rejectDmAcks('Room is closed') // no recipient will reply now
    for (const local of this._localParticipants.values()) local._onLeft({ type: 'closed' })
    this._localParticipants.clear()
    for (const stub of this._stubs) void stub.close().catch(() => {})
    this._syncSubs()
  }

  /** A member's own messages are suppressed for this holder when its participant opted out. */
  private _suppress(from: string): boolean {
    return this._localParticipants.get(from)?.selfDelivery === false
  }

  // ── Client stubs ──

  /** @internal — begin tail relay (`Room.get({ tail: true })`): ingest and hold the room's text from
   *  now, so a history read done before this room is serialized misses no live message. A safety timer
   *  tears the ingestion down if the room is fetched-with-tail but never serialized. */
  _startTail(): void {
    this._tail = true
    this._syncSubs() // bring up text ingestion before any stub exists
    this._tailTimer = setTimeout(() => this._teardownTail(), ROOM_TAIL_ATTACH_TIMEOUT_MS)
  }

  private _teardownTail(): void {
    if (!this._tail) return // already handed off to a stub
    this._tail = false
    this._tailHold.length = 0
    this._tailTimer = null
    this._syncSubs() // drop the text ingestion nothing is consuming
  }

  _attachStub(stub: RoomStubChannel): void {
    this._stubs.add(stub)
    // Tail mode (`Room.get(id, { tail: true })`): hand the pre-attach hold to the stub, which keeps
    // holding server-side (still ingesting, gate closed) until the client's first text want declares
    // the selector — then it flushes the selected messages once, in order, ahead of the live stream
    // (see `_flushTail`). Nothing crosses the wire before the client asks for it, and the client needs
    // no buffer of its own. `_syncSubs()` below keeps text ingestion up while the hold is pending.
    if (this._tail) {
      if (this._tailTimer !== null) {
        clearTimeout(this._tailTimer)
        this._tailTimer = null
      }
      stub._beginTail(this._tailHold.slice(), () => this._syncSubs())
      this._tailHold.length = 0
      this._tail = false
    }
    // The snapshot carries only scalars; the roster streams once the peer is attached (never
    // buffered — a byte-capped pre-peer buffer must not be able to evict it). Everything
    // relayed before it is already reflected in it; later events apply incrementally.
    stub.onOpen(() => {
      void this._ensureRoster()
        .then(() => {
          if (this._stubs.has(stub) && !this._state.closed)
            stub._relayRoster(this._state.snapshotMembers().filter((m) => !m.hidden))
        })
        .catch(reportRoomError)
    })
    stub.onClose(() => {
      this._stubs.delete(stub)
      stub._endTail() // clear any pending tail hold/timer so a closed stub leaves nothing behind
      // The client is gone — presence says its members leave.
      for (const id of [...stub._stubMembers.keys()])
        void this._removeMember(id, { type: 'disconnected' }).catch(reportRoomError)
      stub._stubMembers.clear()
      this._syncSubs()
    })
    this._syncSubs()
  }

  /** @internal — requests arriving on a room stub channel. The return value is the ack. */
  /** Handle a client request on a `Room` stub. The ack-bearing requests (join/leave/set-meta/
   *  set-attrs/dm) return their raw success value or *throw*: the stub's ack encoder (`roomAckError`,
   *  set in `RoomStubChannel`) maps the throw onto the channel's native `ABORT`/`ERROR` status, so
   *  the client's awaiting request rejects with the reconstructed `AbortError`/`Error` — no envelope.
   *  The fire-and-forget requests (dm-reply, sub-binary, sub-text) ride no ack and must never throw. */
  async _handleStubRequest(stub: RoomStubChannel, msg: unknown): Promise<unknown> {
    if (!hasRoomTag(msg)) return undefined
    const req = msg as RoomStubRequest
    switch (req.__r) {
      case 'req-join': {
        const meta = isObject(req.meta) ? req.meta : {}
        // Identity is trusted and therefore server-assigned — a client join never carries one.
        const { id, joinedAt } = await this._admitMember(meta, null, (id) => {
          stub._stubMembers.add(id)
          if (req.selfDelivery === false) stub._selfSuppressed.add(id)
        })
        return { id, joinedAt }
      }
      case 'req-leave':
        this._assertStubMember(stub, req.id)
        stub._stubMembers.delete(req.id)
        await this._removeMember(req.id, { type: 'left' })
        return undefined
      case 'req-set-meta':
        this._assertStubMember(stub, req.id)
        await this._setMemberMeta(req.id, isObject(req.meta) ? req.meta : {})
        return undefined
      case 'req-set-attrs':
        this._assertStubMember(stub, req.id)
        await this._mergeMemberMeta(req.id, isObject(req.attrs) ? req.attrs : {})
        return undefined
      case 'req-dm': {
        this._assertStubMember(stub, req.id)
        if (!req.ack) return await this._sendDm(req.id, req.to, req.data)
        const { receipt, reply } = await this._sendDmAck(req.id, req.to, req.data)
        // The recipient's failure rides home too — throw it so the ack encoder emits the native
        // ABORT/ERROR (a re-catch here would reclassify a carried Abort/RoomError as an opaque bug).
        if (!reply.ok) throw roomFailureError(reply)
        return { ...receipt, response: reply.result }
      }
      case 'dm-reply': {
        // A client-held member replied to an ack DM we relayed it — route the reply to the sender.
        // Fire-and-forget: only an `ackId` we relayed to this stub, replied to by the very member we
        // relayed it to and not yet expired, is honored (`_takeAckDm` is the guard) — a forged,
        // misattributed, or stale reply matches nothing, so nothing here throws onto the (unacked) wire.
        const sender = stub._takeAckDm(req.ackId, req.id)
        if (sender !== undefined) {
          // Rebuild a clean `DmReply` from the spread fields (dropping `__r`/`id`/`ackId`),
          // preserving whether it's a value, a carried `Abort`, or an operational error.
          const reply: DmReply = req.ok
            ? { ok: true, result: req.result }
            : 'abort' in req
              ? { ok: false, abort: true, abortValue: req.abortValue }
              : { ok: false, err: req.err }
          void this._publishDmAck(sender, req.ackId, reply).catch(reportRoomError)
        }
        return undefined
      }
      case 'sub-binary': {
        const wants = sanitizeBinaryWants(req.wants)
        // Fire-and-forget: a malformed declaration is a client bug — report it, don't act on it.
        if (!wants) {
          reportRoomError(new RoomError('Malformed sub-binary declaration'))
          return undefined
        }
        const prev = stub._binaryWants
        stub._binaryWants = wants
        this._syncSubs()
        void this._replayRetainedBinary(stub, prev).catch(reportRoomError)
        return undefined
      }
      case 'sub-text': {
        // Member-scoped text wants — the room-level (all) want rides the broadcast-sub ctrl.
        const members = Array.isArray(req.members) ? req.members.filter((m) => typeof m === 'string') : []
        const prev = stub._textMemberWants
        stub._textMemberWants = new Set(members)
        stub._flushTail() // this selector newly covers held tail — flush it before live/retained relay
        this._syncSubs()
        void this._replayRetainedText(stub, stub._wantsText, prev).catch(reportRoomError)
        return undefined
      }
      default:
        return undefined
    }
  }

  /** @internal — validate a client publish payload against the room's declared message type (`Pub`),
   *  auto-generated from the type (see `RoomShield`). `validate` is the room's `data` shield, carried on
   *  the ingress channel's own `_publishShield` slot — deliberately *not* the channel's `_validators` map,
   *  which the base channel runs against every request envelope (`_dispatchAckReq`): a room stub multiplexes
   *  join/leave/dm through that path, so the payload is shielded explicitly here, not as a request side effect.
   *  A fail throws a `ShieldValidationError` the ack encoder turns into a `SHIELD_ERROR`. A no-op when nothing
   *  declared `Pub` (`unknown` generates no verifier). Both publish ingresses (room stub, standalone-participant
   *  stub) run it before the payload is admitted. */
  _shieldPublishData(validate: ShieldValidator | undefined, data: unknown): void {
    if (!validate) return
    const result = validate(data)
    if (result !== true) throw new ShieldValidationError(result)
  }

  /** @internal — a client publish arriving on a room stub. The client's envelope is only a
   *  claim: membership is validated against this stub, and the publish path re-stamps the verified
   *  `fromMeta` — nothing client-supplied reaches the room stream except the payload itself. */
  async _publishFromStub(
    stub: RoomStubChannel,
    payload: { text: string } | { binary: Uint8Array },
  ): Promise<ChannelPublishAck> {
    if ('text' in payload) {
      const envelope = parse(payload.text) as unknown
      if (!hasRoomTag(envelope) || envelope.__r !== 'data') throw new RoomError('Malformed room publish')
      const publish = envelope as RoomDataPublish
      this._assertStubMember(stub, publish.from)
      // Shield the payload against the room's declared message type (`Pub`) — auto-generated, run only
      // on client-originated publishes (a server-side `me.publish()` is trusted and never comes here).
      // A fail rides the publish ack as `SHIELD_ERROR` (see `roomAckError`), rejecting the client's
      // `publish()` with a `ShieldValidationError` before the payload reaches a guard or the room.
      this._shieldPublishData(stub._publishShield, publish.data)
      return await this._publishText(publish.from, publish.data, publish.retain)
    }
    // Read only the sender prefix to check membership; the full validating unframe happens once, in
    // `_publishBinaryFramed`. A frame too short to carry a sender fails the membership check here.
    const from = binaryFrameSender(payload.binary)
    this._assertStubMember(stub, from)
    return await this._publishBinaryFramed(from, payload.binary)
  }

  private _assertStubMember(stub: RoomStubChannel, id: unknown): asserts id is string {
    if (typeof id !== 'string' || !stub._stubMembers.has(id)) {
      throw new RoomError('Not a participant of this room (joined through this connection)')
    }
  }

  /** @internal — MQTT-retained replay for the text lane. Called when a stub's text want grows
   *  (`_onPeerBroadcastSubscribe` for the whole lane, `sub-text` for member-scoped): replay the
   *  room's one retained-text slot iff this change is what newly covers the message's sender —
   *  an already-covered stub received it live, so it's delivered exactly once per subscription. */
  async _replayRetainedText(
    stub: RoomStubChannel,
    prevWantsText: boolean,
    prevMemberWants: ReadonlySet<string>,
  ): Promise<void> {
    // Wait until the shared text subscription is live at the backend before reading the retained slot.
    // A commit stores the retained copy before it publishes, so once the subscription is live, any
    // publish that raced this subscribe is either already in the copy we read or arrives live — never
    // lost in the gap between subscribing and the read. A synchronous backend (in-memory) resolves
    // instantly. See `SubSlot.ready` and `LaneSubscription.ready`.
    await this._textSub.ready
    const stored = await getRoomBackend().readRetained(this.id, this._inc, SEMANTIC_LANE)
    if (stored === null) return
    const serialized = decodeRoomText(stored.payload)
    const info = { seq: stored.seq, timestamp: stored.timestamp }
    const envelope = parse(serialized) as RoomDataEnvelope
    if (prevWantsText || prevMemberWants.has(envelope.from) || !stub._wantsTextFrom(envelope.from)) return
    // Replay the stored frame as-is (it already carries its real order), and let the stub drop it if a
    // same-or-newer live frame already reached it (a publish that raced this subscribe) — exactly-once.
    stub._emitRetainedText(encodePublishText(serialized, info), envelope.from, info)
  }

  /** @internal — MQTT-retained replay for the binary lanes. Called when a stub's `sub-binary` want
   *  grows: replay each retained (member, track) frame this change newly covers (in the new want,
   *  not the old), so a subscriber gets the last retained frame of every lane it starts watching. The
   *  frame is self-describing, so the sender/track come from the frame itself, not the key. */
  async _replayRetainedBinary(stub: RoomStubChannel, prevWants: BinaryWants): Promise<void> {
    if (!wantsAnyBinary(stub._binaryWants)) return
    // Same handoff as the text lane (see `_replayRetainedText`): wait for the per-(member, track)
    // subscriptions to be live before reading the retained frames, so a frame racing the subscribe rides
    // the retained copy or the live lane instead of the gap. A synchronous backend resolves instantly.
    await this._binaryReady()
    const backend = getRoomBackend()
    for (const lane of await backend.listRetained(this.id, this._inc)) {
      if (lane.kind !== 'binary') continue
      const stored = await backend.readRetained(this.id, this._inc, lane)
      if (stored === null) continue
      const framed = stored.payload
      const frame = unframeMemberId(framed)
      if (!frame) continue
      const track = frame.track ?? DEFAULT_TRACK
      if (binaryWantsCovers(prevWants, frame.from, track) || !stub._wantsBinary(frame.from, track)) continue
      // Replay with the frame's own stored receipt (never seq:0/Date.now()); the stub drops it if a
      // same-or-newer live frame on this lane already reached it — exactly-once, in order.
      const info: WirePublishInfo = { seq: stored.seq, timestamp: stored.timestamp }
      stub._emitRetainedBinary(encodePublishBinary(framed, info), frame.from, track, info)
    }
  }

  // ── Backend lane subscriptions ──

  /** @internal — recompute which backend lanes this instance needs and (un)subscribe to match.
   *  Idempotent; called after every change that can affect the answer (listeners, members,
   *  stubs, close). */
  _syncSubs(): void {
    const backend = getRoomBackend()
    const state = this._state
    const open = !state.closed
    const observed =
      this._stubs.size > 0 ||
      this._localParticipants.size > 0 ||
      state.eventListenerCount + state.dataListenerCount + state.binaryListenerCount > 0

    // Control: one low-rate lane every observer holds — it's what keeps the live view correct.
    const becomesObserved = open && observed && !this._ctrlSub.active
    this._ctrlSub.sync(open && observed, () =>
      backend.subscribeLane(this.id, this._inc, CONTROL_LANE, (payload, info) =>
        this._onCtrlMessage(decodeRoomText(payload), info),
      ),
    )

    // Text: its own lane, brought up only for holders that actually consume messages —
    // presence-only observers never receive the room's chatter. Wants are member-selective,
    // like binary: room-level listeners want it all, participant-scoped ones only their member.
    const textWants = this._aggregateTextWants()
    const wantAnyText = open && (textWants.all || textWants.members.size > 0)
    const wantSemantic = open && (observed || wantAnyText)
    const memberIds = open ? state.listMemberIds() : []

    // Roster loads are need-driven: a resident roster refreshes on the observe transition
    // (events between its cell snapshot and this subscription were missed); a lazy one loads once
    // something actually needs the member view — room-level listeners (onLeave/onEmpty
    // and live senders are only correct against it) or a member-keyed lane. A holder that only
    // joins attaches neither, so `Room.join()` never loads a roster at all —
    // `getParticipants()`/serialization go through `_ensureRoster` on their own.
    const binaryWants = this._aggregateBinaryWants()
    const wantAnyBinary = open && wantsAnyBinary(binaryWants)
    const needsRoster =
      state.eventListenerCount + state.dataListenerCount + state.binaryListenerCount > 0 || wantAnyBinary
    if ((becomesObserved && state.rosterKnown) || (open && !state.rosterKnown && needsRoster)) {
      void this._refreshMembers().catch(reportRoomError)
    }
    // Text: one lane per room. The node ingests it while anyone wants any of it; member-selectivity
    // is enforced at the per-stub relay (see `_onTextData`), never by narrowing the subscription.
    this._textSub.sync(wantSemantic, () =>
      backend.subscribeLane(this.id, this._inc, SEMANTIC_LANE, (payload, info) =>
        this._onTextData(decodeRoomText(payload), info),
      ),
    )

    // Binary: per-(publisher, track) keys in every mode — subscribing want-selectively at the
    // source makes upstream delivery pay-per-want, not filter-after-receive: dropping the last
    // want for a track drops its key, and the publisher's `receivers` hits 0.
    this._syncKeyedSubs(this._binaryKeyUnsubs, wantAnyBinary ? this._binaryLanes(binaryWants, memberIds) : [], (lane) =>
      backend.subscribeLane(this.id, this._inc, lane, (framed, info) => this._onBinary(framed, info)),
    )

    // Demand (`onDemand`): gossip this node's local binary-demand transitions and push the
    // aggregated global count to any of our own members whose demand changed.
    this._demand.sync(open ? this._localDemandPairs(binaryWants, memberIds) : [])

    // Inbox subscriptions follow ownership, not listeners — a holder must always be
    // able to receive direct messages addressed to its members.
    this._syncKeyedSubs(
      this._dmUnsubs,
      open ? this._ownedMemberIds().map((member) => ({ key: member, value: { kind: 'inbox', member } as const })) : [],
      (lane) =>
        backend.subscribeLane(this.id, this._inc, lane, (payload, info) => this._onDm(decodeRoomText(payload), info)),
    )

    this._syncHeartbeat()
  }

  /** Union of this holder's own binary listeners and every client stub's declared wants. */
  private _aggregateBinaryWants(): BinaryWants {
    const local = this._state.binaryWants()
    let everyMember = local.everyMember
    const members = new Map<string, TrackWants>(Object.entries(local.members))
    for (const stub of this._stubs) {
      everyMember = mergeTrackWants(everyMember, stub._binaryWants.everyMember)
      for (const [id, wants] of Object.entries(stub._binaryWants.members)) {
        members.set(id, mergeTrackWants(members.get(id) ?? emptyTrackWants(), wants))
      }
    }
    return { everyMember, members: Object.fromEntries(members) }
  }

  /** The backend lanes the aggregated binary wants resolve to — the exact upstream footprint.
   *  Per member: every-track wants take the default lane plus each *known* track's lane (named
   *  tracks are discovered — see `_ensureTrackAnnounced`); exact wants take exactly their lanes,
   *  eagerly (a lane needs no prior existence, so named subscribers never miss a frame). */
  private _binaryLanes(
    wants: BinaryWants,
    memberIds: string[],
  ): Array<{ key: string; value: Extract<LaneId, { kind: 'binary' }> }> {
    const lanes: Array<{ key: string; value: Extract<LaneId, { kind: 'binary' }> }> = []
    for (const memberId of memberIds) {
      const memberWants = wants.members[memberId]
      const eff = memberWants ? mergeTrackWants(wants.everyMember, memberWants) : wants.everyMember
      const tracks = eff.all ? [DEFAULT_TRACK, ...this._state.memberTracks(memberId)] : eff.tracks
      for (const track of tracks) {
        lanes.push({ key: `${memberId}\u0000${track}`, value: { kind: 'binary', member: memberId, track } })
      }
    }
    return lanes
  }

  /** The (member, track) pairs this instance has local binary demand for — the demand twin of
   *  `_binaryKeys`, kept in member/track terms so it can be gossiped and aggregated. */
  private _localDemandPairs(wants: BinaryWants, memberIds: string[]): Array<[string, string]> {
    const pairs: Array<[string, string]> = []
    for (const memberId of memberIds) {
      const memberWants = wants.members[memberId]
      const eff = memberWants ? mergeTrackWants(wants.everyMember, memberWants) : wants.everyMember
      const tracks = eff.all ? [DEFAULT_TRACK, ...this._state.memberTracks(memberId)] : eff.tracks
      for (const track of tracks) pairs.push([memberId, track])
    }
    return pairs
  }

  /** Whether one of this instance's own members is `id` — the instance that must aggregate and
   *  deliver `id`'s demand. */
  private _ownsMember(id: string): boolean {
    if (this._localParticipants.has(id)) return true
    for (const stub of this._stubs) if (stub._stubMembers.has(id)) return true
    return false
  }

  /** Route a member's freshly-changed track demand (aggregated by `RoomDemand`) to its holder —
   *  the local participant's `onDemand`, or the one client stub it joined through. */
  private _deliverDemand(member: string, track: string, wanted: boolean): void {
    const trackOut = track === DEFAULT_TRACK ? null : track
    const local = this._localParticipants.get(member)
    if (local) {
      local._onDemand(trackOut, wanted)
      return
    }
    for (const stub of this._stubs) {
      if (stub._stubMembers.has(member)) {
        stub._relayDemand({ __r: 'demand', member, track: trackOut, wanted })
        return
      }
    }
  }

  /** The text-lane twin of `_aggregateBinaryWants()` — a stub's broadcast subscription is its
   *  `all`, its `sub-text` set the member-scoped want. */
  private _aggregateTextWants(): { all: boolean; members: Set<string> } {
    if (this._tail) return { all: true, members: new Set() } // pre-attach tail: ingest everything now
    const local: MemberWants = this._state.textWants()
    if (local.all) return { all: true, members: new Set() }
    const members = new Set(local.members)
    for (const stub of this._stubs) {
      // A tail-pending stub ingests everything so its hold captures the whole recent tail; the
      // selector is applied at flush, not here (the want isn't known until the client subscribes).
      if (stub._wantsText || stub._tailPending !== null) return { all: true, members: new Set() }
      for (const id of stub._textMemberWants) members.add(id)
    }
    return { all: false, members }
  }

  /** Reconcile a map of keyed subscriptions (member IDs or lane keys) to the wanted set. */
  private _syncKeyedSubs<T>(
    subs: Map<string, LaneSubscription>,
    wantedEntries: Array<{ key: string; value: T }>,
    subscribe: (value: T) => LaneSubscription,
  ) {
    const wanted = new Map(wantedEntries.map(({ key, value }) => [key, value]))
    for (const [key, unsub] of [...subs]) {
      if (!wanted.has(key)) {
        subs.delete(key)
        void unsub.unsubscribe()
      }
    }
    for (const [key, value] of wanted) {
      if (!subs.has(key)) subs.set(key, subscribe(value))
    }
  }

  /** The binary analogue of `SubSlot.ready`: resolves once every active per-(member, track) binary
   *  subscription this node holds is live at the backend. Awaited before a retained-binary replay so a
   *  keyframe racing the subscribe isn't lost in the gap. A synchronous backend resolves instantly. */
  private _binaryReady(): Promise<void> {
    const pending: Promise<void>[] = []
    for (const subscription of this._binaryKeyUnsubs.values()) pending.push(subscription.ready)
    return pending.length === 0 ? Promise.resolve() : Promise.all(pending).then(() => undefined)
  }

  /** Resolves once the local roster is authoritative: immediately while the live view holds it
   *  (roster known and the event stream attached), else via a backend cell read. */
  private _ensureRoster(): Promise<void> {
    if (this._state.closed || (this._state.rosterKnown && this._ctrlSub.active)) return Promise.resolve()
    return this._refreshMembers()
  }

  /** A message from a sender the loaded roster doesn't know is a drift signal — its join event
   *  was dropped or reordered away (pub/sub is at-most-once between nodes). The message itself
   *  already delivered correctly (identity rides the envelope); this heals the *view*, so the
   *  live participant materializes and long-lived observers can't stay stale forever.
   *  Single-flight, so a burst from the same unknown sender costs one backend snapshot. */
  private _healUnknownSender(from: string): void {
    if (!this._state.rosterKnown || this._state.getRemote(from) !== null) return
    void this._refreshMembers().catch(reportRoomError)
  }

  /** Single-flight roster refresh. A membership event landing mid-read makes the snapshot
   *  ambiguous (its cell write may or may not be in it) — re-read: joins/leaves write cells before
   *  publishing, so the next read includes the event that dirtied this one. */
  private _refreshMembers(): Promise<void> {
    this._pendingRefresh ??= (async () => {
      try {
        while (!this._state.closed) {
          const version = this._state.membershipVersion
          const members = await readMembers(this.id, this._inc)
          if (this._state.membershipVersion !== version) continue
          const drifted = this._state.reconcile(members)
          this._syncSubs() // per-member lanes may need subscriptions for the members just learned
          // Clients seeded from the pre-drift state must be re-synced the same way they were
          // seeded — the streamed roster (position-in-stream consistent, replace semantics).
          if (drifted) {
            for (const stub of this._stubs) stub._relayRoster(this._state.snapshotMembers().filter((m) => !m.hidden))
          }
          return
        }
      } finally {
        this._pendingRefresh = null
      }
    })()
    return this._pendingRefresh
  }

  // ── Liveness heartbeat ──
  //
  // Graceful departures (leave, kick, close, stub death) are handled by events. A hard node
  // crash leaves member records behind — so the node that owns a member refreshes its `seenAt`
  // every interval, and every heartbeat also reaps members whose owner stopped refreshing.

  private _ownedMemberIds(): string[] {
    const owned = [...this._localParticipants.keys()]
    for (const stub of this._stubs) owned.push(...stub._stubMembers.keys())
    return owned
  }

  private _syncHeartbeat(): void {
    // Also runs for a pure observer (binary demand but no owned members), so its demand lease keeps
    // being renewed — otherwise a live watcher on such a node would be swept as if it had crashed.
    const want = !this._state.closed && (this._ownedMemberIds().length > 0 || this._demand.isActive())
    if (want && !this._heartbeatTimer) {
      this._heartbeatTimer = unrefTimer(
        setInterval(() => void this._heartbeatTick().catch(reportRoomError), ROOM_HEARTBEAT_INTERVAL_MS),
      )
    } else if (!want && this._heartbeatTimer) {
      clearInterval(this._heartbeatTimer)
      this._heartbeatTimer = null
    }
  }

  private async _heartbeatTick(): Promise<void> {
    if (this._heartbeatBusy) return // a slow backend must not pile up overlapping ticks
    this._heartbeatBusy = true
    try {
      // Renew this node's binary-demand lease on every owner and sweep any crashed reporter's demand.
      // No cell I/O — runs first so member-cell latency never delays it (the demand TTL has slack for skips).
      this._demand.heartbeat()
      for (const id of this._ownedMemberIds()) {
        // Bump `seenAt` with a read-modify-write, not a whole-record `set`: the update only touches
        // `seenAt` on the record actually present, so a heartbeat can never clobber a concurrent
        // meta or track write. (That clobber is why the meta/track writers used to re-assert; with
        // the heartbeat off the collision course, those loops are gone.)
        const key = roomMemberKvKey(this.id, id)
        const record = await mutateCells(this.id, this._inc, { keys: [key] }, (cells) => {
          const raw = cells.get(key)
          if (raw === undefined) return { value: null, mutations: [] }
          const record = { ...(parse(decodeRoomText(raw)) as RoomMemberRecord), seenAt: Date.now() }
          const mutations: CellMutation[] = [
            { key, set: { bytes: encodeRoomText(stringify(record)), ttlMs: ROOM_MEMBER_KV_TTL_MS } },
          ]
          if (record.identity !== undefined) {
            mutations.push({
              key: roomIdentityMemberKvKey(this.id, record.identity, id),
              set: { bytes: new Uint8Array(), ttlMs: ROOM_MEMBER_KV_TTL_MS },
            })
          }
          if (record.hidden) {
            mutations.push({
              key: roomHiddenMemberKvKey(this.id, id),
              set: { bytes: new Uint8Array(), ttlMs: ROOM_MEMBER_KV_TTL_MS },
            })
          }
          return { value: record, mutations }
        })
        if (record === null) {
          // Reaped or kicked while this node wasn't listening — the reaper already
          // published the leave event; only the local view needs to catch up.
          this._applyLeave(id)
          continue
        }
        // Refresh the member's sibling index keys on the same cadence — they carry the record's TTL,
        // so without this a member that outlives one TTL window would keep its record (heartbeated)
        // yet lose its identity marker (breaking `removeParticipant(id, { identity })` sweeps) and,
        // if hidden, its off-presence marker (the count would drift back to counting it).
      }
      await readMembers(this.id, this._inc)
    } finally {
      this._heartbeatBusy = false
    }
  }
}

// ---------------------------------------------------------------------------
// ServerLocalParticipant
// ---------------------------------------------------------------------------

const SERVER_PARTICIPANT_BRAND: unique symbol = Symbol.for('telefunc.ServerRoomParticipant')

/** Server-side `LocalParticipant`, returned by `ServerRoom.join()`. */
class ServerLocalParticipant extends ParticipantBase {
  readonly [SERVER_PARTICIPANT_BRAND] = true
  /** @internal */ readonly _room: ServerRoom
  /** @internal */ readonly _joinedAt: number
  constructor(
    serverRoom: ServerRoom,
    id: string,
    meta: ParticipantMeta,
    joinedAt: number,
    selfDelivery: boolean,
    identity: string | null,
  ) {
    super(id, meta, selfDelivery, identity)
    this._room = serverRoom
    this._joinedAt = joinedAt
  }

  static isServerLocalParticipant(value: unknown): value is ServerLocalParticipant {
    return value !== null && typeof value === 'object' && SERVER_PARTICIPANT_BRAND in value
  }

  async publish(data: unknown, options?: PublishOptions): Promise<ChannelPublishAck> {
    // `coalesce` bounds a client's uplink under a burst; a server-side publisher has no uplink
    // queue to conflate, so it's accepted for signature parity and otherwise a no-op. `retain`
    // still applies — a server-side publisher retains exactly like a client one.
    this._assertActive()
    return await this._room._publishText(this.id, data, options?.retain)
  }

  async publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck> {
    this._assertActive()
    return await this._room._publishBinaryFramed(this.id, frameWithMemberId(this.id, data, options))
  }

  /** @internal — publish a client-framed payload (the frame already carries this member's ID). */
  _publishFramed(framed: Uint8Array): Promise<ChannelPublishAck> {
    this._assertActive()
    return this._room._publishBinaryFramed(this.id, framed)
  }

  // The impl of the overloaded `LocalParticipant.send` (see the interface for the precise result
  // types callers get); `any` is the overload-implementation signature.
  async send(to: string | Sender, data: unknown, options?: { ack?: boolean }): Promise<any> {
    this._assertActive()
    const toId = typeof to === 'string' ? to : to.id
    if (!options?.ack) return this._room._sendDm(this.id, toId, data)
    const { receipt, reply } = await this._room._sendDmAck(this.id, toId, data)
    if (!reply.ok) throw roomFailureError(reply)
    // Superset of the plain-send receipt: the outbound DM's sequencing plus the recipient's reply.
    return { ...receipt, response: reply.result } satisfies RoomAckReceipt
  }

  async setMeta(meta: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._room._setMemberMeta(this.id, meta)
    this._meta = meta
  }

  async setAttributes(attrs: ParticipantMeta): Promise<void> {
    this._assertActive()
    await this._room._mergeMemberMeta(this.id, attrs)
    this._meta = mergeAttributes(this._meta, attrs)
  }

  async leave(): Promise<void> {
    if (this._left) return
    this._left = true
    await this._room._removeMember(this.id, { type: 'left' })
    this._onLeft({ type: 'left' }) // fires even when the room wasn't observing (no echo applied)
  }

  protected override _resolveSender(id: string): Sender | null {
    return this._room._state.getRemote(id) // sync view read — delivery must not wait on I/O
  }

  protected _reportError(err: unknown): void {
    reportRoomError(err)
  }
}

// ---------------------------------------------------------------------------
// Backend state access
// ---------------------------------------------------------------------------

type CellSelector = { keys: string[] } | { prefix: string }
type CellPlan<T> = { value: T; mutations: CellMutation[] }

async function readCellSet(
  roomId: string,
  inc: string,
  selector: CellSelector,
): Promise<{ revision: string; cells: Map<string, Uint8Array> }> {
  const result = await getRoomBackend().readCells(roomId, inc, selector)
  if ('staleInc' in result) throw new RoomError(`Room is closed: ${roomId}`)
  return result
}

async function readCell(roomId: string, inc: string, key: string): Promise<Uint8Array | null> {
  const { cells } = await readCellSet(roomId, inc, { keys: [key] })
  return cells.get(key) ?? null
}

async function mutateCells<T>(
  roomId: string,
  inc: string,
  selector: CellSelector,
  plan: (cells: ReadonlyMap<string, Uint8Array>) => CellPlan<T>,
): Promise<T> {
  const backend = getRoomBackend()
  for (let attempt = 0; attempt < ROOM_CX_ATTEMPTS; attempt++) {
    const read = await backend.readCells(roomId, inc, selector)
    if ('staleInc' in read) throw new RoomError(`Room is closed: ${roomId}`)
    const next = plan(read.cells)
    if (next.mutations.length === 0) return next.value
    const result = await backend.compareExchangeCells(roomId, inc, read.revision, next.mutations)
    if (result === 'committed') return next.value
    if (result === 'stale-inc') throw new RoomError(`Room is closed: ${roomId}`)
    const ceiling = Math.min(64, 2 ** attempt)
    await new Promise((resolve) => setTimeout(resolve, Math.floor(Math.random() * ceiling) + 1))
  }
  throw new RoomError(`Room update contention: ${roomId}`)
}

async function requireRoom(id: string): Promise<{ config: RoomConfigRecord }> {
  assertRoomId(id)
  const current = await getRoomBackend().readHead(id)
  if (current === null || current.head.state !== 'open' || current.head.currentInc === null) {
    throw new RoomError(`Room not found: ${id}`)
  }
  return { config: configFromHead(current.head) }
}

/** Read a room's member records, reaping members whose owning node stopped heartbeating
 *  (hard crash): their record is deleted and their leave announced to all observers. Pass `ids`
 *  to read a specific subset (e.g. one identity's memberships) instead of scanning the whole roster. */
async function readMembers(roomId: string, inc: string, ids?: string[]): Promise<MemberSnapshot[]> {
  // Roster reads run against the authority, not the replica: `_refreshMembers` relies on read-your-
  // writes (a join/leave writes its record before publishing the event that triggers the read), and
  // the reap below keys off `seenAt` — a replica lag could drop a live member from the roster or reap
  // a member a live heartbeat just refreshed. The reap delete stays replicated so both tiers drop it.
  const memberKeys =
    ids === undefined ? await listMemberKeys(roomId, inc) : ids.map((id) => ({ key: roomMemberKvKey(roomId, id), id }))
  const { cells } = await readCellSet(roomId, inc, { keys: memberKeys.map(({ key }) => key) })
  const members: MemberSnapshot[] = []
  for (const { key, id } of memberKeys) {
    const raw = cells.get(key)
    if (raw === undefined) continue // member left concurrently
    const record = parse(decodeRoomText(raw)) as RoomMemberRecord
    if (record.inc !== inc) continue // a record from a previous incarnation — never in this roster
    if (Date.now() - record.seenAt > ROOM_MEMBER_TTL_MS) {
      const reaped = await mutateCells(roomId, inc, { keys: [key] }, (current) => {
        const latest = current.get(key)
        if (latest === undefined) return { value: false, mutations: [] }
        const latestRecord = parse(decodeRoomText(latest)) as RoomMemberRecord
        return Date.now() - latestRecord.seenAt > ROOM_MEMBER_TTL_MS
          ? { value: true, mutations: [{ key }] }
          : { value: false, mutations: [] }
      })
      if (reaped) await publishCtrl(roomId, inc, { __r: 'leave', id, cause: 'disconnected' })
      continue
    }
    members.push({
      id,
      meta: record.meta,
      joinedAt: record.joinedAt,
      metaSeq: record.metaSeq,
      identity: record.identity ?? null,
      ...(record.tracks === undefined ? {} : { tracks: record.tracks }),
      ...(record.hidden ? { hidden: true } : {}),
    })
  }
  return members
}

/** Keys of a room's member records. Member IDs are UUIDs — anything else under the prefix
 *  belongs to another room whose ID happens to start with `${roomId}:m:` (e.g. its `:config` key). */
async function listMemberKeys(roomId: string, inc: string): Promise<Array<{ key: string; id: string }>> {
  const prefix = roomMemberKvPrefix(roomId)
  const memberKeys: Array<{ key: string; id: string }> = []
  const { cells } = await readCellSet(roomId, inc, { prefix })
  for (const key of cells.keys()) {
    const id = key.slice(prefix.length)
    if (uuidToBytes(id)) memberKeys.push({ key, id })
  }
  return memberKeys
}

/** Presence member count for a lazy seed (`Room.get`, `Room.list`): total members minus the
 *  off-presence ones (`join({ hidden })`). Scan-only — the hidden-marker index lets it exclude
 *  hidden members without reading a single record. Leak-robust: a stale marker whose member is
 *  already gone is ignored (only present member ids are excluded), and a member not yet marked
 *  counts as present (the safe direction). */
async function presenceCount(roomId: string, inc: string): Promise<number> {
  const members = await listMemberKeys(roomId, inc)
  if (members.length === 0) return 0
  const hidden = await listHiddenMemberIds(roomId, inc)
  if (hidden.size === 0) return members.length
  let count = 0
  for (const { id } of members) if (!hidden.has(id)) count++
  return count
}

/** The member IDs a room currently marks off-presence (`join({ hidden })`). */
async function listHiddenMemberIds(roomId: string, inc: string): Promise<Set<string>> {
  const prefix = roomHiddenMemberKvPrefix(roomId)
  const ids = new Set<string>()
  const { cells } = await readCellSet(roomId, inc, { prefix })
  for (const key of cells.keys()) ids.add(key.slice(prefix.length))
  return ids
}

// ── Identity index ──────────────────────────────────────────────────────────
// The (room, identity)→members index is a hint: one marker key per membership (so concurrent
// same-identity joins never clobber and each membership stays independently removable, instead of
// every membership of an identity contending on one shared list value), written before the member
// record and cleared after it. So it may briefly over-include but never silently under-include;
// resolveIdentityMembers() confirms each marker against its member record, making a stale marker
// resolve to nothing. Only server-side statics need it, so it never touches the client wire.

/** Every live member ID of an identity in a room — read O(memberships-of-identity) from the index
 *  (not O(roster)), each confirmed against its member record; a stale marker is pruned, not returned. */
async function resolveIdentityMembers(roomId: string, inc: string, identity: string): Promise<string[]> {
  const prefix = roomIdentityKvPrefix(roomId, identity)
  const members: string[] = []
  // Authority-read the markers and their records: this resolves the targets of `removeParticipant`/
  // `Room.send` by identity, and a replica lag on a just-joined membership would drop it here (or
  // false-prune its marker). The prune delete stays replicated so both tiers drop a stale marker.
  const markers = await readCellSet(roomId, inc, { prefix })
  for (const key of markers.cells.keys()) {
    const memberId = key.slice(prefix.length)
    const memberKey = roomMemberKvKey(roomId, memberId)
    const raw = await readCell(roomId, inc, memberKey)
    if (raw !== null && (parse(decodeRoomText(raw)) as RoomMemberRecord).identity === identity) {
      members.push(memberId)
      continue
    }
    await mutateCells(roomId, inc, { keys: [key, memberKey] }, (cells) => {
      const current = cells.get(memberKey)
      const stillStale =
        current === undefined || (parse(decodeRoomText(current)) as RoomMemberRecord).identity !== identity
      return { value: undefined, mutations: stillStale ? [{ key }] : [] }
    })
  }
  return members
}

/** Delete the room's single retained-text slot iff `memberId` still owns it (the stored envelope's
 *  `from`). One atomic compare-delete: if a newer retained message from someone else has replaced the
 *  slot since, it's left untouched. Called on a member's leave and kick so a departed member's pinned
 *  message doesn't outlive them — symmetric with retained binary — and a ghost's meta/identity stops
 *  replaying to every late joiner. Durable room-level state belongs on a hidden participant, which
 *  never leaves. */
async function dropRetainedTextOwnedBy(roomId: string, inc: string, memberId: string): Promise<void> {
  const backend = getRoomBackend()
  const retained = await backend.readRetained(roomId, inc, SEMANTIC_LANE)
  if (retained === null) return
  const envelope = parse(decodeRoomText(retained.payload)) as RoomDataEnvelope
  if (envelope.from !== memberId) return
  await backend.deleteRetained(roomId, inc, SEMANTIC_LANE, { ifSeq: retained.seq })
}

/** Remove one member from backend cells — its record, identity marker (if any), and hidden marker —
 *  then announce the leave.
 *  The admin-side counterpart to `_removeMember` (which also applies the leave to a live view). */
async function evictMember(
  roomId: string,
  inc: string,
  memberId: string,
  identity: string | undefined,
  cause: LeaveCause,
): Promise<void> {
  const keys = [roomMemberKvKey(roomId, memberId), roomHiddenMemberKvKey(roomId, memberId)]
  if (identity !== undefined) keys.push(roomIdentityMemberKvKey(roomId, identity, memberId))
  await mutateCells(roomId, inc, { keys }, () => ({
    value: undefined,
    mutations: keys.map((key) => ({ key })),
  }))
  // Drop the kicked member's retained frames too (a kick doesn't run `_removeMember`). Binary is per
  // (member, track);
  // text is the room's one slot, cleared only if this member still owns it.
  const backend = getRoomBackend()
  for (const lane of await backend.listRetained(roomId, inc)) {
    if (lane.kind === 'binary' && lane.member === memberId) await backend.deleteRetained(roomId, inc, lane)
  }
  await dropRetainedTextOwnedBy(roomId, inc, memberId)
  await publishCtrl(roomId, inc, { __r: 'leave', id: memberId, ...leaveCauseToWire(cause) })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** One backend lane subscription, reconciled to a desired on/off state. */
class SubSlot {
  private _subscription: LaneSubscription | null = null

  get active(): boolean {
    return this._subscription !== null
  }

  /** Resolves once the current subscription is live at the backend — immediately when inactive or when
   *  the backend is synchronous (in-memory). A retained replay awaits this before reading the retained
   *  value, so a publish racing the just-issued subscribe reaches the node (or rides the retained copy)
   *  instead of slipping through the gap between subscribing and the read. */
  get ready(): Promise<void> {
    return this._subscription?.ready ?? Promise.resolve()
  }

  sync(want: boolean, subscribe: () => LaneSubscription): void {
    if (want && !this._subscription) {
      this._subscription = subscribe()
    } else if (!want && this._subscription) {
      const subscription = this._subscription
      this._subscription = null
      void subscription.unsubscribe()
    }
  }
}

async function publishCtrl(roomId: string, inc: string, event: RoomCtrlEnvelope): Promise<void> {
  const committed = await commitRoomLane(roomId, inc, CONTROL_LANE, encodeRoomText(stringify(event)))
  if (committed === null) throw new RoomError(`Room is closed: ${roomId}`)
}

function assertRoomId(id: unknown): asserts id is string {
  assertUsage(typeof id === 'string' && id.length > 0, 'The room ID should be a non-empty string')
  // Bounded because the ID is URL-encoded into every KV/pub-sub key; keeps the worst-case key within
  // backend limits and closes an unbounded-ID resource vector.
  assertUsage(
    new TextEncoder().encode(id).length <= ROOM_ID_MAX_BYTES,
    `The room ID should be at most ${ROOM_ID_MAX_BYTES} bytes`,
  )
}

/** Identity is trusted — validate the server-side join option. */
function normalizeIdentity(options: JoinOptions | undefined): string | null {
  if (options?.identity === undefined) return null
  assertUsage(
    typeof options.identity === 'string' && options.identity.length > 0,
    'join() options.identity should be a non-empty string',
  )
  return options.identity
}

/** Server-side only, like `identity`: a client `join()` never reads this option, so it can't hide
 *  itself from the room. */
function normalizeHidden(options: JoinOptions | undefined): boolean {
  if (options?.hidden === undefined) return false
  assertUsage(typeof options.hidden === 'boolean', 'join() options.hidden should be a boolean')
  return options.hidden
}

function normalizeOptions(options: RoomOptions | undefined): { meta: RoomMeta } {
  assertUsage(options === undefined || isObject(options), 'Room options should be an object')
  const meta = options?.meta ?? {}
  assertUsage(isObject(meta), 'options.meta should be an object')
  return { meta }
}

function reportRoomError(err: unknown): void {
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}
