export { Room, ServerRoom, ServerLocalParticipant, reportRoomError }

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { handleTelefunctionBug } from '../../node/server/runTelefunc/validateTelefunctionError.js'
import { assert, assertUsage } from '../../utils/assert.js'
import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { isObject } from '../../utils/isObject.js'
import { unrefTimer } from '../../utils/unrefTimer.js'
import { makePublishInfo, type ChannelPublishAck, type ChannelPublishInfo } from '../channel.js'
import { ROOM_HEARTBEAT_INTERVAL_MS, ROOM_MEMBER_KV_TTL_MS, ROOM_MEMBER_TTL_MS } from '../constants.js'
import { getBroadcastAdapter, type BroadcastAdapter, type BroadcastPublishResult } from '../server/broadcast.js'
import { encodePublishBinary, encodePublishText, type WirePublishInfo } from '../shared-ws.js'
import {
  ROOM_KEY_NAMESPACE,
  RoomError,
  roomFailureError,
  leaveCauseFromWire,
  leaveCauseToWire,
  stampNewer,
  frameWithMemberId,
  hasRoomTag,
  mergeAttributes,
  normalizeJoinOptions,
  roomConfigKvKey,
  roomDmKey,
  roomIdFromConfigKey,
  roomCtrlKey,
  roomTextKey,
  roomMemberDataKey,
  roomMemberTrackKey,
  roomMemberKvKey,
  roomMemberKvPrefix,
  roomIdentityMemberKvKey,
  roomIdentityKvPrefix,
  roomIdentityRoomKvPrefix,
  roomRetainedTextKey,
  roomRetainedBinaryKey,
  roomRetainedBinaryPrefix,
  bytesToBase64,
  base64ToBytes,
  unframeMemberId,
  uuidToBytes,
  DEFAULT_TRACK,
  emptyTrackWants,
  mergeTrackWants,
  binaryWantsCovers,
  sanitizeBinaryWants,
  wantsAnyBinary,
  type BinaryWants,
  type MemberWants,
  type TrackWants,
  type MemberSnapshot,
  type RoomConfigRecord,
  type RoomCtrlEnvelope,
  type RoomDataEnvelope,
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

/** How many times a member-meta write re-asserts against a racing heartbeat before deferring to the
 *  event stream — small: the heartbeat writes a member at most once per tick, so one re-assert wins. */
const MEMBER_META_WRITE_MAX_ATTEMPTS = 3

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
  /** Admin: replace the room's metadata wholesale (`isolated` is fixed at creation). The
   *  room-level counterpart to `LocalParticipant.setMeta`. */
  setMeta(id: string, meta: RoomMeta): Promise<void>
  /** Admin: merge the room's metadata per key — provided keys replace, omitted keys keep their
   *  value, a key set to `undefined` is removed (`isolated` untouched). The room-level
   *  counterpart to `LocalParticipant.setAttributes`. */
  setAttributes(id: string, attributes: RoomMeta): Promise<void>
  /** Admin: close the room — disconnects all participants and removes the room. */
  close(id: string): Promise<void>
  /** Admin: remove a participant — one membership by `{ id }` (throws when unknown), or every
   *  membership of an app identity at once (`{ identity }`, an idempotent sweep: kicking a user removes
   *  all their tabs/connections, and 0 matches is fine). `reason` rides in the same descriptor and
   *  travels with the removal — the kicked participant's `onLeave` receives `{ type: 'removed', reason }`,
   *  so "why" never races the removal. */
  removeParticipant(id: string, target: ParticipantRef & { reason?: unknown }): Promise<void>
  /** Publish a room-authored message — no sender, delivered to `onAnnounce()` (e.g. system notices). */
  announce(id: string, data: unknown): Promise<void>
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

async function createRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const { meta } = normalizeOptions(options)
  const kv = getRoomKV()
  if ((await readConfig(kv, id)) !== null) throw new RoomError(`Room already exists: ${id}`)
  const config: RoomConfigRecord = {
    meta,
    isolated: options?.isolated === true,
    at: Date.now(),
    by: writerId(),
  }
  await kv.set(roomConfigKvKey(id), stringify(config))
  return new ServerRoom(id, config, { members: [] }) // fresh room — the roster is known: empty
}

async function getRoom(id: string, options?: RoomGetOptions): Promise<Room> {
  const { kv, config } = await requireRoom(id)
  // One keys scan for the count — but no per-member reads: the roster itself loads lazily,
  // on the first observation that needs it.
  const count = (await listMemberKeys(kv, id)).length
  const room = new ServerRoom(id, config, { count })
  room._tail = options?.tail === true
  return room
}

async function getOrCreateRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  if ((await readConfig(getRoomKV(), id)) !== null) return await getRoom(id)
  try {
    return await createRoom(id, options)
  } catch (err) {
    // Lost the create race — the room exists now. Anything else (KV failure) rethrows.
    if ((await readConfig(getRoomKV(), id)) === null) throw err
    return await getRoom(id)
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
  const kv = getRoomKV()
  const rooms: RoomInfo[] = []
  for (const key of await kv.keys(ROOM_KEY_NAMESPACE + (options?.prefix ?? ''))) {
    const id = roomIdFromConfigKey(key)
    if (id === null) continue
    const config = await readConfig(kv, id)
    if (config === null) continue // closed concurrently
    const count = (await listMemberKeys(kv, id)).length // scan only — no per-member reads
    rooms.push({ id, meta: config.meta, count, isEmpty: count === 0 })
  }
  return rooms
}

async function setRoomMeta(id: string, meta: RoomMeta): Promise<void> {
  assertUsage(isObject(meta), 'Room.setMeta() meta should be an object')
  const { kv, config } = await requireRoom(id)
  await writeRoomConfig(id, kv, config, meta)
}

/** Merge into the room's metadata per key — provided keys replace, omitted keys keep their value,
 *  a key set to `undefined` is removed (`isolated` untouched). The admin counterpart to
 *  `LocalParticipant.setAttributes`: one changed field is one small write, not a whole-`meta` resend. */
async function setRoomAttributes(id: string, attributes: RoomMeta): Promise<void> {
  assertUsage(isObject(attributes), 'Room.setAttributes() attributes should be an object')
  const { kv, config } = await requireRoom(id)
  await writeRoomConfig(id, kv, config, mergeAttributes(config.meta, attributes))
}

/** Commit a room-config change and converge it. The stamp is strictly after the config it derives
 *  from (hybrid-clock): one writer's back-to-back writes always order, cross-writer ties break
 *  wall-clock last-writer-wins. Everywhere the `update` event reaches converges on the `(at, by)`
 *  stamp; the KV *write*, though, races — so read back and, if a stamp-losing write landed on top,
 *  re-assert (only the winner rewrites, so the exchange terminates). Shared by `setMeta()` (replace)
 *  and `setAttributes()` (merge). */
async function writeRoomConfig(id: string, kv: RoomKV, config: RoomConfigRecord, meta: RoomMeta): Promise<void> {
  const at = Math.max(Date.now(), config.at + 1)
  const next: RoomConfigRecord = { meta, isolated: config.isolated, at, by: writerId() }
  await kv.set(roomConfigKvKey(id), stringify(next))
  await publishCtrl(id, { __r: 'update', meta, prev: config.meta, at: next.at, by: next.by })
  const readBack = await readConfig(kv, id)
  if (readBack !== null && stampNewer(next, readBack)) await kv.set(roomConfigKvKey(id), stringify(next))
}

async function closeRoom(id: string): Promise<void> {
  const { kv } = await requireRoom(id)
  // Event first so observers disconnect promptly; then KV cleanup. A join racing the
  // cleanup re-checks the config after writing its member record and rolls back.
  await publishCtrl(id, { __r: 'closed' })
  for (const { key } of await listMemberKeys(kv, id)) await kv.delete(key)
  for (const key of await kv.keys(roomIdentityRoomKvPrefix(id))) await kv.delete(key)
  for (const key of await kv.keys(roomRetainedBinaryPrefix(id))) await kv.delete(key)
  await kv.delete(roomRetainedTextKey(id))
  await kv.delete(roomConfigKvKey(id))
}

/** The `(memberId, identity)` pairs a `ParticipantRef` addresses — shared by `Room.send()` and
 *  `Room.removeParticipant()`. `{ id }` is one membership and must exist (throws otherwise); `{ identity }`
 *  is every membership of that identity, resolved from the identity index in O(memberships) rather than a
 *  full-roster scan (0 matches is fine — an idempotent sweep, a no-op DM). */
async function resolveParticipantRef(
  kv: RoomKV,
  roomId: string,
  target: ParticipantRef,
): Promise<{ memberId: string; identity: string | undefined }[]> {
  if ('id' in target) {
    assertUsage(
      typeof target.id === 'string' && target.id.length > 0,
      'The participant { id } should be a non-empty string',
    )
    const raw = await kv.get(roomMemberKvKey(roomId, target.id))
    if (raw === null) throw new RoomError(`Participant not found: ${target.id}`)
    return [{ memberId: target.id, identity: (parse(raw) as RoomMemberRecord).identity }]
  }
  assertUsage(
    isObject(target) && typeof target.identity === 'string' && target.identity.length > 0,
    'The participant ref should be { id } or { identity }',
  )
  const { identity } = target
  return (await resolveIdentityMembers(kv, roomId, identity)).map((memberId) => ({ memberId, identity }))
}

async function removeParticipant(id: string, target: ParticipantRef & { reason?: unknown }): Promise<void> {
  const cause: LeaveCause =
    target.reason === undefined ? { type: 'removed' } : { type: 'removed', reason: target.reason }
  const { kv } = await requireRoom(id)
  for (const { memberId, identity } of await resolveParticipantRef(kv, id, target)) {
    await evictMember(kv, id, memberId, identity, cause)
  }
}

/** Server-side snapshot read of a room's participants (`Room.getParticipants`) — no live view, no
 *  subscription, unlike the instance `room.getParticipants()`. Omit `target` for the whole roster;
 *  pass `{ identity }` to read one identity's memberships in O(memberships) via the identity index —
 *  the cheap "is this user present / what's their status" read without loading the roster. */
async function getRoomParticipants(id: string, target?: { identity: string }): Promise<ParticipantSnapshotView[]> {
  const { kv } = await requireRoom(id)
  let members: MemberSnapshot[]
  if (target === undefined) {
    members = await readMembers(kv, id)
  } else {
    assertUsage(
      isObject(target) && typeof target.identity === 'string' && target.identity.length > 0,
      'Room.getParticipants() target should be { identity }',
    )
    members = await readMembers(kv, id, await resolveIdentityMembers(kv, id, target.identity))
  }
  return members
    .filter((m) => !m.hidden) // hidden participants aren't presence participants
    .map((m) => ({ id: m.id, identity: m.identity ?? null, meta: m.meta, joinedAt: m.joinedAt }))
}

async function announceToRoom(id: string, data: unknown): Promise<void> {
  await requireRoom(id)
  await getBroadcastAdapter().publish(roomCtrlKey(id), stringify({ __r: 'announce', data } satisfies RoomEnvelope))
}

async function sendToParticipant(id: string, target: ParticipantRef, data: unknown): Promise<void> {
  const { kv } = await requireRoom(id)
  // `{ id }` is one participant; `{ identity }` fans out to every membership (tabs, connections).
  for (const { memberId } of await resolveParticipantRef(kv, id, target)) {
    await sendServerDm(id, memberId, data)
  }
}

/** Publish a server-authored DM to one member's inbox. An empty `from` marks it server-authored —
 *  clients can't spoof it (their DMs are validated against members joined through their own stub). */
async function sendServerDm(roomId: string, memberId: string, data: unknown): Promise<void> {
  const envelope: RoomDmEnvelope = { __r: 'dm', to: memberId, from: '', fromMeta: null, data }
  await getBroadcastAdapter().publish(roomDmKey(roomId, memberId), stringify(envelope))
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
 * All state changes flow through the room's pub/sub key: the node causing a change applies it
 * locally and publishes it; every other observing node — and this node's own echo — applies it
 * through its adapter subscription. Application is idempotent, so the overlap is harmless.
 */
class ServerRoom implements Room {
  readonly [SERVER_ROOM_BRAND] = true

  /** @internal */ readonly _isolated: boolean
  /** @internal — when true, serializing this room starts relaying text immediately (buffered
   *  pre-peer), so a history read after `Room.get(id, { tail: true })` misses no live message. */
  _tail = false
  /** Set once this node stores any retained binary frame, so a member's leave only pays the
   *  retained-frame cleanup in rooms that actually use `publishBinary(…, { retain: true })`. */
  private _hasRetainedBinary = false
  /** In-flight `send(…, { ack: true })`s awaiting the recipient's reply, keyed by `ackId`. `to` is
   *  the recipient, so a leave/close can fail the ones it strands. Empty at steady state. */
  private readonly _pendingDmAcks = new Map<string, { to: string; settle: (reply: DmReply) => void }>()
  private _guards: RoomGuards | null = null
  /** @internal */ readonly _state: RoomState
  private readonly _stubs = new Set<RoomStubChannel>()
  private readonly _localParticipants = new Map<string, ServerLocalParticipant>()

  private readonly _ctrlSub = new SubSlot()
  private readonly _textSub = new SubSlot()
  private readonly _memberTextUnsubs = new Map<string, () => void>()
  /** Upstream binary subscriptions, keyed by the full adapter key — one per wanted (member, track). */
  private readonly _binaryKeyUnsubs = new Map<string, () => void>()
  private readonly _dmUnsubs = new Map<string, () => void>()
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
    this._isolated = config.isolated
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
      (event) => void publishCtrl(roomId, { __r: 'want', ...event }).catch(reportRoomError),
      (id) => this._ownsMember(id),
      (member, track, count) => this._deliverDemand(member, track, count),
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

  onChange(callback: () => void): () => void {
    return this._state.onChange(callback)
  }

  snapshot(): RoomSnapshotView {
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
    await publishCtrl(this.id, {
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

  /** KV half of a join, guarding against a concurrent `Room.close()`. */
  private async _createMember(
    id: string,
    meta: ParticipantMeta,
    identity: string | null,
    hidden = false,
  ): Promise<number> {
    const kv = getRoomKV()
    await this._assertOpen(kv)
    const joinedAt = Date.now()
    const record: RoomMemberRecord = {
      meta,
      joinedAt,
      seenAt: joinedAt,
      metaSeq: 0,
      ...(identity === null ? {} : { identity }),
      ...(hidden ? { hidden: true } : {}),
    }
    // Index the membership before writing its record — never after, so a reader can't miss a live
    // member (an orphan marker is harmless; see resolveIdentityMembers).
    if (identity !== null) {
      await kv.set(roomIdentityMemberKvKey(this.id, identity, id), '', { ttlMs: ROOM_MEMBER_KV_TTL_MS })
    }
    await kv.set(roomMemberKvKey(this.id, id), stringify(record), { ttlMs: ROOM_MEMBER_KV_TTL_MS })
    // The room may have been closed between the check and the write — roll back.
    if ((await readConfig(kv, this.id)) === null) {
      await kv.delete(roomMemberKvKey(this.id, id))
      if (identity !== null) await kv.delete(roomIdentityMemberKvKey(this.id, identity, id))
      throw new RoomError(`Room is closed: ${this.id}`)
    }
    return joinedAt
  }

  /** @internal */
  async _removeMember(id: string, cause: LeaveCause): Promise<void> {
    if (this._state.closed) return // close() already removed everyone
    const kv = getRoomKV()
    const identity = this._state.getRemote(id)?.identity ?? null
    await kv.delete(roomMemberKvKey(this.id, id)) // record first — a lingering marker resolves to nothing
    if (identity !== null) await kv.delete(roomIdentityMemberKvKey(this.id, identity, id))
    // A leaving member's per-track streams end, so their retained frames go too — read before the
    // leave applies, while the member's track set is still known. Room close prefix-sweeps the rest.
    if (this._hasRetainedBinary) await this._dropRetainedBinary(id)
    this._applyLeave(id, cause)
    await publishCtrl(this.id, { __r: 'leave', id, ...leaveCauseToWire(cause) })
  }

  /** Delete a member's retained binary frames — the default lane plus every named track they
   *  published (from the live track set, no KV read). Deleting an absent key is a no-op. */
  private async _dropRetainedBinary(id: string): Promise<void> {
    const kv = getRoomKV()
    await kv.delete(roomRetainedBinaryKey(this.id, id, DEFAULT_TRACK))
    for (const track of this._state.memberTracks(id)) await kv.delete(roomRetainedBinaryKey(this.id, id, track))
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
    const kv = getRoomKV()
    await this._assertOpen(kv)
    const memberKey = roomMemberKvKey(this.id, id)
    const raw = await kv.get(memberKey)
    if (raw === null) throw new RoomError(`Participant not found (left?): ${id}`)
    const record = parse(raw) as RoomMemberRecord
    const prev = this._state.getRemote(id)?.meta ?? record.meta
    const meta = computeMeta(record.meta)
    // The member's single owner serializes its meta writes, so the KV record doubles as
    // the revision counter — no separate sequencer needed.
    const next: RoomMemberRecord = { ...record, meta, metaSeq: record.metaSeq + 1, seenAt: Date.now() }
    await kv.set(memberKey, stringify(next), { ttlMs: ROOM_MEMBER_KV_TTL_MS })
    // Read-back re-assert: a concurrent `_heartbeatTick` rewrites the whole record to bump `seenAt`
    // and, if it read the record before this write, its write carries a stale `meta`/`metaSeq` that
    // reverts ours in KV. Re-assert until a record at least this revision is present (bounded — the
    // `writeRoomConfig`/`_ensureTrackAnnounced` discipline). The `p-meta` event below is the real
    // convergence; this keeps the KV record from serving stale meta to a fresh loader.
    for (let attempt = 0; attempt < MEMBER_META_WRITE_MAX_ATTEMPTS; attempt++) {
      const readBack = await kv.get(memberKey)
      if (readBack === null || (parse(readBack) as RoomMemberRecord).metaSeq >= next.metaSeq) break
      await kv.set(memberKey, stringify(next), { ttlMs: ROOM_MEMBER_KV_TTL_MS })
    }
    this._state.applyParticipantMeta(id, meta, prev, next.metaSeq)
    await publishCtrl(this.id, { __r: 'p-meta', id, meta, prev, seq: next.metaSeq })
  }

  /** @internal — publish a member's text message. The sender's verified meta/identity are
   *  stamped into the envelope here — never client-supplied. Text rides the room's text key,
   *  or the member's own key in isolated mode. `retain` stores the message as the room's one
   *  retained-text slot (MQTT-style), replayed to any later text subscriber (see `_replayRetainedText`). */
  async _publishText(from: string, data: unknown, retain = false): Promise<ChannelPublishAck> {
    const sender = await this._admitPublish(from, data)
    const envelope: RoomDataEnvelope = {
      __r: 'data',
      from,
      fromMeta: sender.meta,
      ...(sender.identity === null ? {} : { fromIdentity: sender.identity }),
      data,
    }
    const serialized = stringify(envelope)
    // Store before publishing live, so a subscriber that arrives around now is never left with a
    // gap: it either receives the live message (subscribed in time) or replays it (subscribed after
    // the store). The reverse order could drop it in the window between publish and store.
    if (retain) await getRoomKV().set(roomRetainedTextKey(this.id), serialized)
    return this._finishPublish(
      sender,
      data,
      getBroadcastAdapter().publish(
        this._isolated ? roomMemberDataKey(this.id, from) : roomTextKey(this.id),
        serialized,
      ),
    )
  }

  /** @internal — publish a member's binary frame (`[16-byte member ID][flags][…]`, validated at
   *  its entry point — the unframe cannot fail). Binary rides per-publisher keys — per
   *  (publisher, track) for named tracks: that's what makes delivery track-selective at the
   *  source, so `receivers: 0` on the ack truthfully means "nobody anywhere wants this track". */
  async _publishBinaryFramed(from: string, framed: Uint8Array): Promise<ChannelPublishAck> {
    const frame = unframeMemberId(framed)!
    // The guard sees exactly what a subscriber would: the payload, without the wire frame.
    const sender = await this._admitPublish(from, frame.payload)
    if (frame.track !== null) await this._ensureTrackAnnounced(from, frame.track)
    // Retained per (member, track), MQTT-style: replayed to any later subscriber of that lane (see
    // `_replayRetainedBinary`). Stored base64 (KV is string-only) before the live publish, so the
    // frame is never lost in the publish→store window. No TTL — like the room config record and the
    // retained-text slot, it's reaped by lifecycle (the publisher's leave, or room close), never by
    // expiry, so text and binary retention behave identically.
    if (frame.retain) {
      this._hasRetainedBinary = true
      await getRoomKV().set(roomRetainedBinaryKey(this.id, from, frame.track ?? DEFAULT_TRACK), bytesToBase64(framed))
    }
    return this._finishPublish(
      sender,
      frame.payload,
      getBroadcastAdapter().publishBinary(
        frame.track === null ? roomMemberDataKey(this.id, from) : roomMemberTrackKey(this.id, from, frame.track),
        framed,
      ),
    )
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
    publishing: BroadcastPublishResult | Promise<BroadcastPublishResult>,
  ): Promise<ChannelPublishAck> {
    const result = await publishing
    const ack = Object.assign(makePublishInfo(this.id, result.seq, result.timestamp), {
      meta: result.meta,
      ...(result.receivers === undefined ? {} : { receivers: result.receivers }),
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

  /** First frame on a new (member, track): record the track on the member's KV record (late
   *  observers discover it from the roster) and announce it on the control lane (live all-track
   *  subscribers bring the key subscription up) — both strictly before the frame. Idempotent
   *  across owner incarnations via the KV record; O(1) per further frame via `_announcedTracks`. */
  private async _ensureTrackAnnounced(from: string, track: string): Promise<void> {
    let announced = this._announcedTracks.get(from)
    if (announced?.has(track)) return
    if (!announced) {
      announced = new Set()
      this._announcedTracks.set(from, announced)
    }
    const kv = getRoomKV()
    const memberKey = roomMemberKvKey(this.id, from)
    while (true) {
      const raw = await kv.get(memberKey)
      if (raw === null) throw new RoomError(`Participant not found (left?): ${from}`)
      const record = parse(raw) as RoomMemberRecord
      if (record.tracks?.includes(track)) break // a previous owner incarnation recorded it
      const next: RoomMemberRecord = { ...record, tracks: [...(record.tracks ?? []), track], seenAt: Date.now() }
      await kv.set(memberKey, stringify(next), { ttlMs: ROOM_MEMBER_KV_TTL_MS })
      // Read back: a concurrent record write (setMeta, heartbeat) may have clobbered the
      // append — loop until it sticks (the `writeRoomConfig` read-back discipline).
      const readBack = await kv.get(memberKey)
      if (readBack !== null && (parse(readBack) as RoomMemberRecord).tracks?.includes(track)) {
        await publishCtrl(this.id, { __r: 'track', id: from, track })
        break
      }
    }
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
    const reply = new Promise<DmReply>((settle) => this._pendingDmAcks.set(ackId, { to, settle }))
    let receipt: RoomSendReceipt
    try {
      receipt = await this._publishDm(from, to, data, ackId)
    } catch (err) {
      this._pendingDmAcks.delete(ackId)
      throw err
    }
    return { receipt, reply: await reply }
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
    const receipt = await getBroadcastAdapter().publish(roomDmKey(this.id, to), stringify(envelope))
    const info: RoomSendReceipt = { seq: receipt.seq, timestamp: receipt.timestamp }
    const onAfterSend = this._guards?.onAfterSend
    if (onAfterSend) await onAfterSend(sender, target, data, info)
    return info
  }

  /** @internal — publish an `{ ack: true }` reply back to the sender's inbox (`to` is the sender). */
  private async _publishDmAck(to: string, ackId: string, reply: DmReply): Promise<void> {
    const envelope: RoomDmAckEnvelope = { __r: 'dm-ack', to, ackId, ...reply }
    await getBroadcastAdapter().publish(roomDmKey(this.id, to), stringify(envelope))
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

  /** The member's live view — falling back to the authoritative KV record, since the local
   *  view lags while unobserved (and briefly after the observe transition, until the KV
   *  resync lands). */
  private async _resolveMember(id: string): Promise<Sender | null> {
    const remote = this._state.getRemote(id)
    if (remote) return remote
    const raw = await getRoomKV().get(roomMemberKvKey(this.id, id))
    if (raw === null) return null
    const record = parse(raw) as RoomMemberRecord
    return { id, meta: record.meta, identity: record.identity ?? null }
  }

  private async _assertOpen(kv: RoomKV): Promise<void> {
    if (this._state.closed || (await readConfig(kv, this.id)) === null) {
      throw new RoomError(`Room is closed: ${this.id}`)
    }
  }

  // ── Event stream (adapter subscription callbacks) ──

  /** The control lane: presence & lifecycle events plus announcements — relayed to every stub
   *  unconditionally, since a client's live view is only correct if it sees every one. */
  private _onCtrlMessage(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the reserved key
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

    if (event.__r === 'announce') {
      this._state.applyAnnounce(event.data, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
    } else {
      this._applyCtrl(event)
    }

    if (this._stubs.size > 0 && !serverOnly) {
      const wireText = encodePublishText(serialized, rawInfo)
      for (const stub of this._stubs) stub._relayPublishText(wireText)
    }

    if (this._state.closed && !wasClosed) this._teardown()
  }

  /** Whether a control event concerns a hidden (server-only) member and so must not reach clients —
   *  their presence never rides the roster or the control lane (see `getParticipants({ hidden })`). */
  private _hidesFromClients(event: RoomEnvelope): boolean {
    switch (event.__r) {
      case 'join':
        return event.hidden === true
      case 'leave':
      case 'p-meta':
      case 'track':
        return this._state.isHidden(event.id)
      default:
        return false // room-level events (update/announce/closed) always reach clients
    }
  }

  /** The text data lane — relayed per stub, skipping the sender's own holder when it opted out. */
  private _onTextData(serialized: string, rawInfo: WirePublishInfo): void {
    let envelope: unknown
    try {
      envelope = parse(serialized)
    } catch {
      return // junk on the reserved key
    }
    if (!hasRoomTag(envelope) || envelope.__r !== 'data') return
    const event = envelope as RoomDataEnvelope
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
        if (!stub._wantsTextFrom(event.from)) continue
        if (stub._selfSuppressed.has(event.from)) continue
        stub._relayPublishText(wireText)
      }
    }
  }

  private _onBinary(framed: Uint8Array, rawInfo: WirePublishInfo): void {
    const unframed = unframeMemberId(framed)
    if (!unframed) return // junk on the reserved key
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
      for (const stub of this._stubs) {
        if (!stub._wantsBinary(unframed.from, unframed.track ?? DEFAULT_TRACK)) continue
        if (stub._selfSuppressed.has(unframed.from)) continue
        stub._relayPublishBinary(wireData)
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
      return // junk on the reserved key
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
      if (dm.ackId) stub._pendingAckDms.set(dm.ackId, dm.from)
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
        this._state.applyRoomUpdate(event.meta, event.prev, event.at, event.by)
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

  /** @internal — called by `roomReplacer` when this room is serialized into a response. */
  _attachStub(stub: RoomStubChannel): void {
    this._stubs.add(stub)
    // Tail mode (`Room.get(id, { tail: true })`): relay text from now, before the client
    // declares a subscription, so a history read after this serialization misses nothing. The
    // frames buffer in the stub's pre-peer buffer and the client holds them until its first
    // subscribe(). `_syncSubs()` below brings up the upstream text ingestion.
    if (this._tail) stub._wantsText = true
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
        // Fire-and-forget: only an `ackId` we actually relayed to this stub is honored (forged ones
        // match nothing), which is the guard, so nothing here throws onto the (unacked) wire.
        const sender = stub._pendingAckDms.get(req.ackId)
        if (sender !== undefined) {
          stub._pendingAckDms.delete(req.ackId)
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
        this._syncSubs()
        void this._replayRetainedText(stub, stub._wantsText, prev).catch(reportRoomError)
        return undefined
      }
      default:
        return undefined
    }
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
      return await this._publishText(publish.from, publish.data, publish.retain)
    }
    const from = unframeMemberId(payload.binary)?.from
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
    const stored = await getRoomKV().get(roomRetainedTextKey(this.id))
    if (stored === null) return
    const from = (parse(stored) as RoomDataEnvelope).from
    if (prevWantsText || prevMemberWants.has(from) || !stub._wantsTextFrom(from)) return
    stub._relayPublishText(encodePublishText(stored, { seq: 0, timestamp: Date.now() }))
  }

  /** @internal — MQTT-retained replay for the binary lanes. Called when a stub's `sub-binary` want
   *  grows: replay each retained (member, track) frame this change newly covers (in the new want,
   *  not the old), so a subscriber gets the last retained frame of every lane it starts watching. The
   *  frame is self-describing, so the sender/track come from the frame itself, not the key. */
  async _replayRetainedBinary(stub: RoomStubChannel, prevWants: BinaryWants): Promise<void> {
    if (!wantsAnyBinary(stub._binaryWants)) return
    const kv = getRoomKV()
    for (const key of await kv.keys(roomRetainedBinaryPrefix(this.id))) {
      const stored = await kv.get(key)
      if (stored === null) continue
      const framed = base64ToBytes(stored)
      const frame = unframeMemberId(framed)
      if (!frame) continue
      const track = frame.track ?? DEFAULT_TRACK
      if (binaryWantsCovers(prevWants, frame.from, track) || !stub._wantsBinary(frame.from, track)) continue
      stub._relayPublishBinary(encodePublishBinary(framed, { seq: 0, timestamp: Date.now() }))
    }
  }

  // ── Adapter subscriptions ──

  /** @internal — recompute which pub/sub keys this instance needs and (un)subscribe to match.
   *  Idempotent; called after every change that can affect the answer (listeners, members,
   *  stubs, close). */
  _syncSubs(): void {
    const adapter = getBroadcastAdapter()
    const state = this._state
    const open = !state.closed
    const observed =
      this._stubs.size > 0 ||
      this._localParticipants.size > 0 ||
      state.eventListenerCount + state.dataListenerCount + state.binaryListenerCount > 0

    // Control: one low-rate lane every observer holds — it's what keeps the live view correct.
    const becomesObserved = open && observed && !this._ctrlSub.active
    this._ctrlSub.sync(open && observed, () =>
      adapter.subscribe(roomCtrlKey(this.id), (serialized, info) => this._onCtrlMessage(serialized, info)),
    )

    // Text: its own lane, brought up only for holders that actually consume messages —
    // presence-only observers never receive the room's chatter. Wants are member-selective,
    // like binary: room-level listeners want it all, participant-scoped ones only their member.
    const textWants = this._aggregateTextWants()
    const wantAnyText = open && (textWants.all || textWants.members.size > 0)
    const memberIds = open ? state.listMemberIds() : []

    // Roster loads are need-driven: a resident roster refreshes on the observe transition
    // (events between its KV read and this subscription were missed); a lazy one loads once
    // something actually needs the member view — room-level listeners (onLeave/onEmpty
    // and live senders are only correct against it) or a member-keyed lane. A holder that only
    // joins attaches neither, so `Room.join()` never loads a roster at all —
    // `getParticipants()`/serialization go through `_ensureRoster` on their own.
    const binaryWants = this._aggregateBinaryWants()
    const wantAnyBinary = open && wantsAnyBinary(binaryWants)
    const needsRoster =
      state.eventListenerCount + state.dataListenerCount + state.binaryListenerCount > 0 ||
      (this._isolated && wantAnyText) ||
      wantAnyBinary
    if ((becomesObserved && state.rosterKnown) || (open && !state.rosterKnown && needsRoster)) {
      void this._refreshMembers().catch(reportRoomError)
    }
    if (this._isolated) {
      // Isolated text rides per-member keys, so upstream delivery narrows to the wanted set.
      const textIds = !wantAnyText
        ? []
        : textWants.all
          ? memberIds
          : memberIds.filter((id) => textWants.members.has(id))
      this._syncKeyedSubs(this._memberTextUnsubs, textIds, (memberId) =>
        adapter.subscribe(roomMemberDataKey(this.id, memberId), (serialized, info) =>
          this._onTextData(serialized, info),
        ),
      )
    } else {
      // One shared key — the node ingests the room's text while anyone wants any of it;
      // member-selectivity is enforced at the per-stub relay.
      this._textSub.sync(wantAnyText, () =>
        adapter.subscribe(roomTextKey(this.id), (serialized, info) => this._onTextData(serialized, info)),
      )
    }

    // Binary: per-(publisher, track) keys in every mode — subscribing want-selectively at the
    // source makes upstream delivery pay-per-want, not filter-after-receive: dropping the last
    // want for a track drops its key, and the publisher's `receivers` hits 0.
    this._syncKeyedSubs(this._binaryKeyUnsubs, wantAnyBinary ? this._binaryKeys(binaryWants, memberIds) : [], (key) =>
      adapter.subscribeBinary(key, (framed, info) => this._onBinary(framed, info)),
    )

    // Demand (`onDemand`): gossip this node's local binary-demand transitions and push the
    // aggregated global count to any of our own members whose demand changed.
    this._demand.sync(open ? this._localDemandPairs(binaryWants, memberIds) : [])

    // Inbox subscriptions follow ownership, not listeners — a holder must always be
    // able to receive direct messages addressed to its members.
    this._syncKeyedSubs(this._dmUnsubs, open ? this._ownedMemberIds() : [], (memberId) =>
      adapter.subscribe(roomDmKey(this.id, memberId), (serialized, info) => this._onDm(serialized, info)),
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

  /** The adapter keys the aggregated binary wants resolve to — the exact upstream footprint.
   *  Per member: every-track wants take the default key plus each *known* track's key (named
   *  tracks are discovered — see `_ensureTrackAnnounced`); exact wants take exactly their keys,
   *  eagerly (a pub/sub key needs no existence, so named subscribers never miss a frame). */
  private _binaryKeys(wants: BinaryWants, memberIds: string[]): string[] {
    const keys: string[] = []
    for (const memberId of memberIds) {
      const memberWants = wants.members[memberId]
      const eff = memberWants ? mergeTrackWants(wants.everyMember, memberWants) : wants.everyMember
      const tracks = eff.all ? [DEFAULT_TRACK, ...this._state.memberTracks(memberId)] : eff.tracks
      for (const track of tracks) {
        keys.push(
          track === DEFAULT_TRACK ? roomMemberDataKey(this.id, memberId) : roomMemberTrackKey(this.id, memberId, track),
        )
      }
    }
    return keys
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

  /** Route a member's freshly-changed global demand count (aggregated by `RoomDemand`) to its
   *  holder — the local participant's `onDemand`, or the one client stub it joined through. */
  private _deliverDemand(member: string, track: string, count: number): void {
    const trackOut = track === DEFAULT_TRACK ? null : track
    const local = this._localParticipants.get(member)
    if (local) {
      local._onDemand(trackOut, count)
      return
    }
    for (const stub of this._stubs) {
      if (stub._stubMembers.has(member)) {
        stub._relayDemand({ __r: 'demand', member, track: trackOut, count })
        return
      }
    }
  }

  /** The text-lane twin of `_aggregateBinaryWants()` — a stub's broadcast subscription is its
   *  `all`, its `sub-text` set the member-scoped want. */
  private _aggregateTextWants(): { all: boolean; members: Set<string> } {
    const local: MemberWants = this._state.textWants()
    if (local.all) return { all: true, members: new Set() }
    const members = new Set(local.members)
    for (const stub of this._stubs) {
      if (stub._wantsText) return { all: true, members: new Set() }
      for (const id of stub._textMemberWants) members.add(id)
    }
    return { all: false, members }
  }

  /** Reconcile a map of keyed subscriptions (member IDs, adapter keys) to the wanted set. */
  private _syncKeyedSubs(subs: Map<string, () => void>, wantedKeys: string[], subscribe: (key: string) => () => void) {
    const wanted = new Set(wantedKeys)
    for (const [key, unsub] of [...subs]) {
      if (!wanted.has(key)) {
        subs.delete(key)
        unsub()
      }
    }
    for (const key of wanted) {
      if (!subs.has(key)) subs.set(key, subscribe(key))
    }
  }

  /** Resolves once the local roster is authoritative: immediately while the live view holds it
   *  (roster known and the event stream attached), else via a KV read. */
  private _ensureRoster(): Promise<void> {
    if (this._state.closed || (this._state.rosterKnown && this._ctrlSub.active)) return Promise.resolve()
    return this._refreshMembers()
  }

  /** A message from a sender the loaded roster doesn't know is a drift signal — its join event
   *  was dropped or reordered away (pub/sub is at-most-once between nodes). The message itself
   *  already delivered correctly (identity rides the envelope); this heals the *view*, so the
   *  live participant materializes and long-lived observers can't stay stale forever.
   *  Single-flight, so a burst from the same unknown sender costs one KV read. */
  private _healUnknownSender(from: string): void {
    if (!this._state.rosterKnown || this._state.getRemote(from) !== null) return
    void this._refreshMembers().catch(reportRoomError)
  }

  /** Single-flight roster refresh. A membership event landing mid-read makes the snapshot
   *  ambiguous (its KV write may or may not be in it) — re-read: joins/leaves write KV before
   *  publishing, so the next read includes the event that dirtied this one. */
  private _refreshMembers(): Promise<void> {
    this._pendingRefresh ??= (async () => {
      try {
        while (!this._state.closed) {
          const version = this._state.membershipVersion
          const members = await readMembers(getRoomKV(), this.id)
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
    const want = !this._state.closed && this._ownedMemberIds().length > 0
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
    if (this._heartbeatBusy) return // a slow KV must not pile up overlapping ticks
    this._heartbeatBusy = true
    try {
      const kv = getRoomKV()
      for (const id of this._ownedMemberIds()) {
        const memberKey = roomMemberKvKey(this.id, id)
        const raw = await kv.get(memberKey)
        if (raw === null) {
          // Reaped or kicked while this node wasn't listening — the reaper already
          // published the leave event; only the local view needs to catch up.
          this._applyLeave(id)
          continue
        }
        const record = parse(raw) as RoomMemberRecord
        await kv.set(memberKey, stringify({ ...record, seenAt: Date.now() } satisfies RoomMemberRecord), {
          ttlMs: ROOM_MEMBER_KV_TTL_MS,
        })
      }
      await readMembers(kv, this.id)
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
// KV access
// ---------------------------------------------------------------------------

/** Room state lives in the broadcast adapter's KV so every server node sees the same rooms. */
type RoomKV = Required<Pick<BroadcastAdapter, 'get' | 'set' | 'delete' | 'keys'>>

function getRoomKV(): RoomKV {
  const adapter = getBroadcastAdapter()
  const missing = (['get', 'set', 'delete', 'keys'] as const).filter((method) => !adapter[method])
  assertUsage(
    missing.length === 0,
    `The installed broadcast adapter doesn't implement ${missing.map((m) => `\`${m}()\``).join(', ')} — the KV methods required by \`Room\`.`,
  )
  return adapter as RoomKV
}

/** Statics prologue: validate the ID and load the room's config — or throw `Room not found`. */
async function requireRoom(id: string): Promise<{ kv: RoomKV; config: RoomConfigRecord }> {
  assertRoomId(id)
  const kv = getRoomKV()
  const config = await readConfig(kv, id)
  if (config === null) throw new RoomError(`Room not found: ${id}`)
  return { kv, config }
}

async function readConfig(kv: RoomKV, roomId: string): Promise<RoomConfigRecord | null> {
  const raw = await kv.get(roomConfigKvKey(roomId))
  return raw === null ? null : (parse(raw) as RoomConfigRecord)
}

/** Read a room's member records, reaping members whose owning node stopped heartbeating
 *  (hard crash): their record is deleted and their leave announced to all observers. Pass `ids`
 *  to read a specific subset (e.g. one identity's memberships) instead of scanning the whole roster. */
async function readMembers(kv: RoomKV, roomId: string, ids?: string[]): Promise<MemberSnapshot[]> {
  const memberKeys =
    ids === undefined ? await listMemberKeys(kv, roomId) : ids.map((id) => ({ key: roomMemberKvKey(roomId, id), id }))
  const members: MemberSnapshot[] = []
  for (const { key, id } of memberKeys) {
    const raw = await kv.get(key)
    if (raw === null) continue // member left concurrently
    const record = parse(raw) as RoomMemberRecord
    if (Date.now() - record.seenAt > ROOM_MEMBER_TTL_MS) {
      await kv.delete(key)
      await publishCtrl(roomId, { __r: 'leave', id, cause: 'disconnected' })
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
async function listMemberKeys(kv: RoomKV, roomId: string): Promise<Array<{ key: string; id: string }>> {
  const prefix = roomMemberKvPrefix(roomId)
  const memberKeys: Array<{ key: string; id: string }> = []
  for (const key of await kv.keys(prefix)) {
    const id = key.slice(prefix.length)
    if (uuidToBytes(id)) memberKeys.push({ key, id })
  }
  return memberKeys
}

// ── Identity index ──────────────────────────────────────────────────────────
// The (room, identity)→members index is a hint: one marker key per membership (so concurrent
// same-identity joins never clobber — the KV has no compare-and-set), written before the member
// record and cleared after it. So it may briefly over-include but never silently under-includes;
// resolveIdentityMembers() confirms each marker against its member record, making a stale marker
// resolve to nothing. Only server-side statics need it, so it never touches the client wire.

/** Every live member ID of an identity in a room — read O(memberships-of-identity) from the index
 *  (not O(roster)), each confirmed against its member record; a stale marker is pruned, not returned. */
async function resolveIdentityMembers(kv: RoomKV, roomId: string, identity: string): Promise<string[]> {
  const prefix = roomIdentityKvPrefix(roomId, identity)
  const members: string[] = []
  for (const key of await kv.keys(prefix)) {
    const memberId = key.slice(prefix.length)
    const raw = await kv.get(roomMemberKvKey(roomId, memberId))
    if (raw !== null && (parse(raw) as RoomMemberRecord).identity === identity) members.push(memberId)
    else await kv.delete(key) // the member left (or its join never committed) — prune the marker
  }
  return members
}

/** Remove one member from KV — its record, its identity marker (if any) — then announce the leave.
 *  The admin-side counterpart to `_removeMember` (which also applies the leave to a live view). */
async function evictMember(
  kv: RoomKV,
  roomId: string,
  memberId: string,
  identity: string | undefined,
  cause: LeaveCause,
): Promise<void> {
  await kv.delete(roomMemberKvKey(roomId, memberId))
  if (identity !== undefined) await kv.delete(roomIdentityMemberKvKey(roomId, identity, memberId))
  await publishCtrl(roomId, { __r: 'leave', id: memberId, ...leaveCauseToWire(cause) })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** One adapter subscription, reconciled to a desired on/off state. */
class SubSlot {
  private _unsub: (() => void) | null = null

  get active(): boolean {
    return this._unsub !== null
  }

  sync(want: boolean, subscribe: () => () => void): void {
    if (want && !this._unsub) {
      this._unsub = subscribe()
    } else if (!want && this._unsub) {
      const unsub = this._unsub
      this._unsub = null
      unsub()
    }
  }
}

async function publishCtrl(roomId: string, event: RoomCtrlEnvelope): Promise<void> {
  await getBroadcastAdapter().publish(roomCtrlKey(roomId), stringify(event))
}

function assertRoomId(id: unknown): asserts id is string {
  assertUsage(typeof id === 'string' && id.length > 0, 'The room ID should be a non-empty string')
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
