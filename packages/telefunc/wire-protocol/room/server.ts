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
  errorMessage,
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
  sizeFromWire,
  sizeToWire,
  unframeMemberId,
  uuidToBytes,
  DEFAULT_TRACK,
  emptyTrackWants,
  mergeTrackWants,
  sanitizeBinaryWants,
  wantsAnyBinary,
  type BinaryWants,
  type MemberWants,
  type TrackWants,
  type MemberSnapshot,
  type ReqJoinAck,
  type ReqOkAck,
  type RoomConfigRecord,
  type RoomCtrlEnvelope,
  type RoomDataEnvelope,
  type RoomDataPublish,
  type RoomDmEnvelope,
  type RoomEnvelope,
  type RoomMemberRecord,
  type RoomStubRequest,
} from './protocol.js'
import { RoomState } from './state.js'
import { ParticipantBase } from './participant.js'
import type { RoomStubChannel } from './stubs.js'
import type {
  BinaryPublishOptions,
  RoomBinaryListener,
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  PublishOptions,
  RemoteParticipant,
  Room as RoomInstance,
  RoomInfo,
  RoomMeta,
  RoomOptions,
  RoomGetOptions,
  RoomSnapshotView,
  RoomIdentitySnapshotView,
  JoinGuard,
  PublishGuard,
  SendGuard,
  AfterJoinHook,
  AfterPublishHook,
  AfterSendHook,
  Sender,
} from './types.js'
assertIsNotBrowser()

/** This process's identity as an LWW writer — breaks `Room.update()` timestamp ties.
 *  Minted lazily: Cloudflare Workers forbid crypto RNG in module scope (this module loads at
 *  worker startup via the serializer registry), and inside a request it's always available. */
let _writerId: string | undefined
function writerId(): string {
  _writerId ??= crypto.randomUUID()
  return _writerId
}

/** Composite key for the demand aggregation maps — `member` + a separator + `track` (the track
 *  name, or `DEFAULT_TRACK` for the plain `publishBinary()` lane). */
const DEMAND_SEP = '\u0000'
function demandKey(member: string, track: string): string {
  return member + DEMAND_SEP + track
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
  /** Shorthand for `(await Room.get(id)).join(meta, options)`. */
  join<P extends ParticipantMeta = ParticipantMeta, Pub = unknown>(
    id: string,
    meta?: P,
    options?: JoinOptions,
  ): Promise<LocalParticipant<P, Pub>>
  /** List all rooms — optionally only those whose ID starts with `prefix`. */
  list(options?: { prefix?: string }): Promise<RoomInfo[]>
  /** Admin: update the room's configuration — provided fields replace, omitted fields keep
   *  their current value (`isolated` is fixed at creation). */
  update(id: string, options: RoomOptions): Promise<void>
  /** Admin: merge the room's metadata per key — provided keys replace, omitted keys keep their
   *  value, a key set to `undefined` is removed (`size`/`isolated` untouched). The room-level
   *  counterpart to `LocalParticipant.setAttributes`. */
  setAttributes(id: string, attributes: RoomMeta): Promise<void>
  /** Admin: close the room — disconnects all participants and removes the room. */
  close(id: string): Promise<void>
  /** Admin: remove a participant — by participant ID (throws when unknown), or every membership
   *  of an app identity at once (`{ identity }`, an idempotent sweep: kicking a user removes all
   *  their tabs/connections, and 0 matches is fine). `reason` travels with the removal — the
   *  kicked participant's `onLeave` receives `{ type: 'removed', reason }`, so "why" never races
   *  the removal. */
  removeParticipant(id: string, target: string | { identity: string }, options?: { reason?: unknown }): Promise<void>
  /** Publish a room-authored message — no sender, delivered to `onAnnounce()` (e.g. system notices). */
  announce(id: string, data: unknown): Promise<void>
  /** Send a server-authored private message — arrives on `listen()` with `from: null`. Target one
   *  participant by ID (throws when unknown), or every membership of an app identity at once
   *  (`{ identity }`, resolved from the identity index; 0 matches is a no-op — a signed-out user). */
  send(id: string, target: string | { identity: string }, data: unknown): Promise<void>
}

/**
 * Multi-party rooms with presence, membership, and admin controls. Server-side entry point —
 * clients receive `Room` and `LocalParticipant` objects by returning them from telefunctions.
 *
 * ```ts
 * import { Room } from 'telefunc'
 *
 * await Room.create('lobby', { meta: { topic: 'general' }, size: 100 })
 * const lobby = await Room.get('lobby')
 * const me = await lobby.join({ name: 'Alice' })
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
  list: listRooms,
  update: updateRoom,
  setAttributes: setRoomAttributes,
  close: closeRoom,
  removeParticipant,
  announce: announceToRoom,
  send: sendToParticipant,
}

async function createRoom(id: string, options?: RoomOptions): Promise<Room> {
  assertRoomId(id)
  const { meta, size } = normalizeOptions(options)
  const kv = getRoomKV()
  if ((await readConfig(kv, id)) !== null) throw new Error(`Room already exists: ${id}`)
  const config: RoomConfigRecord = {
    meta,
    size: sizeToWire(size),
    isolated: options?.isolated === true,
    at: Date.now(),
    by: writerId(),
  }
  await kv.set(roomConfigKvKey(id), stringify(config))
  return new ServerRoom(id, config, { members: [] }) // fresh room — the roster is known: empty
}

async function getRoom(id: string, options?: RoomGetOptions): Promise<Room> {
  const { kv, config } = await requireRoom(id)
  // One keys scan for the count — `isFull` capacity gates stay correct — but no per-member
  // reads: the roster itself loads lazily, on the first observation that needs it.
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

async function joinRoom(id: string, meta?: ParticipantMeta, options?: JoinOptions): Promise<LocalParticipant> {
  // A pure joiner only wants its own participant handle — it never reads the roster, so it
  // skips even the count scan `Room.get()` pays: config read, join, done.
  const { config } = await requireRoom(id)
  return await new ServerRoom(id, config, { count: 0 }).join(meta, options)
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
    const size = sizeFromWire(config.size)
    rooms.push({ id, meta: config.meta, size, count, isEmpty: count === 0, isFull: count >= size })
  }
  return rooms
}

async function updateRoom(id: string, options: RoomOptions): Promise<void> {
  assertUsage(isObject(options), 'Room.update() options should be an object')
  const { kv, config } = await requireRoom(id)
  assertUsage(
    options.isolated === undefined || options.isolated === config.isolated,
    "A room's `isolated` mode is fixed at creation — room.update() cannot change it",
  )
  // Per-field replace — an omitted field keeps its current value, so updating the topic can
  // never silently reset a capacity (and vice versa).
  const meta = options.meta === undefined ? config.meta : options.meta
  assertUsage(isObject(meta), 'options.meta should be an object')
  let sizeWire = config.size
  if (options.size !== undefined) {
    assertUsage(
      typeof options.size === 'number' && options.size > 0 && !Number.isNaN(options.size),
      'options.size should be a positive number',
    )
    sizeWire = sizeToWire(options.size)
  }
  await writeRoomConfig(id, kv, config, meta, sizeWire)
}

/** Merge into the room's metadata per key — provided keys replace, omitted keys keep their value,
 *  a key set to `undefined` is removed (`size`/`isolated` untouched). The admin counterpart to
 *  `LocalParticipant.setAttributes`: one changed field is one small write, not a whole-`meta` resend. */
async function setRoomAttributes(id: string, attributes: RoomMeta): Promise<void> {
  assertUsage(isObject(attributes), 'Room.setAttributes() attributes should be an object')
  const { kv, config } = await requireRoom(id)
  await writeRoomConfig(id, kv, config, mergeAttributes(config.meta, attributes), config.size)
}

/** Commit a room-config change and converge it. The stamp is strictly after the config it derives
 *  from (hybrid-clock): one writer's back-to-back writes always order, cross-writer ties break
 *  wall-clock last-writer-wins. Everywhere the `update` event reaches converges on the `(at, by)`
 *  stamp; the KV *write*, though, races — so read back and, if a stamp-losing write landed on top,
 *  re-assert (only the winner rewrites, so the exchange terminates). Shared by `update()` (replace)
 *  and `setAttributes()` (merge). */
async function writeRoomConfig(
  id: string,
  kv: RoomKV,
  config: RoomConfigRecord,
  meta: RoomMeta,
  sizeWire: number | null,
): Promise<void> {
  const at = Math.max(Date.now(), config.at + 1)
  const next: RoomConfigRecord = { meta, size: sizeWire, isolated: config.isolated, at, by: writerId() }
  await kv.set(roomConfigKvKey(id), stringify(next))
  await publishCtrl(id, { __r: 'update', meta, prev: config.meta, size: sizeWire, at: next.at, by: next.by })
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
  await kv.delete(roomConfigKvKey(id))
}

async function removeParticipant(
  id: string,
  target: string | { identity: string },
  options?: { reason?: unknown },
): Promise<void> {
  const cause: LeaveCause =
    options?.reason === undefined ? { type: 'removed' } : { type: 'removed', reason: options.reason }
  const { kv } = await requireRoom(id)

  if (typeof target === 'string') {
    assertUsage(target.length > 0, 'The participant ID should be a non-empty string')
    const raw = await kv.get(roomMemberKvKey(id, target))
    if (raw === null) throw new Error(`Participant not found: ${target}`)
    await evictMember(kv, id, target, (parse(raw) as RoomMemberRecord).identity, cause)
    return
  }

  assertUsage(
    isObject(target) && typeof target.identity === 'string' && target.identity.length > 0,
    'removeParticipant() target should be a participant ID or { identity }',
  )
  // Identity sweep: every membership of that identity, across tabs and connections — resolved from
  // the identity index in O(memberships), not a full-roster scan.
  for (const memberId of await resolveIdentityMembers(kv, id, target.identity)) {
    await evictMember(kv, id, memberId, target.identity, cause)
  }
}

async function announceToRoom(id: string, data: unknown): Promise<void> {
  await requireRoom(id)
  await getBroadcastAdapter().publish(roomCtrlKey(id), stringify({ __r: 'announce', data } satisfies RoomEnvelope))
}

async function sendToParticipant(id: string, target: string | { identity: string }, data: unknown): Promise<void> {
  const { kv } = await requireRoom(id)
  if (typeof target === 'string') {
    if ((await kv.get(roomMemberKvKey(id, target))) === null) throw new Error(`Participant not found: ${target}`)
    await sendServerDm(id, target, data)
    return
  }
  assertUsage(
    isObject(target) && typeof target.identity === 'string' && target.identity.length > 0,
    'Room.send() target should be a participant ID or { identity }',
  )
  // Fan out to every membership of the identity (tabs, connections) — resolved from the index in
  // O(memberships), never a full-roster scan. 0 matches is a no-op.
  for (const memberId of await resolveIdentityMembers(kv, id, target.identity)) {
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
  /** Unique id tagging this instance's demand gossip, so a member's owning instance can dedupe
   *  demand reports across instances and nodes (see `onDemand`). */
  private readonly _instanceId = crypto.randomUUID()
  /** Composite key → [member, track] for the streams this instance currently has local binary
   *  demand for — diffed each `_syncSubs` to gossip 0↔>0 transitions on the control lane. */
  private _localDemand = new Map<string, [string, string]>()
  /** Owner-side demand aggregation: composite key → the OTHER instance ids reporting demand. */
  private readonly _remoteDemand = new Map<string, Set<string>>()
  /** Owner-side: composite key → the demand count last pushed to the member (change detection). */
  private readonly _pushedDemand = new Map<string, number>()
  private _heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private _heartbeatBusy = false
  private _pendingRefresh: Promise<void> | null = null

  constructor(roomId: string, config: RoomConfigRecord, seed: { members: MemberSnapshot[] } | { count: number }) {
    this._isolated = config.isolated
    this._state = new RoomState({
      roomId,
      meta: config.meta,
      size: sizeFromWire(config.size),
      seed,
      updateStamp: { at: config.at, by: config.by },
      onListenersChanged: () => this._syncSubs(),
      onCallbackError: reportRoomError,
    })
    this._state._owner = this
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
  get size(): number {
    return this._state.size
  }
  get count(): number {
    return this._state.count
  }
  get isEmpty(): boolean {
    return this._state.count === 0
  }
  get isFull(): boolean {
    return this._state.isFull
  }
  get isClosed(): boolean {
    return this._state.closed
  }

  async join(meta: ParticipantMeta = {}, options?: JoinOptions): Promise<LocalParticipant> {
    const selfDelivery = normalizeJoinOptions(meta, options)
    const identity = normalizeIdentity(options)
    let participant!: ServerLocalParticipant
    await this._admitMember(meta, identity, (id, joinedAt) => {
      participant = new ServerLocalParticipant(this, id, meta, joinedAt, selfDelivery, identity)
      this._localParticipants.set(id, participant)
    })
    return participant
  }

  async getParticipants(): Promise<RemoteParticipant[]> {
    await this._ensureRoster()
    return this._state.listRemotes()
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
  onFull(callback: () => void): () => void {
    return this._state.onFull(callback)
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

  snapshot(): RoomSnapshotView
  snapshot(options: { by: 'identity' }): RoomIdentitySnapshotView
  snapshot(options?: { by: 'identity' }): RoomSnapshotView | RoomIdentitySnapshotView {
    // Snapshot consumers want the member view — load it (need-driven, single-flight); the
    // arrival lands as an onChange, and the next snapshot() is complete.
    if (!this._state.rosterKnown) void this._ensureRoster().catch(reportRoomError)
    return options?.by === 'identity' ? this._state.identitySnapshot() : this._state.snapshot()
  }

  // ── Membership operations (shared by local participants and stub requests) ──

  /** Join choreography shared by local `join()` and stub `req-join`. `track` registers the
   *  holder first — the member must count as owned before `_syncSubs()` brings up its inbox
   *  subscription and heartbeat, and before its join is announced. */
  private async _admitMember(
    meta: ParticipantMeta,
    identity: string | null,
    track: (id: string, joinedAt: number) => void,
  ): Promise<{ id: string; joinedAt: number }> {
    const id = crypto.randomUUID()
    // Admission policy runs first, on the definitive member ID — a rejected join writes nothing.
    const onBeforeJoin = this._guards?.onBeforeJoin
    if (onBeforeJoin) await onBeforeJoin({ id, meta, identity })
    const joinedAt = await this._createMember(id, meta, identity)
    track(id, joinedAt)
    this._syncSubs()
    this._state.applyJoin(id, meta, joinedAt, identity)
    await publishCtrl(this.id, {
      __r: 'join',
      id,
      meta,
      joinedAt,
      ...(identity === null ? {} : { identity }),
    })
    // Post-commit: the member exists and its join is announced — the place for side effects.
    const onAfterJoin = this._guards?.onAfterJoin
    if (onAfterJoin) await onAfterJoin({ id, meta, identity }, { joinedAt })
    return { id, joinedAt }
  }

  /** KV half of a join, guarding against a concurrent `Room.close()`. */
  private async _createMember(id: string, meta: ParticipantMeta, identity: string | null): Promise<number> {
    const kv = getRoomKV()
    await this._assertOpen(kv)
    const joinedAt = Date.now()
    const record: RoomMemberRecord = {
      meta,
      joinedAt,
      seenAt: joinedAt,
      metaSeq: 0,
      ...(identity === null ? {} : { identity }),
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
      throw new Error(`Room is closed: ${this.id}`)
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
    this._applyLeave(id, cause)
    await publishCtrl(this.id, { __r: 'leave', id, ...leaveCauseToWire(cause) })
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
    if (raw === null) throw new Error(`Participant not found (left?): ${id}`)
    const record = parse(raw) as RoomMemberRecord
    const prev = this._state.getRemote(id)?.meta ?? record.meta
    const meta = computeMeta(record.meta)
    // The member's single owner serializes its meta writes, so the KV record doubles as
    // the revision counter — no separate sequencer needed.
    const next: RoomMemberRecord = { ...record, meta, metaSeq: record.metaSeq + 1, seenAt: Date.now() }
    await kv.set(memberKey, stringify(next), { ttlMs: ROOM_MEMBER_KV_TTL_MS })
    this._state.applyParticipantMeta(id, meta, prev, next.metaSeq)
    await publishCtrl(this.id, { __r: 'p-meta', id, meta, prev, seq: next.metaSeq })
  }

  /** @internal — publish a member's text message. The sender's verified meta/identity are
   *  stamped into the envelope here — never client-supplied. Text rides the room's text key,
   *  or the member's own key in isolated mode. */
  async _publishText(from: string, data: unknown): Promise<ChannelPublishAck> {
    const sender = await this._admitPublish(from, data)
    const envelope: RoomDataEnvelope = {
      __r: 'data',
      from,
      fromMeta: sender.meta,
      ...(sender.identity === null ? {} : { fromIdentity: sender.identity }),
      data,
    }
    return this._finishPublish(
      sender,
      data,
      getBroadcastAdapter().publish(
        this._isolated ? roomMemberDataKey(this.id, from) : roomTextKey(this.id),
        stringify(envelope),
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
    if (this._state.closed) throw new Error(`Room is closed: ${this.id}`)
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
      if (raw === null) throw new Error(`Participant not found (left?): ${from}`)
      const record = parse(raw) as RoomMemberRecord
      if (record.tracks?.includes(track)) break // a previous owner incarnation recorded it
      const next: RoomMemberRecord = { ...record, tracks: [...(record.tracks ?? []), track], seenAt: Date.now() }
      await kv.set(memberKey, stringify(next), { ttlMs: ROOM_MEMBER_KV_TTL_MS })
      // Read back: a concurrent record write (setMeta, heartbeat) may have clobbered the
      // append — loop until it sticks (the `updateRoom` read-back discipline).
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
  async _sendDm(from: string, to: string, data: unknown): Promise<void> {
    if (this._state.closed) throw new Error(`Room is closed: ${this.id}`)
    const target = await this._resolveMember(to)
    if (!target) throw new Error(`Participant not found: ${to}`)
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
    }
    const receipt = await getBroadcastAdapter().publish(roomDmKey(this.id, to), stringify(envelope))
    const onAfterSend = this._guards?.onAfterSend
    if (onAfterSend) await onAfterSend(sender, target, data, { seq: receipt.seq, timestamp: receipt.timestamp })
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
      throw new Error(`Room is closed: ${this.id}`)
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
      this._applyWant(event) // demand gossip — node-to-node only, never relayed to clients
      return
    }
    const wasClosed = this._state.closed

    if (event.__r === 'announce') {
      this._state.applyAnnounce(event.data, makePublishInfo(this.id, rawInfo.seq, rawInfo.timestamp))
    } else {
      this._applyCtrl(event)
    }

    if (this._stubs.size > 0) {
      const wireText = encodePublishText(serialized, rawInfo)
      for (const stub of this._stubs) stub._relayPublishText(wireText)
    }

    if (this._state.closed && !wasClosed) this._teardown()
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
        if (stub._stubMembers.get(event.from)?.selfDelivery === false) continue
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
      unframed.keyFrame,
      info,
      this._suppress(unframed.from),
    )
    this._healUnknownSender(unframed.from)

    if (this._stubs.size > 0) {
      const wireData = encodePublishBinary(framed, rawInfo)
      for (const stub of this._stubs) {
        if (!stub._wantsBinary(unframed.from, unframed.track ?? DEFAULT_TRACK)) continue
        if (stub._stubMembers.get(unframed.from)?.selfDelivery === false) continue
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
    if (!hasRoomTag(envelope) || envelope.__r !== 'dm') return
    const dm = envelope as RoomDmEnvelope
    const local = this._localParticipants.get(dm.to)
    if (local) {
      local._deliverMessage({
        from: dm.from,
        fromMeta: dm.fromMeta,
        fromIdentity: dm.fromIdentity ?? null,
        data: dm.data,
      })
      return
    }
    const wireText = encodePublishText(serialized, rawInfo)
    for (const stub of this._stubs) {
      if (stub._stubMembers.has(dm.to)) stub._relayPublishText(wireText)
    }
  }

  private _applyCtrl(event: RoomCtrlEnvelope): void {
    switch (event.__r) {
      case 'join':
        this._state.applyJoin(event.id, event.meta, event.joinedAt, event.identity ?? null)
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
        this._state.applyRoomUpdate(event.meta, event.prev, sizeFromWire(event.size), event.at, event.by)
        return
      case 'closed':
        this._state.applyClosed()
    }
  }

  private _applyLeave(id: string, cause?: LeaveCause): void {
    this._state.applyLeave(id, cause)
    this._announcedTracks.delete(id)
    const local = this._localParticipants.get(id)
    if (local) {
      this._localParticipants.delete(id)
      // A live-heartbeating owner can't be reaped (heartbeats outpace the TTL by 4x), so a
      // vanished record with no observed event means the member was removed.
      local._onLeft(cause ?? { type: 'removed' })
    }
    for (const stub of this._stubs) stub._stubMembers.delete(id)
    this._syncSubs()
  }

  /** The room closed — runs once, after the `closed` event has been applied and relayed. */
  private _teardown(): void {
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
          if (this._stubs.has(stub) && !this._state.closed) stub._relayRoster(this._state.snapshotMembers())
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
  async _handleStubRequest(stub: RoomStubChannel, msg: unknown): Promise<ReqJoinAck | ReqOkAck | undefined> {
    if (!hasRoomTag(msg)) return undefined
    const req = msg as RoomStubRequest
    try {
      switch (req.__r) {
        case 'req-join': {
          const meta = isObject(req.meta) ? req.meta : {}
          // Identity is trusted and therefore server-assigned — a client join never carries one.
          const { id, joinedAt } = await this._admitMember(meta, null, (id) =>
            stub._stubMembers.set(id, { selfDelivery: req.selfDelivery !== false }),
          )
          return { ok: true, id, joinedAt }
        }
        case 'req-leave':
          this._assertStubMember(stub, req.id)
          stub._stubMembers.delete(req.id)
          await this._removeMember(req.id, { type: 'left' })
          return { ok: true }
        case 'req-set-meta':
          this._assertStubMember(stub, req.id)
          await this._setMemberMeta(req.id, isObject(req.meta) ? req.meta : {})
          return { ok: true }
        case 'req-set-attrs':
          this._assertStubMember(stub, req.id)
          await this._mergeMemberMeta(req.id, isObject(req.attrs) ? req.attrs : {})
          return { ok: true }
        case 'req-dm':
          this._assertStubMember(stub, req.id)
          await this._sendDm(req.id, req.to, req.data)
          return { ok: true }
        case 'sub-binary': {
          const wants = sanitizeBinaryWants(req.wants)
          if (!wants) throw new Error('Malformed sub-binary declaration')
          stub._binaryWants = wants
          this._syncSubs()
          return { ok: true }
        }
        case 'sub-text': {
          // Member-scoped text wants — the room-level (all) want rides the broadcast-sub ctrl.
          const members = Array.isArray(req.members) ? req.members.filter((m) => typeof m === 'string') : []
          stub._textMemberWants = new Set(members)
          this._syncSubs()
          return { ok: true }
        }
        default:
          return undefined
      }
    } catch (err) {
      return { ok: false, err: errorMessage(err) }
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
      if (!hasRoomTag(envelope) || envelope.__r !== 'data') throw new Error('Malformed room publish')
      const publish = envelope as RoomDataPublish
      this._assertStubMember(stub, publish.from)
      return await this._publishText(publish.from, publish.data)
    }
    const from = unframeMemberId(payload.binary)?.from
    this._assertStubMember(stub, from)
    return await this._publishBinaryFramed(from, payload.binary)
  }

  private _assertStubMember(stub: RoomStubChannel, id: unknown): asserts id is string {
    if (typeof id !== 'string' || !stub._stubMembers.has(id)) {
      throw new Error('Not a participant of this room (joined through this connection)')
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
    // something actually needs the member view — room-level listeners (onLeave/onEmpty/onFull
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
    this._syncDemand(binaryWants, memberIds, open)

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

  /** Diff this instance's local binary demand, gossiping each 0↔>0 transition on the control lane
   *  (so a member's owning instance can aggregate global demand), and refresh the pushed count for
   *  any of our own members whose local contribution changed. */
  private _syncDemand(binaryWants: BinaryWants, memberIds: string[], open: boolean): void {
    const prev = this._localDemand
    const next = new Map<string, [string, string]>()
    if (open) {
      for (const [member, track] of this._localDemandPairs(binaryWants, memberIds)) {
        next.set(demandKey(member, track), [member, track])
      }
    }
    this._localDemand = next
    for (const [k, [member, track]] of next) {
      if (!prev.has(k)) this._onDemandTransition(member, track, true)
    }
    for (const [k, [member, track]] of prev) {
      if (!next.has(k)) this._onDemandTransition(member, track, false)
    }
  }

  private _onDemandTransition(member: string, track: string, on: boolean): void {
    void publishCtrl(this.id, { __r: 'want', member, track, node: this._instanceId, on }).catch(reportRoomError)
    if (this._ownsMember(member)) this._recomputeDemand(member, track)
  }

  /** A demand gossip from another instance/node. Recorded regardless of ownership (ownership can
   *  arrive later); only a member's owning instance pushes the resulting count to it. */
  private _applyWant(event: { member: string; track: string; node: string; on: boolean }): void {
    if (event.node === this._instanceId) return // our own gossip echoed back
    const k = demandKey(event.member, event.track)
    if (event.on) {
      let set = this._remoteDemand.get(k)
      if (!set) this._remoteDemand.set(k, (set = new Set()))
      set.add(event.node)
    } else {
      const set = this._remoteDemand.get(k)
      set?.delete(event.node)
      if (set && set.size === 0) this._remoteDemand.delete(k)
    }
    if (this._ownsMember(event.member)) this._recomputeDemand(event.member, event.track)
  }

  /** Whether one of this instance's own members is `id` — the instance that must aggregate and
   *  deliver `id`'s demand. */
  private _ownsMember(id: string): boolean {
    if (this._localParticipants.has(id)) return true
    for (const stub of this._stubs) if (stub._stubMembers.has(id)) return true
    return false
  }

  /** Global demand for one of our members' tracks = this instance's own local contribution plus
   *  the distinct other instances reporting demand. Push it to the member only when it changed. */
  private _recomputeDemand(member: string, track: string): void {
    const k = demandKey(member, track)
    const count = (this._remoteDemand.get(k)?.size ?? 0) + (this._localDemand.has(k) ? 1 : 0)
    if (this._pushedDemand.get(k) === count || (count === 0 && !this._pushedDemand.has(k))) return
    if (count === 0) this._pushedDemand.delete(k)
    else this._pushedDemand.set(k, count)
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
            for (const stub of this._stubs) stub._relayRoster(this._state.snapshotMembers())
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

  async publish(data: unknown, _options?: PublishOptions): Promise<ChannelPublishAck> {
    // `coalesce` bounds a client's uplink under a burst; a server-side publisher has no uplink
    // queue to conflate, so the option is accepted for signature parity and otherwise a no-op.
    this._assertActive()
    return await this._room._publishText(this.id, data)
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

  async send(to: string | Sender, data: unknown): Promise<void> {
    this._assertActive()
    await this._room._sendDm(this.id, typeof to === 'string' ? to : to.id, data)
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
  if (config === null) throw new Error(`Room not found: ${id}`)
  return { kv, config }
}

async function readConfig(kv: RoomKV, roomId: string): Promise<RoomConfigRecord | null> {
  const raw = await kv.get(roomConfigKvKey(roomId))
  return raw === null ? null : (parse(raw) as RoomConfigRecord)
}

/** Read a room's member records, reaping members whose owning node stopped heartbeating
 *  (hard crash): their record is deleted and their leave announced to all observers. */
async function readMembers(kv: RoomKV, roomId: string): Promise<MemberSnapshot[]> {
  const members: MemberSnapshot[] = []
  for (const { key, id } of await listMemberKeys(kv, roomId)) {
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

function normalizeOptions(options: RoomOptions | undefined): { meta: RoomMeta; size: number } {
  assertUsage(options === undefined || isObject(options), 'Room options should be an object')
  const meta = options?.meta ?? {}
  assertUsage(isObject(meta), 'options.meta should be an object')
  const size = options?.size ?? Infinity
  assertUsage(typeof size === 'number' && size > 0 && !Number.isNaN(size), 'options.size should be a positive number')
  return { meta, size }
}

function reportRoomError(err: unknown): void {
  handleTelefunctionBug(err instanceof Error ? err : new Error(String(err)))
}
