export {
  ROOM_KEY_NAMESPACE,
  roomCtrlKey,
  roomTextKey,
  roomMemberDataKey,
  roomDmKey,
  roomConfigKvKey,
  roomIdFromConfigKey,
  roomMemberKvKey,
  roomMemberKvPrefix,
  sizeToWire,
  sizeFromWire,
  uuidToBytes,
  frameWithMemberId,
  unframeMemberId,
  hasRoomTag,
  normalizeJoinOptions,
  stampNewer,
  errorMessage,
  leaveCauseFromWire,
  leaveCauseToWire,
  remoteBacking,
  RoomState,
  ParticipantBase,
}
export type {
  RoomConfigRecord,
  RoomMemberRecord,
  MemberSnapshot,
  RoomSnapshotMetadata,
  ParticipantStubMetadata,
  RoomEnvelope,
  RoomRosterEvent,
  RoomCtrlEnvelope,
  RoomDataEnvelope,
  RoomDataPublish,
  RoomAnnounceEnvelope,
  RoomDmEnvelope,
  RoomStubRequest,
  ParticipantStubRequest,
  ParticipantStubNotice,
  ReqOkAck,
  ReqJoinAck,
  ReqPublishAck,
  MemberWants,
}

import { assert, assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import type { ChannelPublishAck, ChannelPublishInfo } from '../channel.js'
import type {
  JoinOptions,
  LeaveCause,
  LocalParticipant,
  ParticipantMeta,
  RemoteParticipant,
  RoomMeta,
  Sender,
} from './types.js'

// ---------------------------------------------------------------------------
// Keys & records
// ---------------------------------------------------------------------------

/** Reserved pub/sub + KV namespace for rooms. Don't use it for `BroadcastChannel` keys. */
const ROOM_KEY_NAMESPACE = 'telefunc:room:'

/** Pub/sub key carrying a room's control lane: presence & lifecycle events plus room-authored
 *  announcements. Low-rate, subscribed by every observer — never carries participant data. */
function roomCtrlKey(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomId}`
}
/** Pub/sub key carrying the room's text data in shared mode — its own lane, so holders that
 *  only observe presence never receive it. */
function roomTextKey(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomId}:t`
}
/** Pub/sub key carrying one member's data: binary always (per-publisher keys make delivery
 *  member-selective at the source), text too in isolated mode. */
function roomMemberDataKey(roomId: string, memberId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomId}:m:${memberId}`
}
/** Pub/sub key carrying one member's private inbox — only the member's owning node subscribes. */
function roomDmKey(roomId: string, memberId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomId}:dm:${memberId}`
}
/** KV key of the room's config record. */
function roomConfigKvKey(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomId}:config`
}
/** Inverse of `roomConfigKvKey` — `null` for keys that aren't room config records. */
function roomIdFromConfigKey(key: string): string | null {
  if (!key.startsWith(ROOM_KEY_NAMESPACE) || !key.endsWith(':config')) return null
  return key.slice(ROOM_KEY_NAMESPACE.length, -':config'.length)
}
/** KV key of one member record. */
function roomMemberKvKey(roomId: string, memberId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomId}:m:${memberId}`
}
/** KV prefix under which all of a room's member records live. Member IDs are UUIDs, which is
 *  how member records are told apart from keys of other rooms whose ID shares the prefix. */
function roomMemberKvPrefix(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomId}:m:`
}

/** Stored at `roomConfigKvKey`. `size: null` encodes `Infinity` (not JSON-safe). `at`/`by` is
 *  the last-writer-wins stamp of the latest `Room.update()` (see `applyRoomUpdate`). */
type RoomConfigRecord = {
  meta: RoomMeta
  size: number | null
  isolated: boolean
  at: number
  by: string
}

/** Later timestamp wins; equal timestamps break deterministically by writer ID. */
function stampNewer(a: { at: number; by: string }, b: { at: number; by: string }): boolean {
  return a.at > b.at || (a.at === b.at && a.by > b.by)
}

/** Stored at `roomMemberKvKey`. `seenAt` is the liveness timestamp: the owning node refreshes
 *  it every `ROOM_HEARTBEAT_INTERVAL_MS`; records older than `ROOM_MEMBER_TTL_MS` are reaped. */
type RoomMemberRecord = {
  meta: ParticipantMeta
  joinedAt: number
  seenAt: number
  /** Monotonic meta revision, issued by the member's single owner — orders `p-meta` events. */
  metaSeq: number
}

function sizeToWire(size: number): number | null {
  return Number.isFinite(size) ? size : null
}
function sizeFromWire(size: number | null): number {
  return size ?? Infinity
}

// ---------------------------------------------------------------------------
// Wire shapes
// ---------------------------------------------------------------------------

type MemberSnapshot = {
  id: string
  meta: ParticipantMeta
  joinedAt: number
  metaSeq: number
}

/** Serializer metadata of a `Room` crossing the wire. Carries only scalars — the roster itself
 *  streams as a `RoomRosterEvent` once the stub's peer attaches, so serialization stays O(1)
 *  no matter how many members the room has. */
type RoomSnapshotMetadata = {
  channelId: string
  roomId: string
  meta: RoomMeta
  size: number | null
  isolated: boolean
  closed: boolean
  count: number
  /** LWW stamp of the config the snapshot reflects — seeds `applyRoomUpdate` ordering. */
  stamp: { at: number; by: string }
}

/** Serializer metadata of a `LocalParticipant` crossing the wire. */
type ParticipantStubMetadata = {
  channelId: string
  roomId: string
  id: string
  meta: ParticipantMeta
  joinedAt: number
  selfDelivery: boolean
}

/** Presence & lifecycle events, published on the room's control key by whichever node caused them.
 *
 *  The origin applies its own event locally (for deterministic same-node semantics) and then
 *  receives it back via the pub/sub echo. `join`/`leave`/`closed` are naturally idempotent;
 *  `p-meta` orders by the owner-issued `seq`, `update` by its `at`/`by` stamp — echoes and
 *  concurrent writers converge to the same winner on every node, whatever the arrival order. */
type RoomCtrlEnvelope =
  | { __r: 'join'; id: string; meta: ParticipantMeta; joinedAt: number }
  | { __r: 'leave'; id: string; cause?: 'removed' | 'disconnected'; reason?: unknown }
  | { __r: 'p-meta'; id: string; meta: ParticipantMeta; prev: ParticipantMeta; seq: number }
  | { __r: 'update'; meta: RoomMeta; prev: RoomMeta; size: number | null; at: number; by: string }
  | { __r: 'closed' }

/** A participant's message. Published on the room's text key (shared mode) or the member's own
 *  key (isolated mode). `fromMeta` is the sender's meta as verified by the sender's own node —
 *  never client-supplied — so any receiver can surface a correct sender even before its roster
 *  view catches up (see `RoomState.applyData`). */
type RoomDataEnvelope = { __r: 'data'; from: string; fromMeta: ParticipantMeta; data: unknown }

/** What a client sends upward to publish — its node verifies membership and stamps `fromMeta`. */
type RoomDataPublish = { __r: 'data'; from: string; data: unknown }

/** A room-authored message (`Room.announce()`) — no sender, delivered to `onAnnounce()`. */
type RoomAnnounceEnvelope = { __r: 'announce'; data: unknown }

type RoomEnvelope = RoomCtrlEnvelope | RoomDataEnvelope | RoomAnnounceEnvelope

/** The authoritative roster, pushed once per stub after its peer attaches — never published on
 *  the adapter. Position-in-stream consistency: every event relayed before it is already
 *  reflected in it; later events apply incrementally on top. */
type RoomRosterEvent = { __r: 'roster'; members: MemberSnapshot[] }

/** A direct message, published on the target's inbox key (`roomDmKey`) — transport-level
 *  privacy: only the target's owning node subscribes, only its holder receives the relay.
 *  `to` lets a holder of several participants route the message to the right one. */
type RoomDmEnvelope = { __r: 'dm'; to: string; from: string; fromMeta: ParticipantMeta | null; data: unknown }

/** Client→server requests on a `Room` stub channel. `id` identifies the sending participant.
 *  `sub-binary` declares which members' binary streams the client wants relayed (full replace);
 *  `sub-text` does the same for member-scoped text — the room-level (all) text want rides the
 *  standard broadcast-subscription ctrl instead, keeping its synchronous-declaration fence. */
type RoomStubRequest =
  | { __r: 'req-join'; meta: ParticipantMeta; selfDelivery: boolean }
  | { __r: 'req-leave'; id: string }
  | { __r: 'req-set-meta'; id: string; meta: ParticipantMeta }
  | { __r: 'req-dm'; id: string; to: string; data: unknown }
  | { __r: 'sub-binary'; all: boolean; members: string[] }
  | { __r: 'sub-text'; members: string[] }

/** Client→server requests on a standalone `LocalParticipant` stub channel. */
type ParticipantStubRequest =
  | { __r: 'req-publish'; data: unknown }
  | { __r: 'req-set-meta'; meta: ParticipantMeta }
  | { __r: 'req-dm'; to: string; data: unknown }
  | { __r: 'req-leave' }

/** Server→client notices on a standalone `LocalParticipant` stub channel. */
type ParticipantStubNotice =
  | { __r: 'left'; cause?: 'removed' | 'disconnected' | 'closed'; reason?: unknown }
  | { __r: 'p-meta'; meta: ParticipantMeta }
  | { __r: 'dm'; from: string; fromMeta: ParticipantMeta | null; data: unknown }

/** Which members' streams a holder wants on a data lane (text or binary) — `all` for
 *  room-level listeners, or a specific member set for participant-scoped ones. */
type MemberWants = { all: boolean; members: string[] }

type ReqOkAck = { ok: true } | { ok: false; err: string }
type ReqJoinAck = { ok: true; id: string; joinedAt: number } | { ok: false; err: string }
type ReqPublishAck = { ok: true; ack: ChannelPublishInfo } | { ok: false; err: string }

/** Decode a leave event's cause — an absent wire cause means a voluntary leave. */
function leaveCauseFromWire(event: { cause?: 'removed' | 'disconnected'; reason?: unknown }): LeaveCause {
  if (event.cause === 'removed') {
    return event.reason === undefined ? { type: 'removed' } : { type: 'removed', reason: event.reason }
  }
  return { type: event.cause === 'disconnected' ? 'disconnected' : 'left' }
}

/** Encode a cause into leave-event fields — `'left'` is the wire default and travels as nothing. */
function leaveCauseToWire(cause: LeaveCause): { cause?: 'removed' | 'disconnected'; reason?: unknown } {
  if (cause.type === 'removed')
    return cause.reason === undefined ? { cause: 'removed' } : { cause: 'removed', reason: cause.reason }
  if (cause.type === 'disconnected') return { cause: 'disconnected' }
  return {}
}

/** All room messages are tagged with `__r` — envelopes, requests, and notices alike. */
function hasRoomTag(value: unknown): value is { __r: string } {
  return isObject(value) && typeof value.__r === 'string'
}

/** Validates `join(meta, options)` arguments; returns the resolved `selfDelivery`. */
function normalizeJoinOptions(meta: unknown, options: JoinOptions | undefined): boolean {
  assertUsage(isObject(meta), 'join() meta should be an object')
  assertUsage(options === undefined || isObject(options), 'join() options should be an object')
  return options?.selfDelivery !== false
}

/** How errors travel inside `ok: false` acks. */
function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

// ---------------------------------------------------------------------------
// Member IDs — UUIDs, framed as a fixed 16-byte prefix on binary messages
// ---------------------------------------------------------------------------

const MEMBER_ID_BYTE_LENGTH = 16
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
const BYTE_TO_HEX: string[] = []
for (let byte = 0; byte < 256; byte++) BYTE_TO_HEX.push(byte.toString(16).padStart(2, '0'))

/** Canonical UUID string → 16 bytes. Returns `null` for anything else. */
function uuidToBytes(uuid: string): Uint8Array | null {
  if (!UUID_REGEX.test(uuid)) return null
  const hex = uuid.split('-').join('')
  const bytes = new Uint8Array(MEMBER_ID_BYTE_LENGTH)
  for (let i = 0; i < MEMBER_ID_BYTE_LENGTH; i++) bytes[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16)
  return bytes
}

/** First 16 bytes → canonical lowercase UUID string. */
function bytesToUuid(bytes: Uint8Array): string {
  let uuid = ''
  for (let i = 0; i < MEMBER_ID_BYTE_LENGTH; i++) {
    if (i === 4 || i === 6 || i === 8 || i === 10) uuid += '-'
    uuid += BYTE_TO_HEX[bytes[i]!]!
  }
  return uuid
}

/** Binary relay format: `[16-byte member UUID][payload]`. Fixed-size prefix — no length field needed. */
function frameWithMemberId(memberId: string, payload: Uint8Array): Uint8Array {
  const idBytes = uuidToBytes(memberId)
  assert(idBytes, 'room member IDs are UUIDs')
  const framed = new Uint8Array(MEMBER_ID_BYTE_LENGTH + payload.byteLength)
  framed.set(idBytes, 0)
  framed.set(payload, MEMBER_ID_BYTE_LENGTH)
  return framed
}

/** Split a binary relay frame into sender ID and payload. Returns `null` on truncated frames. */
function unframeMemberId(data: Uint8Array): { from: string; payload: Uint8Array } | null {
  if (data.byteLength < MEMBER_ID_BYTE_LENGTH) return null
  return { from: bytesToUuid(data), payload: data.subarray(MEMBER_ID_BYTE_LENGTH) }
}

// ---------------------------------------------------------------------------
// RoomState — the local view of a room, driven by the event stream
// ---------------------------------------------------------------------------

type ListenerKind = 'data' | 'binary' | 'event'

type MemberEntry = {
  id: string
  meta: ParticipantMeta
  joinedAt: number
  /** Latest applied meta revision — stale and echoed `p-meta` events are absorbed. */
  metaSeq: number
  remote: RemoteParticipant
  dataCbs: Array<(data: unknown, info: ChannelPublishInfo) => unknown>
  binaryCbs: Array<(data: Uint8Array, info: ChannelPublishInfo) => unknown>
  updateCbs: Array<(meta: ParticipantMeta, prev: ParticipantMeta) => void>
  leaveCbs: Array<(cause?: LeaveCause) => void>
}

type RoomStateOptions = {
  roomId: string
  meta: RoomMeta
  size: number
  /** Either the authoritative roster, or just its size — a lazy view seeds with `{ count }`
   *  and learns the members from its first `reconcile()` (KV read / streamed roster). */
  seed: { members: MemberSnapshot[] } | { count: number }
  /** The LWW stamp of the config `meta`/`size` were read from (see `applyRoomUpdate`). */
  updateStamp: { at: number; by: string }
  closed?: boolean
  /** Fired whenever the number of attached listeners changes — lets the owner
   *  (de)activate its event source (adapter subscription, wire subscription). */
  onListenersChanged: () => void
  /** A user callback threw — the owner decides how to report it. */
  onCallbackError: (err: unknown) => void
}

/**
 * The local, event-driven view of a room: membership, metadata, and every user-facing callback.
 * Server and client share this class so event semantics are identical on both sides; only the
 * event *source* differs (adapter subscription vs relayed wire frames).
 *
 * Event application is idempotent — a `join` for a known member or a `leave` for an unknown one
 * is absorbed silently. This lets owners seed state from a snapshot and apply a concurrently
 * produced event stream without double-firing.
 */
/** Stamped (non-enumerably) on every `RemoteParticipant` a `RoomState` mints — the backing
 *  that lets the serializer turn a view object back into (room, member) without a registry. */
const ROOM_REMOTE_BRAND: unique symbol = Symbol.for('telefunc.RoomRemoteParticipant')

type RemoteBacking = { state: RoomState; entry: MemberEntry }

/** The `RoomState` backing of a minted `RemoteParticipant` — `null` for anything else. */
function remoteBacking(value: unknown): RemoteBacking | null {
  return typeof value === 'object' && value !== null && ROOM_REMOTE_BRAND in value
    ? ((value as Record<typeof ROOM_REMOTE_BRAND, RemoteBacking>)[ROOM_REMOTE_BRAND] as RemoteBacking)
    : null
}

class RoomState {
  /** @internal — the owning `ServerRoom`/`ClientRoom`, for serialization backing. */
  _owner: unknown = null
  readonly roomId: string
  meta: RoomMeta
  size: number
  closed: boolean
  /** Bumped on every membership change — guards async KV reconciles against going stale. */
  membershipVersion = 0

  private readonly _members = new Map<string, MemberEntry>()
  private readonly _onListenersChanged: () => void
  private readonly _onCallbackError: (err: unknown) => void

  private readonly _roomDataCbs: Array<(data: unknown, info: ChannelPublishInfo, from: Sender) => unknown> = []
  private readonly _roomBinaryCbs: Array<(data: Uint8Array, info: ChannelPublishInfo, from: Sender) => unknown> = []
  private readonly _joinCbs: Array<(member: RemoteParticipant) => void> = []
  private readonly _leaveCbs: Array<(member: RemoteParticipant, cause?: LeaveCause) => void> = []
  private readonly _updateCbs: Array<(meta: RoomMeta, prev: RoomMeta) => void> = []
  private readonly _emptyCbs: Array<() => void> = []
  private readonly _fullCbs: Array<() => void> = []
  private readonly _closeCbs: Array<() => void> = []
  private readonly _announceCbs: Array<(data: unknown, info: ChannelPublishInfo) => void> = []

  private _eventListenerCount = 0
  private _dataListenerCount = 0
  private _binaryListenerCount = 0
  private _wasFull: boolean
  private _updateStamp: { at: number; by: string }
  private _rosterKnown: boolean
  private _seedCount = 0

  constructor(opts: RoomStateOptions) {
    this.roomId = opts.roomId
    this.meta = opts.meta
    this.size = opts.size
    this.closed = opts.closed === true
    this._updateStamp = opts.updateStamp
    this._onListenersChanged = opts.onListenersChanged
    this._onCallbackError = opts.onCallbackError
    if ('members' in opts.seed) {
      this._rosterKnown = true
      for (const member of opts.seed.members) this._createEntry(member)
    } else {
      this._rosterKnown = false
      this._seedCount = opts.seed.count
    }
    this._wasFull = this.isFull
  }

  // ── Reads ──

  /** Exact once the roster is known (seeded or reconciled); before that, the seeded count
   *  adjusted by the events this view applied since. */
  get count(): number {
    return this._rosterKnown ? this._members.size : this._seedCount
  }

  /** Whether this view holds the authoritative member list (vs just a count). */
  get rosterKnown(): boolean {
    return this._rosterKnown
  }

  get isFull(): boolean {
    return this.count >= this.size
  }

  /** Listeners needing the control stream (presence, lifecycle). */
  get eventListenerCount(): number {
    return this._eventListenerCount
  }
  /** Listeners needing the text data stream. */
  get dataListenerCount(): number {
    return this._dataListenerCount
  }
  /** Listeners needing the binary data stream. */
  get binaryListenerCount(): number {
    return this._binaryListenerCount
  }

  /** Which members' binary streams this holder needs delivered — drives the wire/adapter
   *  subscriptions on both sides (client declares it, server aggregates it per stub). */
  binaryWants(): MemberWants {
    if (this._roomBinaryCbs.length > 0) return { all: true, members: [] }
    const members: string[] = []
    for (const entry of this._members.values()) {
      if (entry.binaryCbs.length > 0) members.push(entry.id)
    }
    return { all: false, members }
  }

  /** The text-lane twin of `binaryWants()`: `all` while room-level `subscribe()`rs exist,
   *  otherwise exactly the members with participant-scoped listeners. */
  textWants(): MemberWants {
    if (this._roomDataCbs.length > 0) return { all: true, members: [] }
    const members: string[] = []
    for (const entry of this._members.values()) {
      if (entry.dataCbs.length > 0) members.push(entry.id)
    }
    return { all: false, members }
  }

  getRemote(id: string): RemoteParticipant | null {
    return this._members.get(id)?.remote ?? null
  }

  listRemotes(): RemoteParticipant[] {
    return [...this._members.values()].map((entry) => entry.remote)
  }

  /** Member IDs currently known — drives isolated-mode per-member key subscriptions. */
  listMemberIds(): string[] {
    return [...this._members.keys()]
  }

  snapshotMembers(): MemberSnapshot[] {
    return [...this._members.values()].map(({ id, meta, joinedAt, metaSeq }) => ({ id, meta, joinedAt, metaSeq }))
  }

  /** Revival side of a serialized `RemoteParticipant`: the live view wins when it already knows
   *  the member; otherwise the entry is seeded silently from the snapshot — no events fire, no
   *  count adjustment (the seed count already included the member), and the streamed roster
   *  reconciles it like any other pre-roster knowledge. */
  ensureRemoteFromSnapshot(snap: MemberSnapshot): RemoteParticipant {
    const existing = this._members.get(snap.id)
    if (existing) return existing.remote
    return this._createEntry(snap).remote
  }

  // ── Listener registration (all return an unlisten function) ──

  subscribe(cb: (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this._register(this._roomDataCbs, cb, 'data')
  }
  subscribeBinary(cb: (data: Uint8Array, info: ChannelPublishInfo, from: Sender) => unknown): () => void {
    return this._register(this._roomBinaryCbs, cb, 'binary')
  }
  onJoin(cb: (member: RemoteParticipant) => void): () => void {
    return this._register(this._joinCbs, cb, 'event')
  }
  onLeave(cb: (member: RemoteParticipant, cause?: LeaveCause) => void): () => void {
    return this._register(this._leaveCbs, cb, 'event')
  }
  onUpdate(cb: (meta: RoomMeta, prev: RoomMeta) => void): () => void {
    return this._register(this._updateCbs, cb, 'event')
  }
  onEmpty(cb: () => void): () => void {
    return this._register(this._emptyCbs, cb, 'event')
  }
  onFull(cb: () => void): () => void {
    return this._register(this._fullCbs, cb, 'event')
  }
  onClose(cb: () => void): () => void {
    return this._register(this._closeCbs, cb, 'event')
  }
  onAnnounce(cb: (data: unknown, info: ChannelPublishInfo) => void): () => void {
    return this._register(this._announceCbs, cb, 'event')
  }

  // ── Event application ──

  applyJoin(id: string, meta: ParticipantMeta, joinedAt: number): void {
    if (this.closed) return
    const existing = this._members.get(id)
    if (existing) {
      // Echo of a member this side already applied (or got via snapshot) — absorb.
      existing.meta = meta
      existing.joinedAt = joinedAt
      return
    }
    const entry = this._createEntry({ id, meta, joinedAt, metaSeq: 0 })
    this._seedCount++ // pre-reconcile, `count` tracks the seed adjusted by applied events
    this.membershipVersion++
    this._fireAll(this._joinCbs, entry.remote)
    this._checkFull()
  }

  applyLeave(id: string, cause?: LeaveCause): void {
    const entry = this._members.get(id)
    if (!entry) return // unknown here: absorbed (pre-reconcile misses correct at reconcile)
    this._members.delete(id)
    this._seedCount = Math.max(0, this._seedCount - 1)
    this.membershipVersion++
    this._fireAll(entry.leaveCbs, cause)
    this._fireAll(this._leaveCbs, entry.remote, cause)
    this._releaseEntryListeners(entry)
    this._wasFull = this.isFull
    if (this._members.size === 0) this._fireAll(this._emptyCbs)
  }

  /** Applies only revisions newer than the entry's — the origin's echo (same seq) and events
   *  arriving behind a fresher reconcile are absorbed. */
  applyParticipantMeta(id: string, meta: ParticipantMeta, prev: ParticipantMeta, seq: number): void {
    const entry = this._members.get(id)
    if (!entry || seq <= entry.metaSeq) return
    entry.metaSeq = seq
    entry.meta = meta
    this._fireAll(entry.updateCbs, meta, prev)
  }

  /** Last-writer-wins by `(at, by)`: concurrent `Room.update()`s converge to the same winner on
   *  every node regardless of arrival order, and the origin's echo (same stamp) is absorbed. */
  applyRoomUpdate(meta: RoomMeta, prev: RoomMeta, size: number, at: number, by: string): void {
    if (!stampNewer({ at, by }, this._updateStamp)) return
    this._updateStamp = { at, by }
    this.meta = meta
    this.size = size
    this._fireAll(this._updateCbs, meta, prev)
    this._checkFull()
  }

  /** The stamp of the config this view currently reflects (serialized into room snapshots). */
  get updateStamp(): { at: number; by: string } {
    return this._updateStamp
  }

  /** Room closed: member-level cleanup callbacks run (decoders etc.), then `onClose`.
   *  Room-level `onLeave`/`onEmpty` intentionally don't fire — `onClose` is the signal. */
  applyClosed(cause: LeaveCause = { type: 'closed' }): void {
    if (this.closed) return
    this.closed = true
    this._rosterKnown = true // authoritatively empty
    this.membershipVersion++
    for (const entry of this._members.values()) {
      this._fireAll(entry.leaveCbs, cause)
      this._releaseEntryListeners(entry)
    }
    this._members.clear()
    this._fireAll(this._closeCbs)
  }

  applyAnnounce(data: unknown, info: ChannelPublishInfo): void {
    this._fireAll(this._announceCbs, data, info)
  }

  /** Messages never wait on the roster: `from` is the live `RemoteParticipant` when this view
   *  knows the sender, else the `{ id, meta }` snapshot the sender's node stamped into the
   *  envelope. Control and data travel on separate lanes, so a message can beat its sender's
   *  join — identity is in the message, delivery is immediate, and nothing drops. */
  applyData(from: string, fromMeta: ParticipantMeta, data: unknown, info: ChannelPublishInfo, suppress: boolean): void {
    if (suppress) return
    const entry = this._members.get(from)
    this._fireAll(this._roomDataCbs, data, info, entry?.remote ?? { id: from, meta: fromMeta })
    if (entry) this._fireAll(entry.dataCbs, data, info)
  }

  /** Binary frames carry only the sender's ID — a pre-join frame surfaces as `{ id, meta: {} }`
   *  (rare: binary pipelines attach per member via `onJoin`, so the roster is normally ahead). */
  applyBinary(from: string, payload: Uint8Array, info: ChannelPublishInfo, suppress: boolean): void {
    if (suppress) return
    const entry = this._members.get(from)
    this._fireAll(this._roomBinaryCbs, payload, info, entry?.remote ?? { id: from, meta: {} })
    if (entry) this._fireAll(entry.binaryCbs, payload, info)
  }

  /** Resync against an authoritative membership snapshot. The first reconcile is the roster
   *  *load* and is silent (the documented pattern reads `getParticipants()` and then follows
   *  events — narrating the load would double-render it). Every later reconcile is discovered
   *  *drift*, and an observed view never mutates silently: the diff replays through the normal
   *  appliers, so `onJoin`/`onLeave`/`onUpdate` fire exactly once per real change — whichever
   *  path (event or reconcile) learns of it first, the other is absorbed as an echo. Returns
   *  whether anything drifted, so the owner can re-sync downstream views (client stubs). */
  reconcile(members: MemberSnapshot[]): boolean {
    if (!this._rosterKnown) {
      // First load: silent — but entries can already exist (pre-roster join events, revived
      // views). Those objects must survive: listeners hang off them and revived handles must
      // stay `===` with the view. Keep the object, refresh the facts; entries the authoritative
      // roster doesn't know left before the load — their leave is narrated like any other.
      this._rosterKnown = true
      this.membershipVersion++
      const seen = new Set<string>()
      for (const member of members) {
        seen.add(member.id)
        const existing = this._members.get(member.id)
        if (!existing) {
          this._createEntry(member)
          continue
        }
        if (member.metaSeq > existing.metaSeq) {
          existing.meta = member.meta
          existing.metaSeq = member.metaSeq
        }
        existing.joinedAt = member.joinedAt
      }
      for (const id of [...this._members.keys()]) {
        if (!seen.has(id)) this.applyLeave(id)
      }
      this._wasFull = this.isFull
      return false
    }

    this.membershipVersion++
    let drifted = false
    const seen = new Set<string>()
    for (const member of members) {
      seen.add(member.id)
      const entry = this._members.get(member.id)
      if (!entry) {
        this.applyJoin(member.id, member.meta, member.joinedAt)
        const created = this._members.get(member.id)
        if (created) created.metaSeq = member.metaSeq
        drifted = true
      } else {
        if (member.metaSeq > entry.metaSeq) {
          this.applyParticipantMeta(member.id, member.meta, entry.meta, member.metaSeq)
          drifted = true
        }
        entry.joinedAt = member.joinedAt
      }
    }
    for (const id of [...this._members.keys()]) {
      if (!seen.has(id)) {
        this.applyLeave(id)
        drifted = true
      }
    }
    return drifted
  }

  // ── Private ──

  private _checkFull(): void {
    const full = this.isFull
    if (full && !this._wasFull) this._fireAll(this._fullCbs)
    this._wasFull = full
  }

  private _createEntry(entrySeed: MemberSnapshot): MemberEntry {
    const { id, meta, joinedAt } = entrySeed
    const entry: MemberEntry = {
      id,
      meta,
      joinedAt,
      metaSeq: entrySeed.metaSeq,
      remote: {
        id,
        get meta() {
          return entry.meta
        },
        get joinedAt() {
          return entry.joinedAt
        },
        subscribe: (cb) => this._register(entry.dataCbs, cb, 'data'),
        subscribeBinary: (cb) => this._register(entry.binaryCbs, cb, 'binary'),
        onUpdate: (cb) => this._register(entry.updateCbs, cb, 'event'),
        onLeave: (cb: (cause?: LeaveCause) => void) => this._register(entry.leaveCbs, cb, 'event'),
      },
      dataCbs: [],
      binaryCbs: [],
      updateCbs: [],
      leaveCbs: [],
    }
    // Non-enumerable: never serialized as data, invisible to user iteration.
    Object.defineProperty(entry.remote, ROOM_REMOTE_BRAND, { value: { state: this, entry } satisfies RemoteBacking })
    this._members.set(id, entry)
    return entry
  }

  private _register<T>(list: T[], cb: T, kind: ListenerKind): () => void {
    list.push(cb)
    this._bumpListenerCount(kind, 1)
    // List membership is the source of truth: `_releaseEntryListeners` may have already
    // emptied the list, and a second unlisten call must not decrement twice.
    return () => {
      const i = list.indexOf(cb)
      if (i < 0) return
      list.splice(i, 1)
      this._bumpListenerCount(kind, -1)
    }
  }

  /** A member entry is being discarded — its listeners die with it. Releasing them keeps the
   *  counters truthful (callers rarely unsubscribe in `onLeave`), which lets the owners drop
   *  wire/adapter subscriptions the departed member was holding open. */
  private _releaseEntryListeners(entry: MemberEntry): void {
    this._bumpListenerCount('data', -entry.dataCbs.length)
    this._bumpListenerCount('binary', -entry.binaryCbs.length)
    this._bumpListenerCount('event', -(entry.updateCbs.length + entry.leaveCbs.length))
    entry.dataCbs.length = 0
    entry.binaryCbs.length = 0
    entry.updateCbs.length = 0
    entry.leaveCbs.length = 0
  }

  private _bumpListenerCount(kind: ListenerKind, delta: number): void {
    if (kind === 'data') this._dataListenerCount += delta
    else if (kind === 'binary') this._binaryListenerCount += delta
    else this._eventListenerCount += delta
    this._onListenersChanged()
  }

  private _fireAll<Args extends unknown[]>(cbs: Array<(...args: Args) => unknown>, ...args: Args): void {
    for (const cb of [...cbs]) {
      try {
        cb(...args)
      } catch (err) {
        this._onCallbackError(err)
      }
    }
  }
}

// ---------------------------------------------------------------------------
// ParticipantBase — the shared half of every LocalParticipant
// ---------------------------------------------------------------------------

/**
 * The private-message inbox and the leave lifecycle, identical on server and client.
 * Flavors supply the transport through the abstract operations and their own error pipeline.
 */
/** Pre-listen inbox hold, count-capped: payloads arrive parsed, so byte-accounting would cost
 *  a re-serialization per message — the wire-side buffers (`ServerChannelBuffer`) already cap
 *  the bytes that can reach us. Drop-oldest beyond the cap. */
const PENDING_INBOX_CAP = 64

abstract class ParticipantBase implements LocalParticipant {
  readonly id: string
  readonly selfDelivery: boolean
  /** @internal */ _meta: ParticipantMeta
  protected _left = false
  private _leftCause: LeaveCause | null = null
  private _leaveCbs: Array<(cause: LeaveCause) => void> = []
  private readonly _messageCbs: Array<(data: unknown, from: Sender | null) => void> = []
  /** DMs delivered before the first `listen()` — held bounded, flushed on attach, then never
   *  allocated again (`null` = flushed or empty; zero steady-state cost). */
  private _pendingInbox: Array<{ from: string; fromMeta: ParticipantMeta | null; data: unknown }> | null = null

  constructor(id: string, meta: ParticipantMeta, selfDelivery: boolean) {
    this.id = id
    this._meta = meta
    this.selfDelivery = selfDelivery
  }

  get meta(): ParticipantMeta {
    return this._meta
  }

  abstract publish(data: unknown): Promise<ChannelPublishAck>
  abstract publishBinary(data: Uint8Array): Promise<ChannelPublishAck>
  abstract send(to: string | Sender, data: unknown): Promise<void>
  abstract setMeta(meta: ParticipantMeta): Promise<void>
  abstract leave(): Promise<void>
  /** A user callback threw — each side reports through its own pipeline. */
  protected abstract _reportError(err: unknown): void

  listen(callback: (data: unknown, from: Sender | null) => void): () => void {
    this._messageCbs.push(callback)
    if (this._pendingInbox) {
      const held = this._pendingInbox
      this._pendingInbox = null
      for (const msg of held) this._deliverMessage(msg.from, msg.fromMeta, msg.data)
    }
    return () => {
      const i = this._messageCbs.indexOf(callback)
      if (i >= 0) this._messageCbs.splice(i, 1)
    }
  }

  /** @internal — a direct message arrived on this member's inbox. `from`/`fromMeta` come from
   *  the wire envelope; `resolve` upgrades to the live `RemoteParticipant` when a room view
   *  exists. An empty `from` is the wire encoding of a room-authored message → `null`. */
  _deliverMessage(from: string, fromMeta: ParticipantMeta | null, data: unknown): void {
    if (this._messageCbs.length === 0) {
      // A reactive send can beat the app's `listen()` by a tick — hold it (bounded) instead of
      // dropping it. A departed participant will never flush: drop.
      if (this._left) return
      const pending = (this._pendingInbox ??= [])
      pending.push({ from, fromMeta, data })
      if (pending.length > PENDING_INBOX_CAP) pending.shift()
      return
    }
    const sender = from === '' ? null : (this._resolveSender(from) ?? { id: from, meta: fromMeta ?? {} })
    for (const cb of [...this._messageCbs]) {
      try {
        cb(data, sender)
      } catch (err) {
        this._reportError(err)
      }
    }
  }

  /** The live room-backed sender, when this flavor has a room view. */
  protected _resolveSender(_id: string): Sender | null {
    return null
  }

  onLeave(callback: (cause: LeaveCause) => void): () => void {
    if (this._leftCause) {
      this._invoke(callback, this._leftCause)
      return () => {}
    }
    this._leaveCbs.push(callback)
    return () => {
      const i = this._leaveCbs.indexOf(callback)
      if (i >= 0) this._leaveCbs.splice(i, 1)
    }
  }

  /** @internal — the member is gone; `cause` says how. A local participant always knows its
   *  cause: its holder either initiated the leave or witnessed the event/closure that caused it. */
  _onLeft(cause: LeaveCause): void {
    this._left = true
    if (this._leftCause) return
    this._leftCause = cause
    this._pendingInbox = null
    const cbs = this._leaveCbs
    this._leaveCbs = []
    for (const cb of cbs) this._invoke(cb, cause)
  }

  protected _assertActive(): void {
    if (this._left) throw new Error('Participant has left the room')
  }

  private _invoke(cb: (cause: LeaveCause) => void, cause: LeaveCause): void {
    try {
      cb(cause)
    } catch (err) {
      this._reportError(err)
    }
  }
}
