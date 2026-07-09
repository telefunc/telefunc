export {
  ROOM_KEY_NAMESPACE,
  roomMainKey,
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
  makeEid,
  errorMessage,
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
  BinaryWants,
}

import { assert, assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import type { ChannelPublishAck, ChannelPublishInfo } from '../channel.js'
import type { JoinOptions, LocalParticipant, ParticipantMeta, RemoteParticipant, RoomMeta, Sender } from './types.js'

// ---------------------------------------------------------------------------
// Keys & records
// ---------------------------------------------------------------------------

/** Reserved pub/sub + KV namespace for rooms. Don't use it for `BroadcastChannel` keys. */
const ROOM_KEY_NAMESPACE = 'telefunc:room:'

/** Pub/sub key carrying a room's control events (and, in shared mode, its data). */
function roomMainKey(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomId}`
}
/** Pub/sub key carrying one member's data in isolated mode. */
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

/** Stored at `roomConfigKvKey`. `size: null` encodes `Infinity` (not JSON-safe). */
type RoomConfigRecord = {
  meta: RoomMeta
  size: number | null
  isolated: boolean
}

/** Stored at `roomMemberKvKey`. `seenAt` is the liveness timestamp: the owning node refreshes
 *  it every `ROOM_HEARTBEAT_INTERVAL_MS`; records older than `ROOM_MEMBER_TTL_MS` are reaped. */
type RoomMemberRecord = {
  meta: ParticipantMeta
  joinedAt: number
  seenAt: number
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
}

/** Serializer metadata of a `Room` crossing the wire. */
type RoomSnapshotMetadata = {
  channelId: string
  roomId: string
  meta: RoomMeta
  size: number | null
  isolated: boolean
  closed: boolean
  members: MemberSnapshot[]
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

/** Presence & lifecycle events, published on the room's main key by whichever node caused them.
 *
 *  The origin applies its own event locally (for deterministic same-node semantics) and then
 *  receives it back via the pub/sub echo. `join`/`leave`/`closed` are naturally idempotent;
 *  `p-meta`/`update` carry an event ID (`eid`) so the echo is absorbed instead of double-firing. */
type RoomCtrlEnvelope =
  | { __r: 'join'; id: string; meta: ParticipantMeta; joinedAt: number }
  | { __r: 'leave'; id: string }
  | { __r: 'p-meta'; id: string; meta: ParticipantMeta; prev: ParticipantMeta; eid: string }
  | { __r: 'update'; meta: RoomMeta; prev: RoomMeta; size: number | null; eid: string }
  | { __r: 'closed' }

/** A participant's message. Published on the room's main key (shared mode) or the member's own key
 *  (isolated mode). `fromMeta` is the sender's meta as verified by the sender's own node — never
 *  client-supplied — so any receiver can surface a correct sender even before its roster view
 *  catches up (see `RoomState.applyData`). */
type RoomDataEnvelope = { __r: 'data'; from: string; fromMeta: ParticipantMeta; data: unknown }

/** What a client sends upward to publish — its node verifies membership and stamps `fromMeta`. */
type RoomDataPublish = { __r: 'data'; from: string; data: unknown }

/** A room-authored message (`Room.announce()`) — no sender, delivered to `onAnnounce()`. */
type RoomAnnounceEnvelope = { __r: 'announce'; data: unknown }

type RoomEnvelope = RoomCtrlEnvelope | RoomDataEnvelope | RoomAnnounceEnvelope

/** A direct message, published on the target's inbox key (`roomDmKey`) — transport-level
 *  privacy: only the target's owning node subscribes, only its holder receives the relay.
 *  `to` lets a holder of several participants route the message to the right one. */
type RoomDmEnvelope = { __r: 'dm'; to: string; from: string; fromMeta: ParticipantMeta | null; data: unknown }

/** Client→server requests on a `Room` stub channel. `id` identifies the sending participant.
 *  `sub-binary` declares which members' binary streams the client wants relayed (full replace). */
type RoomStubRequest =
  | { __r: 'req-join'; meta: ParticipantMeta; selfDelivery: boolean }
  | { __r: 'req-leave'; id: string }
  | { __r: 'req-set-meta'; id: string; meta: ParticipantMeta }
  | { __r: 'req-dm'; id: string; to: string; data: unknown }
  | { __r: 'sub-binary'; all: boolean; members: string[] }

/** Client→server requests on a standalone `LocalParticipant` stub channel. */
type ParticipantStubRequest =
  | { __r: 'req-publish'; data: unknown }
  | { __r: 'req-set-meta'; meta: ParticipantMeta }
  | { __r: 'req-dm'; to: string; data: unknown }
  | { __r: 'req-leave' }

/** Server→client notices on a standalone `LocalParticipant` stub channel. */
type ParticipantStubNotice =
  | { __r: 'left' }
  | { __r: 'p-meta'; meta: ParticipantMeta }
  | { __r: 'dm'; from: string; fromMeta: ParticipantMeta | null; data: unknown }

/** Which members' binary streams a holder wants — `all` (room-level listeners) or a specific set. */
type BinaryWants = { all: boolean; members: string[] }

type ReqOkAck = { ok: true } | { ok: false; err: string }
type ReqJoinAck = { ok: true; id: string; joinedAt: number } | { ok: false; err: string }
type ReqPublishAck = { ok: true; ack: ChannelPublishInfo } | { ok: false; err: string }

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

/** Event ID for `p-meta`/`update` envelopes — only needs to make the origin's own echo
 *  recognizable, not to be globally unique. */
function makeEid(): string {
  return Math.random().toString(36).slice(2, 10)
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
  /** Last applied `p-meta` event ID — absorbs the origin's own echo. */
  lastMetaEid: string | null
  remote: RemoteParticipant
  dataCbs: Array<(data: unknown, info: ChannelPublishInfo) => unknown>
  binaryCbs: Array<(data: Uint8Array, info: ChannelPublishInfo) => unknown>
  updateCbs: Array<(meta: ParticipantMeta, prev: ParticipantMeta) => void>
  leaveCbs: Array<() => void>
}

type RoomStateOptions = {
  roomId: string
  meta: RoomMeta
  size: number
  members: MemberSnapshot[]
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
class RoomState {
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
  private readonly _leaveCbs: Array<(member: RemoteParticipant) => void> = []
  private readonly _updateCbs: Array<(meta: RoomMeta, prev: RoomMeta) => void> = []
  private readonly _emptyCbs: Array<() => void> = []
  private readonly _fullCbs: Array<() => void> = []
  private readonly _closeCbs: Array<() => void> = []
  private readonly _announceCbs: Array<(data: unknown, info: ChannelPublishInfo) => void> = []

  private _eventListenerCount = 0
  private _dataListenerCount = 0
  private _binaryListenerCount = 0
  private _wasFull: boolean
  private _lastUpdateEid: string | null = null

  constructor(opts: RoomStateOptions) {
    this.roomId = opts.roomId
    this.meta = opts.meta
    this.size = opts.size
    this.closed = opts.closed === true
    this._onListenersChanged = opts.onListenersChanged
    this._onCallbackError = opts.onCallbackError
    for (const member of opts.members) this._createEntry(member)
    this._wasFull = this.isFull
  }

  // ── Reads ──

  get count(): number {
    return this._members.size
  }

  get isFull(): boolean {
    return this._members.size >= this.size
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
  binaryWants(): BinaryWants {
    if (this._roomBinaryCbs.length > 0) return { all: true, members: [] }
    const members: string[] = []
    for (const entry of this._members.values()) {
      if (entry.binaryCbs.length > 0) members.push(entry.id)
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
    return [...this._members.values()].map(({ id, meta, joinedAt }) => ({ id, meta, joinedAt }))
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
  onLeave(cb: (member: RemoteParticipant) => void): () => void {
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
    const entry = this._createEntry({ id, meta, joinedAt })
    this.membershipVersion++
    this._fireAll(this._joinCbs, entry.remote)
    this._checkFull()
  }

  applyLeave(id: string): void {
    const entry = this._members.get(id)
    if (!entry) return
    this._members.delete(id)
    this.membershipVersion++
    this._fireAll(entry.leaveCbs)
    this._fireAll(this._leaveCbs, entry.remote)
    this._releaseEntryListeners(entry)
    this._wasFull = this.isFull
    if (this._members.size === 0) this._fireAll(this._emptyCbs)
  }

  applyParticipantMeta(id: string, meta: ParticipantMeta, prev: ParticipantMeta, eid: string): void {
    const entry = this._members.get(id)
    if (!entry || entry.lastMetaEid === eid) return
    entry.lastMetaEid = eid
    entry.meta = meta
    this._fireAll(entry.updateCbs, meta, prev)
  }

  applyRoomUpdate(meta: RoomMeta, prev: RoomMeta, size: number, eid: string): void {
    if (this._lastUpdateEid === eid) return
    this._lastUpdateEid = eid
    this.meta = meta
    this.size = size
    this._fireAll(this._updateCbs, meta, prev)
    this._checkFull()
  }

  /** Room closed: member-level cleanup callbacks run (decoders etc.), then `onClose`.
   *  Room-level `onLeave`/`onEmpty` intentionally don't fire — `onClose` is the signal. */
  applyClosed(): void {
    if (this.closed) return
    this.closed = true
    this.membershipVersion++
    for (const entry of this._members.values()) {
      this._fireAll(entry.leaveCbs)
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

  /** Silent resync against an authoritative membership snapshot. Only for unobserved rooms —
   *  once observed, the event stream is authoritative and the only thing firing callbacks. */
  reconcile(members: MemberSnapshot[]): void {
    this.membershipVersion++
    const seen = new Set<string>()
    for (const member of members) {
      seen.add(member.id)
      const entry = this._members.get(member.id)
      if (entry) {
        entry.meta = member.meta
        entry.joinedAt = member.joinedAt
      } else {
        this._createEntry(member)
      }
    }
    for (const id of [...this._members.keys()]) {
      if (!seen.has(id)) this._members.delete(id)
    }
    this._wasFull = this.isFull
  }

  // ── Private ──

  private _checkFull(): void {
    const full = this.isFull
    if (full && !this._wasFull) this._fireAll(this._fullCbs)
    this._wasFull = full
  }

  private _createEntry({ id, meta, joinedAt }: MemberSnapshot): MemberEntry {
    const entry: MemberEntry = {
      id,
      meta,
      joinedAt,
      lastMetaEid: null,
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
        onLeave: (cb) => this._register(entry.leaveCbs, cb, 'event'),
      },
      dataCbs: [],
      binaryCbs: [],
      updateCbs: [],
      leaveCbs: [],
    }
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
abstract class ParticipantBase implements LocalParticipant {
  readonly id: string
  readonly selfDelivery: boolean
  /** @internal */ _meta: ParticipantMeta
  protected _left = false
  private _leftFired = false
  private _leaveCbs: Array<() => void> = []
  private readonly _messageCbs: Array<(data: unknown, from: Sender | null) => void> = []

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
    return () => {
      const i = this._messageCbs.indexOf(callback)
      if (i >= 0) this._messageCbs.splice(i, 1)
    }
  }

  /** @internal — a direct message arrived on this member's inbox. `from`/`fromMeta` come from
   *  the wire envelope; `resolve` upgrades to the live `RemoteParticipant` when a room view
   *  exists. An empty `from` is the wire encoding of a room-authored message → `null`. */
  _deliverMessage(from: string, fromMeta: ParticipantMeta | null, data: unknown): void {
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

  onLeave(callback: () => void): () => void {
    if (this._leftFired) {
      this._invoke(callback)
      return () => {}
    }
    this._leaveCbs.push(callback)
    return () => {
      const i = this._leaveCbs.indexOf(callback)
      if (i >= 0) this._leaveCbs.splice(i, 1)
    }
  }

  /** @internal — the member is gone (left, kicked, room closed, or holder disconnected). */
  _onLeft(): void {
    this._left = true
    if (this._leftFired) return
    this._leftFired = true
    const cbs = this._leaveCbs
    this._leaveCbs = []
    for (const cb of cbs) this._invoke(cb)
  }

  protected _assertActive(): void {
    if (this._left) throw new Error('Participant has left the room')
  }

  private _invoke(cb: () => void): void {
    try {
      cb()
    } catch (err) {
      this._reportError(err)
    }
  }
}
