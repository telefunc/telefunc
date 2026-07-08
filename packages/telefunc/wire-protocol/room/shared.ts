export {
  ROOM_KEY_NAMESPACE,
  roomMainKey,
  roomMemberDataKey,
  roomConfigKvKey,
  roomMemberKvKey,
  roomMemberKvPrefix,
  sizeToWire,
  sizeFromWire,
  uuidToBytes,
  frameWithMemberId,
  unframeMemberId,
  hasRoomTag,
  RoomState,
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
  RoomStubRequest,
  ParticipantStubRequest,
  ParticipantStubNotice,
  ReqOkAck,
  ReqJoinAck,
  ReqPublishAck,
}

import { assert } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import type { ChannelPublishInfo } from '../channel.js'
import type { ParticipantMeta, RemoteParticipant, RoomMeta } from './types.js'

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
/** KV key of the room's config record. */
function roomConfigKvKey(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomId}:config`
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

/** Stored at `roomMemberKvKey`. */
type RoomMemberRecord = {
  meta: ParticipantMeta
  joinedAt: number
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

/** A participant's message. Published on the room's main key (shared mode) or the member's own key (isolated mode). */
type RoomDataEnvelope = { __r: 'data'; from: string; data: unknown }

type RoomEnvelope = RoomCtrlEnvelope | RoomDataEnvelope

/** Client→server requests on a `Room` stub channel. */
type RoomStubRequest =
  | { __r: 'req-join'; meta: ParticipantMeta }
  | { __r: 'req-leave'; id: string }
  | { __r: 'req-set-meta'; id: string; meta: ParticipantMeta }
  | { __r: 'req-self-delivery'; id: string; on: boolean }

/** Client→server requests on a standalone `LocalParticipant` stub channel. */
type ParticipantStubRequest =
  | { __r: 'req-publish'; data: unknown }
  | { __r: 'req-set-meta'; meta: ParticipantMeta }
  | { __r: 'req-leave' }

/** Server→client notices on a standalone `LocalParticipant` stub channel. */
type ParticipantStubNotice = { __r: 'left' } | { __r: 'p-meta'; meta: ParticipantMeta }

type ReqOkAck = { ok: true } | { ok: false; err: string }
type ReqJoinAck = { ok: true; id: string; joinedAt: number } | { ok: false; err: string }
type ReqPublishAck = { ok: true; ack: ChannelPublishInfo } | { ok: false; err: string }

/** All room messages are tagged with `__r` — envelopes, requests, and notices alike. */
function hasRoomTag(value: unknown): value is { __r: string } {
  return isObject(value) && typeof value.__r === 'string'
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
  closed = false
  /** Bumped on every membership change — guards async KV reconciles against going stale. */
  membershipVersion = 0

  private readonly _members = new Map<string, MemberEntry>()
  private readonly _onListenersChanged: () => void
  private readonly _onCallbackError: (err: unknown) => void

  private readonly _roomDataCbs: Array<(data: unknown, info: ChannelPublishInfo, from: RemoteParticipant) => unknown> =
    []
  private readonly _roomBinaryCbs: Array<
    (data: Uint8Array, info: ChannelPublishInfo, from: RemoteParticipant) => unknown
  > = []
  private readonly _joinCbs: Array<(member: RemoteParticipant) => void> = []
  private readonly _leaveCbs: Array<(member: RemoteParticipant) => void> = []
  private readonly _updateCbs: Array<(meta: RoomMeta, prev: RoomMeta) => void> = []
  private readonly _emptyCbs: Array<() => void> = []
  private readonly _fullCbs: Array<() => void> = []
  private readonly _closeCbs: Array<() => void> = []

  private _eventListenerCount = 0
  private _dataListenerCount = 0
  private _binaryListenerCount = 0
  private _wasFull: boolean
  private _lastUpdateEid: string | null = null

  constructor(opts: RoomStateOptions) {
    this.roomId = opts.roomId
    this.meta = opts.meta
    this.size = opts.size
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

  subscribe(cb: (data: unknown, info: ChannelPublishInfo, from: RemoteParticipant) => unknown): () => void {
    return this._register(this._roomDataCbs, cb, 'data')
  }
  subscribeBinary(cb: (data: Uint8Array, info: ChannelPublishInfo, from: RemoteParticipant) => unknown): () => void {
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
    for (const entry of this._members.values()) this._fireAll(entry.leaveCbs)
    this._members.clear()
    this._fireAll(this._closeCbs)
  }

  applyData(from: string, data: unknown, info: ChannelPublishInfo, suppress: boolean): void {
    const entry = this._members.get(from)
    // Unknown sender: joins are always applied before that member's data can arrive (members
    // are announced before their first publish, delivery is ordered per key, and isolated-mode
    // member keys are only subscribed once the join is known) — this is noise, not a race.
    if (!entry || suppress) return
    this._fireAll(this._roomDataCbs, data, info, entry.remote)
    this._fireAll(entry.dataCbs, data, info)
  }

  applyBinary(from: string, payload: Uint8Array, info: ChannelPublishInfo, suppress: boolean): void {
    const entry = this._members.get(from)
    if (!entry || suppress) return // see applyData
    this._fireAll(this._roomBinaryCbs, payload, info, entry.remote)
    this._fireAll(entry.binaryCbs, payload, info)
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
    let removed = false
    return () => {
      if (removed) return
      removed = true
      const i = list.indexOf(cb)
      if (i >= 0) list.splice(i, 1)
      this._bumpListenerCount(kind, -1)
    }
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
