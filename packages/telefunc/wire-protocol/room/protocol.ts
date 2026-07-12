// The room wire protocol — everything both sides must agree on and nothing that behaves:
// pub/sub + KV keys, stored records, wire envelopes, the binary frame format, and want
// declarations. `RoomState` (state.ts) applies these events; `ServerRoom`/`ClientRoom` move them.

export {
  ROOM_KEY_NAMESPACE,
  roomCtrlKey,
  roomTextKey,
  roomMemberDataKey,
  roomMemberTrackKey,
  roomDmKey,
  roomConfigKvKey,
  roomIdFromConfigKey,
  roomMemberKvKey,
  roomMemberKvPrefix,
  roomIdentityMemberKvKey,
  roomIdentityKvPrefix,
  roomIdentityRoomKvPrefix,
  sizeToWire,
  sizeFromWire,
  stampNewer,
  uuidToBytes,
  frameWithMemberId,
  unframeMemberId,
  hasRoomTag,
  normalizeJoinOptions,
  mergeAttributes,
  errorMessage,
  leaveCauseFromWire,
  leaveCauseToWire,
  DEFAULT_TRACK,
  SUB_BINARY_MEMBERS_MAX,
  SUB_BINARY_TRACKS_MAX,
  emptyTrackWants,
  mergeTrackWants,
  wantsTrack,
  wantsAnyBinary,
  sanitizeBinaryWants,
}
export type {
  RoomConfigRecord,
  RoomMemberRecord,
  MemberSnapshot,
  RoomSnapshotMetadata,
  ParticipantStubMetadata,
  RoomEnvelope,
  RoomRosterEvent,
  RoomDemandEvent,
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
  ReqDmAck,
  MemberWants,
  TrackWants,
  BinaryWants,
}

import { assert, assertUsage } from '../../utils/assert.js'
import { isObject } from '../../utils/isObject.js'
import type { ChannelPublishInfo } from '../channel.js'
import type {
  BinaryPublishOptions,
  JoinOptions,
  LeaveCause,
  ParticipantMeta,
  RoomMeta,
  RoomSendReceipt,
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
/** Pub/sub key carrying one member's data: default-track binary always (per-publisher keys
 *  make delivery member-selective at the source), text too in isolated mode. */
function roomMemberDataKey(roomId: string, memberId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomId}:m:${memberId}`
}
/** Pub/sub key carrying one member's named binary track. Per-(member, track) keys make delivery
 *  track-selective at the source: a holder that stops watching a track drops this subscription,
 *  and the publisher's `receivers` hits 0 when nobody anywhere holds it — bytes stop flowing at
 *  every hop, not just at delivery. */
function roomMemberTrackKey(roomId: string, memberId: string, track: string): string {
  return `${roomMemberDataKey(roomId, memberId)}:t:${track}`
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

/** Reserved KV namespace for the identity→membership index — kept separate from
 *  `ROOM_KEY_NAMESPACE` so a room's member-record scan (`roomMemberKvPrefix`) never sweeps it. */
const IDENTITY_KEY_NAMESPACE = 'telefunc:identity:'

/** KV key marking one membership of an app identity: one key per (room, identity, member), so
 *  concurrent joins of the same identity never clobber each other (a list value would — the KV has
 *  no compare-and-set). The index is a hint — written before the member record and cleared after
 *  it, so it may transiently over-include but never silently under-includes; readers confirm each
 *  member ID against its record (identity match), which makes phantoms impossible. Room and identity
 *  are encoded so a `:` in either can't collide across pairs; the member ID is a delimiter-free UUID. */
function roomIdentityMemberKvKey(roomId: string, identity: string, memberId: string): string {
  return `${IDENTITY_KEY_NAMESPACE}${encodeURIComponent(roomId)}:${encodeURIComponent(identity)}:${memberId}`
}
/** KV prefix enumerating every membership of one identity in one room (`keys()` → member IDs). */
function roomIdentityKvPrefix(roomId: string, identity: string): string {
  return `${IDENTITY_KEY_NAMESPACE}${encodeURIComponent(roomId)}:${encodeURIComponent(identity)}:`
}
/** KV prefix enumerating every identity-index key of a room — for wholesale cleanup on close. */
function roomIdentityRoomKvPrefix(roomId: string): string {
  return `${IDENTITY_KEY_NAMESPACE}${encodeURIComponent(roomId)}:`
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
  /** App identity stamped at (server-side) join — absent: none. Immutable per member. */
  identity?: string
  /** Named binary tracks this member has published — appended by the owner before the first
   *  frame of each track, so late observers can subscribe every track they can't name. */
  tracks?: string[]
  /** An off-presence participant (`join({ hidden: true })`) — a member for routing/discovery but
   *  excluded from presence (count, roster, `onJoin`/`onLeave`/`onEmpty`). Read via
   *  `getParticipants({ hidden: true })`. */
  hidden?: boolean
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
  /** App identity stamped at (server-side) join — `null`/absent: none. Immutable per member. */
  identity?: string | null
  /** Named binary tracks the member has published (see `RoomMemberRecord.tracks`). */
  tracks?: string[]
  /** Whether this participant is off-presence — carried on the roster so observers exclude it from
   *  presence and can surface it via `getParticipants({ hidden: true })` (see `RoomMemberRecord.hidden`). */
  hidden?: boolean
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
  /** Tail mode (`Room.get(id, { tail: true })`): the client holds relayed text until its first
   *  `subscribe()`, so history read after serialization can be stitched to the live stream. */
  tail?: boolean
}

/** Serializer metadata of a `LocalParticipant` crossing the wire. */
type ParticipantStubMetadata = {
  channelId: string
  roomId: string
  id: string
  meta: ParticipantMeta
  joinedAt: number
  selfDelivery: boolean
  identity: string | null
}

/** Presence & lifecycle events, published on the room's control key by whichever node caused them.
 *
 *  The origin applies its own event locally (for deterministic same-node semantics) and then
 *  receives it back via the pub/sub echo. `join`/`leave`/`closed` are naturally idempotent;
 *  `p-meta` orders by the owner-issued `seq`, `update` by its `at`/`by` stamp — echoes and
 *  concurrent writers converge to the same winner on every node, whatever the arrival order. */
type RoomCtrlEnvelope =
  | { __r: 'join'; id: string; meta: ParticipantMeta; joinedAt: number; identity?: string; hidden?: boolean }
  | { __r: 'leave'; id: string; cause?: 'removed' | 'disconnected'; reason?: unknown }
  | { __r: 'p-meta'; id: string; meta: ParticipantMeta; prev: ParticipantMeta; seq: number }
  | { __r: 'update'; meta: RoomMeta; prev: RoomMeta; size: number | null; at: number; by: string }
  // A member's first publish on a new named track — announced before the frame, so live
  // all-track subscribers bring up the track-key subscription (idempotent, like join).
  | { __r: 'track'; id: string; track: string }
  // Track-demand gossip (`onDemand`): a node announces that its local demand for one member's
  // (member, track) stream turned on/off, tagged with its instance id. The member's owning node
  // aggregates these across nodes into a global demand count — node-to-node only, never relayed
  // to clients. `track` is `DEFAULT_TRACK` for the plain `publishBinary()` lane.
  | { __r: 'want'; member: string; track: string; node: string; on: boolean }
  | { __r: 'closed' }

/** A participant's message. Published on the room's text key (shared mode) or the member's own
 *  key (isolated mode). `fromMeta` is the sender's meta as verified by the sender's own node —
 *  never client-supplied — so any receiver can surface a correct sender even before its roster
 *  view catches up (see `RoomState.applyData`). */
type RoomDataEnvelope = { __r: 'data'; from: string; fromMeta: ParticipantMeta; fromIdentity?: string; data: unknown }

/** What a client sends upward to publish — its node verifies membership and stamps `fromMeta`. */
type RoomDataPublish = { __r: 'data'; from: string; data: unknown }

/** A room-authored message (`Room.announce()`) — no sender, delivered to `onAnnounce()`. */
type RoomAnnounceEnvelope = { __r: 'announce'; data: unknown }

type RoomEnvelope = RoomCtrlEnvelope | RoomDataEnvelope | RoomAnnounceEnvelope

/** The authoritative roster, pushed once per stub after its peer attaches — never published on
 *  the adapter. Position-in-stream consistency: every event relayed before it is already
 *  reflected in it; later events apply incrementally on top. */
type RoomRosterEvent = { __r: 'roster'; members: MemberSnapshot[] }

/** Global demand for one of a member's own published tracks, pushed to that member's stub
 *  (`onDemand`) whenever the owning node's aggregate count changes. `track` is `null` for the
 *  default `publishBinary()` lane. `count` is the approximate number of interested subscribers. */
type RoomDemandEvent = { __r: 'demand'; member: string; track: string | null; count: number }

/** A direct message, published on the target's inbox key (`roomDmKey`) — transport-level
 *  privacy: only the target's owning node subscribes, only its holder receives the relay.
 *  `to` lets a holder of several participants route the message to the right one. */
type RoomDmEnvelope = {
  __r: 'dm'
  to: string
  from: string
  fromMeta: ParticipantMeta | null
  fromIdentity?: string
  data: unknown
}

/** Client→server requests on a `Room` stub channel. `id` identifies the sending participant.
 *  `sub-binary` declares the client's binary wants (full replace, see `BinaryWants`);
 *  `sub-text` declares member-scoped text wants — the room-level (all) text want rides the
 *  standard broadcast-subscription ctrl instead, keeping its synchronous-declaration fence. */
type RoomStubRequest =
  | { __r: 'req-join'; meta: ParticipantMeta; selfDelivery: boolean }
  | { __r: 'req-leave'; id: string }
  | { __r: 'req-set-meta'; id: string; meta: ParticipantMeta }
  | { __r: 'req-set-attrs'; id: string; attrs: ParticipantMeta }
  | { __r: 'req-dm'; id: string; to: string; data: unknown }
  | { __r: 'sub-binary'; wants: BinaryWants }
  | { __r: 'sub-text'; members: string[] }

/** Client→server requests on a standalone `LocalParticipant` stub channel. */
type ParticipantStubRequest =
  | { __r: 'req-publish'; data: unknown }
  | { __r: 'req-set-meta'; meta: ParticipantMeta }
  | { __r: 'req-set-attrs'; attrs: ParticipantMeta }
  | { __r: 'req-dm'; to: string; data: unknown }
  | { __r: 'req-leave' }

/** Server→client notices on a standalone `LocalParticipant` stub channel. */
type ParticipantStubNotice =
  | { __r: 'left'; cause?: 'removed' | 'disconnected' | 'closed'; reason?: unknown }
  | { __r: 'p-meta'; meta: ParticipantMeta }
  | { __r: 'dm'; from: string; fromMeta: ParticipantMeta | null; fromIdentity?: string; data: unknown }
  | { __r: 'demand'; track: string | null; count: number }

/** Which members' streams a holder wants on the text lane — `all` for room-level listeners,
 *  or a specific member set for participant-scoped ones. */
type MemberWants = { all: boolean; members: string[] }

type ReqOkAck = { ok: true } | { ok: false; err: string }
type ReqJoinAck = { ok: true; id: string; joinedAt: number } | { ok: false; err: string }
type ReqPublishAck = { ok: true; ack: ChannelPublishInfo } | { ok: false; err: string }
type ReqDmAck = { ok: true; ack: RoomSendReceipt } | { ok: false; err: string }

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

/** Merge `attrs` into `meta` per key, returning a new object — the `setAttributes()` semantics.
 *  A value of `undefined` deletes its key (the serializer preserves `undefined` on the wire). */
function mergeAttributes(meta: ParticipantMeta, attrs: ParticipantMeta): ParticipantMeta {
  const next: ParticipantMeta = { ...meta }
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined) delete next[key]
    else next[key] = value
  }
  return next
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

/** Binary frame flags (one byte after the member ID). */
const FRAME_FLAG_KEY = 0b0000_0001
const FRAME_FLAG_TRACK = 0b0000_0010
/** Track names stay tiny — they ride every frame. */
const TRACK_MAX_BYTES = 64
const frameTextEncoder = /* @__PURE__ */ new TextEncoder()
const frameTextDecoder = /* @__PURE__ */ new TextDecoder()

/** Binary relay format: `[16-byte member UUID][1-byte flags][?1-byte track length + track][payload]`.
 *  A plain publish costs one flag byte; named tracks (mic/camera/screen on one member lane) and
 *  the keyframe bit make media multiplexing first-class — no hand-rolled envelopes. */
function frameWithMemberId(memberId: string, payload: Uint8Array, opts?: BinaryPublishOptions): Uint8Array {
  const idBytes = uuidToBytes(memberId)
  assert(idBytes, 'room member IDs are UUIDs')
  let flags = opts?.keyFrame === true ? FRAME_FLAG_KEY : 0
  let trackBytes: Uint8Array | null = null
  if (opts?.track !== undefined) {
    assertUsage(typeof opts.track === 'string' && opts.track.length > 0, 'track should be a non-empty string')
    trackBytes = frameTextEncoder.encode(opts.track)
    assertUsage(trackBytes.byteLength <= TRACK_MAX_BYTES, `track should be at most ${TRACK_MAX_BYTES} bytes`)
    flags |= FRAME_FLAG_TRACK
  }
  const headerLength = MEMBER_ID_BYTE_LENGTH + 1 + (trackBytes ? 1 + trackBytes.byteLength : 0)
  const framed = new Uint8Array(headerLength + payload.byteLength)
  framed.set(idBytes, 0)
  framed[MEMBER_ID_BYTE_LENGTH] = flags
  if (trackBytes) {
    framed[MEMBER_ID_BYTE_LENGTH + 1] = trackBytes.byteLength
    framed.set(trackBytes, MEMBER_ID_BYTE_LENGTH + 2)
  }
  framed.set(payload, headerLength)
  return framed
}

/** Split a binary relay frame into sender, track, keyframe bit, and payload. `null` on truncation. */
function unframeMemberId(
  data: Uint8Array,
): { from: string; payload: Uint8Array; track: string | null; keyFrame: boolean } | null {
  if (data.byteLength < MEMBER_ID_BYTE_LENGTH + 1) return null
  const flags = data[MEMBER_ID_BYTE_LENGTH]!
  const keyFrame = (flags & FRAME_FLAG_KEY) !== 0
  let track: string | null = null
  let offset = MEMBER_ID_BYTE_LENGTH + 1
  if (flags & FRAME_FLAG_TRACK) {
    if (data.byteLength < offset + 1) return null
    const trackLength = data[offset]!
    offset += 1
    if (data.byteLength < offset + trackLength) return null
    track = frameTextDecoder.decode(data.subarray(offset, offset + trackLength))
    offset += trackLength
  }
  return { from: bytesToUuid(data), payload: data.subarray(offset), track, keyFrame }
}

// ---------------------------------------------------------------------------
// Binary wants — per member, per track
// ---------------------------------------------------------------------------

/** The default (unnamed) track's slot in want sets and key routing — track names are non-empty
 *  by contract (`frameWithMemberId`), so `''` is unambiguous. */
const DEFAULT_TRACK = ''

/** Which of a publisher's tracks a holder wants: every track, or an exact set
 *  (`DEFAULT_TRACK` selects the unnamed lane). */
type TrackWants = { all: boolean; tracks: string[] }

/** A holder's complete binary wants. `everyMember` comes from room-level listeners and applies
 *  to all members; `members` adds participant-scoped wants on top. This one shape drives all
 *  three gates: the client's declaration, the server's upstream key set, and the per-stub relay. */
type BinaryWants = { everyMember: TrackWants; members: Record<string, TrackWants> }

/** Abuse bounds on a client's `sub-binary` declaration — generous for real apps (a media app
 *  uses a handful of tracks), fatal for hostile blowups (tracks multiply upstream keys). */
const SUB_BINARY_MEMBERS_MAX = 4096
const SUB_BINARY_TRACKS_MAX = 64

function emptyTrackWants(): TrackWants {
  return { all: false, tracks: [] }
}

function mergeTrackWants(a: TrackWants, b: TrackWants): TrackWants {
  if (a.all || b.all) return { all: true, tracks: [] }
  return { all: false, tracks: [...new Set([...a.tracks, ...b.tracks])] }
}

function wantsTrack(wants: TrackWants, track: string): boolean {
  return wants.all || wants.tracks.includes(track)
}

function wantsAnyBinary(wants: BinaryWants): boolean {
  return wants.everyMember.all || wants.everyMember.tracks.length > 0 || Object.keys(wants.members).length > 0
}

/** Validate a client-declared `sub-binary` want (untrusted input) — bounded and well-formed,
 *  or `null` to reject the declaration. */
function sanitizeBinaryWants(wants: unknown): BinaryWants | null {
  if (!isObject(wants)) return null
  const everyMember = sanitizeTrackWants(wants.everyMember)
  if (!everyMember || !isObject(wants.members)) return null
  const members: Record<string, TrackWants> = {}
  const entries = Object.entries(wants.members)
  if (entries.length > SUB_BINARY_MEMBERS_MAX) return null
  for (const [memberId, trackWants] of entries) {
    const sanitized = sanitizeTrackWants(trackWants)
    if (!sanitized) return null
    members[memberId] = sanitized
  }
  return { everyMember, members }
}

function sanitizeTrackWants(wants: unknown): TrackWants | null {
  if (!isObject(wants) || typeof wants.all !== 'boolean' || !Array.isArray(wants.tracks)) return null
  if (wants.tracks.length > SUB_BINARY_TRACKS_MAX) return null
  if (!wants.tracks.every((track) => typeof track === 'string' && track.length <= TRACK_MAX_BYTES)) return null
  return { all: wants.all, tracks: wants.tracks as string[] }
}
