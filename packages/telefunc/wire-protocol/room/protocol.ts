// The room wire protocol — everything both sides must agree on and nothing that behaves: pub/sub + KV keys, stored records, wire envelopes, the binary frame format, and want declarations. `RoomState`
// (state.ts) applies these events; `ServerRoom`/`ClientRoom` move them.
export {
  roomCtrlKey,
  roomMemberKvKey,
  roomMemberKvPrefix,
  roomMemberCleanupKvKey,
  roomMemberCleanupKvPrefix,
  roomIdentityMemberKvKey,
  roomIdentityKvPrefix,
  stampNewer,
  uuidToBytes,
  frameWithMemberId,
  unframeMemberId,
  binaryFrameSender,
  hasRoomTag,
  normalizeJoinOptions,
  ownMetadata,
  mergeAttributes,
  RoomError,
  isRoomError,
  toRoomFailure,
  roomFailureError,
  roomAckError,
  ROOM_BUG_MESSAGE,
  DM_PARTICIPANT_LEFT,
  leaveCauseFromWire,
  leaveCauseToWire,
  DEFAULT_TRACK,
  isRoomTrack,
  emptyTrackWants,
  mergeTrackWants,
  wantsTrack,
  wantsAnyBinary,
  binaryWantsCovers,
  sanitizeBinaryWants,
  pushBoundedTail,
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
  RoomOrder,
  RoomDmEnvelope,
  RoomDmAckEnvelope,
  DmReply,
  RoomFailure,
  RoomStubRequest,
  ParticipantStubRequest,
  ParticipantStubNotice,
  MemberWants,
  TrackWants,
  BinaryWants,
}

import { parse } from '@brillout/json-serializer/parse'
import { stringify } from '@brillout/json-serializer/stringify'
import { assert, assertUsage } from '../../utils/assert.js'
import { isBrandedError } from '../../utils/isBrandedError.js'
import { isObject } from '../../utils/isObject.js'
import { createAbortError } from '../../shared/Abort.js'
import { STATUS_BODY_INTERNAL_SERVER_ERROR } from '../../shared/constants.js'
import { ROOM_TAIL_HOLD_MAX, ROOM_TAIL_HOLD_BYTES_MAX } from './constants.js'
import { ACK_STATUS } from '../shared-ws.js'
import type { AckResultStatus } from '../shared-ws.js'
import type { ChannelPublishInfo } from '../channel.js'
import { classifyTelefuncError } from '../error-classification.js'
import type {
  BinaryPublishOptions,
  JoinOptions,
  LeaveCause,
  ParticipantMeta,
  RoomMeta,
  RoomSendReceipt,
} from './types.js'
function isRecord(value: unknown): value is Record<string, unknown> {
  if (!isObject(value) || Array.isArray(value)) return false
  try {
    const prototype = Object.getPrototypeOf(value)
    return prototype === null || (Object.getPrototypeOf(prototype) === null && prototype.constructor?.name === 'Object')
  } catch {
    return false
  }
}
/** Take ownership of metadata at a state boundary and expose only the immutable owned value. */
function ownMetadata<T extends RoomMeta | ParticipantMeta>(meta: T): T {
  return Object.freeze({ ...meta }) as T
}
// Keys & records
/** Reserved pub/sub + KV namespace for rooms. Don't use it for `BroadcastChannel` keys. */
const ROOM_KEY_NAMESPACE = 'telefunc:room:'
/** App-supplied components may contain delimiters; malformed UTF-16 fails as usage. */
function roomKeyComponent(value: string): string {
  assertUsage(value.isWellFormed(), 'Room key components should be well-formed strings')
  return encodeURIComponent(value)
}
/** Stable channel identity used by a Room stub crossing a response. */
function roomCtrlKey(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomKeyComponent(roomId)}`
}
/** KV key of one member record. */
function roomMemberKvKey(roomId: string, memberId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomKeyComponent(roomId)}:m:${memberId}`
}
/** KV prefix under which all of a room's member records live. Member IDs are UUIDs, which is how member records are told apart from keys of other rooms whose ID shares the prefix. */
function roomMemberKvPrefix(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomKeyComponent(roomId)}:m:`
}
/** Durable work left by membership eviction. The marker is committed in the same cell transaction that removes the member, then cleared only after retained data and the semantic leave are done. */
function roomMemberCleanupKvKey(roomId: string, memberId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomKeyComponent(roomId)}:cleanup:${memberId}`
}
function roomMemberCleanupKvPrefix(roomId: string): string {
  return `${ROOM_KEY_NAMESPACE}${roomKeyComponent(roomId)}:cleanup:`
}
/** Reserved KV namespace for the identity→membership index — kept separate from `ROOM_KEY_NAMESPACE` so a room's member-record scan (`roomMemberKvPrefix`) never sweeps it. */
const IDENTITY_KEY_NAMESPACE = 'telefunc:identity:'
/** KV key marking one membership of an app identity: one key per (room, identity, member), so concurrent joins of the same identity never clobber each other and each membership stays independently
 * removable — a shared list value would put every membership of an identity behind one read-modify-write of the same record. The index is a hint — written before the member record and cleared after
 * it, so it may transiently over-include but never silently under-includes; readers confirm each member ID against its record (identity match), which makes phantoms impossible. Room and identity are
 * encoded so a `:` in either can't collide across pairs; the member ID is a delimiter-free UUID.
 */
function roomIdentityMemberKvKey(roomId: string, identity: string, memberId: string): string {
  return `${IDENTITY_KEY_NAMESPACE}${roomKeyComponent(roomId)}:${roomKeyComponent(identity)}:${memberId}`
}
/** KV prefix enumerating every membership of one identity in one room (`keys()` → member IDs). */
function roomIdentityKvPrefix(roomId: string, identity: string): string {
  return `${IDENTITY_KEY_NAMESPACE}${roomKeyComponent(roomId)}:${roomKeyComponent(identity)}:`
}
/** Stored opaquely in the backend head. `at`/`by` is the last-writer-wins stamp of the latest `Room.setMeta()`/`Room.setAttributes()` (see `applyRoomUpdate`). `inc` is the room's incarnation id: a
 * fresh random id on every (re)create, so a member record or mutation from a previous incarnation can't attach to the current one (see `RoomMemberRecord.inc`). Random, not a counter, so a recreation
 * after the tombstone TTL lapses can't reuse a previous incarnation's id and let a stale handle false-match. The authority owns this record; legality (join/mutate/close) is decided against it, never
 * against the eventually-consistent replica.
 */
type RoomConfigRecord = {
  meta: RoomMeta
  at: number
  by: string
  inc: string
}
/** Later timestamp wins; equal timestamps break deterministically by writer ID. */
function stampNewer(a: { at: number; by: string }, b: { at: number; by: string }): boolean {
  return a.at > b.at || (a.at === b.at && a.by > b.by)
}
/** Stored in the incarnation's member cell. `seenAt` is the liveness timestamp: the owning node refreshes it every `ROOM_HEARTBEAT_INTERVAL_MS`; records older than `ROOM_MEMBER_TTL_MS` are reaped. */
type RoomMemberRecord = {
  meta: ParticipantMeta
  joinedAt: number
  seenAt: number
  /** Monotonic meta revision, issued by the member's single owner — orders `p-meta` events. */
  metaSeq: number
  /** App identity stamped at (server-side) join — absent: none. Immutable per member. */
  identity?: string
  /** Named binary tracks this member has published — appended by the owner before the first frame of each track, so late observers can subscribe every track they can't name. */
  tracks?: string[]
  /** An off-presence participant (`join({ hidden: true })`) — a member for routing/discovery but
   *  excluded from presence (count, roster, `onJoin`/`onLeave`/`onEmpty`). Read via
   *  `getParticipants({ hidden: true })`. */
  hidden?: boolean
}
// Wire shapes
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
  closed: boolean
  count: number
  /** LWW stamp of the config the snapshot reflects — seeds `applyRoomUpdate` ordering. */
  stamp: { at: number; by: string }
}
/** Serializer metadata of a `LocalParticipant` crossing the wire. */
type ParticipantStubMetadata = {
  channelId: string
  id: string
  meta: ParticipantMeta
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
  | { __r: 'leave'; id: string; cause?: 'removed' | 'disconnected' | 'closed'; reason?: unknown }
  | { __r: 'p-meta'; id: string; meta: ParticipantMeta; seq: number }
  | { __r: 'update'; meta: RoomMeta; at: number; by: string }
  // A member's first publish on a new named track — announced before the frame, so live all-track subscribers bring up the track-key subscription (idempotent, like join).
  | { __r: 'track'; id: string; track: string }
  // Track-demand gossip (`onDemand`): a node announces that its local demand for one member's (member, track) stream turned on/off, tagged with its instance id. The member's owning node aggregates
  // these across nodes into one wanted transition — node-to-node only, never relayed to clients. `track` is `DEFAULT_TRACK` for the plain `publishBinary()` lane.
  | { __r: 'want'; member: string; track: string; node: string; on: boolean }
  | { __r: 'closed' }

/** A semantic message's position. Within one incarnation `seq` is the strictly increasing domain cursor; `timestamp` is independently clamped authority time and never controls sequence reset. */
type RoomOrder = { seq: number; timestamp: number }
/** One tail-entry: a held recent text message (see `ROOM_TAIL_HOLD_MAX`). */
type TailEntry = { serialized: string; ord: RoomOrder; from: string }
/** Append to a tail hold, drop-oldest under BOTH the count cap and the total-size cap. A single entry
 *  larger than the whole size budget is dropped, never held — the tail is best-effort. Shared by the
 *  room's pre-attach hold (`ServerRoom._tailHold`) and the per-stub hold (`RoomStubChannel._holdTail`)
 *  so neither can grow to ~256 × the ingress limit, nor be fed an uncapped server-side `me.publish()`. */
function pushBoundedTail(hold: TailEntry[], entry: TailEntry): void {
  if (entry.serialized.length > ROOM_TAIL_HOLD_BYTES_MAX) return
  hold.push(entry)
  let size = 0
  for (const e of hold) size += e.serialized.length
  while (hold.length > ROOM_TAIL_HOLD_MAX || size > ROOM_TAIL_HOLD_BYTES_MAX) {
    size -= hold.shift()!.serialized.length
  }
}
/** A participant's message. Committed on the semantic lane, whose receipt rides the transport frame; the order is not duplicated in this envelope. The receiver reads `WirePublishInfo`, the source of
 * the message's place in the room's semantic timeline. `fromMeta` is the sender's meta as verified by the sender's own node — never client-supplied — so any receiver can surface a correct sender even
 * before its roster view catches up (see `RoomState.applyData`).
 */
type RoomDataEnvelope = {
  __r: 'data'
  from: string
  fromMeta: ParticipantMeta
  fromIdentity?: string
  data: unknown
}
/** What a client sends upward to publish — its node verifies membership and stamps `fromMeta`. */
type RoomDataPublish = { __r: 'data'; from: string; data: unknown; retain?: boolean }
/** A room-authored message (`Room.announce()`) — no sender, delivered to `onAnnounce()`. Committed on
 *  the same semantic lane as participant text, so the two share one order domain; like
 *  text, its order rides the transport frame, not this envelope. */
type RoomAnnounceEnvelope = { __r: 'announce'; data: unknown }
type RoomEnvelope = RoomCtrlEnvelope | RoomDataEnvelope | RoomAnnounceEnvelope
/** The authoritative roster response, pushed once when the replayable stub first opens. Position-in-stream consistency: every event relayed before a successful roster is already reflected in it; later
 * events apply incrementally on top. Failure is explicit so client roster getters cannot wait forever after a backend read rejects.
 */
type RoomRosterEvent = { __r: 'roster'; members: MemberSnapshot[] } | { __r: 'roster-error' }
/** Global demand for one of a member's own published tracks, pushed on aggregate state changes. */
type RoomDemandEvent = { __r: 'demand'; member: string; track: string | null; wanted: boolean }
/** A direct message, published on the target's inbox lane — transport-level privacy: only the target's owning node subscribes, only its holder receives the relay. `to` lets a holder of several
 * participants route the message to the right one. `ackId` is present iff the sender wants a reply (`send(…, { ack: true })`) — the recipient's node routes the handler's result back as a
 * `RoomDmAckEnvelope` on the sender's inbox.
 */
type RoomDmEnvelope = {
  __r: 'dm'
  to: string
  from: string
  fromMeta: ParticipantMeta | null
  fromIdentity?: string
  data: unknown
  ackId?: string
}
/** The reply to an `{ ack: true }` DM, published back on the *sender's* inbox lane —
 *  which the sender's own node already subscribes to. `to` is the original sender; `ackId`
 *  correlates it to the pending `send`. Carries the recipient's handler return, or its error. */
type RoomDmAckEnvelope = { __r: 'dm-ack'; to: string; ackId: string } & DmReply
/** The result of handling an `{ ack: true }` DM: the recipient's `listen` return, or its failure (an `Abort` value the handler raised, or an operational/generic error — see `RoomFailure`). */
type DmReply = { ok: true; result: unknown } | RoomFailure
/** The `{ ack: true }` reply a sender gets when its recipient has already left — resolved (never left hanging) with one stable reason, wherever the departure is noticed (its stub, its held inbox). */
const DM_PARTICIPANT_LEFT: DmReply = { ok: false, err: 'Participant left the room' }
/** Client→server requests on a `Room` stub channel. `id` identifies the sending participant. `sub-binary` declares the client's binary wants (full replace, see `BinaryWants`); `sub-text` declares
 * member-scoped text and announcement wants — the room-level (all) text want rides the standard broadcast-subscription ctrl instead, keeping its synchronous-declaration fence.
 */
type RoomStubRequest =
  | { __r: 'req-join'; meta: ParticipantMeta; selfDelivery: boolean }
  | { __r: 'req-leave'; id: string }
  | { __r: 'req-set-meta'; id: string; meta: ParticipantMeta }
  | { __r: 'req-set-attrs'; id: string; attrs: ParticipantMeta }
  | { __r: 'req-dm'; id: string; to: string; data: unknown; ack?: boolean }
  // A client-held member's reply to an `{ ack: true }` DM it received — routed back to the sender.
  | ({ __r: 'dm-reply'; id: string; ackId: string } & DmReply)
  | { __r: 'sub-binary'; wants: BinaryWants }
  | { __r: 'sub-text'; members: string[]; announce: boolean }

/** Client→server requests on a standalone `LocalParticipant` stub channel. (An ack DM this
 *  participant receives replies through the channel's own ack, so there is no `dm-reply` here —
 *  unlike the shared room stub, which multiplexes many members and needs the explicit reply.) */
type ParticipantStubRequest =
  | { __r: 'req-publish'; data: unknown; retain?: boolean }
  | { __r: 'req-set-meta'; meta: ParticipantMeta }
  | { __r: 'req-set-attrs'; attrs: ParticipantMeta }
  | { __r: 'req-dm'; to: string; data: unknown; ack?: boolean }
  | { __r: 'req-leave' }

/** Server→client notices on a standalone `LocalParticipant` stub channel. `dm`'s `ackId`, when present, asks the client to reply (`send(…, { ack: true })`) — see `RoomDmEnvelope`. */
type ParticipantStubNotice =
  | { __r: 'left'; cause?: 'removed' | 'disconnected' | 'closed'; reason?: unknown }
  | { __r: 'p-meta'; meta: ParticipantMeta }
  | { __r: 'dm'; from: string; fromMeta: ParticipantMeta | null; fromIdentity?: string; data: unknown; ackId?: string }
  | { __r: 'demand'; track: string | null; wanted: boolean }

/** Which members' streams a holder wants on the text lane — `all` for room-level listeners, or a specific member set for participant-scoped ones. */
type MemberWants = { all: boolean; members: string[] }
/** Decode a leave event's cause — an absent wire cause means a voluntary leave. */
function leaveCauseFromWire(event: {
  cause?: 'removed' | 'disconnected' | 'closed'
  reason?: unknown
}): LeaveCause {
  if (event.cause === 'removed') {
    return event.reason === undefined ? { type: 'removed' } : { type: 'removed', reason: event.reason }
  }
  return { type: event.cause ?? 'left' }
}
/** Encode a cause into leave-event fields — `'left'` is the wire default and travels as nothing. */
function leaveCauseToWire(cause: LeaveCause): {
  cause?: 'removed' | 'disconnected' | 'closed'
  reason?: unknown
} {
  if (cause.type === 'removed')
    return cause.reason === undefined ? { cause: 'removed' } : { cause: 'removed', reason: cause.reason }
  return cause.type === 'left' ? {} : { cause: cause.type }
}
/** All room messages are tagged with `__r` — envelopes, requests, and notices alike. */
function hasRoomTag(value: unknown): value is { __r: string } {
  return isRecord(value) && typeof value.__r === 'string'
}
/** Merge `attrs` into `meta` per key, returning a new object — the `setAttributes()` semantics. A value of `undefined` deletes its key (the serializer preserves `undefined` on the wire). */
function mergeAttributes(meta: ParticipantMeta, attrs: ParticipantMeta): ParticipantMeta {
  const next: ParticipantMeta = { ...meta }
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined) delete next[key]
    else Object.defineProperty(next, key, { value, enumerable: true, configurable: true, writable: true })
  }
  return Object.freeze(next)
}
/** Validates `join(options)` and resolves the participant `meta` + `selfDelivery`. */
function normalizeJoinOptions(options: JoinOptions | undefined): {
  meta: ParticipantMeta
  selfDelivery: boolean
  identity: string | null
  hidden: boolean
} {
  assertUsage(options === undefined || isRecord(options), 'join() options should be an object')
  const meta = options?.meta ?? {}
  assertUsage(isRecord(meta), 'join() options.meta should be an object')
  assertUsage(
    options?.selfDelivery === undefined || typeof options.selfDelivery === 'boolean',
    'join() options.selfDelivery should be a boolean',
  )
  assertUsage(
    options?.identity === undefined || (typeof options.identity === 'string' && options.identity.length > 0),
    'join() options.identity should be a non-empty string',
  )
  assertUsage(
    options?.hidden === undefined || typeof options.hidden === 'boolean',
    'join() options.hidden should be a boolean',
  )
  return {
    meta: ownMetadata(meta),
    selfDelivery: options?.selfDelivery !== false,
    identity: options?.identity ?? null,
    hidden: options?.hidden ?? false,
  }
}
// Error contract: Abort carries its value, RoomError carries a safe message, and bugs are reported
// on the throwing side and hidden from the caller.
const roomErrorBrand = Symbol.for('telefunc.RoomError')
/** An expected caller-facing rejection; the global brand survives duplicate module graphs. */
class RoomError extends Error {
  readonly [roomErrorBrand] = true as const
  constructor(message: string) {
    super(message)
    this.name = 'RoomError'
    // Restore the prototype chain across the down-levelled `extends Error`.
    Object.setPrototypeOf(this, new.target.prototype)
  }
}
function isRoomError(thing: unknown): thing is RoomError {
  return isBrandedError(thing, roomErrorBrand)
}
const ROOM_BUG_MESSAGE = `${STATUS_BODY_INTERNAL_SERVER_ERROR} — see server logs`
/** Published failure form for the one path that cannot use a native channel ack. */
type RoomFailure = { ok: false; abort: true; abortValue: unknown } | { ok: false; err: string }
function toRoomFailure(err: unknown, report: (err: unknown) => void): RoomFailure {
  const classified = classifyTelefuncError(err, isRoomError)
  if (classified.kind === 'abort') return { ok: false, abort: true, abortValue: classified.error.abortValue }
  if (classified.kind === 'expected') return { ok: false, err: classified.error.message }
  report(err)
  return { ok: false, err: ROOM_BUG_MESSAGE }
}
function roomFailureError(res: RoomFailure): Error {
  if ('abort' in res) return createAbortError(res.abortValue)
  return new RoomError(res.err)
}
function roomAckError(err: unknown, report: (err: unknown) => void): { text: string; status: AckResultStatus } {
  const classified = classifyTelefuncError(err, isRoomError)
  if (classified.kind === 'abort') return { text: stringify(classified.error.abortValue), status: ACK_STATUS.ABORT }
  if (classified.kind === 'expected') return { text: classified.error.message, status: ACK_STATUS.ERROR }
  if (classified.kind === 'shield') return { text: classified.error.message, status: ACK_STATUS.SHIELD_ERROR }
  report(err)
  return { text: ROOM_BUG_MESSAGE, status: ACK_STATUS.ERROR }
}
// Member IDs — UUIDs, framed as a fixed 16-byte prefix on binary messages
const MEMBER_ID_BYTE_LENGTH = 16
const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
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
const FRAME_FLAG_META = 0b0000_0001
const FRAME_FLAG_TRACK = 0b0000_0010
const FRAME_FLAG_RETAIN = 0b0000_0100
/** The only bits the framer defines. Any other bit set is a malformed or forward-incompatible frame. */
const FRAME_FLAGS_KNOWN = FRAME_FLAG_META | FRAME_FLAG_TRACK | FRAME_FLAG_RETAIN
/** Structural maxima of the one-byte track-length and two-byte metadata-length fields. */
const TRACK_LENGTH_FIELD_MAX = 0xff
const META_LENGTH_FIELD_MAX = 0xffff
const frameTextEncoder = /* @__PURE__ */ new TextEncoder()
const frameTextDecoder = /* @__PURE__ */ new TextDecoder('utf-8', { fatal: true, ignoreBOM: true })
/** Binary relay format: `[16-byte member UUID][1-byte flags][?1-byte track length + track][?2-byte meta length + meta][payload]`. A plain publish costs one flag byte; named tracks (mic/camera/screen
 * on one member lane) and optional per-frame `meta` ride only when set, so media multiplexing needs no hand-rolled envelopes.
 */
function frameWithMemberId(memberId: string, payload: Uint8Array, opts?: BinaryPublishOptions): Uint8Array {
  const idBytes = uuidToBytes(memberId)
  assert(idBytes, 'room member IDs are UUIDs')
  let flags = opts?.retain === true ? FRAME_FLAG_RETAIN : 0
  let trackBytes: Uint8Array | null = null
  if (opts?.track !== undefined) {
    assertUsage(
      isRoomTrack(opts.track) && opts.track.length > 0,
      `track should be a non-empty well-formed string of at most ${TRACK_LENGTH_FIELD_MAX} bytes`,
    )
    trackBytes = frameTextEncoder.encode(opts.track)
    flags |= FRAME_FLAG_TRACK
  }
  let metaBytes: Uint8Array | null = null
  if (opts?.meta !== undefined) {
    assertUsage(isRecord(opts.meta), 'publishBinary() meta should be an object')
    metaBytes = frameTextEncoder.encode(stringify(opts.meta))
    assertUsage(
      metaBytes.byteLength <= META_LENGTH_FIELD_MAX,
      `publishBinary() meta should be at most ${META_LENGTH_FIELD_MAX} bytes once serialized`,
    )
    flags |= FRAME_FLAG_META
  }
  const headerLength =
    MEMBER_ID_BYTE_LENGTH +
    1 +
    (trackBytes ? 1 + trackBytes.byteLength : 0) +
    (metaBytes ? 2 + metaBytes.byteLength : 0)
  const framed = new Uint8Array(headerLength + payload.byteLength)
  framed.set(idBytes, 0)
  framed[MEMBER_ID_BYTE_LENGTH] = flags
  let offset = MEMBER_ID_BYTE_LENGTH + 1
  if (trackBytes) {
    framed[offset] = trackBytes.byteLength
    framed.set(trackBytes, offset + 1)
    offset += 1 + trackBytes.byteLength
  }
  if (metaBytes) {
    framed[offset] = (metaBytes.byteLength >> 8) & 0xff
    framed[offset + 1] = metaBytes.byteLength & 0xff
    framed.set(metaBytes, offset + 2)
    offset += 2 + metaBytes.byteLength
  }
  framed.set(payload, headerLength)
  return framed
}
/** Split a binary relay frame into sender, track, per-frame meta, and payload. The one validating seam for an untrusted frame (a hand-crafted publish need not have gone through `frameWithMemberId`):
 * `null` on truncation, an empty named track (`''` is the default lane), or metadata that isn't valid serialized JSON. A `null` return means "reject this frame" — callers never see a half-parsed one.
 */
function unframeMemberId(data: Uint8Array): {
  from: string
  payload: Uint8Array
  track: string | null
  meta: Record<string, unknown> | null
  retain: boolean
} | null {
  if (data.byteLength < MEMBER_ID_BYTE_LENGTH + 1) return null
  const flags = data[MEMBER_ID_BYTE_LENGTH]!
  if (flags & ~FRAME_FLAGS_KNOWN) return null // an unknown flag bit — malformed or forward-incompatible; reject it
  const retain = (flags & FRAME_FLAG_RETAIN) !== 0
  let track: string | null = null
  let meta: Record<string, unknown> | null = null
  let offset = MEMBER_ID_BYTE_LENGTH + 1
  if (flags & FRAME_FLAG_TRACK) {
    if (data.byteLength < offset + 1) return null
    const trackLength = data[offset]!
    offset += 1
    if (trackLength === 0 || data.byteLength < offset + trackLength) return null
    try {
      track = frameTextDecoder.decode(data.subarray(offset, offset + trackLength))
    } catch {
      return null
    }
    offset += trackLength
  }
  if (flags & FRAME_FLAG_META) {
    if (data.byteLength < offset + 2) return null
    const metaLength = (data[offset]! << 8) | data[offset + 1]!
    offset += 2
    if (data.byteLength < offset + metaLength) return null
    try {
      const parsedMeta: unknown = parse(frameTextDecoder.decode(data.subarray(offset, offset + metaLength)))
      if (!isRecord(parsedMeta)) return null // meta must be a record — scalars/arrays are rejected
      meta = parsedMeta
    } catch {
      return null // malformed meta JSON on a hand-crafted frame — reject the whole frame, don't throw
    }
    offset += metaLength
  }
  return { from: bytesToUuid(data), payload: data.subarray(offset), track, meta, retain }
}
/** The sender UUID carried in a binary frame's fixed prefix, or `null` if the frame is too short to
 *  hold one. Cheap (reads only the member-ID prefix): lets the publish path check membership before
 *  the full validating `unframeMemberId`, so a frame is parsed once, not twice. */
function binaryFrameSender(data: Uint8Array): string | null {
  return data.byteLength >= MEMBER_ID_BYTE_LENGTH ? bytesToUuid(data) : null
}
// Binary wants — per member, per track
/** The default (unnamed) track's slot in want sets and key routing — track names are non-empty by contract (`frameWithMemberId`), so `''` is unambiguous. */
const DEFAULT_TRACK = ''
/** Which of a publisher's tracks a holder wants: every track, or an exact set (`DEFAULT_TRACK` selects the unnamed lane). */
type TrackWants = { all: boolean; tracks: string[] }
/** A holder's complete binary wants. `everyMember` comes from room-level listeners and applies
 *  to all members; `members` adds participant-scoped wants on top. This one shape drives all
 *  three gates: the client's declaration, the server's upstream key set, and the per-stub relay. */
type BinaryWants = { everyMember: TrackWants; members: Record<string, TrackWants> }
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
/** Does a complete binary want cover one (member, track)? The one predicate behind both the live
 *  relay gate (`RoomStubChannel._wantsBinary`) and retained-frame replay: `everyMember` applies to
 *  all members, `members` adds participant-scoped wants on top. */
function binaryWantsCovers(wants: BinaryWants, memberId: string, track: string): boolean {
  if (wantsTrack(wants.everyMember, track)) return true
  const memberWants = wants.members[memberId]
  return memberWants !== undefined && wantsTrack(memberWants, track)
}
function wantsAnyBinary(wants: BinaryWants): boolean {
  return wants.everyMember.all || wants.everyMember.tracks.length > 0 || Object.keys(wants.members).length > 0
}
/** Validate a client-declared `sub-binary` want (untrusted input), or return `null`. */
function sanitizeBinaryWants(wants: unknown): BinaryWants | null {
  if (!isRecord(wants)) return null
  const everyMember = sanitizeTrackWants(wants.everyMember)
  if (!everyMember || !isRecord(wants.members)) return null
  const members: Record<string, TrackWants> = Object.create(null)
  const entries = Object.entries(wants.members)
  for (const [memberId, trackWants] of entries) {
    if (uuidToBytes(memberId) === null) return null
    const sanitized = sanitizeTrackWants(trackWants)
    if (!sanitized) return null
    members[memberId] = sanitized
  }
  return { everyMember, members }
}
function sanitizeTrackWants(wants: unknown): TrackWants | null {
  if (!isRecord(wants) || typeof wants.all !== 'boolean' || !Array.isArray(wants.tracks)) return null
  // Bound by UTF-8 bytes, the same unit the frame path uses (`frameWithMemberId`) — a `.length` char count could admit a want that doesn't fit the frame's one-byte track-length field.
  if (!wants.tracks.every(isRoomTrack)) return null
  return { all: wants.all, tracks: wants.tracks as string[] }
}
function isRoomTrack(track: unknown): track is string {
  return (
    typeof track === 'string' &&
    track.isWellFormed() &&
    frameTextEncoder.encode(track).byteLength <= TRACK_LENGTH_FIELD_MAX
  )
}
