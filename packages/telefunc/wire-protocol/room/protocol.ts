// Shared Room storage records and wire envelopes. Behavior lives in the model, error, key, and binary modules.
export { hasRoomTag, pushBoundedTail }
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
  RoomOrder,
  RoomDmEnvelope,
  RoomDmAckEnvelope,
  DmReply,
  RoomFailure,
  RoomStubRequest,
  ParticipantStubRequest,
  ParticipantStubNotice,
  MemberWants,
}

import { ROOM_TAIL_HOLD_CODE_UNITS_MAX, ROOM_TAIL_HOLD_MAX } from './constants.js'
import { isRecord } from './model.js'
import type { BinaryWants } from './binary.js'
import type { ParticipantMeta, RoomMeta } from './types.js'

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
/** Append to a tail hold, drop-oldest under BOTH the count cap and the serialized-code-unit cap. A single
 *  entry larger than the whole code-unit budget is dropped, never held — the tail is best-effort. Shared by the
 *  room's pre-attach hold (`ServerRoom._tailHold`) and the per-stub hold (`RoomStubChannel._holdTail`)
 *  so neither can grow to ~256 × the ingress limit, nor be fed an uncapped server-side `me.publish()`. */
function pushBoundedTail(hold: TailEntry[], entry: TailEntry): void {
  if (entry.serialized.length > ROOM_TAIL_HOLD_CODE_UNITS_MAX) return
  hold.push(entry)
  let size = 0
  for (const e of hold) size += e.serialized.length
  while (hold.length > ROOM_TAIL_HOLD_MAX || size > ROOM_TAIL_HOLD_CODE_UNITS_MAX) {
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

/** Published failure form for the one path that cannot use a native channel ack. */
type RoomFailure = { ok: false; abort: true; abortValue: unknown } | { ok: false; err: string }

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

/** All room messages are tagged with `__r` — envelopes, requests, and notices alike. */
function hasRoomTag(value: unknown): value is { __r: string } {
  return isRecord(value) && typeof value.__r === 'string'
}
