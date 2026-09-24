// Shared Room storage records and wire envelopes.
export { hasRoomTag, joinedMember, decodeDmReply, inboxMessageFromWire, wireDmFromInbox }
export type {
  RoomConfigRecord,
  RoomMemberRecord,
  MemberSnapshot,
  RoomSnapshotMetadata,
  ParticipantStubMetadata,
  RemoteParticipantMetadata,
  RoomEnvelope,
  RoomRosterEvent,
  RoomDemandEvent,
  RoomCtrlEnvelope,
  RoomDataEnvelope,
  RoomDataPublish,
  RoomDmEnvelope,
  RoomDmAckEnvelope,
  DmReply,
  AcceptedMeta,
  RoomFailure,
  RoomStubRequest,
  ParticipantStubRequest,
  ParticipantStubNotice,
  MemberWants,
  InboxMessage,
  WireLeaveCause,
}

import { isRecord } from './model.js'
import type { BinaryWants } from './binary.js'
import type { ParticipantMeta, RoomMeta } from './types.js'

/** The head's config. `at`/`by` stamps the latest room meta write. `inc` is random, not a counter: a room recreated after its tombstone lapses can't reuse an id a stale handle holds. */
type RoomConfigRecord = {
  meta: RoomMeta
  at: number
  by: string
  inc: string
}

/** A member cell. Its owner renews `seenAt` every heartbeat; a record older than the member TTL is reaped. */
type RoomMemberRecord = {
  meta: ParticipantMeta
  joinedAt: number
  seenAt: number
  /** Issued by the member's single owner; orders `p-meta` events. */
  metaSeq: number
  identity?: string
  /** Appended before each named track's first frame, so a late observer can subscribe a track it can't name. */
  tracks?: string[]
  /** Off-presence (`join({ hidden: true })`): routable, but not in count, roster or presence events. */
  hidden?: boolean
}
// Wire shapes
type MemberSnapshot = {
  id: string
  meta: ParticipantMeta
  joinedAt: number
  metaSeq: number
  identity?: string | null
  tracks?: string[]
  hidden?: boolean
}
/** Scalars only: the roster streams once the stub's peer attaches, so serializing a room is O(1) in members. */
type RoomSnapshotMetadata = {
  channelId: string
  roomId: string
  meta: RoomMeta
  closed: boolean
  count: number
  /** The config's last-writer-wins stamp, which orders later updates. */
  stamp: { at: number; by: string }
}
/** Serializer metadata of a `RemoteParticipant` crossing the wire: its room, revived first, and the member snapshot. */
type RemoteParticipantMetadata = MemberSnapshot & { room: unknown; identity: string | null }
/** Serializer metadata of a `LocalParticipant` crossing the wire. */
type ParticipantStubMetadata = {
  channelId: string
  id: string
  meta: ParticipantMeta
  selfDelivery: boolean
  identity: string | null
}
/** Control-lane events. The origin applies its own event and later absorbs the echo: `join`/`leave`/`closed` are
 *  idempotent, `p-meta` orders by `seq` and `update` by its stamp, so every instance converges. */
type RoomCtrlEnvelope =
  | { __r: 'join'; id: string; meta: ParticipantMeta; joinedAt: number; identity?: string; hidden?: boolean }
  | ({ __r: 'leave'; id: string; hidden?: boolean } & WireLeaveCause)
  | { __r: 'p-meta'; id: string; meta: ParticipantMeta; seq: number; hidden?: boolean }
  | { __r: 'update'; meta: RoomMeta; at: number; by: string }
  // Announced before a named track's first frame, so all-track subscribers open its lane.
  | { __r: 'track'; id: string; track: string; hidden?: boolean }
  // Demand gossip between instances, never relayed to clients.
  | { __r: 'want'; member: string; track: string; instance: string; on: boolean }
  | { __r: 'closed' }

/** A `LeaveCause` on the wire: no `cause` means the member left on its own. */
type WireLeaveCause = { cause?: 'removed' | 'disconnected' | 'closed'; reason?: unknown }
/** A member's message; its order rides the transport frame. `fromMeta` is stamped by the sender's instance, never the client, so a receiver behind on the roster still names the sender. */
type RoomDataEnvelope = {
  __r: 'data'
  from: string
  fromMeta: ParticipantMeta
  fromIdentity?: string
  data: unknown
}
/** What a client sends upward to publish. Its instance verifies membership and stamps `fromMeta`. */
type RoomDataPublish = { __r: 'data'; from: string; data: unknown; retain?: boolean }
/** A room-authored message (`Room.announce()`), on the semantic lane so it shares one order with member text. */
type RoomAnnounceEnvelope = { __r: 'announce'; data: unknown }
type RoomEnvelope = RoomCtrlEnvelope | RoomDataEnvelope | RoomAnnounceEnvelope
/** Sent when a stub opens: it reflects every event relayed before it. A failed read says so, so roster getters don't wait forever. */
type RoomRosterEvent = { __r: 'roster'; members: MemberSnapshot[] } | { __r: 'roster-error' }
/** Global demand for one of a member's own published tracks, pushed on aggregate state changes. */
type RoomDemandEvent = { __r: 'demand'; member: string; track: string | null; wanted: boolean }
/** On the recipient's inbox lane, which only its owner subscribes. `ackId` asks for a reply on the sender's inbox. */
type RoomDmEnvelope = {
  __r: 'dm'
  to: string
  from: string
  fromMeta: ParticipantMeta | null
  fromIdentity?: string
  data: unknown
  ackId?: string
}
/** An ack DM's reply, on the original sender's inbox lane. */
type RoomDmAckEnvelope = { __r: 'dm-ack'; to: string; ackId: string } & DmReply
/** The recipient handler's return, or its failure. */
type DmReply = { ok: true; result: unknown } | RoomFailure

/** A client-supplied reply, rebuilt field by field so no other key rides into the `dm-ack` envelope. */
function decodeDmReply(reply: unknown): DmReply | null {
  if (!isRecord(reply)) return null
  if (reply.ok === true) return { ok: true, result: reply.result }
  if (reply.ok !== false) return null
  if (reply.abort === true) return { ok: false, abort: true, abortValue: reply.abortValue }
  return typeof reply.err === 'string' ? { ok: false, err: reply.err } : null
}

/** The ack of a member meta write: the committed value and its sequence. */
type AcceptedMeta = { meta: ParticipantMeta; seq: number }

/** Published failure form for the one path that cannot use a native channel ack. */
type RoomFailure = { ok: false; abort: true; abortValue: unknown } | { ok: false; err: string }

/** Client→server requests on a room stub; `id` is the acting member. The room-wide text want rides the Broadcast subscription, which reattaches before `onOpen`. */
type RoomStubRequest =
  | { __r: 'req-join'; meta: ParticipantMeta; selfDelivery: boolean }
  | { __r: 'req-leave'; id: string }
  | { __r: 'req-set-meta'; id: string; meta: ParticipantMeta }
  | { __r: 'req-set-attrs'; id: string; attrs: ParticipantMeta }
  | { __r: 'req-dm'; id: string; to: string; data: unknown; ack?: boolean }
  | { __r: 'dm-reply'; id: string; ackId: string; reply: DmReply }
  | { __r: 'sub-binary'; wants: BinaryWants }
  | { __r: 'sub-text'; members: string[]; announce: boolean }

/** Client→server requests on a participant stub; an ack DM is answered through the channel's own ack, so there's no `dm-reply`. */
type ParticipantStubRequest =
  | { __r: 'req-publish'; data: unknown; retain?: boolean }
  | { __r: 'req-set-meta'; meta: ParticipantMeta }
  | { __r: 'req-set-attrs'; attrs: ParticipantMeta }
  | { __r: 'req-dm'; to: string; data: unknown; ack?: boolean }
  | { __r: 'req-leave' }

/** Server→client notices on a participant stub. */
type ParticipantStubNotice =
  | ({ __r: 'left' } & WireLeaveCause)
  | { __r: 'p-meta'; meta: ParticipantMeta; seq: number }
  | { __r: 'dm'; from: string; fromMeta: ParticipantMeta | null; fromIdentity?: string; data: unknown; ackId?: string }
  | { __r: 'demand'; track: string | null; wanted: boolean }

/** Which members' streams a holder wants on the text lane: `all` for room-level listeners, or a specific member set for participant-scoped ones. */
type MemberWants = { all: boolean; members: string[] }

/** A delivered private message, as stamped by the sender's instance. `ackId` is present when the sender awaits a reply (`send(…, { ack: true })`). */
type InboxMessage = {
  from: string
  fromMeta: ParticipantMeta | null
  fromIdentity: string | null
  data: unknown
  ackId?: string
}
type WireDm = Omit<RoomDmEnvelope, '__r' | 'to'>
function inboxMessageFromWire(dm: WireDm): InboxMessage {
  return {
    from: dm.from,
    fromMeta: dm.fromMeta,
    fromIdentity: dm.fromIdentity ?? null,
    data: dm.data,
    ...(dm.ackId ? { ackId: dm.ackId } : {}),
  }
}
function wireDmFromInbox(msg: InboxMessage): WireDm {
  return {
    from: msg.from,
    fromMeta: msg.fromMeta,
    ...(msg.fromIdentity === null ? {} : { fromIdentity: msg.fromIdentity }),
    data: msg.data,
    ...(msg.ackId ? { ackId: msg.ackId } : {}),
  }
}

/** The member a `join` event announces, before any meta write or track. */
function joinedMember(event: Extract<RoomCtrlEnvelope, { __r: 'join' }>): MemberSnapshot {
  return {
    id: event.id,
    meta: event.meta,
    joinedAt: event.joinedAt,
    metaSeq: 0,
    identity: event.identity ?? null,
    ...(event.hidden ? { hidden: true } : {}),
  }
}

/** All room messages are tagged with `__r`: envelopes, requests, and notices alike. */
function hasRoomTag(value: unknown): value is { __r: string } {
  return isRecord(value) && typeof value.__r === 'string'
}
