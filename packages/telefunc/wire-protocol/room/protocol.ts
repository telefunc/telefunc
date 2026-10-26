// Room wire envelopes, requests and notices, shared by client and server.
export { hasRoomTag, joinedMember, inboxMessageFromWire, dmNotice, leaveCauseFromWire, leaveCauseToWire }
export type {
  MemberSnapshot,
  RoomSnapshotMetadata,
  ParticipantStubMetadata,
  RemoteParticipantMetadata,
  RoomEnvelope,
  RoomRosterEvent,
  RoomJoinedEvent,
  RoomDemandEvent,
  RoomCtrlEnvelope,
  RoomMetaEntry,
  RoomMetaUpdate,
  RoomDataEvent,
  SenderStamp,
  RoomDataPublish,
  RoomDmEnvelope,
  DmReply,
  MemberMetaUpdate,
  WholeMemberUpdate,
  RoomFailure,
  RoomResult,
  RoomStubRequest,
  ParticipantStubRequest,
  MemberMutationRequest,
  ParticipantStubNotice,
  MemberWants,
  InboxMessage,
  WireLeaveCause,
}

import { isRecord, ownLeaveCause, removedCause } from './model.js'
import type { TrackKey } from './binary.js'
import type { LeaveCause, ParticipantMeta } from './types.js'

type MemberSnapshot = {
  id: string
  meta: ParticipantMeta
  joinedAt: number
  metaSeq: number
  identity?: string | null
  hidden?: boolean
  /** The page holding the member can't be reached, and the room waits for it. */
  disconnected?: true
}

/** Scalars only: the roster streams once the stub's peer attaches, so serializing a room is O(1) in members. */
type RoomSnapshotMetadata = {
  channelId: string
  roomId: string
  meta: RoomMetaEntry[]
  closed: boolean
  count: number
  /** How many meta writes `meta` reflects, which orders later updates. */
  metaSeq: number
  /** The server's `config.channel.bufferLimit`, which bounds the DMs a participant holds before its first
   *  `listen()`. */
  bufferLimit: number
  maxFrameBytes?: number
}

/** Serializer metadata of a `RemoteParticipant` crossing the wire: its room, revived first, and the member snapshot. */
type RemoteParticipantMetadata = MemberSnapshot & { room: unknown; identity: string | null }

/** Serializer metadata of a `LocalParticipant` crossing the wire. */
type ParticipantStubMetadata = {
  channelId: string
  roomId: string
  id: string
  meta: ParticipantMeta
  selfDelivery: boolean
  identity: string | null
  /** As `RoomSnapshotMetadata.bufferLimit`. */
  bufferLimit: number
  maxFrameBytes?: number
}

/** Control-lane events. The origin applies its own event and later absorbs the echo: `join`/`leave`/`closed` are
 *  idempotent, `p-meta` and `update` order by `seq`, so every instance converges. */
type RoomCtrlEnvelope =
  | { __r: 'join'; id: string; meta: ParticipantMeta; joinedAt: number; identity?: string; hidden?: boolean }
  | ({ __r: 'leave'; id: string; hidden?: boolean } & WireLeaveCause)
  | ({ __r: 'p-meta'; id: string; hidden?: boolean } & MemberMetaUpdate)
  | RoomMetaUpdate
  // An instance whose demand lane delivers, from now on or again, asks every other one for its demand; one that missed
  // a page of another's asks that one (`of`).
  | { __r: 'demand-ask'; instance: string; of?: string }
  | { __r: 'closed' }

/** A room meta key with its place: the `meta:seq` the write that added it read, and its index in that write. */
type RoomMetaEntry = [key: string, value: unknown, placeSeq: number, placeIndex: number]

/** The room meta at `seq`, whole, or as the write at `seq` changed it: a key it removed as `[key]`. */
type RoomMetaUpdate =
  | { __r: 'update'; seq: number; meta: RoomMetaEntry[] }
  | { __r: 'update'; seq: number; changes: (RoomMetaEntry | [key: string])[] }

/** A `LeaveCause` on the wire: no `cause` means the member left on its own. */
type WireLeaveCause = { cause?: 'removed' | 'disconnected' | 'closed'; reason?: unknown }
/** The sender as its instance knew it at the send: its meta at `fromMetaSeq`, stamped by that instance, never the
 *  client. */
type SenderStamp = { fromMeta: ParticipantMeta; fromMetaSeq: number; fromIdentity?: string; fromDisconnected?: true }
/** A member's message as a client gets it: stamped only while the client can't name the sender yet. */
type RoomDataEvent = { __r: 'data'; from: string; data: unknown } & Partial<SenderStamp>
/** What a client sends upward to publish. Its instance verifies membership and stamps `fromMeta`. */
type RoomDataPublish = { __r: 'data'; from: string; data: unknown; retain?: boolean }
/** A room-authored message (`Room.announce()`), on the semantic lane so it shares one order with member text. */
type RoomAnnounceEnvelope = { __r: 'announce'; data: unknown }
type RoomEnvelope = RoomCtrlEnvelope | RoomDataEvent | RoomAnnounceEnvelope

/** Sent when a stub opens, a part per frame, numbered from 0, each part reflecting every event relayed before it;
 *  `more` on all but the last. A failed read says so, so roster getters don't wait forever. `rewound`: a failover
 *  rewound the room's meta seqs, so the next whole meta of the room and of each member applies, whatever its seq. */
type RoomRosterEvent =
  | { __r: 'roster'; members: MemberSnapshot[]; part: number; more?: true }
  | { __r: 'roster-error' }
  | { __r: 'rewound' }

/** To the client whose `req-join` (`reqId`) admitted the member, before any event naming it. */
type RoomJoinedEvent = { __r: 'joined'; reqId: number; id: string }

/** Global demand for one of a member's own published tracks, pushed on aggregate state changes; a reattached client
 *  gets the member's whole wanted set, since its offline buffer may have dropped changes. */
type RoomDemandEvent =
  | { __r: 'demand'; member: string; track: TrackKey; wanted: boolean }
  | { __r: 'demand-state'; member: string; tracks: TrackKey[] }

/** On the recipient's inbox lane, which only its owner subscribes. `ackId` asks for a reply on the sender's inbox. A
 *  room-authored one has no sender (`from: ''`, `fromMeta: null`); a client that names the sender gets it unstamped. */
type RoomDmEnvelope = {
  __r: 'dm'
  to: string
  from: string
  fromMeta?: ParticipantMeta | null
  fromMetaSeq?: number
  fromIdentity?: string
  fromDisconnected?: true
  data: unknown
  ackId?: string
}

/** The recipient handler's return, or its failure. */
type DmReply = { ok: true; result: unknown } | RoomFailure

/** A member's meta and connection at `seq`, whole, or as the write at `seq` changed them: a merge, with a key it
 *  removed as `undefined`, or the connection of the page holding the member. */
type MemberMetaUpdate =
  | { seq: number; meta: ParticipantMeta; disconnected?: true }
  | { seq: number; attrs: ParticipantMeta }
  | { seq: number; connected: boolean }
type WholeMemberUpdate = Extract<MemberMetaUpdate, { meta: ParticipantMeta }>

/** A safe Room failure: an Abort value or a caller-facing message. */
type RoomFailure = { ok: false; abort: true; abortValue: unknown } | { ok: false; err: string }

/** A room stub's answer to its client's request or publish: the value the call resolves to, or what it rejects with. */
type RoomResult =
  | { ok: true; value: unknown }
  | RoomFailure
  | { ok: false; shield: string }
  | { ok: false; overflow: string }

/** A member's meta, attributes and DM requests, as a participant stub carries them. */
type MemberMutationRequest =
  | { __r: 'req-set-meta'; meta: ParticipantMeta }
  | { __r: 'req-set-attrs'; attrs: ParticipantMeta }
  | { __r: 'req-dm'; to: string; data: unknown; ack?: boolean }

/** Client→server requests on a room stub; `id` is the acting member. The room-wide text want rides the Broadcast
 *  subscription, which reattaches before `onOpen`. */
type RoomStubRequest =
  | { __r: 'req-join'; reqId: number; meta: ParticipantMeta; selfDelivery: boolean }
  | { __r: 'req-leave'; id: string }
  | (MemberMutationRequest & { id: string })
  | { __r: 'dm-reply'; ackId: string; reply: DmReply }
  /** A `BinaryWantChange`, with `null` for `ALL`. */
  | { __r: 'sub-binary'; member: string | null; track: string | null; on: boolean }
  /** Changes only: announcements, and each member whose own want changed. */
  | { __r: 'sub-text'; announce: boolean; members: Record<string, boolean> }

/** Client→server requests on a participant stub; an ack DM's reply is a `dm-reply`, as on a room stub. */
type ParticipantStubRequest =
  | { __r: 'req-publish'; data: unknown; retain?: boolean }
  | MemberMutationRequest
  | { __r: 'req-leave' }

/** Server→client notices on a participant stub. `rewound`: a failover rewound the participant's meta seq, so its next
 *  whole meta applies, whatever its seq. */
type ParticipantStubNotice =
  | ({ __r: 'left' } & WireLeaveCause)
  | ({ __r: 'p-meta' } & MemberMetaUpdate)
  | { __r: 'rewound' }
  | { __r: 'dm'; from: string; fromMeta: ParticipantMeta | null; fromIdentity?: string; data: unknown; ackId?: string }
  | { __r: 'demand'; track: TrackKey; wanted: boolean }
  | { __r: 'demand-state'; tracks: TrackKey[] }

/** Which members' streams a holder wants on the text lane: `all` for room-level listeners, or a specific member set for
 *  participant-scoped ones. */
type MemberWants = { all: boolean; members: ReadonlySet<string> }

/** A delivered private message, as stamped by the sender's instance. `ackId` is present when the sender awaits a reply
 *  (`send(…, { ack: true })`). */
type InboxMessage = {
  from: string
  fromMeta: ParticipantMeta | null
  fromIdentity: string | null
  data: unknown
  ackId?: string
}

type WireDm = Omit<RoomDmEnvelope, '__r' | 'to'>

function inboxMessageFromWire(dm: WireDm): InboxMessage {
  const msg: InboxMessage = {
    from: dm.from,
    fromMeta: dm.fromMeta ?? null,
    fromIdentity: dm.fromIdentity ?? null,
    data: dm.data,
  }
  if (dm.ackId) msg.ackId = dm.ackId
  return msg
}

function dmNotice(msg: InboxMessage): Extract<ParticipantStubNotice, { __r: 'dm' }> {
  const notice: Extract<ParticipantStubNotice, { __r: 'dm' }> = {
    __r: 'dm',
    from: msg.from,
    fromMeta: msg.fromMeta,
    data: msg.data,
  }
  if (msg.fromIdentity !== null) notice.fromIdentity = msg.fromIdentity
  if (msg.ackId) notice.ackId = msg.ackId
  return notice
}

function leaveCauseFromWire(event: WireLeaveCause): LeaveCause {
  if (event.cause === 'removed') return removedCause(event.reason)
  return ownLeaveCause({ type: event.cause ?? 'left' })
}

function leaveCauseToWire(cause: LeaveCause): WireLeaveCause {
  if (cause.type === 'removed') {
    return cause.reason === undefined ? { cause: 'removed' } : { cause: 'removed', reason: cause.reason }
  }
  return cause.type === 'left' ? {} : { cause: cause.type }
}

/** The member a `join` event announces, before any meta write or track. */
function joinedMember(event: Extract<RoomCtrlEnvelope, { __r: 'join' }>): MemberSnapshot {
  const member: MemberSnapshot = {
    id: event.id,
    meta: event.meta,
    joinedAt: event.joinedAt,
    metaSeq: 0,
    identity: event.identity ?? null,
  }
  if (event.hidden) member.hidden = true
  return member
}

/** All room messages are tagged with `__r`: envelopes, requests, and notices alike. */
function hasRoomTag(value: unknown): value is { __r: string } {
  return isRecord(value) && typeof value.__r === 'string'
}
