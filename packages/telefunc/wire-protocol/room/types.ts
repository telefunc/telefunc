export type {
  Room,
  RoomInfo,
  RoomOptions,
  RoomMeta,
  RoomGetOptions,
  JoinOptions,
  PublishOptions,
  ParticipantMeta,
  LocalParticipant,
  RemoteParticipant,
  Sender,
  SendGuard,
  PublishGuard,
  JoinGuard,
  AfterPublishHook,
  AfterSendHook,
  AfterJoinHook,
  RoomSendReceipt,
  RoomAckReceipt,
  LeaveCause,
  ParticipantRef,
  BinaryFrameInfo,
  BinaryPublishOptions,
  RoomSnapshotView,
  ParticipantSnapshotView,
  RoomBinaryListener,
}

import type { ChannelPublishAck, ChannelPublishInfo } from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'

/** Type-only publish shield, generated from `Pub` and enforced at server ingress. */
type RoomShield<Pub> = {
  readonly [TELEFUNC_SHIELDS]: {
    data: Pub
  }
}

/** Room metadata (e.g. topic). Must be serializable. */
type RoomMeta = Record<string, unknown>
/** Participant metadata (e.g. name, score). Must be serializable. */
type ParticipantMeta = Record<string, unknown>

/** A verified sender snapshot; use `room.getParticipant(id)` for the live handle. */
type Sender<P extends ParticipantMeta = ParticipantMeta> = {
  readonly id: string
  readonly meta: P
  readonly identity: string | null
}

/** Runs before a private send; throw to reject it. */
type SendGuard<P extends ParticipantMeta = ParticipantMeta> = (
  from: Sender<P>,
  to: Sender<P>,
  data: unknown,
) => void | Promise<void>

/** Runs before a room-wide text or binary publish; throw to reject it. */
type PublishGuard<P extends ParticipantMeta = ParticipantMeta> = (
  from: Sender<P>,
  data: unknown,
) => void | Promise<void>

/** Runs before admission; throw to reject before membership is written. */
type JoinGuard<P extends ParticipantMeta = ParticipantMeta> = (member: Sender<P>) => void | Promise<void>

/** The receipt for a delivered private message, passed to `onAfterSend`. */
type RoomSendReceipt = { seq: number; timestamp: number }

/** A delivered-message receipt plus the recipient listener's response. */
type RoomAckReceipt = RoomSendReceipt & { response: unknown }

/** Runs after a room-wide publish commits; throwing cannot undo delivery. */
type AfterPublishHook<P extends ParticipantMeta = ParticipantMeta> = (
  from: Sender<P>,
  data: unknown,
  info: { seq: number; timestamp: number; receivers?: number },
) => void | Promise<void>

/** Runs after a private message is delivered; throwing cannot undo delivery. */
type AfterSendHook<P extends ParticipantMeta = ParticipantMeta> = (
  from: Sender<P>,
  to: Sender<P>,
  data: unknown,
  info: RoomSendReceipt,
) => void | Promise<void>

/** Runs after a join commits and is announced; throwing cannot undo it. */
type AfterJoinHook<P extends ParticipantMeta = ParticipantMeta> = (
  member: Sender<P>,
  info: { joinedAt: number },
) => void | Promise<void>

/** Why a participant left; roster reconciliation may report no cause. */
type LeaveCause = {
  type: 'left' | 'removed' | 'closed' | 'disconnected'
  reason?: unknown
}

/** One membership by id, or all memberships of an app identity. */
type ParticipantRef = { id: string } | { identity: string }

type RoomOptions<M extends RoomMeta = RoomMeta> = {
  /** Room metadata, visible to all observers. Default: `{}`. */
  meta?: M
}

type RoomGetOptions = {
  /** Hold a bounded server-side live tail until the client's first subscription. */
  tail?: boolean
}

/** Options for `join()`; `identity` and `hidden` are server-only. */
type JoinOptions<P extends ParticipantMeta = ParticipantMeta> = {
  /** Your participant metadata (e.g. name, score), visible to all observers. Default: `{}`. */
  meta?: P
  /** Whether the messages you publish are delivered back to the room object on your side
   *  (default: `true`). Turn off e.g. for video, where you don't want your own frames back. */
  selfDelivery?: boolean
  /** Trusted app identity, immutable for this membership. Server-only. */
  identity?: string
  /** Exclude this full participant from presence. Server-only. */
  hidden?: boolean
}

/** One participant inside `room.snapshot()`. */
type ParticipantSnapshotView<P extends ParticipantMeta = ParticipantMeta> = {
  readonly id: string
  readonly identity: string | null
  readonly meta: P
  readonly joinedAt: number
}

/** Immutable and reference-stable until the room changes. */
type RoomSnapshotView<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta> = {
  readonly id: string
  readonly meta: M
  readonly count: number
  readonly isClosed: boolean
  readonly participants: readonly ParticipantSnapshotView<P>[]
}

/** Lightweight room snapshot returned by `Room.list()`. */
type RoomInfo<M extends RoomMeta = RoomMeta> = {
  readonly id: string
  readonly meta: M
  readonly count: number
  readonly isEmpty: boolean
}

/** Per-frame binary metadata, straight from the frame header. */
type BinaryFrameInfo = {
  /** Named substream, or `null` for the default track. */
  track: string | null
  /** Publisher metadata, or `null` when absent. */
  meta: Record<string, unknown> | null
}

/** Publish-side options for text messages. */
type PublishOptions = {
  /** Conflate in-flight updates by key, keeping only the latest pending value. */
  coalesce?: string
  /** Keep this as the room's last-write-wins retained message. */
  retain?: boolean
}

/** Publish-side binary options. */
type BinaryPublishOptions = {
  /** Named substream (≤ 255 UTF-8 bytes) — subscribers can filter by it. Default: the default track. */
  track?: string
  /** Per-frame metadata surfaced as `info.meta` (≤ 65,535 bytes serialized). */
  meta?: Record<string, unknown>
  /** Keep this as the track's last-write-wins retained frame. */
  retain?: boolean
}

type RoomBinaryListener<P extends ParticipantMeta = ParticipantMeta> = (
  data: Uint8Array,
  info: ChannelPublishInfo & BinaryFrameInfo,
  from: Sender<P>,
) => unknown

/** A returnable multi-party room with presence, membership, and events. */
type Room<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = {
  /** The ID the room was created with. */
  readonly id: string
  readonly meta: M
  readonly count: number
  readonly isEmpty: boolean
  readonly isClosed: boolean

  /** Join the room, optionally with your participant `meta`. Returns your own participant handle. */
  join(options?: JoinOptions<P>): Promise<LocalParticipant<P, Pub>>

  /** Visible participants, or hidden participants when requested server-side. */
  getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant<P, Pub>[]>
  /** One participant, or `null`; loads the member view on first need. */
  getParticipant(id: string): Promise<RemoteParticipant<P, Pub> | null>

  /** Receive all participant messages. Returns an unsubscribe function. */
  subscribe(callback: (data: Pub, info: ChannelPublishInfo, from: Sender<P>) => unknown): () => void
  /** Receive all members' binary frames, optionally filtered to one track at the source. */
  subscribeBinary(callback: RoomBinaryListener<P>, options?: { track?: string | null }): () => void

  /** A participant joined. */
  onJoin(callback: (member: RemoteParticipant<P, Pub>) => void): () => void
  /** A participant left; reconciled leaves have no cause. */
  onLeave(callback: (member: RemoteParticipant<P, Pub>, cause?: LeaveCause) => void): () => void
  /** Any participant's metadata changed. */
  onParticipantUpdate(callback: (member: RemoteParticipant<P, Pub>, meta: P, prev: P) => void): () => void
  /** The room's metadata was replaced (`Room.setMeta()`) or merged (`Room.setAttributes()`). */
  onUpdate(callback: (meta: M, prev: M) => void): () => void
  /** A room-authored message arrived (`Room.announce()`) — e.g. system notices. */
  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void): () => void
  /** The last participant left. */
  onEmpty(callback: () => void): () => void
  /** The room was closed via `Room.close()` (on the client, also: the connection is gone). */
  onClose(callback: () => void): () => void

  /** Anything observable changed; pairs with `snapshot()`. */
  onChange(callback: () => void): () => void
  /** Immutable whole-room view, reference-stable until the next change. */
  snapshot(): RoomSnapshotView<M, P>
} & RoomShield<Pub>

/** Your returnable participant handle, returned by `join()`. */
type LocalParticipant<P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = {
  readonly id: string
  readonly meta: P
  /** The app identity this membership was joined with — `null` when none was set. */
  readonly identity: string | null
  /** Whether the messages you publish are delivered back to the room object on your side. Set at `join()`. */
  readonly selfDelivery: boolean

  /** Publish a message to the whole room. */
  publish(data: Pub, options?: PublishOptions): Promise<ChannelPublishAck>
  /** Publish binary, optionally on a named track with per-frame metadata. */
  publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck>

  /** Send privately; `{ ack: true }` waits for the recipient handler's response. */
  send(to: string | Sender<P>, data: unknown): Promise<RoomSendReceipt>
  send(to: string | Sender<P>, data: unknown, options: { ack?: false }): Promise<RoomSendReceipt>
  send(to: string | Sender<P>, data: unknown, options: { ack: true }): Promise<RoomAckReceipt>
  /** Receive private messages; returning a value answers an acknowledged send. */
  listen(callback: (data: unknown, from: Sender<P> | null) => unknown): () => void

  /** Watch whether any observer wants each published track. */
  onDemand(callback: (track: string | null, wanted: boolean) => void): () => void
  /** Replace your metadata wholesale. Propagates to all observers in real time. */
  setMeta(meta: P): Promise<void>
  /** Merge metadata per key; `undefined` deletes a key. */
  setAttributes(attributes: Partial<P>): Promise<void>

  leave(): Promise<void>
  /** You left. `cause.type` says how — `'left'` (you), `'removed'` (kicked, with the kick's
   *  `reason`), `'closed'` (the room), `'disconnected'` (the connection died). */
  onLeave(callback: (cause: LeaveCause) => void): () => void
} & RoomShield<Pub>

/** A returnable view of another room member, bound to its room's live state. */
type RemoteParticipant<P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = {
  readonly id: string
  readonly meta: P
  /** The app identity stamped at join, or `null`. */
  readonly identity: string | null
  /** Unix epoch milliseconds. */
  readonly joinedAt: number

  /** Receive only this member's messages. Returns an unsubscribe function. */
  subscribe(callback: (data: Pub, info: ChannelPublishInfo) => unknown): () => void
  /** Receive only this member's binary frames, optionally filtered to one track. */
  subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo) => unknown,
    options?: { track?: string | null },
  ): () => void

  /** This member's metadata changed. */
  onUpdate(callback: (meta: P, prev: P) => void): () => void
  /** This member left the room. `cause` as on `Room`'s `onLeave`. */
  onLeave(callback: (cause?: LeaveCause) => void): () => void
}
