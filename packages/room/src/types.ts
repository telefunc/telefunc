export type {
  Room,
  ClientRoom,
  RoomInfo,
  RoomOptions,
  RoomMeta,
  RoomGetOptions,
  JoinOptions,
  ClientJoinOptions,
  PublishOptions,
  ParticipantMeta,
  ReadonlyMeta,
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
  RoomMessage,
  RoomSnapshotView,
  ParticipantSnapshotView,
}

import type { ChannelPublishAck, ChannelPublishInfo, TELEFUNC_SHIELDS } from 'telefunc/__internal'

/** Type-only publish shield, generated from `Pub` and enforced at server ingress. */
type RoomShield<Pub> = {
  readonly [TELEFUNC_SHIELDS]: {
    data: Pub
  }
}

/** Room metadata (e.g. topic). Must be serializable. Read-only: freezing doesn't lock a `Map`, `Set`, `Date` or
 *  `Uint8Array` in it, so build a new value to change any of it. */
type RoomMeta = Record<string, unknown>
/** Participant metadata (e.g. name, score). Must be serializable. Read-only: freezing doesn't lock a `Map`, `Set`,
 *  `Date` or `Uint8Array` in it, so build a new value to change any of it. */
type ParticipantMeta = Record<string, unknown>
/** A meta as Room hands it to you: its records and arrays read-only all the way down. A `Map`, `Set`, `Date` or
 *  `Uint8Array` in it keeps its type, and the rule that meta is read-only. */
type ReadonlyMeta<T> = T extends
  | Date
  | Map<unknown, unknown>
  | Set<unknown>
  | ArrayBufferView
  | ((...args: never[]) => unknown)
  ? T
  : T extends object
    ? { readonly [K in keyof T]: ReadonlyMeta<T[K]> }
    : T

/** A verified sender: a plain `{ id, meta, identity }`, or a live `RemoteParticipant`, which has the same fields. */
type Sender<P extends ParticipantMeta = ParticipantMeta> = {
  readonly id: string
  readonly meta: ReadonlyMeta<P>
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
  info: { track: string | null },
) => void | Promise<void>

/** Runs before admission; throw to reject before membership is written. */
type JoinGuard<P extends ParticipantMeta = ParticipantMeta> = (member: Sender<P>) => void | Promise<void>

/** The receipt for a delivered private message, passed to `onAfterSend`. */
type RoomSendReceipt = { seq: number; timestamp: number }

/** A delivered-message receipt plus the recipient listener's response. */
type RoomAckReceipt = RoomSendReceipt & { response: unknown }

/** Runs after a room-wide text or binary publish commits (`data` is a binary frame's bytes); throwing cannot undo
 *  delivery. */
type AfterPublishHook<P extends ParticipantMeta = ParticipantMeta> = (
  from: Sender<P>,
  data: unknown,
  info: RoomSendReceipt & { track: string | null },
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

/** Why a participant left. One a roster no longer lists left with the cause its departing record keeps, or `unknown`
 *  once that record expired. */
type LeaveCause =
  | { readonly type: 'left' }
  | { readonly type: 'removed'; readonly reason?: unknown }
  | { readonly type: 'closed' }
  | { readonly type: 'disconnected' }
  /** It left while this side didn't follow the room, and its cause is no longer kept. */
  | { readonly type: 'unknown' }

/** One membership by id, or all memberships of an app identity. */
type ParticipantRef = { id: string } | { identity: string }

type RoomOptions<M extends RoomMeta = RoomMeta> = {
  /** Room metadata, visible to all observers. Default: `{}`. */
  meta?: M
  /** Milliseconds an unheld room nobody joined stays before it closes; `Infinity` never. Default:
   *  `roomConfig.emptyTimeout` from `@telefunc/room`. */
  emptyTimeout?: number
  /** Milliseconds an unheld room a member had joined stays before it closes; `Infinity` never. Default:
   *  `roomConfig.departureTimeout` from `@telefunc/room`. */
  departureTimeout?: number
}

type RoomGetOptions = {
  /** Hold a bounded server-side live tail until the client's first subscription. */
  tail?: boolean
}

/** Options for `join()`; `identity` and `hidden` are server-only. */
type JoinOptions<P extends ParticipantMeta = ParticipantMeta> = {
  /** Your participant metadata (e.g. name, score), visible to all observers. Default: `{}`. */
  meta?: P
  /** Whether the messages you publish are delivered back to the room object on your side (default: `true`). Turn off
   *  e.g. for video, where you don't want your own frames back. */
  selfDelivery?: boolean
  /** Trusted app identity, immutable for this membership. Server-only. */
  identity?: string
  /** Exclude this full participant from presence. Server-only. */
  hidden?: boolean
}

type ClientJoinOptions<P extends ParticipantMeta = ParticipantMeta> = Omit<JoinOptions<P>, 'identity' | 'hidden'>

/** One participant inside `room.snapshot()`. */
type ParticipantSnapshotView<P extends ParticipantMeta = ParticipantMeta> = {
  readonly id: string
  readonly identity: string | null
  readonly meta: ReadonlyMeta<P>
  readonly joinedAt: number
  /** False while the participant's page can't be reached and the room waits for it. */
  readonly isConnected: boolean
}

/** Immutable and reference-stable until the room changes. */
type RoomSnapshotView<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta> = {
  readonly id: string
  readonly meta: ReadonlyMeta<M>
  readonly count: number
  readonly isClosed: boolean
  readonly participants: readonly ParticipantSnapshotView<P>[]
}

/** Lightweight room snapshot returned by `Room.list()`. */
type RoomInfo<M extends RoomMeta = RoomMeta> = {
  readonly id: string
  readonly meta: ReadonlyMeta<M>
  readonly count: number
  readonly isEmpty: boolean
}

/** Per-frame metadata of a track, straight from the frame header. */
type BinaryFrameInfo = {
  /** Named substream, or `null` for the default track. */
  track: string | null
  /** Publisher metadata, or `null` when absent. */
  meta: Record<string, unknown> | null
}

/** Publish-side options for text messages. */
type PublishOptions = {
  /** Your named track (≤ 255 UTF-8 bytes) to publish on, which viewers `subscribe()` to with `{ track }`. Without it,
   *  the message goes on the room's message stream, where everyone gets every message in one order. */
  track?: string
  /** On a `track`: a newer message replaces one no connection has taken yet, so a page that can't keep up skips to the
   *  newest and the track's `info.seq` jumps over what it skipped. A track keeps the value of its first publish, which
   *  a later one may leave out; naming the other throws. Throws without `track`. Default: `false`. */
  lossy?: boolean
  /** Keep this as the room's last-write-wins retained message, or the track's retained message. */
  retain?: boolean
}

/** Publish-side binary options. */
type BinaryPublishOptions = {
  /** Named substream (≤ 255 UTF-8 bytes). Subscribers can filter by it. Default: the default track. */
  track?: string
  /** Per-frame metadata surfaced as `info.meta` (≤ 65,535 bytes serialized). */
  meta?: Record<string, unknown>
  /** Keep this as the track's last-write-wins retained frame. */
  retain?: boolean
  /** A newer frame replaces one no connection has taken yet, so a page that can't keep up skips to the newest and the
   *  track's `info.seq` jumps over what it skipped. Without it, a page gets every frame, or leaves the room
   *  (`'disconnected'`) where the server can't hold them. A track keeps the value of its first publish, which a later
   *  one may leave out; naming the other throws. Default: `false`. */
  lossy?: boolean
  /** On a lossy track: this frame needs every frame of the track since its newest one without `delta`, as a video's
   *  frame needs those since its key frame. A page that can't keep up then skips to the newest frame without it, and
   *  gets the frames after that one in order, as many as its path takes, never one without those before it; a page
   *  that starts watching gets none until the next frame without it. Throws unless the track is lossy, and with
   *  `retain`. Default: `false`. */
  delta?: boolean
}

/** One message `messages()` yields: what a `subscribe()` or `subscribeBinary()` listener receives, `from` being the
 *  sender as it was when the message arrived. */
type RoomMessage<Pub = unknown, P extends ParticipantMeta = ParticipantMeta> =
  | { type: 'text'; data: Pub; info: ChannelPublishInfo; from: Sender<P> }
  | { type: 'binary'; data: Uint8Array; info: ChannelPublishInfo & BinaryFrameInfo; from: Sender<P> }

type RoomBinaryListener<P extends ParticipantMeta = ParticipantMeta> = (
  data: Uint8Array,
  info: ChannelPublishInfo & BinaryFrameInfo,
  from: Sender<P>,
) => unknown

/** One member's binary frames: the sender is the member subscribed to. */
type MemberBinaryListener = (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo) => unknown

/** A returnable multi-party room with presence, membership, and events. */
type Room<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = {
  /** The ID the room was created with. */
  readonly id: string
  readonly meta: ReadonlyMeta<M>
  readonly count: number
  readonly isEmpty: boolean
  readonly isClosed: boolean

  /** Join the room, optionally with your participant `meta`. Returns your own participant handle. */
  join(options?: JoinOptions<P>): Promise<LocalParticipant<P, Pub>>

  /** Visible participants, or hidden participants when requested server-side. */
  getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant<P, Pub>[]>
  /** One participant, or `null`; loads the member view on first need. */
  getParticipant(id: string): Promise<RemoteParticipant<P, Pub> | null>

  /** Receive the messages published on `track`. Returns an unsubscribe function. */
  subscribe(
    callback: (data: Pub, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender<P>) => unknown,
    options: { track: string; signal?: AbortSignal },
  ): () => void
  /** Receive all participant messages. Returns an unsubscribe function. */
  subscribe(
    callback: (data: Pub, info: ChannelPublishInfo, from: Sender<P>) => unknown,
    options?: { signal?: AbortSignal },
  ): () => void
  /** Receive all members' binary frames, optionally filtered to one track at the source. */
  subscribeBinary(
    callback: RoomBinaryListener<P>,
    options?: { track?: string | null; signal?: AbortSignal },
  ): () => void

  /** A participant joined. */
  onJoin(callback: (member: RemoteParticipant<P, Pub>) => void, options?: { signal?: AbortSignal }): () => void
  /** A participant left, and why. */
  onLeave(
    callback: (member: RemoteParticipant<P, Pub>, cause: LeaveCause) => void,
    options?: { signal?: AbortSignal },
  ): () => void
  /** Any participant's metadata changed. */
  onParticipantUpdate(
    callback: (member: RemoteParticipant<P, Pub>, meta: ReadonlyMeta<P>, prev: ReadonlyMeta<P>) => void,
    options?: { signal?: AbortSignal },
  ): () => void
  /** The room's metadata was replaced (`Room.setMeta()`) or merged (`Room.setAttributes()`). */
  onUpdate(
    callback: (meta: ReadonlyMeta<M>, prev: ReadonlyMeta<M>) => void,
    options?: { signal?: AbortSignal },
  ): () => void
  /** A room-authored message arrived (`Room.announce()`), e.g. system notices. */
  onAnnounce(
    callback: (data: unknown, info: ChannelPublishInfo) => void,
    options?: { signal?: AbortSignal },
  ): () => void
  /** The last participant left. */
  onEmpty(callback: () => void, options?: { signal?: AbortSignal }): () => void
  /** The room was closed via `Room.close()` (on the client, also: the connection is gone, or the room sent it more
   *  that it can't miss than the server holds for it). */
  onClose(callback: () => void, options?: { signal?: AbortSignal }): () => void

  /** Anything observable changed; pairs with `snapshot()`. */
  onChange(callback: () => void, options?: { signal?: AbortSignal }): () => void
  /** Immutable whole-room view, reference-stable until the next change. */
  snapshot(): RoomSnapshotView<M, P>
} & RoomShield<Pub>

type ClientRoom<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = Omit<
  Room<M, P, Pub>,
  'join'
> & {
  join(options?: ClientJoinOptions<P>): Promise<LocalParticipant<P, Pub>>
  /** Iterate participant messages until aborted, closed, or the loop ends. */
  messages(options?: { signal?: AbortSignal }): AsyncIterableIterator<RoomMessage<Pub, P>>
}

/** Your returnable participant handle, returned by `join()`. */
type LocalParticipant<P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = {
  readonly id: string
  readonly meta: ReadonlyMeta<P>
  /** The app identity this membership was joined with (`null` when none was set). */
  readonly identity: string | null
  /** Whether the messages you publish are delivered back to the room object on your side. Set at `join()`. */
  readonly selfDelivery: boolean
  /** False while the page holding you can't reach the room. */
  readonly isConnected: boolean
  /** In bytes per ms, what the path from the page holding you to the server carries, as the server's acknowledgements
   *  measure it: what an encoder's bitrate fits in. `Infinity` until measured, where the page's wire doesn't measure it
   *  (SSE), and on the server. */
  readonly uplinkRate: number

  /** Publish a message to the whole room. */
  publish(data: Pub, options?: PublishOptions): Promise<ChannelPublishAck>
  /** Publish binary, optionally on a named track with per-frame metadata. */
  publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck>

  /** Send privately; `{ ack: true }` waits for the recipient handler's response. */
  send(to: string | Sender<P>, data: unknown): Promise<RoomSendReceipt>
  send(to: string | Sender<P>, data: unknown, options: { ack?: false }): Promise<RoomSendReceipt>
  send(to: string | Sender<P>, data: unknown, options: { ack: true }): Promise<RoomAckReceipt>
  /** Receive private messages; returning a value answers an acknowledged send. */
  listen(callback: (data: unknown, from: Sender<P> | null) => unknown, options?: { signal?: AbortSignal }): () => void

  /** Watch whether any observer wants each published track. */
  onDemand(callback: (track: string | null, wanted: boolean) => void, options?: { signal?: AbortSignal }): () => void
  /** Replace your metadata wholesale. Propagates to all observers in real time. Takes a new object, or one built from
   *  the read-only meta Room handed you. */
  setMeta(meta: ReadonlyMeta<P>): Promise<void>
  /** Merge metadata per key; `undefined` deletes a key. */
  setAttributes(attributes: Partial<ReadonlyMeta<P>>): Promise<void>

  leave(): Promise<void>
  /** You left. `cause.type` says how: `'left'` (you), `'removed'` (kicked, with the kick's `reason`), `'closed'` (the
   *  room), `'disconnected'` (the connection died, or the server couldn't hold what it had to send you). */
  onLeave(callback: (cause: LeaveCause) => void, options?: { signal?: AbortSignal }): () => void
  /** The page holding you can't reach the room; the room keeps you meanwhile. */
  onDisconnect(callback: () => void, options?: { signal?: AbortSignal }): () => void
  /** The page holding you reaches the room again. */
  onReconnect(callback: () => void, options?: { signal?: AbortSignal }): () => void
} & RoomShield<Pub>

/** A returnable view of another room member, bound to its room's live state. */
type RemoteParticipant<P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = {
  readonly id: string
  readonly meta: ReadonlyMeta<P>
  /** The app identity stamped at join, or `null`. */
  readonly identity: string | null
  /** Unix epoch milliseconds. */
  readonly joinedAt: number
  /** False while the member's page can't be reached and the room waits for it. */
  readonly isConnected: boolean

  /** Receive this member's messages on `track`. Returns an unsubscribe function. */
  subscribe(
    callback: (data: Pub, info: ChannelPublishInfo & BinaryFrameInfo) => unknown,
    options: { track: string; signal?: AbortSignal },
  ): () => void
  /** Receive only this member's messages. Returns an unsubscribe function. */
  subscribe(callback: (data: Pub, info: ChannelPublishInfo) => unknown, options?: { signal?: AbortSignal }): () => void
  /** Receive only this member's binary frames, optionally filtered to one track. */
  subscribeBinary(callback: MemberBinaryListener, options?: { track?: string | null; signal?: AbortSignal }): () => void

  /** This member's metadata changed. */
  onUpdate(
    callback: (meta: ReadonlyMeta<P>, prev: ReadonlyMeta<P>) => void,
    options?: { signal?: AbortSignal },
  ): () => void
  /** This member left the room. `cause` as on `Room`'s `onLeave`. */
  onLeave(callback: (cause: LeaveCause) => void, options?: { signal?: AbortSignal }): () => void
  /** This member's page can't be reached. The room keeps it until it's back, or it leaves with `'disconnected'`. */
  onDisconnect(callback: () => void, options?: { signal?: AbortSignal }): () => void
  /** This member's page is back. */
  onReconnect(callback: () => void, options?: { signal?: AbortSignal }): () => void
}
