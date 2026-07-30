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
}

import type { ChannelPublishAck, ChannelPublishInfo } from '../channel.js'
import type { TELEFUNC_SHIELDS } from '../../node/shared/transformer/generateShield/shield-key.js'

/** Shield contract, read by the server to validate a client's incoming `publish()` payload against the
 *  room's declared message type (`Pub`). Like `Broadcast`, this is auto-generated from the type: declare
 *  `Room<Meta, PMeta, Message>` and every client publish is validated against `Message` at the server
 *  ingress, rejected with a `ShieldValidationError` before any guard or handler runs — no separate
 *  shield to write. `Pub` defaults to `unknown` (validates anything) until you declare it. A phantom
 *  type-only field, erased at runtime. */
type RoomShield<Pub> = {
  readonly [TELEFUNC_SHIELDS]: {
    data: Pub
  }
}

/** Room metadata (e.g. topic). Must be serializable. */
type RoomMeta = Record<string, unknown>
/** Participant metadata (e.g. name, score). Must be serializable. */
type ParticipantMeta = Record<string, unknown>

/** A message's verified sender — one concept across every lane (`subscribe()`, `listen()`):
 *  the live `RemoteParticipant` whenever the holder's room view knows the sender, or a
 *  server-stamped snapshot otherwise (a standalone participant, or a message racing ahead of
 *  its sender's join). `identity` is the app identity stamped at (server-side) join — `null`
 *  when none was set. `await room.getParticipant(from.id)` upgrades a snapshot to the live
 *  handle. */
type Sender<P extends ParticipantMeta = ParticipantMeta> = {
  readonly id: string
  readonly meta: P
  readonly identity: string | null
}

/** Guards private messages (`Room.guard(room, { onBeforeSend })`): runs before every `send()`
 *  from a membership granted through that room instance — including client-side `join()`s on it.
 *  Throw to reject (the sender's promise rejects with the error). */
type SendGuard<P extends ParticipantMeta = ParticipantMeta> = (
  from: Sender<P>,
  to: Sender<P>,
  data: unknown,
) => void | Promise<void>

/** Guards room-wide messages (`Room.guard(room, { onBeforePublish })`): runs before every
 *  `publish()` and `publishBinary()` from a membership granted through that room instance —
 *  `data` is the payload a subscriber would receive. Throw to reject (the sender's promise
 *  rejects). */
type PublishGuard<P extends ParticipantMeta = ParticipantMeta> = (
  from: Sender<P>,
  data: unknown,
) => void | Promise<void>

/** Guards admission (`Room.guard(room, { onBeforeJoin })`): runs before every `join()` through
 *  that room instance — server-side and client-side alike. `member` is the joiner: the ID it will
 *  receive and the metadata it requested. Throw to reject (the joiner's `join()` rejects with
 *  the error, before any membership state is written). */
type JoinGuard<P extends ParticipantMeta = ParticipantMeta> = (member: Sender<P>) => void | Promise<void>

/** The receipt for a delivered private message, passed to `onAfterSend`. */
type RoomSendReceipt = { seq: number; timestamp: number }

/** The receipt for a `send(…, { ack: true })` — a strict superset of `RoomSendReceipt`: the same
 *  `seq`/`timestamp` (the message's sequencing on the recipient's inbox), plus `response`, the value
 *  the recipient's `listen` handler returned. */
type RoomAckReceipt = RoomSendReceipt & { response: unknown }

/** Runs after a room-wide message is sequenced and delivered (`Room.guard(room, { onAfterPublish })`):
 *  the same `from`/`data` as `onBeforePublish`, plus the `info` receipt — so you can persist the
 *  message with its authoritative order (see the history guide). Fires for `publish()` and
 *  `publishBinary()` alike (branch on `data` to skip binary frames). Awaited; throwing rejects the
 *  caller but does not undo the delivery. */
type AfterPublishHook<P extends ParticipantMeta = ParticipantMeta> = (
  from: Sender<P>,
  data: unknown,
  info: ChannelPublishAck,
) => void | Promise<void>

/** Runs after a private message is delivered (`Room.guard(room, { onAfterSend })`): the same
 *  `from`/`to`/`data` as `onBeforeSend`, plus the `info` receipt. Awaited; throwing rejects the
 *  caller but does not undo the delivery. */
type AfterSendHook<P extends ParticipantMeta = ParticipantMeta> = (
  from: Sender<P>,
  to: Sender<P>,
  data: unknown,
  info: RoomSendReceipt,
) => void | Promise<void>

/** Runs after a join is committed and announced (`Room.guard(room, { onAfterJoin })`): the joined
 *  `member` plus the `info` receipt — the place for post-join side effects (provision, welcome DM,
 *  audit). Awaited; throwing rejects the caller but does not undo the join. */
type AfterJoinHook<P extends ParticipantMeta = ParticipantMeta> = (
  member: Sender<P>,
  info: { joinedAt: number },
) => void | Promise<void>

/** Why a participant is gone. `reason` is set by `Room.removeParticipant(id, { id, reason })`
 *  and travels with the removal — a kicked client learns it's kicked (and why) from the leave
 *  itself, with nothing to race. On `Room`/`RemoteParticipant` leave callbacks the cause is
 *  `undefined` exactly when the leave was discovered by a roster resync rather than an event —
 *  the actual cause wasn't observed. Your own `LocalParticipant` always knows its cause. */
type LeaveCause = {
  type: 'left' | 'removed' | 'closed' | 'disconnected'
  reason?: unknown
}

/** Addresses a participant in an admin operation (`Room.send()`, `Room.removeParticipant()`): one
 *  membership by its participant `id`, or every membership of an app `identity` at once (a user's
 *  tabs and connections — an idempotent sweep, 0 matches is fine). */
type ParticipantRef = { id: string } | { identity: string }

type RoomOptions<M extends RoomMeta = RoomMeta> = {
  /** Room metadata, visible to all observers. Default: `{}`. */
  meta?: M
}

type RoomGetOptions = {
  /** Start capturing the room's live text the moment this room is serialized into a response, so a
   *  history read done *after* `Room.get(id, { tail: true })` in the same telefunction can't miss a
   *  message published in between. The recent tail is held server-side (bounded, drop-oldest) until
   *  the client's first `subscribe()`, then flushed to it once, selected and in order, ahead of the
   *  live stream. Lets you load history and go live in a single call; the client dedupes the small
   *  overlap by message ID. Best-effort for a prompt subscribe, not a lossless backlog. */
  tail?: boolean
}

/** Options for `join()`. `identity` and `hidden` are server-only: the type carries them because the
 *  `Room` type is shared by server and client (and a room returned from a telefunction keeps its server
 *  type on the client), but a client-side `join()` rejects them at runtime. Set them where trust lives —
 *  in the granting telefunction, server-side. */
type JoinOptions<P extends ParticipantMeta = ParticipantMeta> = {
  /** Your participant metadata (e.g. name, score), visible to all observers. Default: `{}`. */
  meta?: P
  /** Whether the messages you publish are delivered back to the room object on your side
   *  (default: `true`). Turn off e.g. for video, where you don't want your own frames back. */
  selfDelivery?: boolean
  /** App identity (e.g. your user ID), stamped spoof-proof into everything the member does:
   *  `Sender.identity`, `RemoteParticipant.identity`, guards, and
   *  `Room.removeParticipant(id, { identity })` sweeps. Server-side `join()` only — identity is
   *  trusted, so it's assigned where trust lives: in the granting telefunction. A client-side
   *  `join()` rejects it. Immutable for the membership's lifetime. */
  identity?: string
  /** Join without appearing in the room's presence (default: `false`) — a full participant
   *  (publish, `publishBinary`, `send`, `listen`, `onDemand`) that is excluded from `count`,
   *  `getParticipants()`, `snapshot()`, and `onJoin`/`onLeave`/`onEmpty`. For a server
   *  that acts in a room — authoritative game state, a bot, a command sink — or a recorder or a
   *  moderator observing unseen. Any number per room; read them with `getParticipants({ hidden:
   *  true })`. Server-side `join()` only (like `identity`); a client-side `join()` rejects it. */
  hidden?: boolean
}

/** One participant inside `room.snapshot()`. */
type ParticipantSnapshotView<P extends ParticipantMeta = ParticipantMeta> = {
  readonly id: string
  readonly identity: string | null
  readonly meta: P
  readonly joinedAt: number
}

/** Immutable view returned by `room.snapshot()` — the same reference until the room actually
 *  changes, so it plugs straight into `useSyncExternalStore(room.onChange, room.snapshot)`. */
type RoomSnapshotView<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta> = {
  readonly id: string
  readonly meta: M
  readonly count: number
  readonly isClosed: boolean
  readonly participants: readonly ParticipantSnapshotView<P>[]
}

/** Lightweight room snapshot returned by `Room.list()`. `M` types `meta` — pass your room's meta
 *  type (`Room.list<MatchMeta>()`), same caller-assertion relationship as `Room<M>`. */
type RoomInfo<M extends RoomMeta = RoomMeta> = {
  readonly id: string
  readonly meta: M
  readonly count: number
  readonly isEmpty: boolean
}

/** Per-frame binary metadata, straight from the frame header. */
type BinaryFrameInfo = {
  /** The named substream this frame belongs to (`publishBinary(data, { track })`) — `null` for
   *  the default track. Mic/camera/screen multiplex over one member lane by name. */
  track: string | null
  /** The per-frame metadata the publisher attached (`publishBinary(data, { meta })`) — `null` when none.
   *  Where app-level frame info rides: a keyframe marker (`{ key: true }`), a timestamp, dimensions. */
  meta: Record<string, unknown> | null
}

/** Publish-side options for text messages. */
type PublishOptions = {
  /** Conflate high-frequency updates by key (e.g. `'cursor'`): while a publish with this key is
   *  still in flight, newer publishes with the same key collapse into a single pending send — only
   *  the latest value goes out. Bounds the uplink to one message per key under a burst (cursors,
   *  live reactions), at the cost of dropping intermediate values. Omit for lossless delivery. */
  coalesce?: string
  /** Keep this as the room's *retained* message: the server holds the last one and delivers it to
   *  any new subscriber before live messages — the current state a late joiner needs (a status, a
   *  pinned notice). Last-write-wins; one retained value per room. Like MQTT retained messages. */
  retain?: boolean
}

/** Publish-side binary options. */
type BinaryPublishOptions = {
  /** Named substream (≤ 64 bytes) — subscribers can filter by it. Default: the default track. */
  track?: string
  /** Per-frame metadata, surfaced as `info.meta` on every subscriber (≤ 4 KB serialized). For whatever
   *  the receiver needs to interpret the bytes: `{ key: true }` for a keyframe, a timestamp, dimensions. */
  meta?: Record<string, unknown>
  /** Keep this frame as the track's *retained* frame: the server holds the last one and delivers it
   *  to any new subscriber before live frames — so a late joiner is seeded with a keyframe it can
   *  apply deltas onto, with no `onDemand` dance. Last-write-wins per (member, track). Retain your
   *  keyframes. Like MQTT retained messages, the binary twin of `publish({ retain })`. */
  retain?: boolean
}

/**
 * A multi-party room with presence, membership, and events. One type, same on server and
 * client — a `Room` can be returned from a telefunction as-is. Admin operations live on the
 * server-side `Room.*` statics, not on the instance.
 */
type Room<M extends RoomMeta = RoomMeta, P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = {
  /** The ID the room was created with. */
  readonly id: string
  readonly meta: M
  readonly count: number
  readonly isEmpty: boolean
  readonly isClosed: boolean

  /** Join the room, optionally with your participant `meta`. Returns your own participant handle. */
  join(options?: JoinOptions<P>): Promise<LocalParticipant<P, Pub>>

  /** The room's participants. Pass `{ hidden: true }` for the off-presence participants instead —
   *  a server authority, a bot, a recorder (see `JoinOptions.hidden`); addressable (`me.send(p.id,
   *  …)`) and readable (`p.subscribeBinary(…)`) like any member, just not counted as present.
   *  Hidden enumeration is server-only because the browser roster intentionally omits it. */
  getParticipants(options?: { hidden?: boolean }): Promise<RemoteParticipant<P, Pub>[]>
  /** One participant, or `null` if they're not a member. Like `getParticipants()`, loads the
   *  member view on first need; once the view is loaded it resolves from it without I/O. */
  getParticipant(id: string): Promise<RemoteParticipant<P, Pub> | null>

  /** Receive all participant messages. Returns an unsubscribe function. */
  subscribe(callback: (data: Pub, info: ChannelPublishInfo, from: Sender<P>) => unknown): () => void
  /** Receive all members' binary frames — or one track's: `{ track: 'screen' }` for a named
   *  track, `{ track: null }` for the default lane only. Selection is enforced at the source:
   *  unwanted tracks aren't delivered, relayed, or even subscribed upstream — dropping a
   *  track's last subscription stops its bytes at every hop. */
  subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo, from: Sender<P>) => unknown,
    options?: { track?: string | null },
  ): () => void

  /** A participant joined. */
  onJoin(callback: (member: RemoteParticipant<P, Pub>) => void): () => void
  /** A participant left. `cause` says why (kick reasons ride along); it's `undefined` exactly
   *  when the leave was discovered by a roster resync — the event itself wasn't observed. */
  onLeave(callback: (member: RemoteParticipant<P, Pub>, cause?: LeaveCause) => void): () => void
  /** Any participant's metadata changed — `member` is who, with the new and previous meta. One
   *  subscription covering every member (vs. wiring `onUpdate` on each handle from `onJoin`). */
  onParticipantUpdate(callback: (member: RemoteParticipant<P, Pub>, meta: P, prev: P) => void): () => void
  /** The room's metadata was replaced (`Room.setMeta()`) or merged (`Room.setAttributes()`). */
  onUpdate(callback: (meta: M, prev: M) => void): () => void
  /** A room-authored message arrived (`Room.announce()`) — e.g. system notices. */
  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void): () => void
  /** The last participant left. */
  onEmpty(callback: () => void): () => void
  /** The room was closed via `Room.close()` (on the client, also: the connection is gone). */
  onClose(callback: () => void): () => void

  /** Anything observable changed — membership, participant metadata, room config, closure.
   *  One subscription for UI stores; pairs with `snapshot()`. */
  onChange(callback: () => void): () => void
  /** Immutable whole-room view, reference-stable until the next change:
   *  `useSyncExternalStore(room.onChange, room.snapshot)` is the entire React adapter.
   *  Participants appear once the member view loads (subscribing `onChange` loads it). */
  snapshot(): RoomSnapshotView<M, P>
} & RoomShield<Pub>

/**
 * Your own participant handle, returned by `join()`. One type, same on server and client —
 * can be returned from a telefunction as-is. Room-wide messages are received on `Room` and
 * `RemoteParticipant`; only direct messages addressed to you arrive here (`listen()`).
 */
type LocalParticipant<P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = {
  readonly id: string
  readonly meta: P
  /** The app identity this membership was joined with — `null` when none was set. */
  readonly identity: string | null
  /** Whether the messages you publish are delivered back to the room object on your side. Set at `join()`. */
  readonly selfDelivery: boolean

  /** Publish a message to the whole room (`Pub` — the room's message type when declared). Pass
   *  `{ coalesce: key }` to conflate high-frequency updates — see `PublishOptions`. */
  publish(data: Pub, options?: PublishOptions): Promise<ChannelPublishAck>
  /** Publish binary to the whole room — optionally on a named track and/or with per-frame `meta`.
   *  When present, `receivers` is the global live subscription count: `0` means nobody anywhere.
   *  Backends that cannot count globally omit it; use `onDemand` to pause and resume portably. */
  publishBinary(data: Uint8Array, options?: BinaryPublishOptions): Promise<ChannelPublishAck>

  /** Send a private message to one participant (or their ID) — nobody else receives it. Resolves
   *  with the delivery receipt (`{ seq, timestamp }`) once the message is sequenced on the
   *  recipient's inbox — so `await send(...)` waits for the hand-off, not just the local enqueue.
   *
   *  The hand-off is best-effort (fire-and-forget) — the underlying pub/sub is at-most-once on
   *  every adapter. For *confirmed* delivery, pass `{ ack: true }`: it waits for the recipient to
   *  actually handle the message and resolves with a `RoomAckReceipt` — the plain `{ seq, timestamp }`
   *  plus `response`, whatever their `listen` handler returned (the last one, if several) — rejecting
   *  if that handler throws or the recipient leaves before handling. The request/response twin of the
   *  channel `send({ ack: true })`; behaves identically whether the recipient is server-side or a client. */
  send(to: string | Sender<P>, data: unknown): Promise<RoomSendReceipt>
  send(to: string | Sender<P>, data: unknown, options: { ack?: false }): Promise<RoomSendReceipt>
  send(to: string | Sender<P>, data: unknown, options: { ack: true }): Promise<RoomAckReceipt>
  /** Receive private messages addressed to you. `from` is the verified sender — `null` for
   *  room-authored messages (`Room.send()`). Returning a value replies to a `send(…, { ack: true })`
   *  sender (the last handler's return wins). Returns an unlisten function. */
  listen(callback: (data: unknown, from: Sender<P> | null) => unknown): () => void

  /** Watch whether anyone wants your published tracks: `(track, wanted)` fires when a stream of yours
   *  goes watched↔unwatched (`track` is `null` for the default `publishBinary()` lane). `wanted === false`
   *  means nobody anywhere is watching — the event-driven signal to pause the encoder; `true` fires when a
   *  viewer returns, so you can resume without polling. It's a boolean, not a count: demand is aggregated
   *  across all server nodes, and a node reports only "any" (not how many), so the system can know *whether*
   *  a track is wanted but never the precise subscriber number. Returns an unsubscribe function. */
  onDemand(callback: (track: string | null, wanted: boolean) => void): () => void
  /** Replace your metadata wholesale. Propagates to all observers in real time. */
  setMeta(meta: P): Promise<void>
  /** Merge into your metadata per key — other keys keep their value, a key set to `undefined`
   *  is removed. The server applies the merge, so one changed field is one small update instead
   *  of resending the whole object. Propagates to all observers. */
  setAttributes(attributes: Partial<P>): Promise<void>

  leave(): Promise<void>
  /** You left. `cause.type` says how — `'left'` (you), `'removed'` (kicked, with the kick's
   *  `reason`), `'closed'` (the room), `'disconnected'` (the connection died). */
  onLeave(callback: (cause: LeaveCause) => void): () => void
} & RoomShield<Pub>

/** Another room member: subscribe to just their messages, observe their metadata and lifecycle.
 *  Returnable from a telefunction — it arrives bound to its room's live view: the backing room
 *  rides along (deduplicated against a co-returned room), so `room.getParticipant(m.id) === m`. */
type RemoteParticipant<P extends ParticipantMeta = ParticipantMeta, Pub = unknown> = {
  readonly id: string
  readonly meta: P
  /** The app identity stamped at join — `null` when none was set. Correlate the same human
   *  across rooms, connections, and tabs by this, never by participant ID. */
  readonly identity: string | null
  /** Unix epoch milliseconds. */
  readonly joinedAt: number

  /** Receive only this member's messages. Returns an unsubscribe function. */
  subscribe(callback: (data: Pub, info: ChannelPublishInfo) => unknown): () => void
  /** Receive only this member's binary frames — or one track's: `{ track: 'screen' }` for a
   *  named track, `{ track: null }` for the default lane only (source-selective, like the
   *  room-level `subscribeBinary`). */
  subscribeBinary(
    callback: (data: Uint8Array, info: ChannelPublishInfo & BinaryFrameInfo) => unknown,
    options?: { track?: string | null },
  ): () => void

  /** This member's metadata changed. */
  onUpdate(callback: (meta: P, prev: P) => void): () => void
  /** This member left the room. `cause` as on `Room`'s `onLeave`. */
  onLeave(callback: (cause?: LeaveCause) => void): () => void
}
