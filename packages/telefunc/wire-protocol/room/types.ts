export type {
  Room,
  RoomInfo,
  RoomOptions,
  RoomMeta,
  JoinOptions,
  ParticipantMeta,
  LocalParticipant,
  RemoteParticipant,
  Sender,
  SendGuard,
  RoomListener,
  RoomBinaryListener,
  ParticipantListener,
  ParticipantBinaryListener,
}

import type { ChannelPublishAck, ChannelPublishInfo } from '../channel.js'

/** Room metadata (e.g. topic). Must be serializable. */
type RoomMeta = Record<string, unknown>
/** Participant metadata (e.g. name, score). Must be serializable. */
type ParticipantMeta = Record<string, unknown>

/** A message's verified sender — one concept across every lane (`subscribe()`, `listen()`):
 *  the live `RemoteParticipant` whenever the holder's room view knows the sender, or an
 *  `{ id, meta }` snapshot stamped by the sender's own node otherwise (a standalone participant,
 *  or a message racing ahead of its sender's join). `room.getParticipant(from.id)` upgrades a
 *  snapshot to the live handle once the roster catches up. */
type Sender = { readonly id: string; readonly meta: ParticipantMeta }

/** Guards private messages (`Room.get(id, { onSend })`): called before every `send()` from a
 *  membership granted through that room instance — including client-side `join()`s on it.
 *  Throw to reject (the sender's promise rejects with the error). */
type SendGuard = (from: Sender, to: Sender, data: unknown) => void | Promise<void>

type RoomOptions = {
  /** Room metadata, visible to all observers. Default: `{}`. */
  meta?: RoomMeta
  /** Capacity hint (default: `Infinity`). Tracked (`count`, `isFull`, `onFull`) but not
   *  enforced — reject joins yourself, e.g. `if (room.isFull) throw new Error(...)`. */
  size?: number
  /** Give each member their own upstream pub/sub key (default: `false`). Removes publish
   *  contention on platforms that map each key to a separate coordinator (e.g. Cloudflare
   *  Durable Objects). Clients don't see the difference. Fixed at creation. */
  isolated?: boolean
}

type JoinOptions = {
  /** Whether the messages you publish are delivered back to the room object on your side
   *  (default: `true`). Turn off e.g. for video, where you don't want your own frames back. */
  selfDelivery?: boolean
}

/** Lightweight room snapshot returned by `Room.list()`. */
type RoomInfo = {
  readonly id: string
  readonly meta: RoomMeta
  readonly size: number
  readonly count: number
  readonly isEmpty: boolean
  readonly isFull: boolean
}

/** Receives all participant messages, with the verified sender (see `Sender`). */
type RoomListener = (data: unknown, info: ChannelPublishInfo, from: Sender) => unknown
type RoomBinaryListener = (data: Uint8Array, info: ChannelPublishInfo, from: Sender) => unknown
/** Receives a single participant's messages. */
type ParticipantListener = (data: unknown, info: ChannelPublishInfo) => unknown
type ParticipantBinaryListener = (data: Uint8Array, info: ChannelPublishInfo) => unknown

/**
 * A multi-party room with presence, membership, and events. One type, same on server and
 * client — a `Room` can be returned from a telefunction as-is. Admin operations live on the
 * server-side `Room.*` statics, not on the instance.
 */
type Room = {
  /** The ID the room was created with. */
  readonly id: string
  readonly meta: RoomMeta
  /** Capacity hint — not enforced by Telefunc. `Infinity` when unset. */
  readonly size: number
  readonly count: number
  readonly isEmpty: boolean
  readonly isFull: boolean
  readonly isClosed: boolean

  /** Join the room. Returns your own participant handle. */
  join(meta?: ParticipantMeta, options?: JoinOptions): Promise<LocalParticipant>

  getParticipants(): Promise<RemoteParticipant[]>
  getParticipant(id: string): RemoteParticipant | null

  /** Receive all participant messages. Returns an unsubscribe function. */
  subscribe(callback: RoomListener): () => void
  subscribeBinary(callback: RoomBinaryListener): () => void

  /** A participant joined. */
  onJoin(callback: (member: RemoteParticipant) => void): () => void
  /** A participant left (or was removed). */
  onLeave(callback: (member: RemoteParticipant) => void): () => void
  /** The room was reconfigured via `Room.update()`. */
  onUpdate(callback: (meta: RoomMeta, prev: RoomMeta) => void): () => void
  /** A room-authored message arrived (`Room.announce()`) — e.g. system notices. */
  onAnnounce(callback: (data: unknown, info: ChannelPublishInfo) => void): () => void
  /** The last participant left. */
  onEmpty(callback: () => void): () => void
  /** The room reached capacity (`count >= size`). */
  onFull(callback: () => void): () => void
  /** The room was closed via `Room.close()` (on the client, also: the connection is gone). */
  onClose(callback: () => void): () => void
}

/**
 * Your own participant handle, returned by `join()`. One type, same on server and client —
 * can be returned from a telefunction as-is. Room-wide messages are received on `Room` and
 * `RemoteParticipant`; only direct messages addressed to you arrive here (`listen()`).
 */
type LocalParticipant = {
  readonly id: string
  readonly meta: ParticipantMeta
  /** Whether the messages you publish are delivered back to the room object on your side. Set at `join()`. */
  readonly selfDelivery: boolean

  /** Publish a message to the whole room. */
  publish(data: unknown): Promise<ChannelPublishAck>
  publishBinary(data: Uint8Array): Promise<ChannelPublishAck>

  /** Send a private message to one participant (or their ID) — nobody else receives it. */
  send(to: string | Sender, data: unknown): Promise<void>
  /** Receive private messages addressed to you. `from` is the verified sender —
   *  `null` for room-authored messages (`Room.send()`). Returns an unlisten function. */
  listen(callback: (data: unknown, from: Sender | null) => void): () => void

  /** Replace your metadata. Propagates to all observers in real time. */
  setMeta(meta: ParticipantMeta): Promise<void>

  leave(): Promise<void>
  /** You left — voluntarily, kicked, room closed, or disconnected. */
  onLeave(callback: () => void): () => void
}

/** Another room member: subscribe to just their messages, observe their metadata and lifecycle. */
type RemoteParticipant = {
  readonly id: string
  readonly meta: ParticipantMeta
  /** Unix epoch milliseconds. */
  readonly joinedAt: number

  /** Receive only this member's messages. Returns an unsubscribe function. */
  subscribe(callback: ParticipantListener): () => void
  subscribeBinary(callback: ParticipantBinaryListener): () => void

  /** This member's metadata changed. */
  onUpdate(callback: (meta: ParticipantMeta, prev: ParticipantMeta) => void): () => void
  /** This member left the room. */
  onLeave(callback: () => void): () => void
}
