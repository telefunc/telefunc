// Data shapes shared by server and client.
//
// A note on metadata typing: Room/participant metadata is `Record<string, unknown>` on the
// Telefunc side, so the app declares its own shapes and casts at the boundary (`asMemberMeta`,
// `asChannelMeta`). See README finding on untyped metadata.

export type { ChannelMeta, ChannelPublish, ChatMessage, DmMessage, GuildAnnouncement, MemberMeta, SystemNotice }
export { asChannelMeta, asMemberMeta }

import type { ParticipantMeta, RoomMeta } from 'telefunc'

/**
 * Every room membership carries the user's identity. `userId` is the app's *durable* identity
 * (participant IDs are per-room and per-connection) — kicks, member dedupe across tabs, and DM
 * threads all correlate by it. Stamped server-side from `getContext().user`, never by clients.
 */
type MemberMeta = {
  userId: string
  name: string
  color: string
  status: 'online' | 'idle' | 'dnd'
  /** The server-side bot. */
  bot?: boolean
  /** Server owner — kick, announce, delete channels. */
  admin?: boolean
  // Call state (voice-room memberships only):
  muted?: boolean
  camera?: boolean
  screen?: boolean
}

type ChannelMeta = {
  kind: 'text' | 'voice'
  name: string
  topic?: string
}

function asMemberMeta(meta: ParticipantMeta): MemberMeta {
  return meta as MemberMeta
}

function asChannelMeta(meta: RoomMeta): ChannelMeta {
  return meta as ChannelMeta
}

/**
 * Everything published on a text channel's room-wide lane. Chat messages and ephemeral typing
 * signals share the one `publish()` lane, so they ride a discriminated union and every consumer
 * (the persistence guard, the bot, clients) switches on `kind`.
 */
type ChannelPublish =
  | { kind: 'chat'; id: string; text: string } // sender mints `id` (the history/live dedup key)
  | { kind: 'typing' }

/** A chat message as persisted by the `onPublish` guard and rendered by clients. */
type ChatMessage = {
  id: string
  authorId: string
  author: { name: string; color: string; bot?: boolean }
  text: string
  at: number
}

/** A direct message (a `dms` table row, over the wire). */
type DmMessage = {
  id: string
  fromId: string
  fromName: string
  toId: string
  toName: string
  text: string
  at: number
}

/**
 * Everything the guild's announce lane carries (`Room.announce` → `room.onAnnounce`).
 * Rooms have no directory events — nothing tells you a room was created — so the guild lane
 * doubles as the app's channel-directory feed (`channel-created`).
 */
type GuildAnnouncement =
  | { kind: 'announcement'; text: string; by: string }
  | { kind: 'channel-created'; channelId: string }
  | { kind: 'member-kicked'; userId: string; name: string; by: string }

/**
 * Room-authored private notices (`Room.send` → `me.listen` with `from === null`).
 * DMs arrive this way too: they're server-delivered (DB write first, then live fan-out to the
 * recipient's participants), so they work while the recipient is offline — something the
 * member-to-member `send()` lane can't do (see README finding).
 */
type SystemNotice = { kind: 'kicked'; by: string } | { kind: 'dm'; message: DmMessage }
