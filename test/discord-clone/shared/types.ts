// Data shapes shared by server and client.
//
// Rooms are typed end-to-end now — `Room<ChannelMeta, MemberMeta>` flows through telefunction
// returns, guards, and snapshots (the `asMemberMeta` casts this file used to export are gone).

export type {
  ChannelMeta,
  ChannelPublish,
  ChannelRoom,
  ChatMessage,
  DmMessage,
  GuildAnnouncement,
  GuildRoom,
  MemberMeta,
  SystemNotice,
}

import type { Room } from 'telefunc'

/**
 * A member's display state, carried by participant metadata. The durable identity (`userId`)
 * does NOT live here anymore: it's the join's `identity` option — server-stamped, spoof-proof,
 * surfaced as `participant.identity` / `from.identity` everywhere.
 */
type MemberMeta = {
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

/** The one guild room (meta is just a display name). */
type GuildRoom = Room<{ name: string }, MemberMeta>
/** A text or voice channel room. The 3rd type arg is the room's published-message type, so
 *  `publish()`/`subscribe()` are typed to `ChannelPublish` end-to-end — no `as` cast at the
 *  subscriber (README finding 20, adopted upstream). Voice rooms carry binary only, so it's unused there. */
type ChannelRoom = Room<ChannelMeta, MemberMeta, ChannelPublish>

/**
 * Everything published on a text channel's room-wide lane. Chat messages and ephemeral typing
 * signals share the one `publish()` lane, so they ride a discriminated union and every consumer
 * (the persistence guard, the bot, clients) switches on `kind`.
 */
type ChannelPublish =
  | { kind: 'chat'; id: string; text: string } // sender mints `id` (the history/live dedup key)
  | { kind: 'typing' }

/** A chat message as persisted by the `onAfterPublish` hook and rendered by clients. */
type ChatMessage = {
  id: string
  authorId: string
  author: { name: string; color: string; bot?: boolean }
  text: string
  /** The room's authoritative order — the message's central per-channel `seq` (`info.seq`).
   *  History and the live lane both carry it, so they interleave as one timeline. */
  seq: number
  /** Central server clock (`info.timestamp`) — the same value the live lane renders, so history
   *  and live no longer disagree about a message's time (README finding 13). */
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
  // Unread dots: the channel's `onAfterPublish` hook announces activity here (one guild-lane
  // subscription for every channel), replacing the removed per-channel `onActivity` signal
  // (README finding 10).
  | { kind: 'channel-activity'; channelId: string }

/**
 * Room-authored private notices (`Room.send` → `me.listen` with `from === null`).
 * DMs arrive this way: they're server-delivered (DB write first, then live fan-out), so they
 * work while the recipient is offline — something the member-to-member `send()` lane can't do
 * (see README finding 2). Kick notices are gone: the removal itself now carries its reason
 * (`LeaveCause`), so there is nothing to race.
 */
type SystemNotice = { kind: 'dm'; message: DmMessage }
