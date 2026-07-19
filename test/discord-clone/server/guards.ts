export { BANNED_WORDS, getGuardedChannel, getGuardedGuild }

// Server-side policy, declared through `Room.guard()`.
//
// A guard covers every membership granted through *that room instance*, and there is no
// room-global guard — so policy must be attached to every instance that hands out memberships.
// These factories are the only place rooms are fetched (README finding 3, still open upstream).

import { Room } from 'telefunc'
import * as q from '../database/queries'
import type { ChannelMeta, ChannelPublish, GuildRoom, MemberMeta } from '../shared/types'
import { dbChannelIdOf, GUILD_ROOM_ID } from './rooms'

const BANNED_WORDS = ['darn', 'heck'] // it's a family-friendly server

/** The guild room: presence + the room-authored notice lane. */
async function getGuardedGuild(): Promise<GuildRoom> {
  const guild = await Room.get<{ name: string }, MemberMeta>(GUILD_ROOM_ID)
  Room.guard(guild, {
    // DMs are server-delivered (DB-first, offline-capable — see dms.telefunc.ts), so the
    // member-to-member lane is closed outright: policy by guard, not by hoping clients behave.
    onBeforeSend: () => {
      throw new Error('Direct participant messages are disabled — DMs are delivered by the server')
    },
  })
  return guild
}

/** A channel room. Text channels get moderation + persistence; voice rooms get server-enforced
 *  capacity through the `onBeforeJoin` guard (README finding 11). Pass `{ tail: true }` to start
 *  relaying live messages the moment the room is returned to the client — the fence for a
 *  lossless "load history, then go live" (README finding 14). */
async function getGuardedChannel(
  channelRoomId: string,
  options?: { tail?: boolean },
): Promise<Room<ChannelMeta, MemberMeta, ChannelPublish>> {
  const dbChannelId = dbChannelIdOf(channelRoomId)
  if (dbChannelId === null) throw new Error('Not a channel room')
  const channel = await Room.get<ChannelMeta, MemberMeta, ChannelPublish>(channelRoomId, {
    tail: options?.tail ?? false,
  })

  if (channel.meta.kind === 'voice') {
    Room.guard(channel, {
      // Admission control — `size` alone is a hint, but the join guard makes it real: every
      // join granted through this instance (the onJoinVoice telefunction) is capacity-checked
      // on the server.
      onBeforeJoin: () => {
        if (channel.isFull) throw new Error('That voice channel is full')
      },
    })
    return channel
  }

  Room.guard(channel, {
    // Validation runs before the message commits — throwing here rejects the sender's publish
    // through the wire ack, and nothing is delivered or stored.
    onBeforePublish: (from, data) => {
      const published = data as ChannelPublish
      if (published.kind !== 'chat') return // typing signals are ephemeral: not moderated, never stored
      if (published.text.trim() === '') throw new Error('Empty message')
      if (published.text.length > 2000) throw new Error('Message too long (2000 characters max)')
      const banned = BANNED_WORDS.find((word) => published.text.toLowerCase().includes(word))
      if (banned) throw new Error(`Watch your language — "${banned}" is not allowed here`)
      if (from.identity === null) throw new Error('Membership carries no identity') // never: all joins are server-side
    },
    // Persistence runs *after* the message is sequenced and delivered — so it carries the
    // message's authoritative order (`info.seq`) and the central server clock (`info.timestamp`),
    // the exact values the live lane rendered. History and live no longer keep two clocks
    // (README finding 13). What's persisted is exactly what subscribers received — never rewritten.
    onAfterPublish: (from, data, info) => {
      const published = data as ChannelPublish
      if (published.kind !== 'chat') return
      const authorId = from.identity
      if (authorId === null) return // rejected by onBeforePublish already — belt and braces
      q.insertMessage({
        id: published.id,
        channel_id: dbChannelId,
        author_id: authorId, // server-stamped at join — not client-echoed metadata
        author_name: from.meta.name,
        author_color: from.meta.color,
        author_is_bot: from.meta.bot === true ? 1 : 0,
        text: published.text,
        seq: info.seq,
        at: info.timestamp,
      })
      // Unread dots: one activity ping on the guild lane, which every client already listens to.
      // Replaces the per-channel `onActivity` subscription the base removed (README finding 10).
      void Room.announce(GUILD_ROOM_ID, { kind: 'channel-activity', channelId: channelRoomId })
    },
  })
  return channel
}
