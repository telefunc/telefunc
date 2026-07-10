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
    onSend: () => {
      throw new Error('Direct participant messages are disabled — DMs are delivered by the server')
    },
  })
  return guild
}

/** A channel room. Text channels get moderation + persistence; voice rooms get server-enforced
 *  capacity through the new `onJoin` guard (README finding 11, fixed upstream). */
async function getGuardedChannel(channelRoomId: string): Promise<Room<ChannelMeta, MemberMeta>> {
  const dbChannelId = dbChannelIdOf(channelRoomId)
  if (dbChannelId === null) throw new Error('Not a channel room')
  const channel = await Room.get<ChannelMeta, MemberMeta>(channelRoomId)

  if (channel.meta.kind === 'voice') {
    Room.guard(channel, {
      // Admission control — `size` alone is a hint, but the join guard makes it real: every
      // join granted through this instance (the onJoinVoice telefunction) is capacity-checked
      // on the server.
      onJoin: () => {
        if (channel.isFull) throw new Error('That voice channel is full')
      },
    })
    return channel
  }

  Room.guard(channel, {
    // Gates every `publish()` granted through this instance — and doubles as the persistence
    // hook: it runs exactly once per message, on the server, with the verified sender
    // (see /room docs § Load history, then go live). What's persisted is exactly what
    // subscribers receive — the guard validates, it never rewrites.
    onPublish: (from, data) => {
      const published = data as ChannelPublish
      if (published.kind !== 'chat') return // typing signals are ephemeral: not moderated, never stored
      if (published.text.trim() === '') throw new Error('Empty message')
      if (published.text.length > 2000) throw new Error('Message too long (2000 characters max)')
      const banned = BANNED_WORDS.find((word) => published.text.toLowerCase().includes(word))
      if (banned) throw new Error(`Watch your language — "${banned}" is not allowed here`)
      if (from.identity === null) throw new Error('Membership carries no identity') // never: all joins are server-side
      q.insertMessage({
        id: published.id,
        channel_id: dbChannelId,
        author_id: from.identity, // server-stamped at join — not client-echoed metadata
        author_name: from.meta.name,
        author_color: from.meta.color,
        author_is_bot: from.meta.bot === true ? 1 : 0,
        text: published.text,
        at: Date.now(),
      })
    },
  })
  return channel
}
