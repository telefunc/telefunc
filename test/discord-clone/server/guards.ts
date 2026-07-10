export { BANNED_WORDS, getGuardedChannel, getGuardedGuild }

// Server-side policy, declared through `Room.guard()`.
//
// A guard covers every membership granted through *that room instance* — server-side joins and
// client-side joins alike — and there is no room-global guard. So policy must be attached to
// every instance that hands out memberships: these factories are the only place rooms are
// fetched, and a code path calling `Room.get()` directly would silently bypass moderation and
// persistence (see README finding on per-instance guards).

import { Room } from 'telefunc'
import * as q from '../database/queries'
import { asChannelMeta, asMemberMeta, type ChannelPublish } from '../shared/types'
import { dbChannelIdOf, GUILD_ROOM_ID } from './rooms'

const BANNED_WORDS = ['darn', 'heck'] // it's a family-friendly server

/** The guild room: presence + the room-authored notice lane. */
async function getGuardedGuild(): Promise<Room> {
  const guild = await Room.get(GUILD_ROOM_ID)
  Room.guard(guild, {
    // DMs are server-delivered (DB-first, offline-capable — see dms.telefunc.ts), so the
    // member-to-member lane is closed outright: policy by guard, not by hoping clients behave.
    onSend: () => {
      throw new Error('Direct participant messages are disabled — DMs are delivered by the server')
    },
  })
  return guild
}

/** A channel room. Text channels get moderation + persistence; voice carries only ephemeral
 *  media, so there is nothing to guard. */
async function getGuardedChannel(channelRoomId: string): Promise<Room> {
  const dbChannelId = dbChannelIdOf(channelRoomId)
  if (dbChannelId === null) throw new Error('Not a channel room')
  const channel = await Room.get(channelRoomId)
  if (asChannelMeta(channel.meta).kind === 'text') {
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
        const author = asMemberMeta(from.meta)
        q.insertMessage({
          id: published.id,
          channel_id: dbChannelId,
          author_id: author.userId,
          author_name: author.name,
          author_color: author.color,
          author_is_bot: author.bot === true ? 1 : 0,
          text: published.text,
          at: Date.now(),
        })
      },
    })
  }
  return channel
}
