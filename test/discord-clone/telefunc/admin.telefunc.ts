export { onAnnounce, onKickUser }

import { eq } from 'drizzle-orm'
import { Abort, getContext, Room } from 'telefunc'
import { db } from '../database/db'
import { channels, users } from '../database/schema'
import { channelRoomId, GUILD_ROOM_ID } from '../server/rooms'
import { asMemberMeta, type SystemNotice } from '../shared/types'

/**
 * Kick a user — all of their connections, from every room. Admin operations live on the
 * `Room.*` statics (an instance handed to clients carries no privileged methods), and the actor
 * comes from `getContext()`, not from the request payload.
 *
 * Membership is per-room and participants are per-connection: nothing links "the same user"
 * across the guild, text and voice rooms, so the app sweeps every room and matches on the
 * `userId` it stamped into the metadata at join (see README finding on cross-room identity).
 */
async function onKickUser(targetUserId: string): Promise<void> {
  const actor = requireAdmin()
  const target = db.select().from(users).where(eq(users.id, targetUserId)).limit(1).all()[0]
  if (target === undefined) throw Abort('No such user')
  if (target.isBot) throw Abort('The bot stays')
  if (target.id === actor.id) throw Abort("You can't kick yourself")

  // 1. The guild: tell each of their connections why (room-authored — `from === null`), then
  //    remove it. Sent before the removal — you can't message a participant that's gone.
  const guild = await Room.get(GUILD_ROOM_ID)
  const notice: SystemNotice = { kind: 'kicked', by: actor.name }
  for (const participant of await guild.getParticipants()) {
    if (asMemberMeta(participant.meta).userId !== target.id) continue
    await Room.send(GUILD_ROOM_ID, participant.id, notice).catch(() => {})
    await Room.removeParticipant(GUILD_ROOM_ID, participant.id)
  }

  // 2. Their other participants (open text channels, voice) — same sweep, per channel room.
  for (const row of db.select().from(channels).all()) {
    const room = await Room.get(channelRoomId(row.id))
    for (const participant of await room.getParticipants()) {
      if (asMemberMeta(participant.meta).userId === target.id) {
        await Room.removeParticipant(room.id, participant.id)
      }
    }
  }

  // 3. Tell everyone (room-authored, on the guild's announce lane).
  await Room.announce(GUILD_ROOM_ID, { kind: 'member-kicked', userId: target.id, name: target.name, by: actor.name })
}

/** A server-wide banner, room-authored: receivers can't confuse it with a member message. */
async function onAnnounce(text: string): Promise<void> {
  const actor = requireAdmin()
  text = text.trim().slice(0, 200)
  if (text === '') throw Abort('Say something')
  await Room.announce(GUILD_ROOM_ID, { kind: 'announcement', text, by: actor.name })
}

function requireAdmin() {
  const { user } = getContext()
  if (user === null) throw Abort('Not logged in')
  if (!user.isAdmin) throw Abort('Only the server owner can do that')
  return user
}
