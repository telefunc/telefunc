export { onAnnounce, onKickUser }

import { Abort, getContext, Room } from 'telefunc'
import * as q from '../database/queries'
import { channelRoomId, GUILD_ROOM_ID } from '../server/rooms'

/**
 * Kick a user — all of their connections, from every room. Admin operations live on the
 * `Room.*` statics (an instance handed to clients carries no privileged methods), and the actor
 * comes from `getContext()`, not from the request payload.
 *
 * The sweep is one call per room now: `removeParticipant(id, { identity })` removes every
 * membership of that app identity at once, idempotently — no more listing participants and
 * matching metadata by hand (README finding 1, fixed upstream). The kick's `reason` travels
 * with the removal itself, so the kicked client learns "why" from its own `onLeave` — the old
 * pre-kick notice (and its cross-lane race) is gone (finding 12, fixed upstream).
 */
async function onKickUser(targetUserId: string): Promise<void> {
  const actor = requireAdmin()
  const target = q.getUserById(targetUserId)
  if (target === undefined) throw Abort('No such user')
  if (target.is_bot === 1) throw Abort('The bot stays')
  if (target.id === actor.id) throw Abort("You can't kick yourself")

  // Best-effort, room by room: a channel closing mid-sweep must not abort the kick and leave the
  // user still a member of the rest, and the announce below must fire regardless. (That this is a
  // sweep at all — O(channels) round-trips because identity isn't a cross-room address — is
  // itself finding 18.)
  const rooms = [GUILD_ROOM_ID, ...q.listChannels().map((row) => channelRoomId(row.id))]
  await Promise.all(
    rooms.map((roomId) =>
      Room.removeParticipant(roomId, { identity: target.id }, { reason: actor.name }).catch(() => {}),
    ),
  )

  // Tell everyone (room-authored, on the guild's announce lane).
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
