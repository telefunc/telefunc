export { onEnterGuild }

import { getContext } from 'telefunc'
import * as q from '../database/queries'
import { getGuardedChannel, getGuardedGuild } from '../server/guards'
import { channelRoomId, ensureLiveWorld } from '../server/rooms'

/**
 * Boot the app for a logged-in user: join the guild and hand over every live room.
 *
 * The join happens server-side with the identity from `getContext()` — `identity` is the
 * durable app identity (our user ID), stamped spoof-proof into everything this membership does
 * (`from.identity`, kick sweeps). The client never gets to choose it: identity is a server-side
 * join option by design. The rooms travel in this one response: serializing a room is O(1),
 * the member lists stream right behind it.
 */
async function onEnterGuild() {
  const { user } = getContext()
  if (user === null) return { ok: false as const } // not logged in — the client shows the auth screen

  await ensureLiveWorld()
  const guild = await getGuardedGuild()
  const me = await guild.join(
    { name: user.name, color: user.color, status: 'online', admin: user.isAdmin },
    { identity: user.id },
  )

  const channelRooms = await Promise.all(q.listChannels().map((row) => getGuardedChannel(channelRoomId(row.id))))
  return { ok: true as const, user, guild, me, channels: channelRooms }
}
