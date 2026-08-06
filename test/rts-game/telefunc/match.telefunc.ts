export { onEnterMatch, onStartMatch }

import { Abort, getContext, type LocalParticipant } from 'telefunc'
import { BLUE, NEUTRAL, RED, type Team } from '../shared/constants'
import { getMatchRoom, getPlayerTeam, startMatch } from '../server/matches'
import { guardMatchRoom } from '../server/guards'
import type { MatchAuthority, MatchRoom, PlayerMeta } from '../shared/types'

/** Enter a match room. The membership is granted here, server-side, so it carries the trusted
 *  `identity` (the durable user id) — display metadata (name/color/team) is separate. Returns the
 *  room (live view), the caller's own participant, and the match **authority** (the room's hidden
 *  `{ hidden: true }` participant that runs the sim) — the client drives everything else (team pick,
 *  ready, chat, orders, the state stream) off those three handles. Handing the authority back here
 *  is the maintainer's blessed shape: the client commands it with a plain DM and subscribes to its
 *  binary tracks, with no `room.server` accessor and no client-side roster scan (finding 1+2). */
async function onEnterMatch(
  roomId: string,
): Promise<{ room: MatchRoom; me: LocalParticipant<PlayerMeta>; authority: MatchAuthority }> {
  const { user } = getContext()
  if (!user) throw Abort({ error: 'Sign in to play' })

  let room: MatchRoom
  try {
    room = await getMatchRoom(roomId)
  } catch {
    throw Abort({ error: 'That match no longer exists' })
  }
  guardMatchRoom(room)

  // The sim runs on a hidden participant seated at room creation (server/matches.ts → takeSeat), so
  // it's always present by the time anyone enters. Read it off-presence — the roster excludes it.
  const [authority] = await room.getParticipants({ hidden: true })
  if (!authority) throw Abort({ error: 'That match is not available' })

  // Team: in the lobby, auto-balance onto the emptier side (the player can switch before ready).
  // Mid-match, restore the trusted team of a reconnecting player, else join as a spectator.
  let team: Team = NEUTRAL
  if (room.meta.phase === 'lobby') {
    team = pickTeam(await room.getParticipants())
  } else {
    team = getPlayerTeam(roomId, user.id) ?? NEUTRAL
  }

  let me: LocalParticipant<PlayerMeta>
  try {
    me = await room.join({
      meta: { name: user.name, color: user.color, team, ready: false },
      identity: user.id,
    })
  } catch (err) {
    throw Abort({ error: err instanceof Error ? err.message : 'Could not join' })
  }
  return { room, me, authority }
}

async function onStartMatch(roomId: string): Promise<void> {
  const { user } = getContext()
  if (!user) throw Abort({ error: 'Sign in to play' })
  try {
    await startMatch(roomId, user.id)
  } catch (err) {
    throw Abort({ error: err instanceof Error ? err.message : 'Could not start the match' })
  }
}

function pickTeam(participants: { identity: string | null; meta: PlayerMeta }[]): Team {
  let red = 0
  let blue = 0
  const seen = new Set<string>()
  for (const p of participants) {
    if (!p.identity || seen.has(p.identity)) continue
    seen.add(p.identity)
    if (p.meta.team === BLUE) blue++
    else if (p.meta.team === RED) red++
  }
  return red <= blue ? RED : BLUE
}
