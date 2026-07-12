export { onEnterMatch, onStartMatch }

import { Abort, getContext, type LocalParticipant } from 'telefunc'
import { BLUE, NEUTRAL, RED, type Team } from '../shared/constants'
import { getMatchRoom, getPlayerTeam, startMatch } from '../server/matches'
import { guardMatchRoom } from '../server/guards'
import type { MatchRoom, PlayerMeta } from '../shared/types'

/** Enter a match room. The membership is granted here, server-side, so it carries the trusted
 *  `identity` (the durable user id) — display metadata (name/color/team) is separate. Returns the
 *  room (live view) and the caller's own participant together; the client drives everything else
 *  (team pick, ready, chat, commands) off those two handles. */
async function onEnterMatch(roomId: string): Promise<{ room: MatchRoom; me: LocalParticipant<PlayerMeta> }> {
  const { user } = getContext()
  if (!user) throw Abort({ error: 'Sign in to play' })

  let room: MatchRoom
  try {
    room = await getMatchRoom(roomId)
  } catch {
    throw Abort({ error: 'That match no longer exists' })
  }
  guardMatchRoom(room)

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
    me = await room.join({ name: user.name, color: user.color, team, ready: false }, { identity: user.id })
  } catch (err) {
    throw Abort({ error: err instanceof Error ? err.message : 'Could not join' })
  }
  return { room, me }
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
