export { onWhoAmI, onListMatches, onCreateMatch }

import { Abort, getContext } from 'telefunc'
import type { SessionUser } from '../server/identity'
import { createMatch, listMatches } from '../server/matches'
import type { MatchListing } from '../shared/types'

/** The signed-in user, read from the session cookie the HTTP sign-in route set. */
async function onWhoAmI(): Promise<SessionUser | null> {
  return getContext().user
}

async function onListMatches(): Promise<MatchListing[]> {
  return await listMatches()
}

async function onCreateMatch(name: string): Promise<{ roomId: string }> {
  const { user } = getContext()
  if (!user) throw Abort({ error: 'Sign in to create a match' })
  const room = await createMatch(typeof name === 'string' ? name : '', user.id)
  return { roomId: room.id }
}
