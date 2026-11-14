import { describe, expect, it } from 'vitest'
import { pageRoom } from '../test/pageRoom.js'
import type { TrackWatchState } from './protocol.js'
import type { LocalParticipant } from '../types.js'

/** How the page's room hands its member who receives each of its tracks. */
type DemandHolder = LocalParticipant & { _applyDemandState(tracks: TrackWatchState[]): void }

/** The watchers of `me`'s default track, after the room reports `watchers` receiving it. */
function watchersReported(me: DemandHolder, watchers: string[]): string[] {
  me._applyDemandState([{ track: '', watchers, count: watchers.length }])
  return me.demand[0]!.watchers.map((watcher) => watcher.id)
}

async function joinedAmong(members: Record<string, number>): Promise<DemandHolder> {
  return (await pageRoom(members).room.join()) as DemandHolder
}

describe("a track's watchers", () => {
  it('are listed by join time', async () => {
    const me = await joinedAmong({ sam: 2, tia: 1 })
    expect(watchersReported(me, ['sam', 'tia'])).toEqual(['tia', 'sam'])
  })

  it('who joined at the same time keep the order the room reported them in', async () => {
    const me = await joinedAmong({ sam: 1, tia: 1 })
    expect(watchersReported(me, ['sam', 'tia'])).toEqual(['sam', 'tia'])
    expect(watchersReported(me, ['tia', 'sam'])).toEqual(['tia', 'sam'])
  })
})
