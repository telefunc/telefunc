import { describe, expect, it } from 'vitest'
import { pageRoom } from '../test/pageRoom.js'
import type { DemandChange, TrackWatchState } from './protocol.js'
import type { LocalParticipant } from '../types.js'

/** How the page's room hands its member who receives each of its tracks. */
type DemandHolder = LocalParticipant & {
  _applyDemand(track: string, change: DemandChange): void
  _applyDemandState(tracks: TrackWatchState[]): void
}

/** The watchers of `me`'s default track, after the room reports `watchers` receiving it. */
function watchersReported(me: DemandHolder, watchers: string[]): string[] {
  me._applyDemandState([{ track: '', watchers, pages: [] }])
  return me.demand[0]!.watchers.map((watcher) => watcher.id)
}

async function joinedAmong(members: Record<string, number>): Promise<DemandHolder> {
  return (await pageRoom(members).room.join()) as DemandHolder
}

describe("a track's watchers", () => {
  it('are listed in the order the room lists its participants, whatever their join times', async () => {
    const me = await joinedAmong({ sam: 2, tia: 1 })
    expect(watchersReported(me, ['tia', 'sam'])).toEqual(['sam', 'tia'])
  })

  it('who joined at the same time keep that order when the room reports them again in another', async () => {
    const me = await joinedAmong({ sam: 1, tia: 1 })
    expect(watchersReported(me, ['sam', 'tia'])).toEqual(['sam', 'tia'])
    expect(watchersReported(me, ['tia', 'sam'])).toEqual(['sam', 'tia'])
  })
})

describe("a participant's demand", () => {
  it('lists its tracks by name in code-unit order, whatever order the room told them in', async () => {
    const told = async (tracks: string[]) => {
      const me = await joinedAmong({ sam: 1 })
      for (const track of tracks) me._applyDemand(track, { add: ['sam'] })
      return me.demand.map((entry) => entry.track)
    }
    expect(await told(['mic', '', 'Screen'])).toEqual(['Screen', 'main', 'mic'])
    expect(await told(['Screen', 'mic', ''])).toEqual(['Screen', 'main', 'mic'])
  })
})

describe("a participant's leave listeners", () => {
  it('hear that it left, all but one that an earlier one removed', async () => {
    const { room, endWire } = pageRoom()
    const me = await room.join()
    const heard: string[] = []
    me.onLeave(() => removeSecond())
    const removeSecond = me.onLeave(() => heard.push('second'))
    me.onLeave(() => heard.push('third'))

    endWire()

    expect(heard).toEqual(['third'])
  })
})
