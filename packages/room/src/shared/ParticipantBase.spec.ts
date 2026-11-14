import { describe, expect, it } from 'vitest'
import type { ClientBroadcast } from 'telefunc/__internal'
import { ClientRoom } from '../client/room.js'
import type { TrackWatchState } from './protocol.js'
import type { LocalParticipant } from '../types.js'

/** How the page's room hands its member who receives each of its tracks. */
type DemandHolder = LocalParticipant & { _applyDemandState(tracks: TrackWatchState[]): void }

/** A page's room of members with the given join times, and `me`, joined at the page through a channel that answers
 *  the join only. */
async function pageMember(members: Record<string, number>): Promise<DemandHolder> {
  const end = {
    isClosed: false,
    isConnected: true,
    send: async () => ({ ok: true, value: { id: 'me', joinedAt: 0 } }),
    _subscribeLocal: () => () => {},
    _setSubscribed: () => {},
    onClose: () => () => {},
    onDisconnect: () => () => {},
    onReconnect: () => () => {},
  }
  const room = new ClientRoom({
    end: end as unknown as ClientBroadcast,
    snapshot: {
      channelId: 'channel',
      roomId: crypto.randomUUID(),
      meta: [],
      metaSeq: 0,
      closed: false,
      members: Object.entries(members).map(([id, joinedAt]) => ({
        id,
        meta: {},
        metaSeq: 0,
        identity: null,
        joinedAt,
      })),
    },
  })
  return (await room.join()) as DemandHolder
}

/** The watchers of `me`'s default track, after the room reports `watchers` receiving it. */
function watchersReported(me: DemandHolder, watchers: string[]): string[] {
  me._applyDemandState([{ track: '', watchers, count: watchers.length }])
  return me.demand[0]!.watchers.map((watcher) => watcher.id)
}

describe("a track's watchers", () => {
  it('are listed by join time', async () => {
    const me = await pageMember({ sam: 2, tia: 1 })
    expect(watchersReported(me, ['sam', 'tia'])).toEqual(['tia', 'sam'])
  })

  it('who joined at the same time keep the order the room reported them in', async () => {
    const me = await pageMember({ sam: 1, tia: 1 })
    expect(watchersReported(me, ['sam', 'tia'])).toEqual(['sam', 'tia'])
    expect(watchersReported(me, ['tia', 'sam'])).toEqual(['tia', 'sam'])
  })
})
