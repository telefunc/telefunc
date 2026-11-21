import { afterEach, describe, expect, it, vi } from 'vitest'
import { pageRoom } from '../test/pageRoom.js'
import { Room } from '../index.js'
import { RoomRemotes } from './RoomView.js'
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

/** `me`, joined to a page's room of `members`, with what ends its wire and what hands it an event. */
async function joinedIn(members: Record<string, number>) {
  const page = pageRoom(members)
  return { ...page, me: (await page.room.join()) as DemandHolder }
}

async function joinedAmong(members: Record<string, number>): Promise<DemandHolder> {
  return (await joinedIn(members)).me
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

  it('reads who receives a track once per change of it, though a listener reads the whole list at each change', async () => {
    const me = await joinedAmong({ sam: 1 })
    me.onChange(() => void me.demand)
    const shownOf = vi.spyOn(RoomRemotes.prototype, 'shownOf')
    for (let i = 0; i < 1_000; i++) {
      me._applyDemand(`t${i}`, { add: ['sam'] })
      // Each change in a task of its own, as the room tells them.
      await Promise.resolve()
    }
    expect({ tracks: me.demand.length, reads: shownOf.mock.calls.length }).toEqual({ tracks: 1_000, reads: 1_000 })
  })

  it('keeps an entry, and the list, the same object while nothing in them changes', async () => {
    const me = await joinedAmong({ sam: 1 })
    me.onChange(() => {})
    me._applyDemand('a', { add: ['sam'] })
    const before = me.demand
    me._applyDemand('b', { add: ['sam'] })
    const after = me.demand
    expect({
      list: after === before,
      a: after.find((entry) => entry.track === 'a') === before[0],
      again: me.demand === after,
    }).toEqual({ list: false, a: true, again: true })
  })

  it('shows a watcher once the room shows them', async () => {
    const { me, deliver } = await joinedIn({ sam: 1 })
    me.onChange(() => {})
    me._applyDemand('a', { add: ['tia'] })
    const before = me.demand.map((entry) => ({
      watchers: entry.watchers.map((watcher) => watcher.id),
      count: entry.watcherCount,
    }))
    deliver({ __r: 'join', id: 'tia', meta: {}, joinedAt: 2 })
    await Promise.resolve()
    const after = me.demand.map((entry) => ({
      watchers: entry.watchers.map((watcher) => watcher.id),
      count: entry.watcherCount,
    }))
    expect({ before, after }).toEqual({
      before: [{ watchers: [], count: 1 }],
      after: [{ watchers: ['tia'], count: 1 }],
    })
  })

  it('is empty once it left', async () => {
    const { me, endWire } = await joinedIn({ sam: 1 })
    me.onChange(() => {})
    me._applyDemand('a', { add: ['sam'] })
    expect(me.demand.length).toBe(1)
    endWire()
    expect(me.demand).toEqual([])
  })
})

describe("a server-side participant's demand", () => {
  /** A server-side room whose member `me` hears its changes, and a second view of it that subscribes to tracks. */
  async function serverRoom() {
    const id = `demand-${crypto.randomUUID()}`
    const room = await Room.create(id)
    const me = await room.join()
    me.onChange(() => void me.demand)
    const viewer = await Room.getOrCreate(id)
    return { me, viewer }
  }
  const until = async (done: () => boolean) => {
    for (let turn = 0; turn < 1_000 && !done(); turn++) await new Promise((resolve) => setTimeout(resolve, 0))
  }

  it('reads who receives a track once per change of it, though a listener reads the whole list at each change', async () => {
    const { me, viewer } = await serverRoom()
    const shownOf = vi.spyOn(RoomRemotes.prototype, 'shownOf')
    for (let i = 0; i < 1_000; i++) viewer.subscribeBinary(() => {}, { track: `t${i}` })
    await until(() => me.demand.length === 1_000)
    expect({ tracks: me.demand.length, reads: shownOf.mock.calls.length }).toEqual({ tracks: 1_000, reads: 1_000 })
  })

  it('keeps an entry, and the list, the same object while nothing in them changes, and is empty once it left', async () => {
    const { me, viewer } = await serverRoom()
    viewer.subscribeBinary(() => {}, { track: 'a' })
    await until(() => me.demand.length === 1)
    const before = me.demand
    viewer.subscribeBinary(() => {}, { track: 'b' })
    await until(() => me.demand.length === 2)
    const after = me.demand
    const kept = {
      list: after === before,
      a: after.find((entry) => entry.track === 'a') === before[0],
      again: me.demand === after,
    }
    await me.leave()
    expect({ kept, left: me.demand }).toEqual({ kept: { list: false, a: true, again: true }, left: [] })
  })
})

afterEach(() => {
  vi.restoreAllMocks()
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
